"""Resurrector Bridge WebSocket server.

Streams bag data (playback or live) over WebSocket in PlotJuggler-compatible
format. Includes REST endpoints for playback control and topic discovery,
and serves a built-in web viewer.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from resurrector.bridge.buffer import RingBuffer, BufferedMessage
from resurrector.bridge.playback import PlaybackEngine, PlaybackState
from resurrector.bridge.protocol import (
    flatten_to_plotjuggler,
    encode_status_message,
    encode_topics_message,
)
from resurrector.bridge.recorder import BridgeRecorder
from resurrector.ingest.parser import Message

logger = logging.getLogger("resurrector.bridge.server")


class BridgeServer:
    """Manages WebSocket connections, playback/live engine, and message routing."""

    def __init__(
        self,
        mode: str = "playback",
        bag_path: Path | None = None,
        speed: float = 1.0,
        topics: list[str] | None = None,
        max_rate_hz: float = 50.0,
        buffer_size: int = 10_000,
        loop_playback: bool = False,
        record_path: Path | str | None = None,
    ):
        self.mode = mode
        self.bag_path = bag_path
        self.max_rate_hz = max_rate_hz
        self._buffer = RingBuffer(capacity=buffer_size)
        self._playback: PlaybackEngine | None = None
        self._live_subscriber = None
        self._recorder: BridgeRecorder | None = None
        # Per-WS-connection event queue. Time-anchored events
        # (annotations, custom markers) are broadcast through these
        # queues so they're independent of the per-topic message buffer.
        # Bounded at 100 events/connection — drop on full so a slow
        # client doesn't block the broadcast for everyone else.
        self._event_subscribers: list[asyncio.Queue] = []

        if mode == "playback" and bag_path:
            self._playback = PlaybackEngine(
                bag_path=bag_path,
                speed=speed,
                topics=topics,
                loop=loop_playback,
                message_callback=self._on_message,
            )

        # Optional record-while-streaming. Construct the recorder with the
        # source bag's topic info so schemas are populated. For live mode
        # the live_subscriber needs to surface a similar topic_info list
        # (live wiring is a v0.6+ follow-up).
        if record_path is not None and self._playback is not None:
            from resurrector.ingest.parser import MCAPParser
            parser = MCAPParser(bag_path)
            metadata = parser.get_metadata()
            self._recorder = BridgeRecorder(
                output_path=record_path,
                topic_info=metadata.topics,
            )
            logger.info("Bridge recording to %s", record_path)
        elif record_path is not None:
            logger.warning(
                "record_path set but bridge mode is %r without a playback "
                "source — recording is wired only for playback in v0.5.0. "
                "Live-mode recording lands in v0.6+.", mode,
            )

    def _on_message(self, msg: Message) -> None:
        """Called by PlaybackEngine or LiveSubscriber for each message.

        Fans out to (a) the WebSocket buffer for client streaming, and
        (b) the optional recorder for write-to-disk. Both fan-outs are
        synchronous and cheap; recorder writes happen in this thread,
        not on a background queue, so a slow disk DOES backpressure
        the message callback — acceptable trade-off for v0.5 since
        recording is opt-in.

        The buffered message includes the raw decoded ``data`` dict so
        downstream WS handlers can apply per-client filter expressions
        without re-parsing.
        """
        encoded = flatten_to_plotjuggler(msg.topic, msg.data, msg.timestamp_ns / 1e9)
        raw_json = json.dumps(encoded)
        self._buffer.put(BufferedMessage(
            topic=msg.topic,
            timestamp_sec=msg.timestamp_ns / 1e9,
            encoded=encoded,
            raw_json=raw_json,
            data=msg.data,  # for per-message filter evaluation
        ))
        if self._recorder is not None and msg.raw_data:
            try:
                self._recorder.record(msg.topic, msg.timestamp_ns, msg.raw_data)
            except Exception as e:
                # Never let a recorder error kill the streaming path
                logger.error("Recorder write failed for %s: %s", msg.topic, e)

    async def broadcast_event(
        self,
        topic: str,
        timestamp_ns: int,
        text: str,
        kind: str = "annotation",
    ) -> int:
        """Broadcast a time-anchored event to every connected WebSocket client.

        Wire format::

            {"type": "event", "topic": "/imu/data",
             "timestamp_ns": 1234567890, "text": "spike here",
             "kind": "annotation"}

        Clients (PlotJuggler via custom plugin, the built-in viewer.js)
        render these as vertical event lines on time-series plots at
        the matching timestamp.

        Args:
            topic: Topic name to anchor the event to. Use empty string
                ``""`` for bag-global events that should appear on every plot.
            timestamp_ns: Event timestamp.
            text: Free-text caption shown on the marker.
            kind: ``"annotation"`` (default), ``"alert"``, ``"bookmark"``,
                or any custom string. Clients can choose to filter / style
                by kind.

        Returns:
            Number of subscribers the event was broadcast to (best-effort —
            queues that are full silently drop the event).
        """
        msg = {
            "type": "event",
            "topic": topic,
            "timestamp_ns": int(timestamp_ns),
            "text": text,
            "kind": kind,
        }
        delivered = 0
        for q in list(self._event_subscribers):
            try:
                q.put_nowait(msg)
                delivered += 1
            except asyncio.QueueFull:
                # Slow client; drop the event for them rather than blocking
                logger.debug("Event queue full for one subscriber; dropping")
        return delivered

    def close_recorder(self) -> None:
        """Flush + close the recorder if one is attached. Idempotent.

        Called from the FastAPI lifespan shutdown handler so the MCAP
        finishes cleanly with a summary index. If the process is killed
        before this runs, the file is still readable but won't have a
        summary — readers iterate messages, ``get_summary()`` returns None.
        """
        if self._recorder is not None:
            self._recorder.close()
            self._recorder = None

    def create_app(self) -> FastAPI:
        """Build the FastAPI application with all routes."""
        app = FastAPI(
            title="Resurrector Bridge",
            description="WebSocket bridge for rosbag data streaming",
        )
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # Bridge is meant to be accessed from various clients
            allow_methods=["*"],
            allow_headers=["*"],
        )

        bridge = self  # Closure reference

        # --- REST endpoints ---

        @app.get("/api/topics")
        async def get_topics() -> dict[str, Any]:
            if bridge._playback:
                return encode_topics_message(bridge._playback.get_topics_info())
            if bridge._live_subscriber:
                topics = bridge._live_subscriber.get_available_topics()
                return encode_topics_message(topics)
            return encode_topics_message([])

        @app.get("/api/metadata")
        async def get_metadata() -> dict[str, Any]:
            if bridge._playback:
                meta = bridge._playback.metadata
                return {
                    "mode": "playback",
                    "bag_path": str(bridge.bag_path),
                    "duration_sec": meta.duration_sec,
                    "start_time_sec": meta.start_time_ns / 1e9,
                    "end_time_sec": meta.end_time_ns / 1e9,
                    "message_count": meta.message_count,
                    "topic_count": len(meta.topics),
                }
            return {"mode": bridge.mode}

        @app.get("/api/status")
        async def get_status() -> dict[str, Any]:
            if bridge._playback:
                return encode_status_message(
                    mode="playback",
                    state=bridge._playback.state.value,
                    speed=bridge._playback.speed,
                    timestamp_sec=bridge._playback.current_timestamp_sec,
                    progress=bridge._playback.progress,
                )
            return encode_status_message(
                mode=bridge.mode, state="running",
            )

        @app.post("/api/playback/play")
        async def playback_play():
            if bridge._playback:
                await bridge._playback.play()
                return {"status": "playing"}
            return JSONResponse({"error": "Not in playback mode"}, 400)

        @app.post("/api/playback/pause")
        async def playback_pause():
            if bridge._playback:
                await bridge._playback.pause()
                return {"status": "paused"}
            return JSONResponse({"error": "Not in playback mode"}, 400)

        @app.post("/api/playback/seek")
        async def playback_seek(t: float = Query(description="Timestamp in seconds")):
            if bridge._playback:
                await bridge._playback.seek(t)
                return {"status": "seeked", "timestamp": t}
            return JSONResponse({"error": "Not in playback mode"}, 400)

        @app.post("/api/playback/speed")
        async def playback_speed(v: float = Query(description="Speed factor")):
            if bridge._playback:
                await bridge._playback.set_speed(v)
                return {"status": "speed_changed", "speed": v}
            return JSONResponse({"error": "Not in playback mode"}, 400)

        @app.post("/api/events")
        async def broadcast_event_endpoint(payload: dict[str, Any]):
            """POST a time-anchored event for fan-out to every WS client.

            Body: ``{topic, timestamp_ns, text, kind?}``. ``text`` and
            ``timestamp_ns`` are required; ``topic`` defaults to ``""``
            (bag-global), ``kind`` defaults to ``"annotation"``.

            Returns the number of WS subscribers the event was delivered to.
            Cross-process integrations (the dashboard's annotation creation,
            CLI tooling, custom alert pipelines) all converge through this
            single endpoint.
            """
            text = str(payload.get("text", "")).strip()
            if not text:
                return JSONResponse({"error": "'text' is required"}, 400)
            try:
                ts_ns = int(payload.get("timestamp_ns", 0))
            except (TypeError, ValueError):
                return JSONResponse({"error": "'timestamp_ns' must be integer"}, 400)
            topic = str(payload.get("topic", ""))
            kind = str(payload.get("kind", "annotation"))
            n = await bridge.broadcast_event(topic, ts_ns, text, kind)
            return {"status": "broadcast", "subscribers": n}

        # --- WebSocket endpoint ---

        @app.websocket("/ws")
        async def websocket_endpoint(ws: WebSocket):
            await ws.accept()
            client_id = str(uuid.uuid4())
            bridge._buffer.register_consumer(client_id)
            # Per-connection event queue; bounded so a slow consumer
            # doesn't pile up unbounded memory.
            event_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
            bridge._event_subscribers.append(event_queue)
            logger.info("Client %s connected", client_id[:8])

            # Send initial topic list
            if bridge._playback:
                topics_msg = encode_topics_message(bridge._playback.get_topics_info())
                await ws.send_text(json.dumps(topics_msg))

            subscribed_topics: set[str] | None = None  # None = all
            # Per-topic filter expressions (Polars expression strings).
            # Empty dict = no filtering. Caller validates expressions on
            # subscribe; bad expressions are NACK'd, not silently dropped.
            topic_filters: dict[str, str] = {}

            def _passes_filter(msg: BufferedMessage) -> bool:
                """Apply this connection's topic-level filter if any."""
                expr = topic_filters.get(msg.topic)
                if not expr:
                    return True
                if msg.data is None:
                    # No raw data means we can't evaluate; pass
                    return True
                from resurrector.core.transforms import evaluate_message_filter
                try:
                    return evaluate_message_filter(msg.data, expr)
                except ValueError:
                    # Bad filter expression — already NACK'd at subscribe
                    # time, but if we end up here just drop the message.
                    return False

            async def send_loop():
                interval = 1.0 / bridge.max_rate_hz
                while True:
                    messages = bridge._buffer.get_since(client_id, max_count=50)
                    for msg in messages:
                        if subscribed_topics is not None and msg.topic not in subscribed_topics:
                            continue
                        if not _passes_filter(msg):
                            continue
                        try:
                            await ws.send_text(msg.raw_json)
                        except WebSocketDisconnect:
                            return
                        except Exception as e:
                            logger.warning(
                                "ws send failed for client %s: %s",
                                client_id[:8], e,
                            )
                            try:
                                await ws.close(code=1011)
                            except Exception:
                                pass
                            return
                    await asyncio.sleep(interval)

            async def event_loop():
                """Drain the per-connection event queue and forward over WS."""
                while True:
                    event = await event_queue.get()
                    try:
                        await ws.send_text(json.dumps(event))
                    except WebSocketDisconnect:
                        return
                    except Exception as e:
                        logger.warning(
                            "ws event send failed for client %s: %s",
                            client_id[:8], e,
                        )
                        return

            async def receive_loop():
                nonlocal subscribed_topics
                while True:
                    try:
                        data = await ws.receive_text()
                    except WebSocketDisconnect:
                        return

                    try:
                        cmd = json.loads(data)
                    except json.JSONDecodeError:
                        continue

                    cmd_type = cmd.get("type")
                    if cmd_type == "subscribe":
                        topics = cmd.get("topics", [])
                        subscribed_topics = set(topics) if topics else None
                        # Optional per-topic filter expressions.
                        # Validate each filter at subscribe time; reject
                        # the whole subscribe with an error message rather
                        # than silently ignoring bad expressions.
                        new_filters = cmd.get("filters", {}) or {}
                        if new_filters:
                            from resurrector.core.transforms import (
                                validate_polars_expression,
                            )
                            invalid: list[tuple[str, str]] = []
                            for tp, expr in new_filters.items():
                                if not isinstance(expr, str):
                                    invalid.append((tp, "filter must be a string"))
                                    continue
                                if not expr.strip():
                                    # Empty filter = pass-through; no need to validate
                                    continue
                                try:
                                    validate_polars_expression(expr)
                                except ValueError as e:
                                    invalid.append((tp, str(e)))
                            if invalid:
                                err_payload = {
                                    "type": "error",
                                    "kind": "subscribe_invalid_filter",
                                    "details": [
                                        {"topic": t, "reason": r} for t, r in invalid
                                    ],
                                }
                                try:
                                    await ws.send_text(json.dumps(err_payload))
                                except Exception:
                                    pass
                                continue
                            topic_filters.update(new_filters)
                        logger.info(
                            "Client %s subscribed to %s (filters: %d)",
                            client_id[:8], topics or "all", len(topic_filters),
                        )
                    elif cmd_type == "unsubscribe":
                        topics = cmd.get("topics", [])
                        if subscribed_topics:
                            subscribed_topics -= set(topics)
                        for tp in topics:
                            topic_filters.pop(tp, None)
                    elif cmd_type == "playback_control" and bridge._playback:
                        action = cmd.get("action")
                        if action == "play":
                            await bridge._playback.play()
                        elif action == "pause":
                            await bridge._playback.pause()
                        elif action == "seek":
                            await bridge._playback.seek(cmd.get("timestamp", 0))
                        elif action == "speed":
                            await bridge._playback.set_speed(cmd.get("value", 1.0))

            try:
                await asyncio.gather(send_loop(), receive_loop(), event_loop())
            except (WebSocketDisconnect, Exception):
                pass
            finally:
                bridge._buffer.unregister_consumer(client_id)
                if event_queue in bridge._event_subscribers:
                    bridge._event_subscribers.remove(event_queue)
                logger.info("Client %s disconnected", client_id[:8])

        # --- Lifespan shutdown: flush + close the recorder cleanly ---
        if bridge._recorder is not None:
            @app.on_event("shutdown")
            async def _flush_recorder() -> None:
                bridge.close_recorder()

        # --- Serve web viewer ---
        web_dir = Path(__file__).parent / "web"
        if web_dir.exists() and (web_dir / "index.html").exists():
            @app.get("/")
            async def serve_viewer():
                return HTMLResponse((web_dir / "index.html").read_text())

            @app.get("/viewer.js")
            async def serve_viewer_js():
                js_path = web_dir / "viewer.js"
                if js_path.exists():
                    return HTMLResponse(
                        js_path.read_text(),
                        media_type="application/javascript",
                    )
        else:
            @app.get("/")
            async def root():
                return {
                    "message": "Resurrector Bridge",
                    "ws_endpoint": "/ws",
                    "docs": "/docs",
                }

        return app


def create_bridge_app(
    mode: str = "playback",
    bag_path: Path | None = None,
    speed: float = 1.0,
    topics: list[str] | None = None,
    max_rate_hz: float = 50.0,
    buffer_size: int = 10_000,
    loop_playback: bool = False,
    record_path: Path | str | None = None,
) -> FastAPI:
    """Factory function to create a configured bridge app.

    Args:
        record_path: Optional MCAP destination — if set, every message
            relayed by the bridge also gets written to this file. Only
            wired for ``mode="playback"`` in v0.5.0; live-mode recording
            arrives in v0.6+ once rclpy schema introspection lands.
    """
    bridge = BridgeServer(
        mode=mode,
        bag_path=bag_path,
        speed=speed,
        topics=topics,
        max_rate_hz=max_rate_hz,
        buffer_size=buffer_size,
        loop_playback=loop_playback,
        record_path=record_path,
    )
    return bridge.create_app()
