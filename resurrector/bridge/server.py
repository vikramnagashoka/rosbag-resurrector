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
    ):
        self.mode = mode
        self.bag_path = bag_path
        self.max_rate_hz = max_rate_hz
        self._buffer = RingBuffer(capacity=buffer_size)
        self._playback: PlaybackEngine | None = None
        self._live_subscriber = None

        if mode == "playback" and bag_path:
            self._playback = PlaybackEngine(
                bag_path=bag_path,
                speed=speed,
                topics=topics,
                loop=loop_playback,
                message_callback=self._on_message,
            )

    def _on_message(self, msg: Message) -> None:
        """Called by PlaybackEngine or LiveSubscriber for each message."""
        encoded = flatten_to_plotjuggler(msg.topic, msg.data, msg.timestamp_ns / 1e9)
        raw_json = json.dumps(encoded)
        self._buffer.put(BufferedMessage(
            topic=msg.topic,
            timestamp_sec=msg.timestamp_ns / 1e9,
            encoded=encoded,
            raw_json=raw_json,
        ))

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

        # --- WebSocket endpoint ---

        @app.websocket("/ws")
        async def websocket_endpoint(ws: WebSocket):
            await ws.accept()
            client_id = str(uuid.uuid4())
            bridge._buffer.register_consumer(client_id)
            logger.info("Client %s connected", client_id[:8])

            # Send initial topic list
            if bridge._playback:
                topics_msg = encode_topics_message(bridge._playback.get_topics_info())
                await ws.send_text(json.dumps(topics_msg))

            subscribed_topics: set[str] | None = None  # None = all

            async def send_loop():
                interval = 1.0 / bridge.max_rate_hz
                while True:
                    messages = bridge._buffer.get_since(client_id, max_count=50)
                    for msg in messages:
                        if subscribed_topics is None or msg.topic in subscribed_topics:
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
                        logger.info("Client %s subscribed to %s", client_id[:8], topics or "all")
                    elif cmd_type == "unsubscribe":
                        topics = cmd.get("topics", [])
                        if subscribed_topics:
                            subscribed_topics -= set(topics)
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
                await asyncio.gather(send_loop(), receive_loop())
            except (WebSocketDisconnect, Exception):
                pass
            finally:
                bridge._buffer.unregister_consumer(client_id)
                logger.info("Client %s disconnected", client_id[:8])

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
) -> FastAPI:
    """Factory function to create a configured bridge app."""
    bridge = BridgeServer(
        mode=mode,
        bag_path=bag_path,
        speed=speed,
        topics=topics,
        max_rate_hz=max_rate_hz,
        buffer_size=buffer_size,
        loop_playback=loop_playback,
    )
    return bridge.create_app()
