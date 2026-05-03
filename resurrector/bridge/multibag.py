"""Multi-bag synchronized playback for the bridge (Sub-feature 2.1).

Wraps N :class:`PlaybackEngine` instances behind a single play/pause/stop
control surface. Each bag plays through its own engine so the timing
logic is reused; "synchronization" is achieved by staggering the start
times via per-bag wall-clock offsets.

Topic namespacing
-----------------
Every emitted message has its topic name prefixed with the bag's
``bag_id`` (e.g. ``bag1:/imu/data``) before the user callback fires.
This is intentional: it keeps PlotJuggler happy (each prefixed topic
shows as its own trace) and the cross-bag-overlay UI gets the per-bag
provenance for free, without protocol-versioning the WebSocket
discovery message.

Wall-clock alignment
--------------------
``offset_sec > 0`` delays the bag's start by that many seconds of wall
time, so when bag A is at content-time T, bag B (offset=2.5) is at
content-time T-2.5. ``offset_sec < 0`` is rejected — the natural
inversion is "give bag A a positive offset" instead.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from resurrector.bridge.playback import PlaybackEngine, PlaybackState
from resurrector.ingest.parser import Message

logger = logging.getLogger("resurrector.bridge.multibag")


@dataclass
class BagPlaybackConfig:
    """One bag's contribution to a multi-bag playback session.

    Attributes:
        bag_path: Path to the MCAP file.
        bag_id: Short identifier used in topic namespacing (e.g. ``"bag1"``).
            Must be non-empty and unique across the configs for one session.
        offset_sec: Wall-clock delay before this bag starts playing.
            ``0.0`` means "play immediately"; ``2.5`` means "wait 2.5s then start."
            Negative values are rejected.
        label: Human-friendly display name (e.g. ``"Baseline run"``).
            Defaults to ``bag_id`` when empty.
    """
    bag_path: str | Path
    bag_id: str
    offset_sec: float = 0.0
    label: str = ""


# Multi-bag callback gets (bag_id, message). Single-bag PlaybackEngine
# callback was just (message,); this signature change is the visible
# difference for downstream consumers (server, viewer, etc.).
MultiBagMessageCallback = Callable[[str, Message], None]


def _validate_configs(configs: list[BagPlaybackConfig]) -> None:
    """Raise ValueError on duplicate bag_id, empty bag_id, or negative offsets."""
    if not configs:
        raise ValueError("MultiBagPlayback requires at least one bag config")
    seen_ids: set[str] = set()
    for cfg in configs:
        if not cfg.bag_id:
            raise ValueError(f"BagPlaybackConfig.bag_id must be non-empty (got {cfg.bag_id!r})")
        if cfg.bag_id in seen_ids:
            raise ValueError(f"Duplicate bag_id {cfg.bag_id!r} in configs")
        seen_ids.add(cfg.bag_id)
        if cfg.offset_sec < 0:
            raise ValueError(
                f"BagPlaybackConfig.offset_sec must be >= 0 "
                f"(got {cfg.offset_sec} for bag_id={cfg.bag_id!r}). "
                f"To play bag B before bag A, give bag A a positive offset instead."
            )


class MultiBagPlayback:
    """Synchronized playback of N MCAP bags with per-bag offsets.

    Args:
        configs: List of :class:`BagPlaybackConfig`. Order is preserved
            for the discovery payload but doesn't affect playback timing.
        speed: Playback speed multiplier. Applied uniformly to all bags;
            changing the speed mid-session re-applies to every engine.
        topics: Optional topic filter. Applied to ALL bags (each bag's
            engine sees only these topic names). Topics that don't exist
            in a particular bag are silently absent from that bag's stream.
        loop: When True, every bag loops independently. (No global
            cross-bag re-sync happens at loop boundaries — each bag's
            loop boundary is independent.)
        message_callback: Called for every message as ``cb(bag_id, msg)``.
            ``msg.topic`` is rewritten to the namespaced form
            (``"<bag_id>:<original_topic>"``) before the callback fires.

    Example::

        from resurrector.bridge.multibag import (
            BagPlaybackConfig, MultiBagPlayback,
        )

        configs = [
            BagPlaybackConfig(bag_path="run_a.mcap", bag_id="a"),
            BagPlaybackConfig(bag_path="run_b.mcap", bag_id="b", offset_sec=2.5),
        ]
        mp = MultiBagPlayback(
            configs=configs, speed=1.0,
            message_callback=lambda bid, m: print(bid, m.topic, m.timestamp_ns),
        )
        await mp.play()
        # ... later
        await mp.stop()
    """

    def __init__(
        self,
        configs: list[BagPlaybackConfig],
        speed: float = 1.0,
        topics: list[str] | None = None,
        loop: bool = False,
        message_callback: MultiBagMessageCallback | None = None,
    ):
        _validate_configs(configs)
        self._configs = configs
        self._speed = speed
        self._topics = topics
        self._loop = loop
        self._callback = message_callback

        # Per-config engine, with a wrapping callback that tags bag_id +
        # rewrites msg.topic to the namespaced form. We use a closure
        # capture (bid=cfg.bag_id) so each lambda binds to the right id.
        self._engines: list[PlaybackEngine] = []
        for cfg in configs:
            bid = cfg.bag_id
            engine = PlaybackEngine(
                bag_path=cfg.bag_path,
                speed=speed,
                topics=topics,
                loop=loop,
                message_callback=self._make_per_bag_callback(bid) if message_callback else None,
            )
            self._engines.append(engine)

        self._delay_tasks: list[asyncio.Task] = []
        self._stopped = False

    # ----- Public API -----

    @property
    def configs(self) -> list[BagPlaybackConfig]:
        """The bag configs as supplied at construction time (order preserved)."""
        return list(self._configs)

    @property
    def speed(self) -> float:
        return self._speed

    @property
    def state(self) -> PlaybackState:
        """Combined state. PLAYING if any engine is playing; else the most-common state."""
        states = [e.state for e in self._engines]
        if any(s == PlaybackState.PLAYING for s in states):
            return PlaybackState.PLAYING
        if all(s == PlaybackState.STOPPED for s in states):
            return PlaybackState.STOPPED
        return PlaybackState.PAUSED

    def get_discovery_info(self) -> dict:
        """Return the WebSocket discovery payload describing the multi-bag session.

        Shape::

            {
                "bags": [
                    {"id": "a", "label": "Run A", "offset_sec": 0.0,
                     "duration_sec": 12.5, "topics": [...]},
                    ...
                ],
                "namespaced_topics": ["a:/imu/data", "a:/joint_states", "b:/imu/data", ...]
            }

        Clients (PlotJuggler, the built-in viewer) use ``namespaced_topics``
        to subscribe; the per-bag entries provide context for grouping/coloring.
        """
        bag_entries = []
        all_namespaced: list[str] = []
        for cfg, engine in zip(self._configs, self._engines):
            topics = engine.get_topics_info()
            namespaced = [f"{cfg.bag_id}:{t['name']}" for t in topics]
            all_namespaced.extend(namespaced)
            bag_entries.append({
                "id": cfg.bag_id,
                "label": cfg.label or cfg.bag_id,
                "offset_sec": cfg.offset_sec,
                "duration_sec": engine.duration_sec,
                "topics": topics,
            })
        return {"bags": bag_entries, "namespaced_topics": all_namespaced}

    async def play(self) -> None:
        """Start (or resume) playback of every bag, respecting per-bag offsets.

        For each bag with ``offset_sec > 0``, schedules a delayed
        engine.play() via an asyncio task. Returns immediately — playback
        runs in the background until stop() is called or the bags finish.
        """
        self._stopped = False
        # Cancel any leftover delay tasks from a prior play() call
        await self._cancel_delay_tasks()

        for cfg, engine in zip(self._configs, self._engines):
            if cfg.offset_sec > 0:
                self._delay_tasks.append(
                    asyncio.create_task(self._delayed_play(engine, cfg.offset_sec))
                )
            else:
                await engine.play()
        logger.info(
            "MultiBagPlayback started: %d bags, speed=%.2fx",
            len(self._engines), self._speed,
        )

    async def pause(self) -> None:
        """Pause every engine in parallel."""
        await asyncio.gather(*[e.pause() for e in self._engines])
        logger.info("MultiBagPlayback paused")

    async def stop(self) -> None:
        """Stop every engine and cancel any pending delay tasks."""
        self._stopped = True
        await self._cancel_delay_tasks()
        await asyncio.gather(*[e.stop() for e in self._engines], return_exceptions=True)
        logger.info("MultiBagPlayback stopped")

    async def seek(self, timestamp_sec: float) -> None:
        """Seek every bag to the same content-time. Per-bag offsets are NOT applied
        to the seek target — the user gives a single absolute time and each bag
        clamps to its own bounds.
        """
        await asyncio.gather(*[e.seek(timestamp_sec) for e in self._engines])
        logger.info("MultiBagPlayback seeked to %.2fs", timestamp_sec)

    async def set_speed(self, speed: float) -> None:
        """Update speed on every engine."""
        self._speed = speed
        await asyncio.gather(*[e.set_speed(speed) for e in self._engines])
        logger.info("MultiBagPlayback speed = %.2fx", speed)

    # ----- Internals -----

    def _make_per_bag_callback(self, bag_id: str) -> Callable[[Message], None]:
        """Build the per-engine callback that tags + namespaces messages."""
        user_cb = self._callback
        if user_cb is None:
            return lambda _msg: None  # type: ignore[return-value]

        def _wrapper(msg: Message) -> None:
            # Rewrite the topic name to the namespaced form before fanout.
            # Mutating the dataclass is OK because the parser yields
            # fresh Message instances per message — we don't share refs.
            msg.topic = f"{bag_id}:{msg.topic}"
            user_cb(bag_id, msg)
        return _wrapper

    async def _delayed_play(self, engine: PlaybackEngine, delay_sec: float) -> None:
        """Sleep for delay_sec/speed, then start the engine. Honors stop()."""
        try:
            await asyncio.sleep(delay_sec / max(self._speed, 0.001))
            if not self._stopped:
                await engine.play()
        except asyncio.CancelledError:
            pass

    async def _cancel_delay_tasks(self) -> None:
        for t in self._delay_tasks:
            if not t.done():
                t.cancel()
        if self._delay_tasks:
            await asyncio.gather(*self._delay_tasks, return_exceptions=True)
        self._delay_tasks = []
