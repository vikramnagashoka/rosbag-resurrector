"""Bag playback engine — replay MCAP data with timing control.

Reads messages from an MCAP file and replays them respecting the original
timing, scaled by a configurable speed factor. Supports play, pause, seek,
and speed changes.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from pathlib import Path
from typing import Callable

from resurrector.ingest.parser import MCAPParser, Message, BagMetadata

logger = logging.getLogger("resurrector.bridge.playback")


class PlaybackState(enum.Enum):
    STOPPED = "stopped"
    PLAYING = "playing"
    PAUSED = "paused"


class PlaybackEngine:
    """Replays MCAP bag messages with timing control.

    The playback loop is an asyncio coroutine that integrates cleanly
    with FastAPI/uvicorn's event loop.
    """

    def __init__(
        self,
        bag_path: str | Path,
        speed: float = 1.0,
        topics: list[str] | None = None,
        loop: bool = False,
        message_callback: Callable[[Message], None] | None = None,
    ):
        self._bag_path = Path(bag_path)
        self._parser = MCAPParser(self._bag_path)
        self._metadata: BagMetadata = self._parser.get_metadata()
        self._speed = max(0.1, min(speed, 20.0))
        self._topics = topics
        self._loop = loop
        self._callback = message_callback
        self._state = PlaybackState.STOPPED
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # Not paused initially
        self._current_timestamp_ns: int = self._metadata.start_time_ns
        self._task: asyncio.Task | None = None
        self._stop_requested = False

    @property
    def metadata(self) -> BagMetadata:
        return self._metadata

    @property
    def state(self) -> PlaybackState:
        return self._state

    @property
    def speed(self) -> float:
        return self._speed

    @property
    def progress(self) -> float:
        """0.0 to 1.0 progress through the bag."""
        duration = self._metadata.end_time_ns - self._metadata.start_time_ns
        if duration <= 0:
            return 0.0
        elapsed = self._current_timestamp_ns - self._metadata.start_time_ns
        return max(0.0, min(1.0, elapsed / duration))

    @property
    def current_timestamp_sec(self) -> float:
        return self._current_timestamp_ns / 1e9

    @property
    def duration_sec(self) -> float:
        return self._metadata.duration_sec

    def get_topics_info(self) -> list[dict]:
        """Return topic metadata for discovery."""
        return [
            {
                "name": t.name,
                "type": t.message_type,
                "count": t.message_count,
                "hz": t.frequency_hz,
            }
            for t in self._metadata.topics
        ]

    async def play(self) -> None:
        """Start or resume playback."""
        if self._state == PlaybackState.PAUSED:
            self._pause_event.set()
            self._state = PlaybackState.PLAYING
            logger.info("Resumed playback at %.1fx", self._speed)
            return

        if self._state == PlaybackState.PLAYING:
            return

        self._stop_requested = False
        self._state = PlaybackState.PLAYING
        self._task = asyncio.create_task(self._playback_loop())
        logger.info("Started playback at %.1fx", self._speed)

    async def pause(self) -> None:
        """Pause playback."""
        if self._state != PlaybackState.PLAYING:
            return
        self._pause_event.clear()
        self._state = PlaybackState.PAUSED
        logger.info("Paused at %.2fs", self.current_timestamp_sec)

    async def seek(self, timestamp_sec: float) -> None:
        """Seek to a specific time. Restarts playback from new position."""
        was_playing = self._state == PlaybackState.PLAYING
        await self.stop()

        # Clamp to bag bounds
        target_ns = int(timestamp_sec * 1e9)
        target_ns = max(self._metadata.start_time_ns, min(target_ns, self._metadata.end_time_ns))
        self._current_timestamp_ns = target_ns

        if was_playing:
            await self.play()
        logger.info("Seeked to %.2fs", timestamp_sec)

    async def set_speed(self, speed: float) -> None:
        """Change playback speed."""
        self._speed = max(0.1, min(speed, 20.0))
        logger.info("Speed set to %.1fx", self._speed)

    async def stop(self) -> None:
        """Stop playback entirely."""
        self._stop_requested = True
        self._pause_event.set()  # Unblock if paused
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._state = PlaybackState.STOPPED

    async def _playback_loop(self) -> None:
        """Core playback loop: iterate messages with timing control."""
        while not self._stop_requested:
            wall_start = time.monotonic()
            bag_start_ns = self._current_timestamp_ns

            # Create a fresh parser for each loop iteration (to support seek)
            parser = MCAPParser(self._bag_path)

            for msg in parser.read_messages(
                topics=self._topics,
                start_time_ns=self._current_timestamp_ns,
            ):
                if self._stop_requested:
                    return

                # Wait if paused
                await self._pause_event.wait()

                if self._stop_requested:
                    return

                # Compute how long to sleep
                bag_elapsed_ns = msg.timestamp_ns - bag_start_ns
                target_wall_elapsed = bag_elapsed_ns / (self._speed * 1e9)
                actual_wall_elapsed = time.monotonic() - wall_start
                sleep_time = target_wall_elapsed - actual_wall_elapsed

                if sleep_time > 0.001:  # Only sleep if > 1ms
                    await asyncio.sleep(sleep_time)

                self._current_timestamp_ns = msg.timestamp_ns

                if self._callback:
                    self._callback(msg)

            # Bag finished
            if self._loop and not self._stop_requested:
                self._current_timestamp_ns = self._metadata.start_time_ns
                logger.info("Looping playback")
                continue
            else:
                self._state = PlaybackState.STOPPED
                logger.info("Playback finished")
                return
