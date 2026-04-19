"""Thread-safe ring buffer for decoupling message producers from WebSocket consumers.

Each message is JSON-serialized once when entering the buffer, then shared
across all consumers (avoiding N serializations for N clients).

When a consumer falls behind by more than half the buffer's capacity, a
warning is logged so operators can spot slow clients before messages
start dropping silently.
"""

from __future__ import annotations

import json
import logging
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("resurrector.bridge.buffer")


@dataclass
class BufferedMessage:
    """A pre-encoded message in the ring buffer."""
    topic: str
    timestamp_sec: float
    encoded: dict[str, Any]
    raw_json: str  # Pre-serialized JSON


class RingBuffer:
    """Thread-safe ring buffer with multi-consumer support.

    Each consumer tracks its own read position. The buffer evicts the
    oldest messages when at capacity. Consumers that fall more than half
    a buffer behind are logged so the operator can investigate before
    messages start dropping silently.
    """

    LAG_WARN_THRESHOLD = 0.5  # warn when consumer lag > 50% of capacity

    def __init__(self, capacity: int = 10_000):
        self._capacity = capacity
        self._buffer: deque[BufferedMessage] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        # Each consumer tracks how many messages it has read
        # (offset from the "start" of the buffer's lifetime)
        self._global_write_count: int = 0
        self._consumer_positions: dict[str, int] = {}
        # Per-consumer flag to avoid spamming the log; reset on catch-up
        self._consumer_lag_warned: dict[str, bool] = {}

    def put(self, msg: BufferedMessage) -> None:
        """Add a message, evicting oldest if at capacity."""
        with self._lock:
            self._buffer.append(msg)
            self._global_write_count += 1

    def get_since(self, consumer_id: str, max_count: int = 50) -> list[BufferedMessage]:
        """Get unread messages for a consumer, up to max_count.

        Returns empty list if consumer has caught up or doesn't exist.
        Logs a warning when a consumer's lag exceeds half the buffer
        capacity (and the buffer is full enough for that to be meaningful).
        """
        with self._lock:
            if consumer_id not in self._consumer_positions:
                return []

            consumer_pos = self._consumer_positions[consumer_id]
            total_written = self._global_write_count
            buf_len = len(self._buffer)
            oldest_global = total_written - buf_len

            lag = total_written - consumer_pos
            warn_at = int(self._capacity * self.LAG_WARN_THRESHOLD)
            if buf_len >= warn_at and lag >= warn_at:
                if not self._consumer_lag_warned.get(consumer_id, False):
                    logger.warning(
                        "Bridge consumer %s is %d messages behind "
                        "(capacity=%d). Slow client may start dropping.",
                        consumer_id[:8], lag, self._capacity,
                    )
                    self._consumer_lag_warned[consumer_id] = True

            # If consumer is behind the buffer, jump to oldest available
            if consumer_pos < oldest_global:
                consumer_pos = oldest_global

            unread = total_written - consumer_pos
            if unread <= 0:
                # Consumer caught up; clear the warned flag so a future
                # fall-behind triggers a fresh warning.
                self._consumer_lag_warned[consumer_id] = False
                return []

            start_offset = consumer_pos - oldest_global
            count = min(unread, max_count)
            messages = list(self._buffer)[start_offset:start_offset + count]
            new_pos = consumer_pos + len(messages)
            self._consumer_positions[consumer_id] = new_pos

            # If this read brought us within tolerance, re-arm the warning
            # so a future regression logs again.
            if total_written - new_pos < warn_at:
                self._consumer_lag_warned[consumer_id] = False

            return messages

    def register_consumer(self, consumer_id: str) -> None:
        """Register a new consumer, starting from the current position."""
        with self._lock:
            self._consumer_positions[consumer_id] = self._global_write_count
            self._consumer_lag_warned[consumer_id] = False

    def unregister_consumer(self, consumer_id: str) -> None:
        """Remove a consumer."""
        with self._lock:
            self._consumer_positions.pop(consumer_id, None)
            self._consumer_lag_warned.pop(consumer_id, None)

    @property
    def size(self) -> int:
        """Current number of messages in the buffer."""
        with self._lock:
            return len(self._buffer)

    @property
    def consumer_count(self) -> int:
        """Number of registered consumers."""
        with self._lock:
            return len(self._consumer_positions)
