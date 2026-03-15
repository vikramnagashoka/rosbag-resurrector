"""Thread-safe ring buffer for decoupling message producers from WebSocket consumers.

Each message is JSON-serialized once when entering the buffer, then shared
across all consumers (avoiding N serializations for N clients).
"""

from __future__ import annotations

import json
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any


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
    oldest messages when at capacity.
    """

    def __init__(self, capacity: int = 10_000):
        self._capacity = capacity
        self._buffer: deque[BufferedMessage] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        # Each consumer tracks how many messages it has read
        # (offset from the "start" of the buffer's lifetime)
        self._global_write_count: int = 0
        self._consumer_positions: dict[str, int] = {}

    def put(self, msg: BufferedMessage) -> None:
        """Add a message, evicting oldest if at capacity."""
        with self._lock:
            self._buffer.append(msg)
            self._global_write_count += 1

    def get_since(self, consumer_id: str, max_count: int = 50) -> list[BufferedMessage]:
        """Get unread messages for a consumer, up to max_count.

        Returns empty list if consumer has caught up or doesn't exist.
        """
        with self._lock:
            if consumer_id not in self._consumer_positions:
                return []

            consumer_pos = self._consumer_positions[consumer_id]
            # How many total messages have been written
            total_written = self._global_write_count
            # How many are available in the buffer right now
            buf_len = len(self._buffer)
            # Oldest message index in global terms
            oldest_global = total_written - buf_len

            # If consumer is behind the buffer, jump to oldest available
            if consumer_pos < oldest_global:
                consumer_pos = oldest_global

            # How many unread messages
            unread = total_written - consumer_pos
            if unread <= 0:
                return []

            # Compute buffer offset for the consumer's position
            start_offset = consumer_pos - oldest_global
            count = min(unread, max_count)
            messages = list(self._buffer)[start_offset:start_offset + count]

            # Advance consumer position
            self._consumer_positions[consumer_id] = consumer_pos + len(messages)
            return messages

    def register_consumer(self, consumer_id: str) -> None:
        """Register a new consumer, starting from the current position."""
        with self._lock:
            self._consumer_positions[consumer_id] = self._global_write_count

    def unregister_consumer(self, consumer_id: str) -> None:
        """Remove a consumer."""
        with self._lock:
            self._consumer_positions.pop(consumer_id, None)

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
