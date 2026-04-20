"""In-memory LRU caches for dashboard responses.

Two caches live here:

- `topic_data_cache` — downsampled topic data keyed on
  (bag_id, topic, start_sec, end_sec, max_points, bag_mtime).
  Invalidates automatically when the .mcap file's mtime changes,
  so a user editing the bag externally doesn't see stale plots.

- `frame_build_locks` — per-(bag_id, topic) async lock registry.
  Serializes lazy frame-offset index builds so a burst of 20
  semantic-search thumbnails for the same bag doesn't trigger 20
  concurrent MCAP scans.
"""

from __future__ import annotations

import asyncio
from functools import lru_cache
from pathlib import Path
from typing import Any

# Keyed on (bag_id, topic, start_sec, end_sec, max_points, mtime_ns).
# Value is the pre-serialized JSON-safe dict the endpoint returns.
_TOPIC_DATA_CACHE: dict[tuple, dict[str, Any]] = {}
_TOPIC_DATA_ORDER: list[tuple] = []
_TOPIC_DATA_MAX = 64


def topic_cache_key(
    bag_id: int,
    topic: str,
    start_sec: float | None,
    end_sec: float | None,
    max_points: int,
    bag_path: str | Path,
) -> tuple:
    """Build a cache key that auto-invalidates when the .mcap is modified."""
    try:
        mtime_ns = Path(bag_path).stat().st_mtime_ns
    except OSError:
        mtime_ns = -1
    return (bag_id, topic, start_sec, end_sec, max_points, mtime_ns)


def get_topic_cache(key: tuple) -> dict[str, Any] | None:
    v = _TOPIC_DATA_CACHE.get(key)
    if v is not None:
        # Refresh LRU position
        try:
            _TOPIC_DATA_ORDER.remove(key)
        except ValueError:
            pass
        _TOPIC_DATA_ORDER.append(key)
    return v


def set_topic_cache(key: tuple, value: dict[str, Any]) -> None:
    _TOPIC_DATA_CACHE[key] = value
    try:
        _TOPIC_DATA_ORDER.remove(key)
    except ValueError:
        pass
    _TOPIC_DATA_ORDER.append(key)
    while len(_TOPIC_DATA_ORDER) > _TOPIC_DATA_MAX:
        evict = _TOPIC_DATA_ORDER.pop(0)
        _TOPIC_DATA_CACHE.pop(evict, None)


def clear_topic_cache() -> None:
    _TOPIC_DATA_CACHE.clear()
    _TOPIC_DATA_ORDER.clear()


# Per-(bag_id, topic) async locks. The lock dict itself is protected by
# a regular threading lock because we mutate it from both sync and async
# entry points.
import threading
_FRAME_LOCK_DICT_LOCK = threading.Lock()
_FRAME_LOCKS: dict[tuple[int, str], asyncio.Lock] = {}


def get_frame_build_lock(bag_id: int, topic: str) -> asyncio.Lock:
    """Return the asyncio.Lock scoped to (bag_id, topic), creating if needed."""
    with _FRAME_LOCK_DICT_LOCK:
        key = (bag_id, topic)
        lock = _FRAME_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _FRAME_LOCKS[key] = lock
        return lock
