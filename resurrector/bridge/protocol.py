"""PlotJuggler-compatible WebSocket message encoding.

Converts Resurrector's nested Message.data dicts into the flat JSON format
that PlotJuggler's WebSocket client plugin expects:

    {"timestamp": 1700000001.005, "/imu/data/orientation/x": 0.001, ...}

Keys use "/" separator. Small numeric lists expand to indexed keys.
Internal keys (starting with "_") are excluded.
"""

from __future__ import annotations

from typing import Any


def flatten_to_plotjuggler(
    topic: str,
    data: dict[str, Any],
    timestamp_sec: float,
) -> dict[str, Any]:
    """Convert a parsed Message.data dict to PlotJuggler flat format.

    Args:
        topic: ROS topic name (e.g., "/imu/data").
        data: The Message.data dict from the parser.
        timestamp_sec: Message timestamp as float seconds.

    Returns:
        Flat dict with "timestamp" key and "{topic}/{field/path}" keys.
    """
    result: dict[str, Any] = {"timestamp": timestamp_sec}
    _flatten_slash(data, topic, result)
    return result


def _flatten_slash(
    d: dict[str, Any],
    prefix: str,
    out: dict[str, Any],
    max_list_expand: int = 20,
) -> None:
    """Recursively flatten a nested dict using "/" separator.

    Skips keys starting with "_" (internal parser fields).
    Expands small numeric lists to indexed keys.
    Skips the header/stamp fields (timestamp is top-level).
    """
    for key, value in d.items():
        if key.startswith("_"):
            continue
        # Skip header stamp fields — timestamp is already top-level
        if key == "header":
            continue

        full_key = f"{prefix}/{key}"

        if isinstance(value, dict):
            _flatten_slash(value, full_key, out, max_list_expand)
        elif isinstance(value, list):
            if (
                len(value) <= max_list_expand
                and value
                and all(isinstance(v, (int, float)) for v in value)
            ):
                for i, v in enumerate(value):
                    out[f"{full_key}/{i}"] = v
            # Large arrays (e.g., lidar ranges) are omitted by default
        elif isinstance(value, (int, float, bool)):
            out[full_key] = value
        elif isinstance(value, str):
            # Skip string fields in numeric stream (e.g., joint names)
            pass


def encode_status_message(
    mode: str,
    state: str,
    speed: float = 1.0,
    timestamp_sec: float = 0.0,
    progress: float = 0.0,
) -> dict[str, Any]:
    """Encode a server status message."""
    return {
        "type": "status",
        "mode": mode,
        "state": state,
        "speed": speed,
        "timestamp": timestamp_sec,
        "progress": progress,
    }


def encode_topics_message(
    topics: list[dict[str, Any]],
) -> dict[str, Any]:
    """Encode a topic discovery message."""
    return {
        "type": "topics",
        "available": topics,
    }
