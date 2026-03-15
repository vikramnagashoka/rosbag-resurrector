"""Unified parser for MCAP (ROS2) and legacy rosbag (ROS1) formats.

Provides a common interface to read topics, metadata, and messages
from any supported bag format without requiring a ROS installation.
"""

from __future__ import annotations

import logging
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import numpy as np

logger = logging.getLogger("resurrector.ingest.parser")

from mcap.reader import make_reader


@dataclass
class TopicInfo:
    """Metadata about a topic in a bag file."""
    name: str
    message_type: str
    message_count: int
    frequency_hz: float | None = None
    schema_encoding: str = ""
    schema_data: str = ""


@dataclass
class BagMetadata:
    """Metadata about an entire bag file."""
    path: Path
    format: str  # "mcap", "ros1bag", "ros2db3"
    duration_sec: float
    start_time_ns: int
    end_time_ns: int
    message_count: int
    topics: list[TopicInfo] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def start_time_sec(self) -> float:
        return self.start_time_ns / 1e9

    @property
    def end_time_sec(self) -> float:
        return self.end_time_ns / 1e9


@dataclass
class Message:
    """A single deserialized message from a bag."""
    topic: str
    timestamp_ns: int
    data: dict[str, Any]
    raw_data: bytes | None = None
    sequence: int = 0

    @property
    def timestamp_sec(self) -> float:
        return self.timestamp_ns / 1e9


class MCAPParser:
    """Parser for MCAP format files (ROS2 default)."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"MCAP file not found: {self.path}")

    def get_metadata(self) -> BagMetadata:
        """Read bag metadata without loading messages."""
        with open(self.path, "rb") as f:
            reader = make_reader(f)
            summary = reader.get_summary()

            if summary is None:
                raise ValueError(f"Could not read summary from {self.path}")

            # Build topic info from channels and statistics
            topics: list[TopicInfo] = []
            schemas_by_id = {s.id: s for s in summary.schemas.values()}

            # Get per-channel message counts from statistics
            channel_msg_counts: dict[int, int] = {}
            if summary.statistics:
                channel_msg_counts = dict(summary.statistics.channel_message_counts)

            start_time = summary.statistics.message_start_time if summary.statistics else 0
            end_time = summary.statistics.message_end_time if summary.statistics else 0
            total_count = summary.statistics.message_count if summary.statistics else 0
            duration_ns = end_time - start_time if end_time > start_time else 0
            duration_sec = duration_ns / 1e9

            for channel in summary.channels.values():
                schema = schemas_by_id.get(channel.schema_id)
                msg_count = channel_msg_counts.get(channel.id, 0)

                freq = None
                if duration_sec > 0 and msg_count > 1:
                    freq = round(msg_count / duration_sec, 2)

                topics.append(TopicInfo(
                    name=channel.topic,
                    message_type=schema.name if schema else "unknown",
                    message_count=msg_count,
                    frequency_hz=freq,
                    schema_encoding=schema.encoding if schema else "",
                    schema_data=schema.data.decode("utf-8", errors="replace") if schema else "",
                ))

            # Sort topics by name
            topics.sort(key=lambda t: t.name)

            # Read MCAP-level metadata
            extra_metadata: dict[str, Any] = {}
            # metadata_indexes contains index records; reading full metadata
            # requires iterating messages, so we skip for summary-only reads

            return BagMetadata(
                path=self.path,
                format="mcap",
                duration_sec=duration_sec,
                start_time_ns=start_time,
                end_time_ns=end_time,
                message_count=total_count,
                topics=topics,
                metadata=extra_metadata,
            )

    def read_messages(
        self,
        topics: list[str] | None = None,
        start_time_ns: int | None = None,
        end_time_ns: int | None = None,
    ) -> Iterator[Message]:
        """Read messages from the bag, optionally filtered.

        Yields raw Message objects with CDR-encoded data in raw_data
        and a best-effort parsed dict in data.
        """
        with open(self.path, "rb") as f:
            reader = make_reader(f)
            summary = reader.get_summary()
            schemas_by_id = {}
            channel_schemas = {}
            if summary:
                schemas_by_id = {s.id: s for s in summary.schemas.values()}
                for ch in summary.channels.values():
                    schema = schemas_by_id.get(ch.schema_id)
                    if schema:
                        channel_schemas[ch.id] = schema.name

            warned_types: set[str] = set()

            for schema, channel, message in reader.iter_messages(
                topics=topics,
                start_time=start_time_ns,
                end_time=end_time_ns,
            ):
                msg_type = schema.name if schema else channel_schemas.get(channel.id, "unknown")

                # Try to parse the CDR data
                parsed = _parse_cdr_message(msg_type, message.data)

                # Warn once per unsupported type
                if parsed.get("_unparsed") and msg_type not in warned_types:
                    logger.warning(
                        "Topic '%s': no parser for message type '%s' — "
                        "data will be available as raw bytes only",
                        channel.topic, msg_type,
                    )
                    warned_types.add(msg_type)

                yield Message(
                    topic=channel.topic,
                    timestamp_ns=message.log_time,
                    data=parsed,
                    raw_data=message.data,
                    sequence=message.sequence,
                )


def _parse_cdr_message(msg_type: str, data: bytes) -> dict[str, Any]:
    """Best-effort CDR deserialization for common ROS2 message types.

    This handles the most common sensor message types without needing
    the full ROS2 type system.
    """
    result: dict[str, Any] = {}
    if len(data) < 4:
        return result

    # Skip CDR encapsulation header (4 bytes)
    buf = data[4:]
    try:
        if msg_type == "sensor_msgs/msg/Imu":
            result = _parse_imu(buf)
        elif msg_type == "sensor_msgs/msg/JointState":
            result = _parse_joint_state(buf)
        elif msg_type == "sensor_msgs/msg/Image":
            result = _parse_image(buf)
        elif msg_type == "sensor_msgs/msg/LaserScan":
            result = _parse_laser_scan(buf)
        else:
            logger.debug("No CDR parser for message type '%s' (%d bytes)", msg_type, len(data))
            result = {"_unparsed": True, "_msg_type": msg_type, "_raw_size": len(data)}
    except Exception as exc:
        logger.warning(
            "CDR parse error for '%s' (%d bytes): %s", msg_type, len(data), exc
        )
        result = {"_parse_error": True, "_msg_type": msg_type, "_raw_size": len(data)}

    return result


def _read_header(buf: bytes, offset: int) -> tuple[int, int, str, int]:
    """Read a std_msgs/Header from CDR buffer. Returns (sec, nsec, frame_id, new_offset)."""
    sec, nsec = struct.unpack_from("<II", buf, offset)
    offset += 8
    str_len = struct.unpack_from("<I", buf, offset)[0]
    offset += 4
    frame_id = buf[offset:offset + str_len].decode("utf-8", errors="replace").rstrip("\x00")
    offset += str_len
    # Align to 4 bytes
    offset = (offset + 3) & ~3
    return sec, nsec, frame_id, offset


def _parse_imu(buf: bytes) -> dict[str, Any]:
    """Parse sensor_msgs/Imu from CDR buffer."""
    sec, nsec, frame_id, off = _read_header(buf, 0)
    # orientation: 4 x float64
    qx, qy, qz, qw = struct.unpack_from("<4d", buf, off)
    off += 32
    # orientation_covariance: 9 x float64
    off += 72
    # angular_velocity: 3 x float64
    gx, gy, gz = struct.unpack_from("<3d", buf, off)
    off += 24
    # angular_velocity_covariance: 9 x float64
    off += 72
    # linear_acceleration: 3 x float64
    ax, ay, az = struct.unpack_from("<3d", buf, off)

    return {
        "header": {"stamp_sec": sec, "stamp_nsec": nsec, "frame_id": frame_id},
        "orientation": {"x": qx, "y": qy, "z": qz, "w": qw},
        "angular_velocity": {"x": gx, "y": gy, "z": gz},
        "linear_acceleration": {"x": ax, "y": ay, "z": az},
    }


def _parse_joint_state(buf: bytes) -> dict[str, Any]:
    """Parse sensor_msgs/JointState from CDR buffer."""
    sec, nsec, frame_id, off = _read_header(buf, 0)

    # names: string[]
    n_names = struct.unpack_from("<I", buf, off)[0]
    off += 4
    names = []
    for _ in range(n_names):
        str_len = struct.unpack_from("<I", buf, off)[0]
        off += 4
        name = buf[off:off + str_len].decode("utf-8", errors="replace").rstrip("\x00")
        off += str_len
        off = (off + 3) & ~3
        names.append(name)

    def read_float64_array() -> tuple[list[float], int]:
        nonlocal off
        n = struct.unpack_from("<I", buf, off)[0]
        off += 4
        # Align to 8 bytes for float64
        off = (off + 7) & ~7
        values = list(struct.unpack_from(f"<{n}d", buf, off))
        off += n * 8
        return values, off

    positions, off = read_float64_array()
    velocities, off = read_float64_array()
    efforts, off = read_float64_array()

    return {
        "header": {"stamp_sec": sec, "stamp_nsec": nsec, "frame_id": frame_id},
        "name": names,
        "position": positions,
        "velocity": velocities,
        "effort": efforts,
    }


def _parse_image(buf: bytes) -> dict[str, Any]:
    """Parse sensor_msgs/Image from CDR buffer (metadata only, not pixel data)."""
    sec, nsec, frame_id, off = _read_header(buf, 0)
    height, width = struct.unpack_from("<II", buf, off)
    off += 8
    enc_len = struct.unpack_from("<I", buf, off)[0]
    off += 4
    encoding = buf[off:off + enc_len].decode("utf-8", errors="replace").rstrip("\x00")
    off += enc_len
    off = (off + 3) & ~3
    is_bigendian = struct.unpack_from("<B", buf, off)[0]
    off += 1
    off = (off + 3) & ~3
    step = struct.unpack_from("<I", buf, off)[0]
    off += 4
    data_len = struct.unpack_from("<I", buf, off)[0]
    off += 4
    # Don't store pixel data in the dict — too large
    return {
        "header": {"stamp_sec": sec, "stamp_nsec": nsec, "frame_id": frame_id},
        "height": height,
        "width": width,
        "encoding": encoding,
        "is_bigendian": bool(is_bigendian),
        "step": step,
        "data_length": data_len,
        "_pixel_data_offset": off,  # Offset into raw CDR data where pixels start
    }


def _parse_laser_scan(buf: bytes) -> dict[str, Any]:
    """Parse sensor_msgs/LaserScan from CDR buffer."""
    sec, nsec, frame_id, off = _read_header(buf, 0)
    angle_min, angle_max, angle_inc, time_inc, scan_time = struct.unpack_from("<5f", buf, off)
    off += 20
    range_min, range_max = struct.unpack_from("<2f", buf, off)
    off += 8
    n_ranges = struct.unpack_from("<I", buf, off)[0]
    off += 4
    ranges = list(struct.unpack_from(f"<{n_ranges}f", buf, off))
    off += n_ranges * 4
    n_intensities = struct.unpack_from("<I", buf, off)[0]
    off += 4
    intensities = list(struct.unpack_from(f"<{n_intensities}f", buf, off))

    return {
        "header": {"stamp_sec": sec, "stamp_nsec": nsec, "frame_id": frame_id},
        "angle_min": angle_min,
        "angle_max": angle_max,
        "angle_increment": angle_inc,
        "time_increment": time_inc,
        "scan_time": scan_time,
        "range_min": range_min,
        "range_max": range_max,
        "ranges": ranges,
        "intensities": intensities,
    }


def get_image_array(msg: Message) -> np.ndarray | None:
    """Extract image data from a parsed Image message as a numpy array."""
    if msg.raw_data is None or "height" not in msg.data:
        return None
    offset = msg.data.get("_pixel_data_offset")
    if offset is None:
        return None
    height = msg.data["height"]
    width = msg.data["width"]
    encoding = msg.data["encoding"]

    raw = msg.raw_data[4:]  # Skip CDR header
    pixel_data = raw[offset:]

    if encoding in ("rgb8", "bgr8"):
        channels = 3
    elif encoding in ("rgba8", "bgra8"):
        channels = 4
    elif encoding == "mono8":
        channels = 1
    else:
        return None

    expected_size = height * width * channels
    if len(pixel_data) < expected_size:
        return None

    arr = np.frombuffer(pixel_data[:expected_size], dtype=np.uint8)
    if channels == 1:
        return arr.reshape(height, width)
    return arr.reshape(height, width, channels)


def parse_bag(path: str | Path) -> MCAPParser:
    """Create a parser for the given bag file.

    Currently supports MCAP format. Legacy ROS1 .bag support
    requires the 'rosbags' optional dependency.
    """
    path = Path(path)
    ext = path.suffix.lower()

    if ext == ".mcap":
        return MCAPParser(path)
    elif ext == ".bag":
        raise NotImplementedError(
            "ROS1 .bag format support requires the 'rosbags' package. "
            "Install with: pip install rosbag-resurrector[ros1]"
        )
    elif ext == ".db3":
        raise NotImplementedError(
            "ROS2 .db3 (SQLite) format is not yet supported. "
            "Convert to MCAP with: ros2 bag convert"
        )
    else:
        raise ValueError(f"Unsupported file format: {ext}")
