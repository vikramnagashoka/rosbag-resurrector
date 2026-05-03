"""Generate a tiny synthetic MCAP bag with TF + PointCloud2 for scene tests.

Schema strings here are minimal — just enough that the bag passes
through resurrector's ingest path. The CDR payload bytes are correct
(otherwise our scene parser would fail to round-trip them).

Used by tests/test_api_scene.py and tests/test_scene.py for any
real-bag round-trip coverage. Doubles as sub-feature 3.8 (3D scene
test fixtures).
"""

from __future__ import annotations

import struct
from pathlib import Path
from typing import Any

import numpy as np


_TF_SCHEMA = """\
geometry_msgs/TransformStamped[] transforms
================================================================================
MSG: geometry_msgs/TransformStamped
std_msgs/Header header
string child_frame_id
geometry_msgs/Transform transform
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: geometry_msgs/Transform
geometry_msgs/Vector3 translation
geometry_msgs/Quaternion rotation
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
"""


_PC2_SCHEMA = """\
std_msgs/Header header
uint32 height
uint32 width
sensor_msgs/PointField[] fields
bool is_bigendian
uint32 point_step
uint32 row_step
uint8[] data
bool is_dense
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: sensor_msgs/PointField
string name
uint32 offset
uint8 datatype
uint32 count
"""


def _build_string(s: str) -> bytes:
    raw = s.encode("utf-8") + b"\x00"
    return struct.pack("<I", len(raw)) + raw


def _align(buf: bytes, alignment: int) -> bytes:
    pad = (-len(buf)) % alignment
    return buf + b"\x00" * pad


def encode_tf_message(transforms: list[dict[str, Any]]) -> bytes:
    """Encode a tf2_msgs/TFMessage CDR payload.

    Each input dict has: parent, child, ts_sec, ts_nsec, translation
    (3-tuple), rotation (4-tuple xyzw).
    """
    cdr = b"\x00\x01\x00\x00"
    body = struct.pack("<I", len(transforms))
    for t in transforms:
        body += struct.pack("<iI", t["ts_sec"], t["ts_nsec"])
        body += _build_string(t["parent"])
        body += _build_string(t["child"])
        body = _align(body, 8)
        body += struct.pack("<3d", *t["translation"])
        body += struct.pack("<4d", *t["rotation"])
    return cdr + body


def encode_pointcloud2(
    points: np.ndarray, frame_id: str, ts_sec: int, ts_nsec: int,
) -> bytes:
    """Encode a sensor_msgs/PointCloud2 CDR payload from an Nx3 float32 array."""
    n = points.shape[0]
    cdr = b"\x00\x01\x00\x00"
    body = struct.pack("<iI", ts_sec, ts_nsec)
    body += _build_string(frame_id)
    body = _align(body, 4)
    body += struct.pack("<II", 1, n)
    body += struct.pack("<I", 3)
    for name, off in (("x", 0), ("y", 4), ("z", 8)):
        body += _build_string(name)
        body = _align(body, 4)
        body += struct.pack("<I", off)
        body += struct.pack("<B", 7)
        body += b"\x00\x00\x00"
        body += struct.pack("<I", 1)
    body += struct.pack("<B", 0)
    body = _align(body, 4)
    body += struct.pack("<II", 12, 12 * n)
    body += struct.pack("<I", points.nbytes)
    body += points.astype(np.float32).tobytes()
    body += struct.pack("<B", 1)
    return cdr + body


def generate_scene_bag(output_path: str | Path) -> Path:
    """Write a small MCAP with /tf, /tf_static, and /lidar/points.

    Layout:
    - /tf_static: one message at t=0 with base_link → camera_link
    - /tf: 5 messages at 100 ms intervals with world → base_link
      (translation x sweeps from 0 → 0.4)
    - /lidar/points: 3 PointCloud2 messages with 100 points each
    """
    from mcap.writer import Writer

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_ns = 1_700_000_000_000_000_000

    with open(output_path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="resurrector-test-scene")

        tf_schema = writer.register_schema(
            name="tf2_msgs/msg/TFMessage", encoding="ros2msg",
            data=_TF_SCHEMA.encode("utf-8"),
        )
        pc2_schema = writer.register_schema(
            name="sensor_msgs/msg/PointCloud2", encoding="ros2msg",
            data=_PC2_SCHEMA.encode("utf-8"),
        )
        tf_chan = writer.register_channel(
            topic="/tf", message_encoding="cdr", schema_id=tf_schema,
        )
        tf_static_chan = writer.register_channel(
            topic="/tf_static", message_encoding="cdr", schema_id=tf_schema,
        )
        pc_chan = writer.register_channel(
            topic="/lidar/points", message_encoding="cdr", schema_id=pc2_schema,
        )

        # Static TF: base_link → camera_link at (0.2, 0, 0.3)
        sec0 = base_ns // 1_000_000_000
        nsec0 = base_ns % 1_000_000_000
        static_payload = encode_tf_message([{
            "parent": "base_link", "child": "camera_link",
            "ts_sec": sec0, "ts_nsec": nsec0,
            "translation": (0.2, 0.0, 0.3),
            "rotation": (0.0, 0.0, 0.0, 1.0),
        }])
        writer.add_message(
            channel_id=tf_static_chan,
            log_time=base_ns, publish_time=base_ns,
            data=static_payload,
        )

        # Dynamic TF: world → base_link, 5 samples
        for i in range(5):
            ts = base_ns + i * 100_000_000  # 100 ms apart
            sec = ts // 1_000_000_000
            nsec = ts % 1_000_000_000
            payload = encode_tf_message([{
                "parent": "world", "child": "base_link",
                "ts_sec": sec, "ts_nsec": nsec,
                "translation": (0.1 * i, 0.0, 0.0),
                "rotation": (0.0, 0.0, 0.0, 1.0),
            }])
            writer.add_message(
                channel_id=tf_chan,
                log_time=ts, publish_time=ts, data=payload,
            )

        # PointCloud2: 3 frames, 100 points each
        rng = np.random.default_rng(42)
        for i in range(3):
            ts = base_ns + i * 100_000_000
            sec = ts // 1_000_000_000
            nsec = ts % 1_000_000_000
            pts = rng.normal(0, 1, size=(100, 3)).astype(np.float32)
            payload = encode_pointcloud2(pts, "lidar_link", sec, nsec)
            writer.add_message(
                channel_id=pc_chan,
                log_time=ts, publish_time=ts, data=payload,
            )

        writer.finish()

    return output_path
