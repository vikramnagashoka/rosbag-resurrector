"""Build a richer demo bag for the v0.6 Scene tab and the e2e tests.

The shipping synth bag (``resurrector.demo.sample_bag``) declares /tf
but writes zero TF messages, and has no PointCloud2 / Marker data —
so the Scene tab renders empty. This script writes a real /tf chain
that animates over 8 seconds plus a dense rotating point cloud, so
the Scene viewer (and the Playwright e2e suite) has meaningful data.

Run:
    python tests/fixtures/make_scene_demo_bag.py [output_path]

Default output: ``~/rosbag-demo/scene_demo.mcap``. Pass an explicit
path to override (the e2e launcher does this).
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np

# Encoders live next door. Support both `python -m tests.fixtures.make...`
# (relative import) and `python tests/fixtures/make_scene_demo_bag.py`
# (script invocation, no package context).
try:
    from tests.fixtures.scene_bag import encode_tf_message, encode_pointcloud2
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from scene_bag import encode_tf_message, encode_pointcloud2  # type: ignore

from mcap.writer import Writer  # noqa: E402


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
PointField[] fields
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


def _yaw_quat(yaw_rad: float) -> tuple[float, float, float, float]:
    return (0.0, 0.0, math.sin(yaw_rad / 2.0), math.cos(yaw_rad / 2.0))


def make_bag(out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    base_ns = 1_700_000_000_000_000_000
    duration_sec = 8.0
    tf_hz = 30.0
    pc_hz = 10.0

    tf_count = int(duration_sec * tf_hz)
    pc_count = int(duration_sec * pc_hz)

    with open(out, "wb") as f:
        w = Writer(f)
        w.start(profile="ros2", library="resurrector-scene-demo")

        tf_sid = w.register_schema(name="tf2_msgs/msg/TFMessage", encoding="ros2msg", data=_TF_SCHEMA.encode())
        pc_sid = w.register_schema(name="sensor_msgs/msg/PointCloud2", encoding="ros2msg", data=_PC2_SCHEMA.encode())

        tf_chan = w.register_channel(topic="/tf", message_encoding="cdr", schema_id=tf_sid)
        tfs_chan = w.register_channel(topic="/tf_static", message_encoding="cdr", schema_id=tf_sid)
        pc_chan = w.register_channel(topic="/lidar/points", message_encoding="cdr", schema_id=pc_sid)

        sec0, nsec0 = base_ns // 1_000_000_000, base_ns % 1_000_000_000

        static = encode_tf_message([
            {"parent": "base_link", "child": "lidar_link",
             "ts_sec": sec0, "ts_nsec": nsec0,
             "translation": (0.0, 0.0, 0.4), "rotation": (0.0, 0.0, 0.0, 1.0)},
            {"parent": "base_link", "child": "camera_link",
             "ts_sec": sec0, "ts_nsec": nsec0,
             "translation": (0.25, 0.0, 0.3), "rotation": (0.0, 0.0, 0.0, 1.0)},
        ])
        w.add_message(channel_id=tfs_chan, log_time=base_ns, publish_time=base_ns, data=static)

        for i in range(tf_count):
            t_sec = i / tf_hz
            ts = base_ns + int(t_sec * 1_000_000_000)
            sec, nsec = ts // 1_000_000_000, ts % 1_000_000_000

            angle = (t_sec / duration_sec) * 2.0 * math.pi
            x = math.cos(angle) * 1.2
            y = math.sin(angle) * 1.2
            yaw = angle + math.pi / 2.0

            arm_yaw = math.sin(t_sec * 2.0) * 1.2

            payload = encode_tf_message([
                {"parent": "world", "child": "base_link",
                 "ts_sec": sec, "ts_nsec": nsec,
                 "translation": (x, y, 0.0), "rotation": _yaw_quat(yaw)},
                {"parent": "base_link", "child": "arm_link",
                 "ts_sec": sec, "ts_nsec": nsec,
                 "translation": (0.0, 0.0, 0.5), "rotation": _yaw_quat(arm_yaw)},
                {"parent": "arm_link", "child": "arm_tip",
                 "ts_sec": sec, "ts_nsec": nsec,
                 "translation": (0.4, 0.0, 0.0), "rotation": (0.0, 0.0, 0.0, 1.0)},
            ])
            w.add_message(channel_id=tf_chan, log_time=ts, publish_time=ts, data=payload)

        rng = np.random.default_rng(7)
        for i in range(pc_count):
            t_sec = i / pc_hz
            ts = base_ns + int(t_sec * 1_000_000_000)
            sec, nsec = ts // 1_000_000_000, ts % 1_000_000_000

            n_points = 4000
            theta = rng.uniform(0.0, 2.0 * math.pi, n_points)
            r = rng.uniform(0.4, 3.0, n_points)
            zoff = rng.normal(0.0, 0.15, n_points)
            sweep = (t_sec / duration_sec) * 2.0 * math.pi
            r = r + 0.3 * np.sin(theta * 4.0 + sweep)
            xs = r * np.cos(theta)
            ys = r * np.sin(theta)
            zs = 0.2 + zoff
            pts = np.stack([xs, ys, zs], axis=1).astype(np.float32)

            payload = encode_pointcloud2(pts, "lidar_link", sec, nsec)
            w.add_message(channel_id=pc_chan, log_time=ts, publish_time=ts, data=payload)

        w.finish()

    return out


if __name__ == "__main__":
    if len(sys.argv) > 1:
        out_path = Path(sys.argv[1])
    else:
        out_path = Path.home() / "rosbag-demo" / "scene_demo.mcap"
    p = make_bag(out_path)
    size_mb = p.stat().st_size / (1024 * 1024)
    print(f"wrote {p}  ({size_mb:.1f} MB)")
    print(f"  /tf: 30 Hz x 8s = 240 messages (world→base_link→arm_link→arm_tip)")
    print(f"  /tf_static: base_link→lidar_link, base_link→camera_link")
    print(f"  /lidar/points: 10 Hz x 8s = 80 messages, 4000 points each")
