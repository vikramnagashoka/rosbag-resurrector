"""
Generate synthetic MCAP files with realistic robotics data for testing.

Creates bags with:
- IMU data at 200Hz (accelerometer + gyroscope + orientation quaternion)
- Joint states at 100Hz (6-DOF robot arm: positions, velocities, efforts)
- Camera images at 30Hz (raw RGB, 64x48 by default, synthetic colored frames)
- Compressed camera images at 10Hz (JPEG, same size and colors)
- Lidar scans at 10Hz (2D laser scan, 360 points per scan)
- TF transforms (base_link -> arm_link chain)

Also generates "unhealthy" bags with:
- Dropped messages (simulating buffer overflow)
- Time gaps (simulating sensor disconnects)
- Out-of-order timestamps
- Partial topic recordings
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import struct
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator

import numpy as np
from mcap.writer import Writer

# Written into every generated bag's MCAP header; stale_sample_reason()
# uses it to recognise a bag as ours before regenerating it.
GENERATOR_LIBRARY = "rosbag-resurrector-testgen"

# ROS2 CDR serialization helpers
# We write raw CDR-encoded messages so we don't need actual ROS2 installed.

# Schema definitions matching ROS2 message types
SCHEMAS = {
    "sensor_msgs/msg/Imu": {
        "encoding": "ros2msg",
        "data": (
            "std_msgs/Header header\n"
            "geometry_msgs/Quaternion orientation\n"
            "float64[9] orientation_covariance\n"
            "geometry_msgs/Vector3 angular_velocity\n"
            "float64[9] angular_velocity_covariance\n"
            "geometry_msgs/Vector3 linear_acceleration\n"
            "float64[9] linear_acceleration_covariance\n"
        ),
    },
    "sensor_msgs/msg/JointState": {
        "encoding": "ros2msg",
        "data": (
            "std_msgs/Header header\n"
            "string[] name\n"
            "float64[] position\n"
            "float64[] velocity\n"
            "float64[] effort\n"
        ),
    },
    "sensor_msgs/msg/Image": {
        "encoding": "ros2msg",
        "data": (
            "std_msgs/Header header\n"
            "uint32 height\n"
            "uint32 width\n"
            "string encoding\n"
            "uint8 is_bigendian\n"
            "uint32 step\n"
            "uint8[] data\n"
        ),
    },
    "sensor_msgs/msg/LaserScan": {
        "encoding": "ros2msg",
        "data": (
            "std_msgs/Header header\n"
            "float32 angle_min\n"
            "float32 angle_max\n"
            "float32 angle_increment\n"
            "float32 time_increment\n"
            "float32 scan_time\n"
            "float32 range_min\n"
            "float32 range_max\n"
            "float32[] ranges\n"
            "float32[] intensities\n"
        ),
    },
    "geometry_msgs/msg/TransformStamped": {
        "encoding": "ros2msg",
        "data": (
            "std_msgs/Header header\n"
            "string child_frame_id\n"
            "geometry_msgs/Transform transform\n"
        ),
    },
    "tf2_msgs/msg/TFMessage": {
        "encoding": "ros2msg",
        "data": "geometry_msgs/TransformStamped[] transforms\n",
    },
    "sensor_msgs/msg/CompressedImage": {
        "encoding": "ros2msg",
        "data": (
            "std_msgs/Header header\n"
            "string format\n"
            "uint8[] data\n"
        ),
    },
}


def _encode_cdr_header(sec: int, nsec: int, frame_id: str) -> bytes:
    """Encode a std_msgs/Header in CDR format (little-endian)."""
    # CDR encapsulation header (not included here — added at message level)
    # Header: stamp (sec uint32 + nsec uint32) + frame_id (string)
    frame_bytes = frame_id.encode("utf-8") + b"\x00"
    # Align string length to 4 bytes
    padding = (4 - (len(frame_bytes) % 4)) % 4
    return (
        struct.pack("<II", sec, nsec)
        + struct.pack("<I", len(frame_bytes))
        + frame_bytes
        + b"\x00" * padding
    )


def _cdr_encapsulate(data: bytes) -> bytes:
    """Add CDR encapsulation header."""
    # 0x00 0x01 = CDR little-endian, then 2 bytes padding
    return b"\x00\x01\x00\x00" + data


def _encode_imu_message(
    t_sec: int, t_nsec: int, ax: float, ay: float, az: float,
    gx: float, gy: float, gz: float,
    qx: float, qy: float, qz: float, qw: float,
) -> bytes:
    """Encode a sensor_msgs/Imu message in CDR."""
    header = _encode_cdr_header(t_sec, t_nsec, "imu_link")
    orientation = struct.pack("<dddd", qx, qy, qz, qw)
    orient_cov = struct.pack("<9d", *([0.0] * 9))
    angular_vel = struct.pack("<ddd", gx, gy, gz)
    angular_cov = struct.pack("<9d", *([0.0] * 9))
    linear_acc = struct.pack("<ddd", ax, ay, az)
    linear_cov = struct.pack("<9d", *([0.0] * 9))
    return _cdr_encapsulate(
        header + orientation + orient_cov + angular_vel + angular_cov
        + linear_acc + linear_cov
    )


def _encode_joint_state(
    t_sec: int, t_nsec: int,
    names: list[str],
    positions: list[float],
    velocities: list[float],
    efforts: list[float],
) -> bytes:
    """Encode a sensor_msgs/JointState message in CDR."""
    header = _encode_cdr_header(t_sec, t_nsec, "")

    def encode_string_array(strings: list[str]) -> bytes:
        result = struct.pack("<I", len(strings))
        for s in strings:
            s_bytes = s.encode("utf-8") + b"\x00"
            padding = (4 - (len(s_bytes) % 4)) % 4
            result += struct.pack("<I", len(s_bytes)) + s_bytes + b"\x00" * padding
        return result

    def encode_float64_array(values: list[float], current_offset: int) -> tuple[bytes, int]:
        # CDR rule: pad before float64 data so it lands on an 8-byte boundary,
        # measured from the start of the inner CDR payload (post-encapsulation).
        count_bytes = struct.pack("<I", len(values))
        offset_after_count = current_offset + 4
        pad_len = (-offset_after_count) % 8
        data_bytes = struct.pack(f"<{len(values)}d", *values) if values else b""
        return count_bytes + b"\x00" * pad_len + data_bytes, offset_after_count + pad_len + len(data_bytes)

    names_data = encode_string_array(names)
    cur = len(header) + len(names_data)
    pos_data, cur = encode_float64_array(positions, cur)
    vel_data, cur = encode_float64_array(velocities, cur)
    eff_data, _ = encode_float64_array(efforts, cur)

    return _cdr_encapsulate(header + names_data + pos_data + vel_data + eff_data)


def _encode_image(
    t_sec: int, t_nsec: int, width: int, height: int, rgb_data: bytes,
) -> bytes:
    """Encode a sensor_msgs/Image message in CDR."""
    header = _encode_cdr_header(t_sec, t_nsec, "camera_rgb_optical_frame")
    encoding_str = b"rgb8\x00"
    padding = (4 - (len(encoding_str) % 4)) % 4
    step = width * 3
    body = (
        struct.pack("<II", height, width)
        + struct.pack("<I", len(encoding_str))
        + encoding_str
        + b"\x00" * padding
        + struct.pack("<B", 0)  # is_bigendian
        + b"\x00" * 3  # padding to align step
        + struct.pack("<I", step)
        + struct.pack("<I", len(rgb_data))
        + rgb_data
    )
    return _cdr_encapsulate(header + body)


def _encode_laser_scan(
    t_sec: int, t_nsec: int,
    ranges: list[float],
    intensities: list[float],
) -> bytes:
    """Encode a sensor_msgs/LaserScan message in CDR."""
    header = _encode_cdr_header(t_sec, t_nsec, "laser_link")
    n = len(ranges)
    angle_min = -math.pi
    angle_max = math.pi
    angle_inc = (angle_max - angle_min) / n
    body = struct.pack(
        "<fffff",
        angle_min, angle_max, angle_inc,
        0.0,  # time_increment
        0.1,  # scan_time
    )
    body += struct.pack("<ff", 0.1, 30.0)  # range_min, range_max
    body += struct.pack("<I", n) + struct.pack(f"<{n}f", *ranges)
    body += struct.pack("<I", n) + struct.pack(f"<{n}f", *intensities)
    return _cdr_encapsulate(header + body)


def _encode_compressed_image(
    t_sec: int, t_nsec: int, jpeg_data: bytes,
) -> bytes:
    """Encode a sensor_msgs/CompressedImage message in CDR."""
    header = _encode_cdr_header(t_sec, t_nsec, "camera_rgb_optical_frame")
    fmt_str = b"jpeg\x00"
    padding = (4 - (len(fmt_str) % 4)) % 4
    body = (
        struct.pack("<I", len(fmt_str))
        + fmt_str
        + b"\x00" * padding
        + struct.pack("<I", len(jpeg_data))
        + jpeg_data
    )
    return _cdr_encapsulate(header + body)


def _require_pillow():
    """Return ``PIL.Image``, or a one-line ImportError naming the fix.

    No placeholder fallback: Pillow is a base dependency, and the old
    hard-coded 1x1 grayscale JPEG broke consumers that trust the frame
    size (LeRobot export rejected it as a single-channel image).
    """
    try:
        from PIL import Image as PILImage
    except ImportError as e:
        # Only reachable on a partial install (e.g. pip --no-deps).
        raise ImportError(
            "Synthetic camera frames require Pillow, which "
            "rosbag-resurrector depends on but this environment lacks. "
            "Install with: pip install Pillow"
        ) from e
    return PILImage


def _make_test_jpeg(width: int, height: int, r: int, g: int, b: int) -> bytes:
    """Encode a solid-colour ``width`` x ``height`` RGB JPEG."""
    import io

    img = _require_pillow().new("RGB", (width, height), (r, g, b))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=50)
    return buf.getvalue()


@dataclass
class BagConfig:
    """Configuration for generating a synthetic bag."""
    duration_sec: float = 10.0
    imu_hz: float = 200.0
    joint_hz: float = 100.0
    camera_hz: float = 30.0
    lidar_hz: float = 10.0
    image_width: int = 64  # Small for tests
    image_height: int = 48
    num_joints: int = 6
    include_tf: bool = True
    include_compressed: bool = True
    compressed_hz: float = 10.0
    # Unhealthy properties
    drop_messages: bool = False
    drop_topic: str | None = None
    drop_start_sec: float = 3.0
    drop_duration_sec: float = 2.0
    drop_rate: float = 0.7  # Fraction of messages to drop in the drop window
    time_gap: bool = False
    gap_topic: str | None = None
    gap_start_sec: float = 4.0
    gap_duration_sec: float = 1.5
    out_of_order: bool = False
    partial_topic: bool = False
    partial_topic_name: str | None = None
    partial_start_delay_sec: float = 2.0
    partial_end_early_sec: float = 3.0


@contextlib.contextmanager
def _atomic_output(path: Path) -> Iterator[BinaryIO]:
    """Write to a temp file beside ``path``; rename it onto ``path`` on success.

    An MCAP cut off mid-write has no summary, and every later reader
    (including ``resurrector demo``'s "already exists" path) fails on it.
    The rename keeps ``path`` either absent, the previous complete file,
    or the new complete file. The temp name ends in ``.tmp`` so scanners
    looking for ``*.mcap`` never pick it up.
    """
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        with open(tmp, "xb") as f:
            yield f
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def generate_bag(output_path: str | Path, config: BagConfig | None = None) -> Path:
    """Generate a synthetic MCAP bag file.

    The file appears at ``output_path`` only once it is complete. Raises
    ImportError before touching the disk if camera frames are enabled
    and Pillow is missing.
    """
    if config is None:
        config = BagConfig()

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if config.include_compressed:
        _require_pillow()

    rng = np.random.default_rng(42)
    joint_names = [f"joint_{i}" for i in range(config.num_joints)]

    with _atomic_output(output_path) as f:
        writer = Writer(f)
        writer.start(profile="ros2", library=GENERATOR_LIBRARY)

        # Register schemas and channels
        schema_ids = {}
        channel_ids = {}

        for msg_type, schema_info in SCHEMAS.items():
            sid = writer.register_schema(
                name=msg_type,
                encoding=schema_info["encoding"],
                data=schema_info["data"].encode("utf-8"),
            )
            schema_ids[msg_type] = sid

        topics = {
            "/imu/data": "sensor_msgs/msg/Imu",
            "/joint_states": "sensor_msgs/msg/JointState",
            "/camera/rgb": "sensor_msgs/msg/Image",
            "/lidar/scan": "sensor_msgs/msg/LaserScan",
        }
        if config.include_tf:
            topics["/tf"] = "tf2_msgs/msg/TFMessage"
        if config.include_compressed:
            topics["/camera/compressed"] = "sensor_msgs/msg/CompressedImage"

        for topic, msg_type in topics.items():
            cid = writer.register_channel(
                topic=topic,
                message_encoding="cdr",
                schema_id=schema_ids[msg_type],
                metadata={"offered_qos_profiles": json.dumps([{"reliability": "reliable"}])},
            )
            channel_ids[topic] = cid

        # Generate messages sorted by time
        base_time_ns = 1_700_000_000_000_000_000  # ~Nov 2023

        def t_ns(sec_offset: float) -> int:
            return base_time_ns + int(sec_offset * 1e9)

        def t_parts(sec_offset: float) -> tuple[int, int]:
            total_ns = t_ns(sec_offset)
            sec = total_ns // 1_000_000_000
            nsec = total_ns % 1_000_000_000
            return sec, nsec

        def should_drop(sec_offset: float, topic: str) -> bool:
            if not config.drop_messages:
                return False
            if config.drop_topic and config.drop_topic != topic:
                return False
            if config.drop_start_sec <= sec_offset < config.drop_start_sec + config.drop_duration_sec:
                return rng.random() < config.drop_rate
            return False

        def is_in_gap(sec_offset: float, topic: str) -> bool:
            if not config.time_gap:
                return False
            if config.gap_topic and config.gap_topic != topic:
                return False
            return config.gap_start_sec <= sec_offset < config.gap_start_sec + config.gap_duration_sec

        def is_partial_excluded(sec_offset: float, topic: str) -> bool:
            if not config.partial_topic:
                return False
            if config.partial_topic_name and config.partial_topic_name != topic:
                return False
            if sec_offset < config.partial_start_delay_sec:
                return True
            if sec_offset > config.duration_sec - config.partial_end_early_sec:
                return True
            return False

        # Collect all messages with timestamps, then sort and write
        messages: list[tuple[int, str, bytes]] = []  # (timestamp_ns, topic, data)

        # IMU messages
        num_imu = int(config.duration_sec * config.imu_hz)
        for i in range(num_imu):
            t = i / config.imu_hz
            if should_drop(t, "/imu/data") or is_in_gap(t, "/imu/data"):
                continue
            if is_partial_excluded(t, "/imu/data"):
                continue
            sec, nsec = t_parts(t)
            # Simulate gentle sinusoidal motion
            ax = 0.1 * math.sin(2 * math.pi * 0.5 * t) + rng.normal(0, 0.01)
            ay = 0.05 * math.cos(2 * math.pi * 0.3 * t) + rng.normal(0, 0.01)
            az = 9.81 + rng.normal(0, 0.02)
            gx = 0.02 * math.sin(2 * math.pi * 0.2 * t) + rng.normal(0, 0.001)
            gy = 0.01 * math.cos(2 * math.pi * 0.15 * t) + rng.normal(0, 0.001)
            gz = rng.normal(0, 0.001)
            # Simple quaternion (near identity)
            angle = 0.1 * math.sin(2 * math.pi * 0.1 * t)
            qw = math.cos(angle / 2)
            qx = 0.0
            qy = 0.0
            qz = math.sin(angle / 2)

            data = _encode_imu_message(sec, nsec, ax, ay, az, gx, gy, gz, qx, qy, qz, qw)
            messages.append((t_ns(t), "/imu/data", data))

        # Joint state messages
        num_joints_msgs = int(config.duration_sec * config.joint_hz)
        for i in range(num_joints_msgs):
            t = i / config.joint_hz
            if should_drop(t, "/joint_states") or is_in_gap(t, "/joint_states"):
                continue
            if is_partial_excluded(t, "/joint_states"):
                continue
            sec, nsec = t_parts(t)
            positions = [
                math.sin(2 * math.pi * (0.1 + 0.05 * j) * t) * (0.5 + 0.1 * j)
                for j in range(config.num_joints)
            ]
            velocities = [
                2 * math.pi * (0.1 + 0.05 * j) * math.cos(2 * math.pi * (0.1 + 0.05 * j) * t) * (0.5 + 0.1 * j)
                for j in range(config.num_joints)
            ]
            efforts = [rng.normal(5.0, 1.0) for _ in range(config.num_joints)]

            data = _encode_joint_state(sec, nsec, joint_names, positions, velocities, efforts)
            messages.append((t_ns(t), "/joint_states", data))

        # Camera messages (small synthetic images for testing)
        num_camera = int(config.duration_sec * config.camera_hz)
        for i in range(num_camera):
            t = i / config.camera_hz
            if should_drop(t, "/camera/rgb") or is_in_gap(t, "/camera/rgb"):
                continue
            if is_partial_excluded(t, "/camera/rgb"):
                continue
            sec, nsec = t_parts(t)
            # Generate a simple colored frame that changes over time
            r = int(127 + 127 * math.sin(2 * math.pi * t / config.duration_sec))
            g = int(127 + 127 * math.sin(2 * math.pi * t / config.duration_sec + 2.094))
            b = int(127 + 127 * math.sin(2 * math.pi * t / config.duration_sec + 4.189))
            pixel = bytes([r, g, b])
            rgb_data = pixel * (config.image_width * config.image_height)

            data = _encode_image(sec, nsec, config.image_width, config.image_height, rgb_data)
            messages.append((t_ns(t), "/camera/rgb", data))

        # Lidar messages
        num_lidar = int(config.duration_sec * config.lidar_hz)
        for i in range(num_lidar):
            t = i / config.lidar_hz
            if should_drop(t, "/lidar/scan") or is_in_gap(t, "/lidar/scan"):
                continue
            if is_partial_excluded(t, "/lidar/scan"):
                continue
            sec, nsec = t_parts(t)
            n_points = 360
            ranges = [
                float(3.0 + 1.0 * math.sin(math.radians(a) + 0.5 * t) + rng.normal(0, 0.05))
                for a in range(n_points)
            ]
            intensities = [float(rng.uniform(100, 255)) for _ in range(n_points)]

            data = _encode_laser_scan(sec, nsec, ranges, intensities)
            messages.append((t_ns(t), "/lidar/scan", data))

        # Compressed image messages
        if config.include_compressed:
            num_compressed = int(config.duration_sec * config.compressed_hz)
            for i in range(num_compressed):
                t = i / config.compressed_hz
                if should_drop(t, "/camera/compressed") or is_in_gap(t, "/camera/compressed"):
                    continue
                if is_partial_excluded(t, "/camera/compressed"):
                    continue
                sec, nsec = t_parts(t)
                r = int(127 + 127 * math.sin(2 * math.pi * t / config.duration_sec))
                g = int(127 + 127 * math.sin(2 * math.pi * t / config.duration_sec + 2.094))
                b = int(127 + 127 * math.sin(2 * math.pi * t / config.duration_sec + 4.189))
                jpeg_data = _make_test_jpeg(config.image_width, config.image_height, r, g, b)
                data = _encode_compressed_image(sec, nsec, jpeg_data)
                messages.append((t_ns(t), "/camera/compressed", data))

        # Optionally introduce out-of-order timestamps
        if config.out_of_order:
            # Swap some adjacent messages
            for i in range(10, len(messages) - 1, 50):
                messages[i], messages[i + 1] = messages[i + 1], messages[i]
        else:
            # Sort by timestamp
            messages.sort(key=lambda m: m[0])

        # Write all messages
        seq = 0
        for timestamp_ns, topic, data in messages:
            writer.add_message(
                channel_id=channel_ids[topic],
                log_time=timestamp_ns,
                data=data,
                publish_time=timestamp_ns,
                sequence=seq,
            )
            seq += 1

        # Add metadata
        writer.add_metadata(
            "resurrector_test",
            {
                "generator": GENERATOR_LIBRARY,
                "duration_sec": str(config.duration_sec),
                "description": "Synthetic test bag",
            },
        )

        writer.finish()

    return output_path


def stale_sample_reason(path: str | Path) -> str | None:
    """Why an existing sample bag at ``path`` should be regenerated, or None.

    Flags a bag this generator wrote that is empty, cut off mid-write, or
    whose ``/camera/compressed`` frames are the 1x1 grayscale placeholders
    that installs without Pillow used to write (LeRobot export and the
    dashboard's frame views break on those). Reads the MCAP header and the
    first compressed frame only. A file whose header names another writer
    is never flagged, so a user's own bag is never overwritten.
    """
    from mcap.reader import make_reader

    from resurrector.ingest.parser import MCAPParser, get_compressed_image_array

    path = Path(path)
    if path.stat().st_size == 0:
        return "it is empty"
    try:
        with open(path, "rb") as f:
            library = make_reader(f).get_header().library
    except Exception:
        return None
    if library != GENERATOR_LIBRARY:
        return None
    try:
        # Reading starts with the summary at the end of the file, so a
        # bag cut off mid-write fails here.
        msgs = MCAPParser(path).read_messages(topics=["/camera/compressed"])
        with contextlib.closing(msgs):
            msg = next(msgs, None)
    except Exception:
        return "it is incomplete (an earlier run was interrupted)"
    if msg is None:
        return None
    try:
        frame = get_compressed_image_array(msg)
    except ImportError:
        return None  # can't check without Pillow, and couldn't regenerate either
    if frame is None:
        return "its camera frames don't decode"
    if frame.ndim != 3:
        h, w = frame.shape[:2]
        return f"its camera frames are {w}x{h} placeholders from an install without Pillow"
    return None


def generate_test_suite(output_dir: str | Path = "tests/fixtures") -> dict[str, Path]:
    """Generate a complete suite of test bags."""
    output_dir = Path(output_dir)

    bags = {}

    # 1. Healthy bag — clean data, all topics running perfectly
    bags["healthy"] = generate_bag(
        output_dir / "healthy.mcap",
        BagConfig(duration_sec=10.0),
    )

    # 2. Dropped messages — simulates buffer overflow on lidar
    bags["dropped"] = generate_bag(
        output_dir / "dropped_messages.mcap",
        BagConfig(
            duration_sec=10.0,
            drop_messages=True,
            drop_topic="/lidar/scan",
            drop_start_sec=3.0,
            drop_duration_sec=3.0,
            drop_rate=0.7,
        ),
    )

    # 3. Time gap — simulates sensor disconnect on IMU
    bags["gap"] = generate_bag(
        output_dir / "time_gap.mcap",
        BagConfig(
            duration_sec=10.0,
            time_gap=True,
            gap_topic="/imu/data",
            gap_start_sec=4.0,
            gap_duration_sec=1.5,
        ),
    )

    # 4. Out-of-order timestamps
    bags["ooo"] = generate_bag(
        output_dir / "out_of_order.mcap",
        BagConfig(duration_sec=10.0, out_of_order=True),
    )

    # 5. Partial topic — lidar starts late and ends early
    bags["partial"] = generate_bag(
        output_dir / "partial_topic.mcap",
        BagConfig(
            duration_sec=10.0,
            partial_topic=True,
            partial_topic_name="/lidar/scan",
            partial_start_delay_sec=2.0,
            partial_end_early_sec=3.0,
        ),
    )

    # 6. Short bag for quick tests
    bags["short"] = generate_bag(
        output_dir / "short.mcap",
        BagConfig(duration_sec=2.0),
    )

    return bags


if __name__ == "__main__":
    print("Generating test bag suite...")
    bags = generate_test_suite()
    for name, path in bags.items():
        size = path.stat().st_size
        print(f"  {name}: {path} ({size:,} bytes)")
    print("Done!")
