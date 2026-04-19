"""
Generate synthetic MCAP files with realistic robotics data for testing.

Creates bags with:
- IMU data at 200Hz (accelerometer + gyroscope + orientation quaternion)
- Joint states at 100Hz (6-DOF robot arm: positions, velocities, efforts)
- Camera images at 30Hz (640x480 RGB, synthetic colored frames)
- Lidar scans at 10Hz (2D laser scan, 360 points per scan)
- TF transforms (base_link -> arm_link chain)

Also generates "unhealthy" bags with:
- Dropped messages (simulating buffer overflow)
- Time gaps (simulating sensor disconnects)
- Out-of-order timestamps
- Partial topic recordings
"""

from __future__ import annotations

import json
import math
import struct
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from mcap.writer import Writer

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


def _make_test_jpeg(width: int, height: int, r: int, g: int, b: int) -> bytes:
    """Create a minimal JPEG image for testing. Requires Pillow."""
    try:
        from PIL import Image as PILImage
        import io
        img = PILImage.new("RGB", (width, height), (r, g, b))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=50)
        return buf.getvalue()
    except ImportError:
        # Fallback: return a minimal valid JPEG (1x1 red pixel)
        return (
            b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01'
            b'\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06'
            b'\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x14\r\x0c\x0b\x0b'
            b'\x0c\x19\x12\x13\x0f\x14\x1d\x1a\x1f\x1e\x1d\x1a\x1c'
            b'\x1c $.\' ",#\x1c\x1c(7),01444\x1f\'9=82<.342\xff\xc0'
            b'\x00\x0b\x08\x00\x01\x00\x01\x01\x01\x11\x00\xff\xc4'
            b'\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00'
            b'\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06'
            b'\x07\x08\t\n\x0b\xff\xc4\x00\xb5\x10\x00\x02\x01\x03'
            b'\x03\x02\x04\x03\x05\x05\x04\x04\x00\x00\x01}\x01\x02'
            b'\x03\x00\x04\x11\x05\x12!1A\x06\x13Qa\x07"q\x142\x81'
            b'\x91\xa1\x08#B\xb1\xc1\x15R\xd1\xf0$3br\x82\t\n\x16'
            b'\x17\x18\x19\x1a%&\'()*456789:CDEFGHIJSTUVWXYZcdefghij'
            b'stuvwxyz\x83\x84\x85\x86\x87\x88\x89\x8a\x92\x93\x94'
            b'\x95\x96\x97\x98\x99\x9a\xa2\xa3\xa4\xa5\xa6\xa7\xa8'
            b'\xa9\xaa\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xc2\xc3'
            b'\xc4\xc5\xc6\xc7\xc8\xc9\xca\xd2\xd3\xd4\xd5\xd6\xd7'
            b'\xd8\xd9\xda\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea'
            b'\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xff\xda\x00'
            b'\x08\x01\x01\x00\x00?\x00T\xdb\xa8\xa1 \x03\xff\xd9'
        )


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


def generate_bag(output_path: str | Path, config: BagConfig | None = None) -> Path:
    """Generate a synthetic MCAP bag file."""
    if config is None:
        config = BagConfig()

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(42)
    joint_names = [f"joint_{i}" for i in range(config.num_joints)]

    with open(output_path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="rosbag-resurrector-testgen")

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
                "generator": "rosbag-resurrector-testgen",
                "duration_sec": str(config.duration_sec),
                "description": "Synthetic test bag",
            },
        )

        writer.finish()

    return output_path


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
