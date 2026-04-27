"""Synthetic MCAP bag builders for sync streaming tests.

Each builder produces a tiny bag with a known timing pathology so the
streaming sync engine can be tested for equivalence with the eager
engine on every documented edge case. All bags share the same two
topics so the test harness doesn't have to remember per-fixture
schemas:

    /joint_states  — anchor topic (lower frequency)
    /imu/data      — non-anchor topic (varied per fixture)

The values stored in each message are deterministic functions of
timestamp so the equivalence test can verify the sync engine returns
the exact value the eager engine would have selected.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

from mcap.writer import Writer

from resurrector.demo.sample_bag import (
    SCHEMAS,
    _encode_imu_message,
    _encode_joint_state,
)

BASE_TIME_NS = 1_700_000_000_000_000_000  # ~Nov 2023, same as sample_bag


def _t_parts(timestamp_ns: int) -> tuple[int, int]:
    sec = timestamp_ns // 1_000_000_000
    nsec = timestamp_ns % 1_000_000_000
    return sec, nsec


def _imu_value_at(timestamp_ns: int) -> float:
    """Deterministic IMU value-of-x for this timestamp.

    Used so the test can assert `synced["/imu/data__linear_acceleration.x"]`
    matches the value at the picked timestamp without hard-coding numbers.
    """
    # offset-from-base in seconds, scaled and shifted so the value is
    # distinctive across small intervals
    sec_offset = (timestamp_ns - BASE_TIME_NS) / 1e9
    return 100.0 + sec_offset


def _joint_value_at(timestamp_ns: int) -> float:
    """Deterministic joint-position value."""
    sec_offset = (timestamp_ns - BASE_TIME_NS) / 1e9
    return 200.0 + sec_offset * 2.0


def _encode_imu_at(timestamp_ns: int, value: float | None = None) -> bytes:
    """Encode an IMU message whose linear_acceleration.x is `value`."""
    sec, nsec = _t_parts(timestamp_ns)
    v = value if value is not None else _imu_value_at(timestamp_ns)
    return _encode_imu_message(
        sec, nsec,
        ax=v, ay=0.0, az=9.81,
        gx=0.0, gy=0.0, gz=0.0,
        qx=0.0, qy=0.0, qz=0.0, qw=1.0,
    )


def _encode_joint_at(timestamp_ns: int, value: float | None = None) -> bytes:
    """Encode a JointState message whose position[0] is `value`."""
    sec, nsec = _t_parts(timestamp_ns)
    v = value if value is not None else _joint_value_at(timestamp_ns)
    return _encode_joint_state(
        sec, nsec,
        names=["joint_0"],
        positions=[v],
        velocities=[0.0],
        efforts=[0.0],
    )


def write_sync_fixture(
    output_path: Path,
    imu_timestamps_ns: list[int],
    joint_timestamps_ns: list[int],
) -> Path:
    """Write a minimal MCAP with the requested timestamps on each topic.

    Messages on each topic are written in the order of the input lists
    — so the caller controls whether a topic is sorted, has duplicates,
    or contains out-of-order regressions.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="rosbag-resurrector-syncfixture")

        # Register schemas for the two topics we use.
        imu_schema = SCHEMAS["sensor_msgs/msg/Imu"]
        joint_schema = SCHEMAS["sensor_msgs/msg/JointState"]
        imu_sid = writer.register_schema(
            name="sensor_msgs/msg/Imu",
            encoding=imu_schema["encoding"],
            data=imu_schema["data"].encode("utf-8"),
        )
        joint_sid = writer.register_schema(
            name="sensor_msgs/msg/JointState",
            encoding=joint_schema["encoding"],
            data=joint_schema["data"].encode("utf-8"),
        )

        imu_cid = writer.register_channel(
            topic="/imu/data",
            message_encoding="cdr",
            schema_id=imu_sid,
        )
        joint_cid = writer.register_channel(
            topic="/joint_states",
            message_encoding="cdr",
            schema_id=joint_sid,
        )

        # The MCAP writer accepts messages out of log_time order, but
        # sorts them in the chunk index. We exploit this to write
        # out-of-order timestamps for the regression-test fixture:
        # log_time tracks publication order, publish_time is the
        # actual timestamp.
        for t_ns in imu_timestamps_ns:
            writer.add_message(
                channel_id=imu_cid,
                log_time=t_ns,
                data=_encode_imu_at(t_ns),
                publish_time=t_ns,
            )
        for t_ns in joint_timestamps_ns:
            writer.add_message(
                channel_id=joint_cid,
                log_time=t_ns,
                data=_encode_joint_at(t_ns),
                publish_time=t_ns,
            )

        writer.finish()

    return output_path


# ---------------------------------------------------------------------------
# Fixture builders — one per documented edge case.
# Each returns the bag path AND the timestamp lists so tests can assert
# against the source-of-truth without re-reading the bag.
# ---------------------------------------------------------------------------


@dataclass
class SyncFixture:
    path: Path
    imu_timestamps_ns: list[int]
    joint_timestamps_ns: list[int]
    description: str


def fast_vs_slow(out_dir: Path) -> SyncFixture:
    """/imu at 1000 Hz, /joint_states (anchor) at 10 Hz, 1 second total."""
    imu = [BASE_TIME_NS + int(i * 1e6) for i in range(1000)]   # every 1 ms
    joint = [BASE_TIME_NS + int(i * 1e8) for i in range(10)]   # every 100 ms
    path = write_sync_fixture(out_dir / "fast_vs_slow.mcap", imu, joint)
    return SyncFixture(path, imu, joint, "fast vs slow")


def tie_at_anchor(out_dir: Path) -> SyncFixture:
    """Anchor sample at exact midpoint between two IMU samples."""
    # Anchor at t=10ms; IMU samples at t=5ms and t=15ms (equidistant).
    imu = [BASE_TIME_NS + 5_000_000, BASE_TIME_NS + 15_000_000]
    joint = [BASE_TIME_NS + 10_000_000]
    path = write_sync_fixture(out_dir / "tie_at_anchor.mcap", imu, joint)
    return SyncFixture(path, imu, joint, "tie at anchor")


def missing_before_first(out_dir: Path) -> SyncFixture:
    """IMU first sample arrives 200 ms after first anchor."""
    joint = [BASE_TIME_NS + int(i * 1e8) for i in range(5)]  # 0, 100, 200, 300, 400 ms
    imu = [BASE_TIME_NS + int((200 + i * 100) * 1e6) for i in range(3)]  # 200, 300, 400 ms
    path = write_sync_fixture(out_dir / "missing_before_first.mcap", imu, joint)
    return SyncFixture(path, imu, joint, "missing before first")


def missing_after_last(out_dir: Path) -> SyncFixture:
    """IMU stops 200 ms before last anchor."""
    joint = [BASE_TIME_NS + int(i * 1e8) for i in range(5)]  # 0, 100, 200, 300, 400 ms
    imu = [BASE_TIME_NS + int(i * 1e8) for i in range(3)]    # 0, 100, 200 ms
    path = write_sync_fixture(out_dir / "missing_after_last.mcap", imu, joint)
    return SyncFixture(path, imu, joint, "missing after last")


def out_of_order_within_topic(out_dir: Path) -> SyncFixture:
    """IMU has 2 timestamps regressed by 5 ms (in log order)."""
    joint = [BASE_TIME_NS + int(i * 1e8) for i in range(5)]
    # IMU log order: 0, 10, 20, 15 (regressed), 30, 25 (regressed), 40 ms
    imu_log_order_ms = [0, 10, 20, 15, 30, 25, 40]
    imu = [BASE_TIME_NS + int(t * 1e6) for t in imu_log_order_ms]
    path = write_sync_fixture(out_dir / "out_of_order_within_topic.mcap", imu, joint)
    return SyncFixture(path, imu, joint, "out of order within topic")


def bursty_fast(out_dir: Path) -> SyncFixture:
    """IMU produces 10K samples in a 100 ms burst centered on anchor.

    Anchor at t=250ms, burst from 200-300ms. With tolerance >= 100ms,
    every burst sample falls in the lookahead window [150, 350ms],
    which is the scenario the bounded buffer is designed to detect.
    """
    joint = [BASE_TIME_NS + int(250 * 1e6)]  # one anchor at 250 ms
    burst_start_ns = BASE_TIME_NS + int(200 * 1e6)
    burst_end_ns = BASE_TIME_NS + int(300 * 1e6)
    n = 10_000
    imu = [
        burst_start_ns + int(i * (burst_end_ns - burst_start_ns) / n)
        for i in range(n)
    ]
    path = write_sync_fixture(out_dir / "bursty_fast.mcap", imu, joint)
    return SyncFixture(path, imu, joint, "bursty fast")


def sparse_no_match(out_dir: Path) -> SyncFixture:
    """IMU has gaps wider than tolerance (no IMU sample within ±50 ms of
    every anchor)."""
    # Anchor at 0, 100, 200, 300, 400 ms. IMU only at 0 and 400 ms (gap of
    # 400 ms in the middle, way beyond a 50 ms tolerance).
    joint = [BASE_TIME_NS + int(i * 1e8) for i in range(5)]
    imu = [BASE_TIME_NS, BASE_TIME_NS + int(400 * 1e6)]
    path = write_sync_fixture(out_dir / "sparse_no_match.mcap", imu, joint)
    return SyncFixture(path, imu, joint, "sparse no match")


def duplicate_timestamps(out_dir: Path) -> SyncFixture:
    """Two consecutive IMU samples at identical t."""
    joint = [BASE_TIME_NS + int(i * 1e8) for i in range(3)]
    # Two samples at t=50ms (duplicate), then one at 150 ms.
    imu = [
        BASE_TIME_NS + int(50 * 1e6),
        BASE_TIME_NS + int(50 * 1e6),
        BASE_TIME_NS + int(150 * 1e6),
    ]
    path = write_sync_fixture(out_dir / "duplicate_timestamps.mcap", imu, joint)
    return SyncFixture(path, imu, joint, "duplicate timestamps")


def topic_stops_halfway(out_dir: Path) -> SyncFixture:
    """IMU ends at t=2.5s, anchor continues to t=5s."""
    joint = [BASE_TIME_NS + int(i * 0.5e9) for i in range(11)]  # 0 .. 5s, every 500 ms
    imu = [BASE_TIME_NS + int(i * 0.1e9) for i in range(26)]    # 0 .. 2.5s, every 100 ms
    path = write_sync_fixture(out_dir / "topic_stops_halfway.mcap", imu, joint)
    return SyncFixture(path, imu, joint, "topic stops halfway")


# Catalog used by the equivalence test parametrization.
ALL_FIXTURE_BUILDERS = [
    fast_vs_slow,
    tie_at_anchor,
    missing_before_first,
    missing_after_last,
    out_of_order_within_topic,
    bursty_fast,
    sparse_no_match,
    duplicate_timestamps,
    topic_stops_halfway,
]
