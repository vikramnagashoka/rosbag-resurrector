"""Multi-stream synchronization — align topics with different rates.

Demonstrates: ``BagFrame.sync()`` with the three methods.

Run:
    python examples/03_multi_stream_sync.py

What you'll see: IMU (200Hz) and joint_states (100Hz) aligned three
different ways. The classic bag-tooling problem: topics publish at
independent rates and you need them on a shared timeline before any
ML pipeline can use them.
"""

from __future__ import annotations

from _common import ensure_sample_bag, header, section

from resurrector import BagFrame


def main() -> None:
    header("03 — Multi-stream synchronization")
    bag_path = ensure_sample_bag()
    bf = BagFrame(bag_path)

    topics = ["/imu/data", "/joint_states"]
    print(f"  Source rates:")
    for t in topics:
        info = next(x for x in bf.topics if x.name == t)
        print(f"    {t:<20} {info.message_count:>5} msgs · {info.frequency_hz:.1f}Hz")

    section("Method 1 — nearest (timestamp matching with tolerance)")
    nearest = bf.sync(topics, method="nearest", tolerance_ms=10)
    print(f"  Output: {nearest.height} rows, {len(nearest.columns)} columns")
    print(f"  First 3 rows:")
    print(nearest.head(3))

    section("Method 2 — interpolate (linear interp for numeric streams)")
    interp = bf.sync(topics, method="interpolate")
    print(f"  Output: {interp.height} rows, {len(interp.columns)} columns")
    print(f"  Same shape as nearest, but values are smoothed across timestamps.")

    section("Method 3 — sample_and_hold (carry forward last value)")
    sah = bf.sync(topics, method="sample_and_hold")
    print(f"  Output: {sah.height} rows, {len(sah.columns)} columns")
    print(f"  Useful when one topic is much slower than the other (e.g.\n"
          f"  config or state messages); the slow value 'sticks' between\n"
          f"  emissions instead of being NaN-padded.")

    section("Picking a method")
    print(
        "  • nearest      — events / discrete topics, tolerance_ms controls match\n"
        "  • interpolate  — numeric continuous streams (IMU, joints, odom)\n"
        "  • sample_and_hold — slow + fast pairs (camera + state, config + signal)\n"
    )

    print(
        "\n  ✓ The sync result is a single Polars DataFrame with one row per\n"
        "    aligned timestamp and one column per (topic, field) pair.\n"
        "    Drop into any ML pipeline as-is.\n"
    )


if __name__ == "__main__":
    main()
