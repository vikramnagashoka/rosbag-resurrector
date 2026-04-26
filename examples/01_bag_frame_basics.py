"""BagFrame basics — the pandas-like API for any rosbag.

Demonstrates: ``BagFrame`` (resurrector/core/bag_frame.py).

Run:
    python examples/01_bag_frame_basics.py

What you'll see: open a bag, list its topics, slice by time, convert
to Polars/Pandas, iterate raw messages. The "first 5 minutes" of using
the tool from Python.
"""

from __future__ import annotations

from _common import ensure_sample_bag, header, section

from resurrector import BagFrame


def main() -> None:
    header("01 — BagFrame basics")
    bag_path = ensure_sample_bag()

    section("Open and inspect")
    bf = BagFrame(bag_path)
    bf.info()  # prints a rich table; same output as `resurrector info`

    section("List topics")
    for t in bf.topics:
        freq = f"{t.frequency_hz:.1f}Hz" if t.frequency_hz else "?Hz"
        print(f"  {t.name:<30} {t.message_type:<35} {t.message_count:>6,} msgs  {freq}")

    section("Topic data as Polars DataFrame")
    df = bf["/imu/data"].to_polars()
    print(f"  shape: {df.shape}")
    print(f"  columns: {df.columns}")
    print(df.head(3))

    section("Same data as Pandas (for sklearn / matplotlib pipelines)")
    pdf = bf["/imu/data"].to_pandas()
    print(f"  type: {type(pdf).__name__}, shape: {pdf.shape}")

    section("Time slice — only [1.0s, 3.0s] of the bag")
    sliced = bf.time_slice("1s", "3s")
    sliced_imu = sliced["/imu/data"].to_polars()
    print(f"  full IMU rows: {df.height}")
    print(f"  sliced IMU rows: {sliced_imu.height}")

    section("Iterate raw messages (memory-friendly for large bags)")
    count = 0
    for msg in bf["/joint_states"].iter_messages():
        count += 1
        if count <= 2:
            print(f"  msg #{count}: t={msg.timestamp_ns}, "
                  f"name[0]={msg.data.get('name', ['?'])[0]!r}")
        if count >= 5:
            break
    print(f"  ... ({count} messages iterated; stops on demand)")

    print(
        "\n  ✓ This is the foundational API. Every other feature in the\n"
        "    toolkit (sync, export, dashboard, bridge) builds on BagFrame.\n"
    )


if __name__ == "__main__":
    main()
