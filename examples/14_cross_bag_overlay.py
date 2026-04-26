"""Cross-bag overlay — same topic across multiple runs on one axis.

Demonstrates: ``resurrector/core/cross_bag.py`` :func:`align_bags_by_offset`.

Run:
    python examples/04_cross_bag_overlay.py

What you'll see: two synthetic bags overlaid on a relative time axis,
then the same overlay with a 0.5s offset applied to the second bag —
the kind of fine-tuning the dashboard's per-bag offset slider does.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from _common import ensure_sample_bag, ensure_output_dir, header, section, sparkline

from resurrector.core.cross_bag import align_bags_by_offset


def main() -> None:
    header("04 — Cross-bag overlay")
    out = ensure_output_dir()

    # Make a second bag so we have something to overlay.
    primary = ensure_sample_bag()
    second = out / "second_run.mcap"
    if not second.exists():
        print(f"  Creating a second synthetic bag at {second}...")
        from resurrector.demo.sample_bag import BagConfig, generate_bag
        generate_bag(second, BagConfig(duration_sec=4.0))

    print(f"  Bag A: {primary.name}")
    print(f"  Bag B: {second.name}")
    print(f"  Topic: /imu/data\n")

    section("Default alignment (relative-to-each-bag's-start)")
    df = align_bags_by_offset([primary, second], topic="/imu/data")
    for label in df.get_column("bag_label").unique().to_list():
        sub = df.filter(pl.col("bag_label") == label)
        first = sub.get_column("relative_t_sec").min()
        last = sub.get_column("relative_t_sec").max()
        rows = sub.height
        print(f"  {label:<20}  rows={rows:<5}  t=[{first:.2f}s, {last:.2f}s]")

    accel = df.filter(pl.col("bag_label") == primary.stem).get_column(
        "linear_acceleration.x"
    )
    print(f"\n  bag A linear_acceleration.x: {sparkline(accel.to_list())}")

    section("With 0.5s offset on bag B")
    df_off = align_bags_by_offset(
        [primary, second], topic="/imu/data", offsets_sec=[0.0, 0.5],
    )
    for label in df_off.get_column("bag_label").unique().to_list():
        sub = df_off.filter(pl.col("bag_label") == label)
        first = sub.get_column("relative_t_sec").min()
        last = sub.get_column("relative_t_sec").max()
        print(f"  {label:<20}  t=[{first:.2f}s, {last:.2f}s]")

    section("Long-format dataframe shape")
    print(f"  columns: {df.columns}")
    print(f"  total rows: {df.height}")
    print(
        f"  -> ready for one Plotly trace per bag, grouped by bag_label."
    )

    print(
        "\n  ✓ This is the engine behind the dashboard's /compare-runs page.\n"
        "    The page exposes the same offset slider per bag.\n"
    )


if __name__ == "__main__":
    main()
