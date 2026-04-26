"""Lazy Polars on a topic — filter pushdown without OOM.

Demonstrates: ``BagFrame[topic].materialize_ipc_cache()`` (v0.4.0
explicit-lifecycle replacement for the old ``to_lazy_polars()``) plus
how it composes with the v0.3.1 transforms.

Run:
    python examples/18_polars_lazy_filter.py

What you'll see: peak memory footprint of a lazy filter+collect on the
demo IMU topic, compared to materializing the full DataFrame eagerly.
"""

from __future__ import annotations

import os
import time

import polars as pl

from _common import ensure_sample_bag, header, section

from resurrector.core.bag_frame import BagFrame


def proc_rss_mb() -> float:
    """Best-effort current-process resident set size in MB."""
    try:
        import psutil
        return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    except ImportError:
        # Without psutil we can't measure peak; return 0 and just print
        # timings instead.
        return 0.0


def main() -> None:
    header("08 — Lazy Polars filter pushdown")
    bag_path = ensure_sample_bag()

    bf = BagFrame(bag_path)
    topic = "/imu/data"
    print(f"  Topic: {topic}\n")

    section("Eager: load entire topic into memory, then filter")
    rss_before = proc_rss_mb()
    t0 = time.perf_counter()
    eager = bf[topic].to_polars()
    eager_filtered = eager.filter(pl.col("linear_acceleration.x").abs() > 0.1).head(10)
    t1 = time.perf_counter()
    rss_after = proc_rss_mb()
    print(f"  rows materialized: {eager.height}")
    print(f"  filtered rows:     {eager_filtered.height}")
    print(f"  time:              {(t1 - t0) * 1000:.1f} ms")
    if rss_before > 0:
        print(f"  RSS delta:         {rss_after - rss_before:.1f} MB")

    section("Lazy: scan-IPC backed, filter pushed down")
    # Fresh BagFrame so the cache from the eager call isn't reused.
    bf2 = BagFrame(bag_path)
    rss_before = proc_rss_mb()
    t0 = time.perf_counter()
    with bf2[topic].materialize_ipc_cache() as cache:
        lazy_filtered = (
            cache.scan()
            .filter(pl.col("linear_acceleration.x").abs() > 0.1)
            .head(10)
            .collect()
        )
    t1 = time.perf_counter()
    rss_after = proc_rss_mb()
    print(f"  filtered rows:     {lazy_filtered.height}")
    print(f"  time:              {(t1 - t0) * 1000:.1f} ms")
    if rss_before > 0:
        print(f"  RSS delta:         {rss_after - rss_before:.1f} MB")

    section("Lazy filter columns selected for free")
    bf3 = BagFrame(bag_path)
    with bf3[topic].materialize_ipc_cache() as cache:
        proj = (
            cache.scan()
            .select([pl.col("timestamp_ns"), pl.col("linear_acceleration.x")])
            .head(5)
            .collect()
        )
    print(f"  projection sample (5 rows, 2 cols):")
    print(f"\n{proj}\n")

    print(
        "  ✓ Lazy mode is the path the dashboard uses for the topic-data\n"
        "    endpoint when ?max_points is set: scan, filter on time range,\n"
        "    LTTB-downsample, return ~2k points to the chart.\n"
    )


if __name__ == "__main__":
    main()
