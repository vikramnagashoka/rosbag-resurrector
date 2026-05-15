"""N-bag concatenated stream API (v0.6.0 — Bundle B.2).

Demonstrates ``resurrector.concatenate_bags()`` — treat a list of bags
as a single time-aligned stream. Promised in the ROS Discourse reply
to AnthonyCvn ("if that turns out to be a common pattern I'll promote
it to first-class") and now made good on.

Run:
    python examples/25_concatenate_bags.py

What you'll see:
  1. Three small synth bags generated to ``_exploration_output/``
  2. ``concatenate_bags(mode='time')`` sorts them by start time
  3. ``concatenate_bags(mode='index')`` preserves the user-supplied order
  4. Cross-bag ``to_polars()`` produces one DataFrame spanning every bag
  5. A topic missing from one bag is silently handled — only present-bags
     contribute to iteration
"""

from __future__ import annotations

from pathlib import Path

from _common import ensure_output_dir, header, section

import resurrector
from resurrector.demo.sample_bag import generate_bag, BagConfig


def main() -> None:
    header("25 — v0.6.0: N-bag concatenated stream API")
    out = ensure_output_dir()

    section("Generate 3 synth bags with different durations")
    bag_paths = []
    for i, dur in enumerate([0.5, 0.3, 0.4]):
        p = out / f"v06_concat_bag{i}.mcap"
        if p.exists():
            p.unlink()
        generate_bag(p, BagConfig(duration_sec=dur, imu_hz=100.0))
        bag_paths.append(p)
        print(f"    bag{i}: {dur:.1f}s @ 100Hz IMU → {p.name}")

    section("Time mode (default) — bags ordered by start_time_ns")
    bf = resurrector.concatenate_bags(bag_paths, mode="time")
    print(f"  ConcatenatedBagFrame: {bf.n_bags} bags · {len(bf.topics)} topics")
    print(f"  Order: {[p.name for p in bf.bag_paths]}")
    print(f"  Total duration (sum of underlying bags): {bf.total_duration_sec:.2f}s")

    section("Index mode — preserves user-supplied order")
    bf_idx = resurrector.concatenate_bags(bag_paths[::-1], mode="index")
    print(f"  Order: {[p.name for p in bf_idx.bag_paths]}")

    section("Topics view — union across all bags")
    print(f"  topics: {bf.topics}")

    section("Cross-bag DataFrame — one .to_polars() spans every bag")
    view = bf["/imu/data"]
    print(f"  Topic '/imu/data' present in {view.n_bags_with_topic} of {bf.n_bags} bags")
    df = view.to_polars()
    # 0.5 + 0.3 + 0.4 = 1.2s × 100Hz = 120 messages
    print(f"  DataFrame shape: {df.shape}")
    print(f"  First / last timestamps:")
    print(f"    {df['timestamp_ns'][0]}")
    print(f"    {df['timestamp_ns'][-1]}")

    section("Memory-bounded — iter_chunks() yields per-bag chunks")
    n = 0
    for chunk in view.iter_chunks(chunk_size=50):
        n += 1
        print(f"  chunk {n}: {chunk.shape}")
    print(f"  Total chunks: {n}")

    section("Topic missing from some bags — silently handled")
    # Generate a bag without TF, mix with one that has TF
    no_tf = out / "v06_concat_no_tf.mcap"
    if no_tf.exists():
        no_tf.unlink()
    generate_bag(no_tf, BagConfig(duration_sec=0.2, include_tf=False))
    bf_partial = resurrector.concatenate_bags([bag_paths[0], no_tf], mode="index")
    tf_view = bf_partial["/tf"]
    print(f"  /tf is present in {tf_view.n_bags_with_topic} of {bf_partial.n_bags} bags")
    print(f"  Iteration silently skips bags without the topic; no error.")

    section("Aggregate topic_info across the fleet")
    for ti in bf.topic_info:
        if ti.message_count > 0:
            print(f"    {ti.name:<30} {ti.message_type:<35} msgs={ti.message_count:,}")

    print(
        "\n  ✓ N-bag stream behaves like a single BagFrame for the common\n"
        "    iter_chunks / to_polars / topics access patterns.\n"
        "  ✓ Memory bounded: composes per-bag iter_chunks rather than eager.\n"
        "  ✓ time mode sorts by start_time_ns; index mode preserves caller order.\n"
        "  ✓ Use the bag-side QC tool (next example) to detect schema drift\n"
        "    or rate anomalies across bags before relying on concatenation.\n"
    )


if __name__ == "__main__":
    main()
