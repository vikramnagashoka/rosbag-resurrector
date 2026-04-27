"""Per-topic message-density histogram for the dashboard timeline ribbon.

   bag.mcap                  ┌────────────────────────────┐
   ────────────              │  /imu/data:    ▆▇█▇▆▅▄▃▂▂  │
   (one scan)  ──┐           │  /joint:       ▂▂▃▄▅▆▇█▇▆  │  ← density
                 ▼           │  /camera/rgb:  █████░░░░░  │    ribbon
        get_density(bag,    │  /lidar:       ▇▇░░░░▇▇▇▇  │
              topics)        └────────────────────────────┘
                 │                       ▲
                 ▼                       │
        per-topic counters               │
        (one increment per message) ─────┘

A ribbon row makes message gaps and rate drops visible at a glance, which
is the motivating use case from the v0.4.0 plan and the rqt_bag pattern
that aged well.

Implementation streams the MCAP once and increments per-topic bin
counters in place — never accumulates timestamp lists. Memory bound is
``O(num_topics * bins)`` (a few KB for the typical 200-bin × 10-topic
case), independent of bag size. Required by the v0.4.0 performance
contract; the v0.3.x version held all timestamps per topic in memory,
which on a 100M-message bag would consume 800+ MB just for the int64s.

Results are cached in the dashboard layer keyed on (bag_id, topic, mtime).
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np


def compute_density(
    bag_path: str | Path,
    topics: Iterable[str] | None = None,
    bins: int = 200,
) -> dict[str, dict]:
    """Read a bag once and return per-topic message-count histograms.

    Args:
        bag_path: Path to the bag file. Auto-converts legacy formats via
            ``parse_bag``.
        topics: Optional allowlist. If None, every topic in the bag is
            included.
        bins: Number of time bins per topic. 200 is a good ribbon
            resolution at typical viewport widths.

    Returns:
        A dict mapping ``topic_name`` to a dict with keys:
            - ``bins``: list[int], length == bins, count of messages per bucket
            - ``start_time_ns``: int, the bag's first-message timestamp
            - ``end_time_ns``: int, the bag's last-message timestamp
            - ``total``: int, total messages across all buckets
            - ``bin_width_ns``: int, time-span of a single bucket

    Topics with zero messages return an entry with ``total = 0`` and
    a zero-filled bins array so the frontend can render an "absent"
    placeholder.

    Memory: O(num_topics * bins). The whole histogram for a typical
    10-topic 200-bin density fits in <100 KB regardless of bag size.
    """
    from resurrector.ingest.parser import parse_bag

    if bins < 1:
        raise ValueError(f"bins must be >= 1, got {bins}")

    parser = parse_bag(bag_path)
    metadata = parser.get_metadata()
    target = set(topics) if topics is not None else {t.name for t in metadata.topics}

    # Compute global window from bag metadata so all topics share the same
    # x-axis. A topic that doesn't span the full bag will have empty
    # buckets at the ends, which is exactly the "drop" signal we want to
    # surface.
    start_ns = metadata.start_time_ns
    end_ns = metadata.end_time_ns
    if end_ns <= start_ns:
        # Empty or single-message bag — return empty histograms.
        return {
            t: {
                "bins": [],
                "start_time_ns": start_ns,
                "end_time_ns": end_ns,
                "total": 0,
                "bin_width_ns": 0,
            }
            for t in target
        }

    duration_ns = end_ns - start_ns
    bin_width_ns = duration_ns / bins  # float; may be fractional

    # Allocate one int64 counter array per topic up front. ~1.6 KB
    # per topic at bins=200 — bounded by num_topics * bins, NOT by
    # message count.
    counters: dict[str, np.ndarray] = {
        t: np.zeros(bins, dtype=np.int64) for t in target
    }

    # Single pass over messages. For each message:
    #   bin_idx = floor((ts - start_ns) / bin_width_ns)
    #   clamp to [0, bins-1]
    #   counters[topic][bin_idx] += 1
    # Memory: bounded by num_topics * bins. Never materializes
    # per-topic timestamp lists.
    last_bin = bins - 1
    for msg in parser.read_messages(topics=list(target)):
        c = counters.get(msg.topic)
        if c is None:
            continue
        offset = msg.timestamp_ns - start_ns
        # Compute and clamp the bin index inline. Avoids np.clip
        # per-message overhead.
        idx = int(offset / bin_width_ns)
        if idx < 0:
            idx = 0
        elif idx > last_bin:
            idx = last_bin
        c[idx] += 1

    result: dict[str, dict] = {}
    for topic in target:
        c = counters[topic]
        result[topic] = {
            "bins": c.tolist(),
            "start_time_ns": start_ns,
            "end_time_ns": end_ns,
            "total": int(c.sum()),
            "bin_width_ns": int(bin_width_ns),
        }
    return result
