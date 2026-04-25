"""Per-topic message-density histogram for the dashboard timeline ribbon.

   bag.mcap                  ┌────────────────────────────┐
   ────────────              │  /imu/data:    ▆▇█▇▆▅▄▃▂▂  │
   (one scan)  ──┐           │  /joint:       ▂▂▃▄▅▆▇█▇▆  │  ← density
                 ▼           │  /camera/rgb:  █████░░░░░  │    ribbon
        get_density(bag,    │  /lidar:       ▇▇░░░░▇▇▇▇  │
              topics)        └────────────────────────────┘
                 │                       ▲
                 ▼                       │
        per-topic timestamps             │
        bucketed into N bins ────────────┘

A ribbon row makes message gaps and rate drops visible at a glance, which
is the motivating use case from the v0.4.0 plan and the rqt_bag pattern
that aged well. Implementation reads the MCAP once and bucketizes
timestamps in-memory; results are cached in the dashboard layer keyed on
(bag_id, topic, mtime).
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

    Topics with zero messages return an entry with ``total = 0`` and an
    empty bins array so the frontend can render an "absent" placeholder.
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

    # numpy float64 has 52-bit mantissa; ROS2 nanosecond timestamps fit
    # but we shift to start-relative to keep precision when bag durations
    # exceed a few hours.
    duration_ns = end_ns - start_ns
    bin_edges = np.linspace(0.0, float(duration_ns), bins + 1)
    bin_width_ns = duration_ns / bins

    per_topic_relative_ts: dict[str, list[float]] = {t: [] for t in target}

    for msg in parser.read_messages(topics=list(target)):
        if msg.topic not in per_topic_relative_ts:
            continue
        per_topic_relative_ts[msg.topic].append(float(msg.timestamp_ns - start_ns))

    result: dict[str, dict] = {}
    for topic in target:
        ts = per_topic_relative_ts.get(topic, [])
        if not ts:
            result[topic] = {
                "bins": [0] * bins,
                "start_time_ns": start_ns,
                "end_time_ns": end_ns,
                "total": 0,
                "bin_width_ns": int(bin_width_ns),
            }
            continue
        counts, _ = np.histogram(ts, bins=bin_edges)
        result[topic] = {
            "bins": counts.tolist(),
            "start_time_ns": start_ns,
            "end_time_ns": end_ns,
            "total": int(counts.sum()),
            "bin_width_ns": int(bin_width_ns),
        }
    return result
