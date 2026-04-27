"""Cross-bag overlay alignment.

Given multiple bag paths and one topic name, returns a long-format
DataFrame with a ``bag_label`` column so a single Plotly call can color
one trace per bag.

   bag_a.mcap (start=t_a)        bag_b.mcap (start=t_b)
       │                                │
       ▼                                ▼
   read /imu/data                   read /imu/data
   relative_t = t - t_a             relative_t = t - t_b
   apply offset_a                   apply offset_b
       │                                │
       └──────────┬─────────────────────┘
                  ▼
       pl.concat(how="vertical")
                  ▼
       columns: bag_label, relative_t_sec, value_columns…

Default alignment is "relative-to-start" so two runs of the same task
(different absolute times) overlay sensibly. Per-bag offsets fine-tune
e.g. when one run was started a beat earlier than the other.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from resurrector.core.bag_frame import BagFrame
from resurrector.core.streaming import stream_bucketed_minmax


def align_bags_by_offset(
    bag_paths: list[str | Path],
    topic: str,
    offsets_sec: list[float] | None = None,
    labels: list[str] | None = None,
    max_points_per_bag: int = 2000,
) -> pl.DataFrame:
    """Read the same topic from multiple bags, overlay on a common time axis.

    Args:
        bag_paths: List of bag files. All must contain ``topic``.
        topic: The shared topic to overlay.
        offsets_sec: Per-bag time offsets applied AFTER the
            relative-to-start normalization. Defaults to all zeros.
            ``offsets_sec[i] > 0`` shifts ``bag_paths[i]`` to the right.
        labels: Per-bag display labels. Defaults to the bag's filename.
        max_points_per_bag: Each bag's series is bucket-downsampled to
            at most ``2 * (max_points_per_bag // 2)`` points before
            concatenation, so a 100-message bag and a 1M-message bag
            both render smoothly. Memory bounded per bag — never
            materializes the full topic.

    Returns:
        Long-format DataFrame with columns:
            - ``bag_label`` (str): for Plotly's color/group axis
            - ``relative_t_sec`` (float): seconds from the first bag's
              first message, with offsets applied
            - all numeric columns from the topic, flattened by dot
              notation (whatever the BagFrame produces)

    Raises:
        ValueError: if ``bag_paths`` is empty, lengths don't match, or
            a bag is missing the topic.
    """
    if not bag_paths:
        raise ValueError("bag_paths must contain at least one path")
    n = len(bag_paths)
    offsets_sec = offsets_sec if offsets_sec is not None else [0.0] * n
    if len(offsets_sec) != n:
        raise ValueError(
            f"offsets_sec length {len(offsets_sec)} != bag_paths length {n}"
        )
    if labels is not None and len(labels) != n:
        raise ValueError(
            f"labels length {len(labels)} != bag_paths length {n}"
        )

    num_buckets = max(1, max_points_per_bag // 2)

    pieces: list[pl.DataFrame] = []
    for i, raw_path in enumerate(bag_paths):
        path = Path(raw_path)
        bf = BagFrame(path)
        try:
            view = bf[topic]
        except KeyError:
            raise ValueError(
                f"Bag {path.name!r} does not contain topic {topic!r}"
            )

        # Stream-aggregate this bag's topic to bucketed min/max points.
        # Time range is the bag's full bounds — we want the visual
        # envelope across the whole recording, not just whatever
        # window happens to be active.
        bag_start = int(bf.metadata.start_time_ns or 0)
        bag_end = int(bf.metadata.end_time_ns or bag_start + 1)
        df = stream_bucketed_minmax(
            view.iter_chunks(),
            num_buckets=num_buckets,
            time_range=(bag_start, bag_end),
        )
        if df.height == 0:
            continue

        # Normalize the (already-aggregated) timestamps to
        # relative-to-this-bag's-first-message, then apply user offset.
        first_ns = int(df.get_column("timestamp_ns").min())
        offset_ns = int(offsets_sec[i] * 1e9)
        df = df.with_columns(
            (
                (pl.col("timestamp_ns") - first_ns + offset_ns)
                / 1e9
            ).alias("relative_t_sec")
        )
        label = labels[i] if labels else path.stem
        df = df.with_columns(pl.lit(label).alias("bag_label"))
        pieces.append(df)

    if not pieces:
        return pl.DataFrame({
            "bag_label": [], "relative_t_sec": [], "timestamp_ns": [],
        })

    # diagonal_relaxed handles the case where one bag has additional
    # nested columns the other doesn't — fills nulls instead of erroring.
    return pl.concat(pieces, how="diagonal_relaxed")
