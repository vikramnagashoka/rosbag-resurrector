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
from resurrector.core.downsample import downsample_dataframe


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
        max_points_per_bag: Each bag's series is LTTB-downsampled to at
            most this many points before concatenation, so a 100-message
            bag and a 1M-message bag still both render smoothly.

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
        df = view.to_polars()
        if df.height == 0:
            continue
        # Normalize timestamps to relative-to-this-bag's-first-message
        # then apply user offset.
        first_ns = int(df.get_column("timestamp_ns").min())
        offset_ns = int(offsets_sec[i] * 1e9)
        df = df.with_columns(
            (
                (pl.col("timestamp_ns") - first_ns + offset_ns)
                / 1e9
            ).alias("relative_t_sec")
        )
        # Downsample for plot performance — each bag capped independently.
        if df.height > max_points_per_bag:
            # Downsample driven by relative_t_sec so the cross-bag axis
            # is the LTTB anchor rather than the raw timestamp.
            df_ds = _downsample_on(df, max_points_per_bag)
            df = df_ds
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


def _downsample_on(df: pl.DataFrame, max_points: int) -> pl.DataFrame:
    """LTTB-downsample using relative_t_sec as the time axis.

    The shared LTTB helper assumes the time column is ``timestamp_ns``,
    so we temporarily swap and then restore.
    """
    saved = df.get_column("timestamp_ns")
    df = df.drop("timestamp_ns").rename({"relative_t_sec": "timestamp_ns"})
    df = downsample_dataframe(df, max_points=max_points, time_col="timestamp_ns")
    df = df.rename({"timestamp_ns": "relative_t_sec"})
    # Restore timestamp_ns column at the corresponding selected indices —
    # but downsample_dataframe doesn't expose indices, and we can't slice
    # the original column to match. So just drop timestamp_ns from the
    # output; downstream consumers care about relative_t_sec, not raw ns.
    return df
