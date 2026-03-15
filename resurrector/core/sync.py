"""Multi-stream temporal synchronization.

Solves the core robotics problem of topics publishing at independent rates.
Supports multiple sync strategies:
- nearest: Match to nearest timestamp within tolerance
- interpolate: Linear interpolation for numeric streams
- sample_and_hold: Use last known value for slow topics
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import polars as pl

if TYPE_CHECKING:
    from resurrector.core.bag_frame import TopicView


def synchronize(
    topic_views: dict[str, "TopicView"],
    method: str = "nearest",
    tolerance_ms: float = 50.0,
    anchor: str | None = None,
) -> pl.DataFrame:
    """Synchronize multiple topics by timestamp.

    Args:
        topic_views: Dict mapping topic name -> TopicView.
        method: Sync method — "nearest", "interpolate", or "sample_and_hold".
        tolerance_ms: Maximum time difference for nearest matching (ms).
        anchor: Topic to use as time reference. Defaults to highest-frequency topic.

    Returns:
        Unified Polars DataFrame with columns prefixed by topic name.
    """
    if not topic_views:
        return pl.DataFrame()

    # Load DataFrames for each topic
    dfs: dict[str, pl.DataFrame] = {}
    for name, view in topic_views.items():
        df = view.to_polars()
        if df.height == 0:
            continue
        # Prefix columns with topic name (except timestamp_ns)
        safe_name = name.lstrip("/").replace("/", "_")
        renamed = {}
        for col in df.columns:
            if col == "timestamp_ns":
                renamed[col] = col
            else:
                renamed[col] = f"{safe_name}__{col}"
        dfs[name] = df.rename(renamed)

    if not dfs:
        return pl.DataFrame()

    # Determine anchor topic
    if anchor is None:
        anchor = max(dfs.keys(), key=lambda k: dfs[k].height)
    elif anchor not in dfs:
        raise KeyError(f"Anchor topic '{anchor}' not found in provided topics")

    anchor_df = dfs[anchor]
    anchor_timestamps = anchor_df["timestamp_ns"].to_numpy()
    tolerance_ns = int(tolerance_ms * 1e6)

    if method == "nearest":
        return _sync_nearest(anchor_df, dfs, anchor, anchor_timestamps, tolerance_ns)
    elif method == "interpolate":
        return _sync_interpolate(anchor_df, dfs, anchor, anchor_timestamps)
    elif method == "sample_and_hold":
        return _sync_sample_and_hold(anchor_df, dfs, anchor, anchor_timestamps)
    else:
        raise ValueError(f"Unknown sync method: {method}. Use 'nearest', 'interpolate', or 'sample_and_hold'.")


def _sync_nearest(
    anchor_df: pl.DataFrame,
    dfs: dict[str, pl.DataFrame],
    anchor_name: str,
    anchor_timestamps: np.ndarray,
    tolerance_ns: int,
) -> pl.DataFrame:
    """Nearest-timestamp matching within tolerance."""
    result = anchor_df.clone()

    for name, df in dfs.items():
        if name == anchor_name:
            continue

        other_timestamps = df["timestamp_ns"].to_numpy()
        if len(other_timestamps) == 0:
            continue

        # For each anchor timestamp, find nearest in other topic
        indices = np.searchsorted(other_timestamps, anchor_timestamps)
        indices = np.clip(indices, 0, len(other_timestamps) - 1)

        # Check both the found index and the previous one
        best_indices = np.empty(len(anchor_timestamps), dtype=np.int64)
        for i in range(len(anchor_timestamps)):
            idx = indices[i]
            if idx == 0:
                best_indices[i] = 0
            else:
                d1 = abs(other_timestamps[idx] - anchor_timestamps[i])
                d2 = abs(other_timestamps[idx - 1] - anchor_timestamps[i])
                best_indices[i] = idx if d1 <= d2 else idx - 1

        # Check tolerance
        diffs = np.abs(other_timestamps[best_indices] - anchor_timestamps)
        valid = diffs <= tolerance_ns

        # Add columns from other topic
        other_cols = [c for c in df.columns if c != "timestamp_ns"]
        for col in other_cols:
            values = df[col].to_numpy()
            matched = values[best_indices]
            # Set invalid matches to None
            if matched.dtype.kind in ('f', 'i'):
                matched = matched.astype(float)
                matched[~valid] = float('nan')
                result = result.with_columns(pl.Series(col, matched))
            else:
                # For non-numeric, convert to list and set invalid to None
                matched_list = [matched[i] if valid[i] else None for i in range(len(matched))]
                result = result.with_columns(pl.Series(col, matched_list))

    return result


def _sync_interpolate(
    anchor_df: pl.DataFrame,
    dfs: dict[str, pl.DataFrame],
    anchor_name: str,
    anchor_timestamps: np.ndarray,
) -> pl.DataFrame:
    """Linear interpolation for numeric streams."""
    result = anchor_df.clone()

    for name, df in dfs.items():
        if name == anchor_name:
            continue

        other_timestamps = df["timestamp_ns"].to_numpy().astype(float)
        if len(other_timestamps) < 2:
            continue

        other_cols = [c for c in df.columns if c != "timestamp_ns"]
        for col in other_cols:
            try:
                values = df[col].to_numpy().astype(float)
            except (ValueError, TypeError):
                continue

            # NumPy interpolation
            interpolated = np.interp(
                anchor_timestamps.astype(float),
                other_timestamps,
                values,
            )
            result = result.with_columns(pl.Series(col, interpolated))

    return result


def _sync_sample_and_hold(
    anchor_df: pl.DataFrame,
    dfs: dict[str, pl.DataFrame],
    anchor_name: str,
    anchor_timestamps: np.ndarray,
) -> pl.DataFrame:
    """Use the last known value for each anchor timestamp."""
    result = anchor_df.clone()

    for name, df in dfs.items():
        if name == anchor_name:
            continue

        other_timestamps = df["timestamp_ns"].to_numpy()
        if len(other_timestamps) == 0:
            continue

        # For each anchor timestamp, find the last message at or before it
        indices = np.searchsorted(other_timestamps, anchor_timestamps, side="right") - 1
        valid = indices >= 0

        other_cols = [c for c in df.columns if c != "timestamp_ns"]
        for col in other_cols:
            values = df[col].to_numpy()
            clipped_indices = np.clip(indices, 0, len(values) - 1)
            matched = values[clipped_indices]

            if matched.dtype.kind in ('f', 'i'):
                matched = matched.astype(float)
                matched[~valid] = float('nan')
                result = result.with_columns(pl.Series(col, matched))
            else:
                matched_list = [matched[i] if valid[i] else None for i in range(len(matched))]
                result = result.with_columns(pl.Series(col, matched_list))

    return result
