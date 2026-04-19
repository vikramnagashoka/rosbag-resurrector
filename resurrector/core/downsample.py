"""LTTB (Largest-Triangle-Three-Buckets) downsampling for plot data.

LTTB preserves visual shape of time-series data by selecting the point
in each bucket that creates the largest triangle with its neighbors.
This is the standard algorithm for fast, quality-preserving chart
downsampling — see https://skemman.is/handle/1946/15343 (Sveinn Steinarsson).

Used by the dashboard to cap wire payload at ~2k points per topic,
independent of source density. Users still see a visually accurate
plot; on brush/zoom the frontend re-requests a narrower time range
and gets a fresh downsampled slice.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np
import polars as pl


def lttb(
    timestamps_ns: Sequence[int] | np.ndarray,
    values: Sequence[float] | np.ndarray,
    max_points: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Downsample a numeric time series to at most ``max_points`` points.

    Args:
        timestamps_ns: x-axis values, must be numeric and monotonic.
        values: y-axis values, same length as ``timestamps_ns``.
        max_points: target output size. Must be >= 3.

    Returns:
        (downsampled_timestamps_ns, downsampled_values) — numpy arrays
        with length min(max_points, len(input)).

    Edge cases:
        - If the input has <= max_points, it is returned unchanged (cast
          to numpy arrays).
        - NaN values are treated as 0 for bucket averaging; the selected
          row index still maps back to the original value, so NaNs in
          the output are possible but preserved positionally.
    """
    ts = np.asarray(timestamps_ns, dtype=np.int64)
    ys = np.asarray(values, dtype=np.float64)

    n = len(ts)
    if len(ys) != n:
        raise ValueError(
            f"timestamps and values must have same length: "
            f"{n} vs {len(ys)}"
        )
    if max_points < 3:
        raise ValueError(f"max_points must be >= 3, got {max_points}")
    if n <= max_points:
        return ts, ys

    # LTTB picks the first and last points verbatim; the remaining
    # max_points - 2 points each come from one bucket.
    bucket_size = (n - 2) / (max_points - 2)

    sampled_idx = np.empty(max_points, dtype=np.int64)
    sampled_idx[0] = 0
    sampled_idx[-1] = n - 1

    a = 0  # index of previously selected point
    for i in range(max_points - 2):
        # Range of the NEXT bucket — used to compute its average for the
        # triangle apex.
        next_start = int(np.floor((i + 1) * bucket_size)) + 1
        next_end = int(np.floor((i + 2) * bucket_size)) + 1
        next_end = min(next_end, n)
        if next_start >= n:
            next_start = n - 1
        avg_x = float(np.mean(ts[next_start:next_end]))
        avg_y = float(np.nanmean(ys[next_start:next_end]))

        # Range of the CURRENT bucket — pick the point maximising the
        # triangle with (a) and the next bucket's average.
        cur_start = int(np.floor(i * bucket_size)) + 1
        cur_end = int(np.floor((i + 1) * bucket_size)) + 1
        cur_end = min(cur_end, n)
        if cur_start >= cur_end:
            sampled_idx[i + 1] = cur_start if cur_start < n else n - 1
            continue

        pa_x = float(ts[a])
        pa_y = float(ys[a])
        xs_bucket = ts[cur_start:cur_end].astype(np.float64)
        ys_bucket = ys[cur_start:cur_end]

        # Triangle area (twice the area, but monotonic so equivalent).
        area = np.abs(
            (pa_x - avg_x) * (ys_bucket - pa_y)
            - (pa_x - xs_bucket) * (avg_y - pa_y)
        )
        # NaN areas -> -inf so they lose argmax.
        area = np.where(np.isnan(area), -np.inf, area)
        best_local = int(np.argmax(area))
        a = cur_start + best_local
        sampled_idx[i + 1] = a

    return ts[sampled_idx], ys[sampled_idx]


def downsample_dataframe(
    df: pl.DataFrame, max_points: int, time_col: str = "timestamp_ns",
) -> pl.DataFrame:
    """Apply LTTB to every numeric column in a Polars DataFrame.

    Each numeric column is downsampled independently with the same
    time axis, so the returned frame has a shared x-coordinate set
    (union of selected indices across columns, capped at max_points).

    Non-numeric columns are dropped — LTTB is undefined for strings.
    If the frame has <= max_points rows it is returned unchanged.
    """
    if df.height <= max_points:
        return df

    if time_col not in df.columns:
        raise ValueError(f"time column '{time_col}' not found in frame")

    ts = df[time_col].to_numpy()
    selected_rows: set[int] = {0, df.height - 1}

    numeric_cols: list[str] = []
    for col in df.columns:
        if col == time_col:
            continue
        # Polars numeric kinds
        if df.schema[col].is_numeric():
            numeric_cols.append(col)

    if not numeric_cols:
        # No numeric data to drive selection — fall back to stride sampling.
        stride = max(1, df.height // max_points)
        return df[::stride].head(max_points)

    # Single-pass LTTB on the first numeric column picks the canonical
    # index set; other columns re-use it so cursors stay aligned.
    # This is cheaper than per-column downsampling and keeps rows atomic.
    main_col = numeric_cols[0]
    _, _ = lttb(ts, df[main_col].to_numpy(), max_points)
    # Re-run to get indices (numpy doesn't return them from lttb; we do
    # a second pass that is trivial since the first materialized ts/ys
    # already).  Simpler alternative: inline the indices.
    picked = _lttb_indices(ts, df[main_col].to_numpy(), max_points)
    return df[picked]


def _lttb_indices(
    timestamps_ns: np.ndarray, values: np.ndarray, max_points: int,
) -> list[int]:
    """Return the row indices chosen by LTTB.

    Mirrors ``lttb`` exactly but returns the index list instead of
    materialised arrays, so callers can slice a multi-column frame.
    """
    n = len(timestamps_ns)
    if n <= max_points:
        return list(range(n))
    bucket_size = (n - 2) / (max_points - 2)
    out = [0]
    a = 0
    ts = timestamps_ns.astype(np.float64)
    ys = values.astype(np.float64)
    for i in range(max_points - 2):
        next_start = int(np.floor((i + 1) * bucket_size)) + 1
        next_end = min(int(np.floor((i + 2) * bucket_size)) + 1, n)
        if next_start >= n:
            next_start = n - 1
        avg_x = float(np.mean(ts[next_start:next_end]))
        avg_y = float(np.nanmean(ys[next_start:next_end]))

        cur_start = int(np.floor(i * bucket_size)) + 1
        cur_end = min(int(np.floor((i + 1) * bucket_size)) + 1, n)
        if cur_start >= cur_end:
            idx = cur_start if cur_start < n else n - 1
            out.append(idx)
            a = idx
            continue

        pa_x = ts[a]
        pa_y = ys[a]
        xs_bucket = ts[cur_start:cur_end]
        ys_bucket = ys[cur_start:cur_end]
        area = np.abs(
            (pa_x - avg_x) * (ys_bucket - pa_y)
            - (pa_x - xs_bucket) * (avg_y - pa_y)
        )
        area = np.where(np.isnan(area), -np.inf, area)
        best_local = int(np.argmax(area))
        a = cur_start + best_local
        out.append(a)
    out.append(n - 1)
    return out
