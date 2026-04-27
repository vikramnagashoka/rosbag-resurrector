"""Streaming aggregations for bounded-memory plot data.

The dashboard plot endpoint can't ``view.to_polars()`` a multi-million
message topic — that defeats the v0.4.0 performance contract. Instead,
it streams ``view.iter_chunks()`` through a single-pass aggregator that
emits a fixed number of representative points regardless of input size.

The chosen aggregator is **bucketed min/max**: divide the requested
time range into N buckets, track per-column running min and max within
each bucket. Emit two points per bucket per column (the min and max
with their original timestamps). For a line chart this preserves the
visual envelope of the signal — drops, spikes, and clipping are all
visible. Cheaper and more robust than streaming LTTB, which needs
out-of-band area computations.

Memory footprint: O(num_buckets × num_columns × constant). Independent
of chunk count and topic size. The bucket structures hold a few
floats and ints each.
"""

from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import polars as pl


def stream_bucketed_minmax(
    chunks: Iterable[pl.DataFrame],
    *,
    time_col: str = "timestamp_ns",
    value_cols: list[str] | None = None,
    num_buckets: int,
    time_range: tuple[int, int] | None = None,
) -> pl.DataFrame:
    """Single-pass min/max aggregation over time-bucketed chunks.

    Produces a DataFrame with at most ``2 * num_buckets`` rows, sorted
    by ``time_col`` ascending. For each bucket and each value column,
    two rows are emitted: one at the timestamp of the bucket's min and
    one at the timestamp of the bucket's max.

    The output is a multi-column wide frame compatible with the existing
    dashboard wire format (timestamp_ns + value columns). Only numeric
    value columns are supported; non-numeric columns are dropped.

    Args:
        chunks: iterator of DataFrames, each containing the time column
            and at least the requested value columns. Empty chunks are
            skipped.
        time_col: name of the timestamp column. Defaults to
            ``"timestamp_ns"``. Must contain monotonically-increasing
            integers within each chunk (cross-chunk order is not
            assumed; we sort the final output).
        value_cols: list of value columns to track. If ``None``, every
            numeric column found in the first non-empty chunk is used.
        num_buckets: how many time buckets to divide the range into.
            Output has at most ``2 * num_buckets`` points per column.
            Must be >= 1.
        time_range: optional (start_ns, end_ns) bounds. If provided,
            buckets span this range exactly; values outside the range
            are dropped. If None, the range is taken from the first and
            last timestamps observed across the stream — meaning we
            need a peek-ahead pass. To avoid that, callers that know
            the bag's bounds should pass them in.

    Returns:
        Polars DataFrame with columns ``[time_col, *value_cols]``,
        rows sorted ascending by time. Empty input → empty frame with
        the schema preserved.

    Raises:
        ValueError: if ``num_buckets < 1``.
    """
    if num_buckets < 1:
        raise ValueError(f"num_buckets must be >= 1, got {num_buckets}")

    # Materialize the iterator if we need a two-pass strategy (no
    # time_range supplied). For the dashboard's case we always have the
    # bounds from the bag's start/end, so this is the common path.
    if time_range is None:
        chunks = list(chunks)
        seen: int | None = None
        last: int | None = None
        for chunk in chunks:
            if chunk.height == 0:
                continue
            ts = chunk[time_col]
            seen = int(ts.min()) if seen is None else min(seen, int(ts.min()))
            last = int(ts.max()) if last is None else max(last, int(ts.max()))
        if seen is None or last is None:
            return pl.DataFrame({time_col: [], **{c: [] for c in (value_cols or [])}})
        time_range = (seen, last)

    start_ns, end_ns = time_range
    if end_ns <= start_ns:
        # Degenerate range — emit at most one bucket.
        end_ns = start_ns + 1
    bucket_width_ns = (end_ns - start_ns) / num_buckets

    # Per-bucket per-column running min/max + timestamps. Stored sparsely
    # (only buckets that have data) to keep wide-but-sparse topics cheap.
    # State shape: bucket_idx -> {col: (min_val, min_ts, max_val, max_ts)}
    state: dict[int, dict[str, tuple[float, int, float, int]]] = {}

    resolved_cols: list[str] | None = value_cols

    for chunk in chunks:
        if chunk.height == 0:
            continue

        # Resolve columns lazily from the first non-empty chunk if the
        # caller didn't pre-specify.
        if resolved_cols is None:
            resolved_cols = [
                c for c in chunk.columns
                if c != time_col and chunk.schema[c].is_numeric()
            ]
            if not resolved_cols:
                # Nothing to plot.
                return pl.DataFrame({time_col: []})

        # Skip if the chunk doesn't have any of the value columns
        # (e.g. cross-chunk schema drift on a topic that changed).
        present = [c for c in resolved_cols if c in chunk.columns]
        if not present:
            continue

        ts_arr = chunk[time_col].to_numpy()
        # Compute bucket index for every row at once.
        bucket_idx = np.floor((ts_arr - start_ns) / bucket_width_ns).astype(np.int64)
        np.clip(bucket_idx, 0, num_buckets - 1, out=bucket_idx)

        # Drop rows outside [start_ns, end_ns] explicitly so a clipped
        # bucket index doesn't get attributed to bucket 0 / last bucket.
        in_range = (ts_arr >= start_ns) & (ts_arr <= end_ns)
        if not in_range.all():
            keep = np.where(in_range)[0]
            if keep.size == 0:
                continue
            bucket_idx = bucket_idx[keep]
            row_indices = keep
        else:
            row_indices = None  # use all rows

        for col in present:
            col_vals = chunk[col].to_numpy()
            if col_vals.dtype.kind not in ("f", "i", "u"):
                continue
            vals_f = col_vals.astype(np.float64) if col_vals.dtype.kind != "f" else col_vals
            if row_indices is not None:
                vals_f = vals_f[row_indices]
                ts_for_col = ts_arr[row_indices]
            else:
                ts_for_col = ts_arr

            # Group by bucket — this is the only per-row work that
            # scales with chunk size, but it's vectorized via numpy.
            unique_b, inverse = np.unique(bucket_idx, return_inverse=True)
            for b_pos, b in enumerate(unique_b):
                mask = inverse == b_pos
                if not mask.any():
                    continue
                bucket_vals = vals_f[mask]
                bucket_ts = ts_for_col[mask]

                # NaN-safe min/max — np.nanargmin/nanargmax raises on
                # an all-NaN slice, so guard.
                if np.all(np.isnan(bucket_vals)):
                    continue
                min_pos = int(np.nanargmin(bucket_vals))
                max_pos = int(np.nanargmax(bucket_vals))
                local_min = float(bucket_vals[min_pos])
                local_min_ts = int(bucket_ts[min_pos])
                local_max = float(bucket_vals[max_pos])
                local_max_ts = int(bucket_ts[max_pos])

                cell = state.get(int(b), {})
                prev = cell.get(col)
                if prev is None:
                    cell[col] = (
                        local_min, local_min_ts,
                        local_max, local_max_ts,
                    )
                else:
                    pmin, pmin_ts, pmax, pmax_ts = prev
                    if local_min < pmin:
                        pmin, pmin_ts = local_min, local_min_ts
                    if local_max > pmax:
                        pmax, pmax_ts = local_max, local_max_ts
                    cell[col] = (pmin, pmin_ts, pmax, pmax_ts)
                state[int(b)] = cell

    if resolved_cols is None:
        return pl.DataFrame({time_col: []})

    if not state:
        # Time range supplied but no rows fell in it — return empty
        # frame with the right schema.
        return pl.DataFrame(
            {time_col: [], **{c: [] for c in resolved_cols}},
            schema={time_col: pl.Int64, **{c: pl.Float64 for c in resolved_cols}},
        )

    # Emit exactly 2 rows per bucket: one at the bucket's start time
    # carrying every column's min, one at the bucket's end time
    # carrying every column's max. This keeps the output bounded at
    # 2 * num_buckets regardless of how many value columns there are
    # — a wide topic with 14 columns would otherwise emit up to 28
    # timestamps per bucket if every column's extrema land at
    # distinct timestamps. Using bucket-aligned timestamps trades
    # exact min/max location fidelity for predictable output size,
    # which is what plotting needs (the visual envelope is preserved).
    bucket_width = (end_ns - start_ns) / num_buckets

    timestamps: list[int] = []
    cols_data: dict[str, list[float]] = {c: [] for c in resolved_cols}

    for b in sorted(state.keys()):
        cell = state[b]
        bucket_t_start = int(start_ns + b * bucket_width)
        bucket_t_end = int(start_ns + (b + 1) * bucket_width) - 1
        if bucket_t_end <= bucket_t_start:
            bucket_t_end = bucket_t_start + 1

        # Min row.
        timestamps.append(bucket_t_start)
        for col in resolved_cols:
            entry = cell.get(col)
            cols_data[col].append(entry[0] if entry is not None else float("nan"))

        # Max row.
        timestamps.append(bucket_t_end)
        for col in resolved_cols:
            entry = cell.get(col)
            cols_data[col].append(entry[2] if entry is not None else float("nan"))

    return pl.DataFrame(
        {time_col: timestamps, **cols_data},
        schema={time_col: pl.Int64, **{c: pl.Float64 for c in resolved_cols}},
    )


def stream_bucketed_minmax_from_view(
    view,
    *,
    num_buckets: int,
    bag_start_ns: int,
    bag_end_ns: int,
    chunk_size: int = 50_000,
) -> pl.DataFrame:
    """Convenience wrapper for a TopicView.

    Pulls the time range from the parent bag (passed in by the caller,
    since TopicView's own start/end_time_ns are only set when the view
    was produced by a time_slice). Streams ``view.iter_chunks(chunk_size)``
    through ``stream_bucketed_minmax``. Saves dashboard endpoints from
    re-implementing the wiring at every callsite.
    """
    # Prefer the time-slice bounds when set, fall back to the bag's
    # full bounds.
    start = view._start_time_ns if view._start_time_ns is not None else bag_start_ns  # noqa: SLF001
    end = view._end_time_ns if view._end_time_ns is not None else bag_end_ns  # noqa: SLF001

    return stream_bucketed_minmax(
        view.iter_chunks(chunk_size=chunk_size),
        num_buckets=num_buckets,
        time_range=(int(start), int(end)),
    )
