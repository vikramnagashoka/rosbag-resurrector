"""Multi-stream temporal synchronization.

Aligns multiple topics that publish at independent rates to a single
anchor stream. Two engines, both produce DataFrames in the same wire
format:

- **eager** (v0.3.x behavior): materializes every topic via
  ``view.to_polars()`` and matches via ``np.searchsorted``. Globally
  correct on every edge case but O(N) memory per topic. Available
  for backward compat and small bags via ``engine="eager"``.

- **streaming** (v0.4.0): per-topic bounded lookahead buffers around
  the current anchor timestamp. Memory bounded by
  ``max_topic_rate * 2 * tolerance``. Picks ``nearest`` /
  ``interpolate`` / ``sample_and_hold`` per the same per-method rules
  as eager, with explicit policies for out-of-order timestamps and
  interpolation boundaries. Selected via ``engine="streaming"``.

``engine="auto"`` (the default) routes to eager when every topic is
under ``LARGE_TOPIC_THRESHOLD`` and to streaming otherwise — small bags
keep the v0.3.x behavior, big bags get the bounded-memory path.

Failure modes are surfaced as typed exceptions:

- :class:`SyncBufferExceededError` — a non-anchor topic produced more
  than ``max_buffer_messages`` samples inside the lookahead window
  (likely a pathological rate mismatch).
- :class:`SyncOutOfOrderError` — only when ``out_of_order="error"``.
- :class:`SyncBoundaryError` — only when ``boundary="error"`` and an
  interpolation lacks bracketing samples.
"""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Iterator

import numpy as np
import polars as pl

from resurrector.core.bag_frame import LARGE_TOPIC_THRESHOLD
from resurrector.core.exceptions import (
    SyncBoundaryError,
    SyncBufferExceededError,
    SyncOutOfOrderError,
)

if TYPE_CHECKING:
    from resurrector.core.bag_frame import TopicView


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def synchronize(
    topic_views: dict[str, "TopicView"],
    method: str = "nearest",
    tolerance_ms: float = 50.0,
    anchor: str | None = None,
    *,
    engine: str = "auto",
    out_of_order: str = "error",
    boundary: str = "null",
    max_buffer_messages: int = 100_000,
    max_lateness_ms: float = 0.0,
) -> pl.DataFrame:
    """Synchronize multiple topics by timestamp.

    Args:
        topic_views: dict mapping topic name -> TopicView.
        method: ``"nearest"`` | ``"interpolate"`` | ``"sample_and_hold"``.
        tolerance_ms: max time difference between an anchor sample and
            the matched non-anchor sample, in milliseconds. Used by
            ``nearest`` for the lookahead window and by all methods for
            "no match" detection.
        anchor: topic name to use as the time reference. Defaults to
            the highest-frequency topic per the topic metadata.
        engine: ``"eager"`` (load everything, v0.3.x behavior),
            ``"streaming"`` (per-topic bounded buffers), or ``"auto"``
            (eager when all topics are under ``LARGE_TOPIC_THRESHOLD``,
            streaming otherwise — the default).
        out_of_order: streaming-only. How to handle a regression in
            timestamps within a topic:
              - ``"error"`` (default): raise SyncOutOfOrderError.
              - ``"warn_drop"``: log a warning, drop the regression.
              - ``"reorder"``: bounded watermark reorder buffer; emit
                samples older than ``current - max_lateness_ms``. Late
                arrivals beyond the window get dropped.
        boundary: streaming-only, interpolate-only. How to handle an
            anchor timestamp that lacks bracketing samples on a topic:
              - ``"null"`` (default): emit None/NaN for that column.
              - ``"drop"``: skip the entire anchor row.
              - ``"hold"``: use whichever edge sample exists.
              - ``"error"``: raise SyncBoundaryError.
        max_buffer_messages: streaming-only, per-topic cap on the
            lookahead buffer. Tripped raises SyncBufferExceededError.
        max_lateness_ms: streaming-only. Watermark lateness window
            for the ``reorder`` policy. Ignored otherwise.

    Returns:
        Unified Polars DataFrame with columns prefixed by topic name
        (except ``timestamp_ns``, which is the anchor topic's
        timestamp).
    """
    if not topic_views:
        return pl.DataFrame()

    # Engine selection.
    if engine == "auto":
        engine = (
            "streaming"
            if any(
                v.message_count > LARGE_TOPIC_THRESHOLD
                for v in topic_views.values()
            )
            else "eager"
        )

    if engine == "eager":
        return _synchronize_eager(
            topic_views, method=method, tolerance_ms=tolerance_ms, anchor=anchor,
        )
    if engine == "streaming":
        return _synchronize_streaming(
            topic_views,
            method=method,
            tolerance_ms=tolerance_ms,
            anchor=anchor,
            out_of_order=out_of_order,
            boundary=boundary,
            max_buffer_messages=max_buffer_messages,
            max_lateness_ms=max_lateness_ms,
        )
    raise ValueError(
        f"Unknown engine: {engine!r}. Use 'eager', 'streaming', or 'auto'."
    )


# ---------------------------------------------------------------------------
# Eager engine — v0.3.x behavior, kept for backward compat
# ---------------------------------------------------------------------------


def _synchronize_eager(
    topic_views: dict[str, "TopicView"],
    method: str = "nearest",
    tolerance_ms: float = 50.0,
    anchor: str | None = None,
) -> pl.DataFrame:
    """Eager sync — materializes every topic. v0.3.x behavior.

    Memory: O(N) per topic. Use ``engine="streaming"`` for large bags.
    """
    dfs: dict[str, pl.DataFrame] = {}
    for name, view in topic_views.items():
        # Eager engine MUST materialize. The LargeTopicError guard
        # would refuse big topics under the contract; pass force=True
        # so the user explicitly chose this engine.
        df = view.to_polars(force=True)
        if df.height == 0:
            continue
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

    if anchor is None:
        anchor = max(dfs.keys(), key=lambda k: dfs[k].height)
    elif anchor not in dfs:
        raise KeyError(f"Anchor topic '{anchor}' not found in provided topics")

    anchor_df = dfs[anchor]
    anchor_timestamps = anchor_df["timestamp_ns"].to_numpy()
    tolerance_ns = int(tolerance_ms * 1e6)

    if method == "nearest":
        return _eager_nearest(anchor_df, dfs, anchor, anchor_timestamps, tolerance_ns)
    if method == "interpolate":
        return _eager_interpolate(anchor_df, dfs, anchor, anchor_timestamps)
    if method == "sample_and_hold":
        return _eager_sample_and_hold(anchor_df, dfs, anchor, anchor_timestamps)
    raise ValueError(
        f"Unknown sync method: {method}. "
        f"Use 'nearest', 'interpolate', or 'sample_and_hold'."
    )


def _eager_nearest(
    anchor_df: pl.DataFrame,
    dfs: dict[str, pl.DataFrame],
    anchor_name: str,
    anchor_timestamps: np.ndarray,
    tolerance_ns: int,
) -> pl.DataFrame:
    result = anchor_df.clone()
    for name, df in dfs.items():
        if name == anchor_name:
            continue
        other_timestamps = df["timestamp_ns"].to_numpy()
        if len(other_timestamps) == 0:
            continue
        # Sort the non-anchor topic — eager mode handles out-of-order silently.
        sort_idx = np.argsort(other_timestamps)
        other_timestamps = other_timestamps[sort_idx]

        indices = np.searchsorted(other_timestamps, anchor_timestamps)
        indices = np.clip(indices, 0, len(other_timestamps) - 1)

        best_indices = np.empty(len(anchor_timestamps), dtype=np.int64)
        for i in range(len(anchor_timestamps)):
            idx = indices[i]
            if idx == 0:
                best_indices[i] = 0
            else:
                d1 = abs(other_timestamps[idx] - anchor_timestamps[i])
                d2 = abs(other_timestamps[idx - 1] - anchor_timestamps[i])
                best_indices[i] = idx if d1 <= d2 else idx - 1

        diffs = np.abs(other_timestamps[best_indices] - anchor_timestamps)
        valid = diffs <= tolerance_ns

        other_cols = [c for c in df.columns if c != "timestamp_ns"]
        for col in other_cols:
            values = df[col].to_numpy()[sort_idx]
            matched = values[best_indices]
            if matched.dtype.kind in ('f', 'i'):
                matched = matched.astype(float)
                matched[~valid] = float('nan')
                result = result.with_columns(pl.Series(col, matched))
            else:
                matched_list = [matched[i] if valid[i] else None for i in range(len(matched))]
                result = result.with_columns(pl.Series(col, matched_list))
    return result


def _eager_interpolate(
    anchor_df: pl.DataFrame,
    dfs: dict[str, pl.DataFrame],
    anchor_name: str,
    anchor_timestamps: np.ndarray,
) -> pl.DataFrame:
    result = anchor_df.clone()
    for name, df in dfs.items():
        if name == anchor_name:
            continue
        other_timestamps = df["timestamp_ns"].to_numpy().astype(float)
        if len(other_timestamps) < 2:
            continue
        # Sort
        sort_idx = np.argsort(other_timestamps)
        other_timestamps = other_timestamps[sort_idx]

        other_cols = [c for c in df.columns if c != "timestamp_ns"]
        for col in other_cols:
            try:
                values = df[col].to_numpy()[sort_idx].astype(float)
            except (ValueError, TypeError):
                continue
            interpolated = np.interp(
                anchor_timestamps.astype(float),
                other_timestamps,
                values,
            )
            result = result.with_columns(pl.Series(col, interpolated))
    return result


def _eager_sample_and_hold(
    anchor_df: pl.DataFrame,
    dfs: dict[str, pl.DataFrame],
    anchor_name: str,
    anchor_timestamps: np.ndarray,
) -> pl.DataFrame:
    result = anchor_df.clone()
    for name, df in dfs.items():
        if name == anchor_name:
            continue
        other_timestamps = df["timestamp_ns"].to_numpy()
        if len(other_timestamps) == 0:
            continue
        sort_idx = np.argsort(other_timestamps)
        other_timestamps = other_timestamps[sort_idx]

        indices = np.searchsorted(other_timestamps, anchor_timestamps, side="right") - 1
        valid = indices >= 0
        other_cols = [c for c in df.columns if c != "timestamp_ns"]
        for col in other_cols:
            values = df[col].to_numpy()[sort_idx]
            clipped = np.clip(indices, 0, len(values) - 1)
            matched = values[clipped]
            if matched.dtype.kind in ('f', 'i'):
                matched = matched.astype(float)
                matched[~valid] = float('nan')
                result = result.with_columns(pl.Series(col, matched))
            else:
                matched_list = [matched[i] if valid[i] else None for i in range(len(matched))]
                result = result.with_columns(pl.Series(col, matched_list))
    return result


# ---------------------------------------------------------------------------
# Streaming engine
# ---------------------------------------------------------------------------


def _synchronize_streaming(
    topic_views: dict[str, "TopicView"],
    method: str,
    tolerance_ms: float,
    anchor: str | None,
    out_of_order: str,
    boundary: str,
    max_buffer_messages: int,
    max_lateness_ms: float,
) -> pl.DataFrame:
    """Streaming sync — bounded-memory per-topic buffers."""
    if anchor is None:
        # Pick the topic with the highest message_count (proxy for highest
        # frequency). Same heuristic as the eager engine.
        anchor = max(topic_views.keys(), key=lambda k: topic_views[k].message_count)
    elif anchor not in topic_views:
        raise KeyError(f"Anchor topic '{anchor}' not found in provided topics")

    tolerance_ns = int(tolerance_ms * 1e6)
    max_lateness_ns = int(max_lateness_ms * 1e6)

    # Build per-non-anchor-topic chunk iterators that yield
    # (timestamp_ns, row_dict) tuples — flattening across chunks so the
    # strategy code can pull row-by-row.
    non_anchor_views = {
        name: view for name, view in topic_views.items() if name != anchor
    }
    non_anchor_iters: dict[str, Iterator[tuple[int, dict]]] = {
        name: _row_iter(
            view, name,
            out_of_order=out_of_order,
            max_lateness_ns=max_lateness_ns,
        )
        for name, view in non_anchor_views.items()
    }

    # Anchor topic provides the row schema. We materialize anchor rows
    # one at a time as well; the anchor is allowed to be large because
    # the output is the same size as the anchor.
    anchor_iter = _row_iter(
        topic_views[anchor], anchor,
        out_of_order=out_of_order,
        max_lateness_ns=max_lateness_ns,
    )

    if method == "nearest":
        return _streaming_nearest(
            anchor, anchor_iter, non_anchor_iters,
            tolerance_ns=tolerance_ns,
            max_buffer_messages=max_buffer_messages,
        )
    if method == "sample_and_hold":
        return _streaming_sample_and_hold(
            anchor, anchor_iter, non_anchor_iters,
            max_buffer_messages=max_buffer_messages,
        )
    if method == "interpolate":
        return _streaming_interpolate(
            anchor, anchor_iter, non_anchor_iters,
            boundary=boundary,
            max_buffer_messages=max_buffer_messages,
        )
    raise ValueError(
        f"Unknown sync method: {method}. "
        f"Use 'nearest', 'interpolate', or 'sample_and_hold'."
    )


def _row_iter(
    view,
    topic_name: str,
    *,
    out_of_order: str,
    max_lateness_ns: int,
) -> Iterator[tuple[int, dict]]:
    """Stream rows from a topic, applying the out-of-order policy.

    Yields (timestamp_ns, row_dict) tuples. The row_dict has columns
    prefixed with ``topic_name__`` (slashes -> underscores) so multiple
    topics can share an output frame without column collisions.

    Out-of-order policy:
      - "error": raise SyncOutOfOrderError on the first regression.
      - "warn_drop": log + drop regressing samples.
      - "reorder": bounded watermark reorder buffer.
    """
    import logging
    log = logging.getLogger("resurrector.core.sync")

    safe_prefix = topic_name.lstrip("/").replace("/", "_")
    last_ts: int | None = None

    if out_of_order == "reorder":
        # Watermark-style reorder buffer. Keep messages in a min-heap
        # by timestamp; emit anything older than (max_seen - lateness).
        import heapq
        heap: list[tuple[int, dict]] = []
        max_seen: int | None = None

        for chunk in view.iter_chunks():
            if chunk.height == 0:
                continue
            ts_arr = chunk["timestamp_ns"].to_numpy()
            row_dicts = chunk.to_dicts()
            for ts, row in zip(ts_arr, row_dicts):
                ts = int(ts)
                if max_seen is None or ts > max_seen:
                    max_seen = ts
                # Late arrival check: drop if it's already past the watermark.
                if max_seen is not None and ts < max_seen - max_lateness_ns:
                    log.warning(
                        "Dropping late arrival on %s: ts=%d, watermark=%d",
                        topic_name, ts, max_seen - max_lateness_ns,
                    )
                    continue
                renamed = {
                    f"{safe_prefix}__{k}" if k != "timestamp_ns" else "timestamp_ns": v
                    for k, v in row.items()
                }
                heapq.heappush(heap, (ts, renamed))
                # Emit anything safe to release.
                while heap and heap[0][0] <= max_seen - max_lateness_ns:
                    out_ts, out_row = heapq.heappop(heap)
                    yield out_ts, out_row
        # Drain remaining heap at end of stream.
        while heap:
            out_ts, out_row = heapq.heappop(heap)
            yield out_ts, out_row
        return

    # Non-reorder policies: "error" or "warn_drop".
    for chunk in view.iter_chunks():
        if chunk.height == 0:
            continue
        ts_arr = chunk["timestamp_ns"].to_numpy()
        row_dicts = chunk.to_dicts()
        for ts, row in zip(ts_arr, row_dicts):
            ts = int(ts)
            if last_ts is not None and ts < last_ts:
                if out_of_order == "error":
                    raise SyncOutOfOrderError(
                        topic_name=topic_name,
                        prev_ts=last_ts,
                        regressing_ts=ts,
                    )
                # "warn_drop"
                log.warning(
                    "Dropping out-of-order sample on %s: ts=%d after %d",
                    topic_name, ts, last_ts,
                )
                continue
            last_ts = ts
            renamed = {
                f"{safe_prefix}__{k}" if k != "timestamp_ns" else "timestamp_ns": v
                for k, v in row.items()
            }
            yield ts, renamed


def _streaming_nearest(
    anchor_name: str,
    anchor_iter: Iterator[tuple[int, dict]],
    non_anchor_iters: dict[str, Iterator[tuple[int, dict]]],
    *,
    tolerance_ns: int,
    max_buffer_messages: int,
) -> pl.DataFrame:
    """Lookahead-window nearest matching.

    Memory bound: O(rate * 2 * tolerance) per topic.

    For each non-anchor topic we maintain a deque of samples whose
    timestamps fall in [anchor - tolerance, anchor + tolerance]. We
    advance each topic forward until the next sample crosses
    (anchor + tolerance), then pick the closest to the anchor.
    """
    # Per-topic state: deque of (ts, row_dict) pairs in time order, plus
    # a single look-ahead "peeked" sample that we couldn't push to the
    # deque yet because it was already past the current window.
    buffers: dict[str, deque[tuple[int, dict]]] = {
        name: deque() for name in non_anchor_iters
    }
    peeked: dict[str, tuple[int, dict] | None] = {
        name: None for name in non_anchor_iters
    }
    exhausted: dict[str, bool] = {name: False for name in non_anchor_iters}

    output_rows: list[dict] = []

    for anchor_ts, anchor_row in anchor_iter:
        window_lo = anchor_ts - tolerance_ns
        window_hi = anchor_ts + tolerance_ns

        merged = dict(anchor_row)

        for name, it in non_anchor_iters.items():
            buf = buffers[name]

            # Drop stale entries (older than window_lo).
            while buf and buf[0][0] < window_lo:
                buf.popleft()

            # If we have a peeked sample, see if it fits in the window now.
            if peeked[name] is not None:
                pts, prow = peeked[name]
                if pts <= window_hi:
                    buf.append((pts, prow))
                    peeked[name] = None

            # Pull forward until the next sample is past window_hi.
            while peeked[name] is None and not exhausted[name]:
                try:
                    ts, row = next(it)
                except StopIteration:
                    exhausted[name] = True
                    break
                if ts < window_lo:
                    # Already stale relative to the current anchor — drop.
                    continue
                if ts > window_hi:
                    # Past the window — peek and stop.
                    peeked[name] = (ts, row)
                    break
                buf.append((ts, row))
                if len(buf) > max_buffer_messages:
                    raise SyncBufferExceededError(
                        topic_name=name,
                        buffer_size=len(buf),
                        max_buffer_messages=max_buffer_messages,
                    )

            # Pick closest to anchor_ts. Tie-break: prefer the LATER
            # sample, matching eager's `idx if d1 <= d2 else idx - 1`
            # rule. `idx` is the upper bound from np.searchsorted, so
            # equal-distance ties resolve to the later sample.
            best: tuple[int, dict] | None = None
            best_delta = tolerance_ns + 1
            for ts, row in buf:
                delta = abs(ts - anchor_ts)
                if delta < best_delta or (delta == best_delta and best is not None and ts >= best[0]):
                    best = (ts, row)
                    best_delta = delta

            if best is not None:
                # Merge non-anchor columns into the output row. Skip the
                # non-anchor topic's own timestamp_ns to avoid clobbering
                # the anchor's.
                for k, v in best[1].items():
                    if k != "timestamp_ns":
                        merged[k] = v
            # else: no sample within tolerance → no columns added →
            # downstream sees them as null. That matches eager.

        output_rows.append(merged)

    return _rows_to_dataframe(output_rows)


def _streaming_sample_and_hold(
    anchor_name: str,
    anchor_iter: Iterator[tuple[int, dict]],
    non_anchor_iters: dict[str, Iterator[tuple[int, dict]]],
    *,
    max_buffer_messages: int,
) -> pl.DataFrame:
    """Use the most recent non-anchor sample at or before each anchor ts."""
    # Per-topic state: most recent sample (ts, row) at or before current
    # anchor, plus a peeked sample that's after.
    held: dict[str, tuple[int, dict] | None] = {
        name: None for name in non_anchor_iters
    }
    peeked: dict[str, tuple[int, dict] | None] = {
        name: None for name in non_anchor_iters
    }
    exhausted: dict[str, bool] = {name: False for name in non_anchor_iters}

    output_rows: list[dict] = []

    for anchor_ts, anchor_row in anchor_iter:
        merged = dict(anchor_row)

        for name, it in non_anchor_iters.items():
            # If we have a peeked sample that's now <= anchor_ts, it
            # becomes the new held sample.
            if peeked[name] is not None and peeked[name][0] <= anchor_ts:
                held[name] = peeked[name]
                peeked[name] = None

            # Pull forward until the next sample is > anchor_ts.
            while peeked[name] is None and not exhausted[name]:
                try:
                    ts, row = next(it)
                except StopIteration:
                    exhausted[name] = True
                    break
                if ts <= anchor_ts:
                    held[name] = (ts, row)
                else:
                    peeked[name] = (ts, row)
                    break

            if held[name] is not None:
                for k, v in held[name][1].items():
                    if k != "timestamp_ns":
                        merged[k] = v

        output_rows.append(merged)

    return _rows_to_dataframe(output_rows)


def _streaming_interpolate(
    anchor_name: str,
    anchor_iter: Iterator[tuple[int, dict]],
    non_anchor_iters: dict[str, Iterator[tuple[int, dict]]],
    *,
    boundary: str,
    max_buffer_messages: int,
) -> pl.DataFrame:
    """Linear interpolation per anchor timestamp.

    For each anchor row, each non-anchor topic needs a `prev` sample
    at or before the anchor and a `next` sample at or after. Boundary
    policy decides what happens when one is missing.
    """
    prev: dict[str, tuple[int, dict] | None] = {
        name: None for name in non_anchor_iters
    }
    next_: dict[str, tuple[int, dict] | None] = {
        name: None for name in non_anchor_iters
    }
    exhausted: dict[str, bool] = {name: False for name in non_anchor_iters}

    output_rows: list[dict] = []

    for anchor_ts, anchor_row in anchor_iter:
        # Per-topic merged columns (built locally so we can drop the
        # whole row if boundary=="drop").
        per_topic_cols: dict[str, dict[str, float | None] | None] = {}
        drop_row = False

        for name, it in non_anchor_iters.items():
            # Advance: if next_ is set and <= anchor, slide it into prev
            # and pull a new next_.
            while next_[name] is not None and next_[name][0] <= anchor_ts:
                prev[name] = next_[name]
                next_[name] = None
            while next_[name] is None and not exhausted[name]:
                try:
                    ts, row = next(it)
                except StopIteration:
                    exhausted[name] = True
                    break
                if ts <= anchor_ts:
                    prev[name] = (ts, row)
                else:
                    next_[name] = (ts, row)

            p = prev[name]
            n = next_[name]

            if p is not None and n is not None and n[0] != p[0]:
                # Interpolate every numeric column.
                t0 = p[0]
                t1 = n[0]
                alpha = (anchor_ts - t0) / (t1 - t0)
                interp_cols: dict[str, float | None] = {}
                for k, v0 in p[1].items():
                    if k == "timestamp_ns":
                        continue
                    v1 = n[1].get(k)
                    if isinstance(v0, (int, float)) and isinstance(v1, (int, float)):
                        interp_cols[k] = v0 + (v1 - v0) * alpha
                    else:
                        # Non-numeric — hold the prev value.
                        interp_cols[k] = v0
                per_topic_cols[name] = interp_cols
            else:
                # Boundary case.
                if boundary == "error":
                    pos = (
                        "before_first" if p is None
                        else "after_last" if n is None
                        else "no_data"
                    )
                    raise SyncBoundaryError(
                        topic_name=name,
                        anchor_ts=anchor_ts,
                        position=pos,
                    )
                if boundary == "drop":
                    drop_row = True
                    break
                if boundary == "hold":
                    edge = p if p is not None else n
                    if edge is not None:
                        per_topic_cols[name] = {
                            k: v for k, v in edge[1].items() if k != "timestamp_ns"
                        }
                    else:
                        per_topic_cols[name] = None
                else:  # "null"
                    per_topic_cols[name] = None

        if drop_row:
            continue

        merged = dict(anchor_row)
        for name, cols in per_topic_cols.items():
            if cols is None:
                # Inject NaN/None for the topic's columns. We need to
                # know its column names; pull from prev or next.
                source = prev[name] if prev[name] is not None else next_[name]
                if source is None:
                    continue
                for k in source[1]:
                    if k != "timestamp_ns":
                        merged[k] = None
            else:
                for k, v in cols.items():
                    merged[k] = v
        output_rows.append(merged)

    return _rows_to_dataframe(output_rows)


def _rows_to_dataframe(rows: list[dict]) -> pl.DataFrame:
    """Build a Polars DataFrame from a row list, schema unioned across rows.

    Streaming sync may produce rows where some topics' columns are
    missing (no match in tolerance). We need a single schema so the
    DataFrame constructor doesn't reject the input.
    """
    if not rows:
        return pl.DataFrame()
    # Union of all keys, preserving first-seen order.
    seen: list[str] = []
    seen_set: set[str] = set()
    for r in rows:
        for k in r:
            if k not in seen_set:
                seen.append(k)
                seen_set.add(k)
    # Pad each row with nulls for missing columns.
    padded: list[dict] = []
    for r in rows:
        if len(r) == len(seen):
            padded.append(r)
        else:
            padded.append({k: r.get(k) for k in seen})
    return pl.DataFrame(padded, infer_schema_length=len(padded))
