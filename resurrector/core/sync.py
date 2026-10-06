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

Both engines produce output in chunks through :func:`iter_synchronize`;
:func:`synchronize` concatenates them. The output schema is fixed
before the first chunk, so every chunk can go to the same streaming
writer (this is how synced exports stay bounded):

- eager: exactly the schema the one-shot v0.3.x frame had.
- streaming: taken from each topic's first input chunk. Anchor columns
  come first, then each other topic's columns in the order the topics
  were passed. A topic with no match anywhere still gets its columns
  (all null), and ``interpolate`` makes numeric and bool columns
  Float64.

Failure modes are surfaced as typed exceptions:

- :class:`SyncBufferExceededError` — a non-anchor topic produced more
  than ``max_buffer_messages`` samples inside the lookahead window
  (likely a pathological rate mismatch).
- :class:`SyncOutOfOrderError` — only when ``out_of_order="error"``.
- :class:`SyncBoundaryError` — only when ``boundary="error"`` and an
  interpolation lacks bracketing samples.
"""

from __future__ import annotations

import itertools
import logging
from collections import deque
from typing import TYPE_CHECKING, Iterable, Iterator

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

log = logging.getLogger("resurrector.core.sync")

# Rows per chunk yielded by iter_synchronize, and per input chunk the
# streaming engine reads. Matches TopicView.iter_chunks and the export
# CHUNK_SIZE.
SYNC_CHUNK_SIZE = 50_000

# Floor on the streaming engine's input chunk: a tiny output chunk
# shouldn't turn every message into its own DataFrame.
_MIN_READ_CHUNK = 1_000

_METHODS = ("nearest", "interpolate", "sample_and_hold")


# ---------------------------------------------------------------------------
# Public entry points
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
        timestamp). Built by concatenating :func:`iter_synchronize`;
        use that directly to process a long sync chunk by chunk.
    """
    chunks = list(iter_synchronize(
        topic_views,
        method=method,
        tolerance_ms=tolerance_ms,
        anchor=anchor,
        engine=engine,
        out_of_order=out_of_order,
        boundary=boundary,
        max_buffer_messages=max_buffer_messages,
        max_lateness_ms=max_lateness_ms,
    ))
    if not chunks:
        return pl.DataFrame()
    return pl.concat(chunks, how="vertical", rechunk=True)


def iter_synchronize(
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
    chunk_size: int = SYNC_CHUNK_SIZE,
) -> Iterator[pl.DataFrame]:
    """Synchronize topics, yielding the result in chunks of ``chunk_size`` rows.

    Same arguments and semantics as :func:`synchronize`; concatenating
    the chunks gives exactly its DataFrame. Every chunk has the same
    schema (see the module docstring), and no chunk is empty.

    Memory:
        - streaming: one output chunk, one input chunk per topic, and
          the per-topic lookahead buffers. Never the whole result.
        - eager: every input topic is materialized (eager only runs when
          they are all under ``LARGE_TOPIC_THRESHOLD``, or when asked
          for), but output is built one chunk at a time.

    Sync errors (:class:`SyncOutOfOrderError`,
    :class:`SyncBoundaryError`, :class:`SyncBufferExceededError`) are
    raised during iteration, possibly after earlier chunks were yielded.

    Args:
        chunk_size: Maximum rows per yielded DataFrame. The streaming
            engine also reads its inputs in chunks of this size (at
            least 1,000 rows).

    Raises:
        ValueError: Unknown ``engine`` or ``method``, or
            ``chunk_size < 1``. Raised by the call itself, before
            iteration.
        KeyError: Unknown ``anchor``. Streaming raises it on the call;
            eager on the first ``next()`` (it must read the topics to
            know which are empty).

    Example::

        from resurrector.core.sync import iter_synchronize
        views = {t: bf[t] for t in ["/imu/data", "/joint_states"]}
        for chunk in iter_synchronize(views, method="nearest"):
            writer.write_table(chunk.to_arrow())
    """
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
    if not topic_views:
        return iter(())

    if engine == "auto":
        engine = (
            "streaming"
            if any(
                v.message_count > LARGE_TOPIC_THRESHOLD
                for v in topic_views.values()
            )
            else "eager"
        )
    if engine not in ("eager", "streaming"):
        raise ValueError(
            f"Unknown engine: {engine!r}. Use 'eager', 'streaming', or 'auto'."
        )
    if method not in _METHODS:
        raise ValueError(
            f"Unknown sync method: {method}. "
            f"Use 'nearest', 'interpolate', or 'sample_and_hold'."
        )

    if engine == "eager":
        return _iter_eager(
            topic_views,
            method=method,
            tolerance_ms=tolerance_ms,
            anchor=anchor,
            chunk_size=chunk_size,
        )
    return _iter_streaming(
        topic_views,
        method=method,
        tolerance_ms=tolerance_ms,
        anchor=anchor,
        out_of_order=out_of_order,
        boundary=boundary,
        max_buffer_messages=max_buffer_messages,
        max_lateness_ms=max_lateness_ms,
        chunk_size=chunk_size,
    )


# ---------------------------------------------------------------------------
# Eager engine — v0.3.x behavior, kept for backward compat
# ---------------------------------------------------------------------------


def _iter_eager(
    topic_views: dict[str, "TopicView"],
    *,
    method: str,
    tolerance_ms: float,
    anchor: str | None,
    chunk_size: int,
) -> Iterator[pl.DataFrame]:
    """Eager sync — materializes every topic. v0.3.x behavior.

    Memory: O(N) per input topic, plus one output chunk. Matching is
    planned once per topic (``nearest`` / ``sample_and_hold``: a source
    row and a match flag per anchor row; ``interpolate``: the topic's
    sorted float columns), then each chunk is cut from the plans. Use
    ``engine="streaming"`` for large bags.
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
        return

    if anchor is None:
        anchor = max(dfs.keys(), key=lambda k: dfs[k].height)
    elif anchor not in dfs:
        raise KeyError(f"Anchor topic '{anchor}' not found in provided topics")

    anchor_df = dfs[anchor]
    anchor_timestamps = anchor_df["timestamp_ns"].to_numpy()
    others = [df for name, df in dfs.items() if name != anchor]

    plans: list[_GatherPlan] | list[_InterpolatePlan]
    if method == "interpolate":
        anchor_float = anchor_timestamps.astype(float)
        plans = [_InterpolatePlan(df, anchor_float) for df in others]
    else:
        tolerance_ns = int(tolerance_ms * 1e6)
        plans = [
            _GatherPlan(df, anchor_timestamps, method, tolerance_ns)
            for df in others
        ]

    for start in range(0, anchor_df.height, chunk_size):
        out = anchor_df.slice(start, chunk_size)
        for plan in plans:
            out = plan.apply(out, start)
        yield out


class _GatherPlan:
    """Eager ``nearest`` / ``sample_and_hold`` for one non-anchor topic.

    Precomputes, for every anchor row, which source row it takes and
    whether that is a match; ``apply`` gathers one chunk's worth.
    Column handling matches the v0.3.x one-shot build:

    - numeric (NumPy kind ``f``/``i``): Float64, NaN where unmatched.
    - anything else: the source dtype, null where unmatched; Null dtype
      if no anchor row gets a non-null value (what inference gave).
    """

    def __init__(
        self,
        df: pl.DataFrame,
        anchor_timestamps: np.ndarray,
        method: str,
        tolerance_ns: int,
    ):
        other_timestamps = df["timestamp_ns"].to_numpy()
        # Sort the non-anchor topic — eager mode handles out-of-order silently.
        sort_idx = np.argsort(other_timestamps)
        other_timestamps = other_timestamps[sort_idx]
        last = len(other_timestamps) - 1

        if method == "nearest":
            idx = np.clip(np.searchsorted(other_timestamps, anchor_timestamps), 0, last)
            # Equal distances resolve to the later sample (idx, the
            # searchsorted upper bound). The streaming engine mirrors this.
            d_at = np.abs(other_timestamps[idx] - anchor_timestamps)
            d_before = np.abs(other_timestamps[np.maximum(idx - 1, 0)] - anchor_timestamps)
            best = np.where((idx == 0) | (d_at <= d_before), idx, idx - 1)
            self.valid = np.abs(other_timestamps[best] - anchor_timestamps) <= tolerance_ns
        else:  # sample_and_hold
            held = np.searchsorted(other_timestamps, anchor_timestamps, side="right") - 1
            self.valid = held >= 0
            best = np.clip(held, 0, last)
        self.rows = sort_idx[best]

        # (column, source series, mode): "numeric", "typed", or "null".
        self.columns: list[tuple[str, pl.Series, str]] = []
        for col in df.columns:
            if col == "timestamp_ns":
                continue
            series = df[col]
            # The whole column's NumPy kind decides, as it did when the
            # column was matched in one piece (Int64 with nulls is "f").
            if series.to_numpy().dtype.kind in ("f", "i"):
                mode = "numeric"
            elif (series.is_not_null().to_numpy()[self.rows] & self.valid).any():
                mode = "typed"
            else:
                mode = "null"
            self.columns.append((col, series, mode))

    def apply(self, out: pl.DataFrame, start: int) -> pl.DataFrame:
        stop = start + out.height
        rows = self.rows[start:stop]
        valid = self.valid[start:stop]
        for col, series, mode in self.columns:
            if mode == "numeric":
                matched = series.gather(rows).to_numpy().astype(float)
                matched[~valid] = float("nan")
                out = out.with_columns(pl.Series(col, matched))
            elif mode == "typed":
                out = out.with_columns(series.gather(rows).set(pl.Series(~valid), None))
            else:
                out = out.with_columns(pl.Series(col, [None] * out.height))
        return out


class _InterpolatePlan:
    """Eager ``interpolate`` for one non-anchor topic.

    Holds the topic's sorted timestamps and each float-convertible
    column (columns that won't convert are dropped, as in v0.3.x);
    ``apply`` runs ``np.interp`` for one chunk of anchor rows.
    """

    def __init__(self, df: pl.DataFrame, anchor_float: np.ndarray):
        self.anchor_float = anchor_float
        self.columns: list[tuple[str, np.ndarray]] = []
        other_timestamps = df["timestamp_ns"].to_numpy().astype(float)
        if len(other_timestamps) < 2:
            return
        sort_idx = np.argsort(other_timestamps)
        self.other_timestamps = other_timestamps[sort_idx]
        for col in df.columns:
            if col == "timestamp_ns":
                continue
            try:
                values = df[col].to_numpy()[sort_idx].astype(float)
            except (ValueError, TypeError):
                continue
            self.columns.append((col, values))

    def apply(self, out: pl.DataFrame, start: int) -> pl.DataFrame:
        anchor = self.anchor_float[start:start + out.height]
        for col, values in self.columns:
            interpolated = np.interp(anchor, self.other_timestamps, values)
            out = out.with_columns(pl.Series(col, interpolated))
        return out


# ---------------------------------------------------------------------------
# Streaming engine
# ---------------------------------------------------------------------------


def _iter_streaming(
    topic_views: dict[str, "TopicView"],
    *,
    method: str,
    tolerance_ms: float,
    anchor: str | None,
    out_of_order: str,
    boundary: str,
    max_buffer_messages: int,
    max_lateness_ms: float,
    chunk_size: int,
) -> Iterator[pl.DataFrame]:
    """Streaming sync — bounded-memory per-topic buffers."""
    if anchor is None:
        # Pick the topic with the highest message_count (proxy for highest
        # frequency). Same heuristic as the eager engine.
        anchor = max(topic_views.keys(), key=lambda k: topic_views[k].message_count)
    elif anchor not in topic_views:
        raise KeyError(f"Anchor topic '{anchor}' not found in provided topics")

    return _streaming_chunks(
        topic_views,
        anchor=anchor,
        method=method,
        tolerance_ns=int(tolerance_ms * 1e6),
        out_of_order=out_of_order,
        boundary=boundary,
        max_buffer_messages=max_buffer_messages,
        max_lateness_ns=int(max_lateness_ms * 1e6),
        chunk_size=chunk_size,
    )


def _streaming_chunks(
    topic_views: dict[str, "TopicView"],
    *,
    anchor: str,
    method: str,
    tolerance_ns: int,
    out_of_order: str,
    boundary: str,
    max_buffer_messages: int,
    max_lateness_ns: int,
    chunk_size: int,
) -> Iterator[pl.DataFrame]:
    # Peek one input chunk per topic so the output schema is fixed
    # before the first row is matched.
    sources = {
        name: _peek_schema(view.iter_chunks(max(chunk_size, _MIN_READ_CHUNK)))
        for name, view in topic_views.items()
    }
    schema = _streaming_schema(
        anchor, {name: s for name, (s, _) in sources.items()}, method,
    )

    def rows_of(name: str) -> Iterator[tuple[int, dict]]:
        topic_schema, chunks = sources[name]
        if topic_schema is not None:
            chunks = _warn_on_new_columns(chunks, name, set(topic_schema))
        return _rows_from_chunks(
            chunks, name,
            out_of_order=out_of_order,
            max_lateness_ns=max_lateness_ns,
        )

    # Build per-non-anchor-topic row iterators that yield
    # (timestamp_ns, row_dict) tuples — flattening across chunks so the
    # strategy code can pull row-by-row. The anchor streams the same way;
    # each output row is merged onto one anchor row.
    non_anchor_iters = {
        name: rows_of(name) for name in topic_views if name != anchor
    }
    anchor_iter = rows_of(anchor)

    if method == "nearest":
        rows = _streaming_nearest(
            anchor, anchor_iter, non_anchor_iters,
            tolerance_ns=tolerance_ns,
            max_buffer_messages=max_buffer_messages,
        )
    elif method == "sample_and_hold":
        rows = _streaming_sample_and_hold(
            anchor, anchor_iter, non_anchor_iters,
            max_buffer_messages=max_buffer_messages,
        )
    else:
        rows = _streaming_interpolate(
            anchor, anchor_iter, non_anchor_iters,
            boundary=boundary,
            max_buffer_messages=max_buffer_messages,
        )
    yield from _row_chunks(rows, schema, chunk_size)


def _peek_schema(
    chunks: Iterator[pl.DataFrame],
) -> tuple[pl.Schema | None, Iterator[pl.DataFrame]]:
    """Return the first non-empty chunk's schema and an iterator that
    still yields that chunk. ``(None, empty)`` for an empty topic."""
    for chunk in chunks:
        if chunk.height:
            return chunk.schema, itertools.chain([chunk], chunks)
    return None, iter(())


def _streaming_schema(
    anchor: str,
    topic_schemas: dict[str, pl.Schema | None],
    method: str,
) -> dict[str, pl.DataType]:
    """Output schema in row-merge order: anchor, then the other topics.

    Interpolation turns Python ints, floats and bools into floats, so
    those columns are Float64 under ``interpolate``; boundary ``hold``
    values are cast to match.
    """
    out: dict[str, pl.DataType] = {}
    order = [anchor] + [name for name in topic_schemas if name != anchor]
    for name in order:
        topic_schema = topic_schemas[name]
        if topic_schema is None:
            continue
        prefix = name.lstrip("/").replace("/", "_")
        for col, dtype in topic_schema.items():
            if col == "timestamp_ns":
                if name == anchor:
                    out[col] = dtype
                continue
            if name != anchor and method == "interpolate" and (
                dtype.is_integer() or dtype.is_float() or dtype == pl.Boolean
            ):
                dtype = pl.Float64
            out[f"{prefix}__{col}"] = dtype
    return out


def _warn_on_new_columns(
    chunks: Iterator[pl.DataFrame], topic_name: str, known: set[str],
) -> Iterator[pl.DataFrame]:
    """Pass chunks through, warning once if a column shows up that the
    topic's first chunk didn't have (the output schema can't grow)."""
    warned = False
    for chunk in chunks:
        if not warned:
            new = [c for c in chunk.columns if c not in known]
            if new:
                log.warning(
                    "Topic %s gained columns %s after its first chunk; "
                    "the synced output's columns are fixed from the first "
                    "chunk, so these are left out.",
                    topic_name, new,
                )
                warned = True
        yield chunk


def _row_chunks(
    rows: Iterator[dict],
    schema: dict[str, pl.DataType],
    chunk_size: int,
) -> Iterator[pl.DataFrame]:
    """Group merged row dicts into DataFrames of ``chunk_size`` rows.

    Missing keys (no match within tolerance) become nulls.
    """
    batch: list[dict] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= chunk_size:
            df = pl.from_dicts(batch, schema=schema)
            batch = []
            yield df
    if batch:
        yield pl.from_dicts(batch, schema=schema)


def _row_iter(
    view,
    topic_name: str,
    *,
    out_of_order: str,
    max_lateness_ns: int,
) -> Iterator[tuple[int, dict]]:
    """Stream rows from a topic view; see :func:`_rows_from_chunks`."""
    yield from _rows_from_chunks(
        view.iter_chunks(), topic_name,
        out_of_order=out_of_order,
        max_lateness_ns=max_lateness_ns,
    )


def _rows_from_chunks(
    chunks: Iterable[pl.DataFrame],
    topic_name: str,
    *,
    out_of_order: str,
    max_lateness_ns: int,
) -> Iterator[tuple[int, dict]]:
    """Stream rows from a topic's chunks, applying the out-of-order policy.

    Yields (timestamp_ns, row_dict) tuples. The row_dict has columns
    prefixed with ``topic_name__`` (slashes -> underscores) so multiple
    topics can share an output frame without column collisions. Row
    dicts are built lazily, not a chunk's worth at a time.

    Out-of-order policy:
      - "error": raise SyncOutOfOrderError on the first regression.
      - "warn_drop": log + drop regressing samples.
      - "reorder": bounded watermark reorder buffer.
    """
    safe_prefix = topic_name.lstrip("/").replace("/", "_")
    last_ts: int | None = None

    if out_of_order == "reorder":
        # Watermark-style reorder buffer. Keep messages in a min-heap
        # by timestamp; emit anything older than (max_seen - lateness).
        import heapq
        heap: list[tuple[int, dict]] = []
        max_seen: int | None = None

        for chunk in chunks:
            if chunk.height == 0:
                continue
            ts_arr = chunk["timestamp_ns"].to_numpy()
            for ts, row in zip(ts_arr, chunk.iter_rows(named=True)):
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
    for chunk in chunks:
        if chunk.height == 0:
            continue
        ts_arr = chunk["timestamp_ns"].to_numpy()
        for ts, row in zip(ts_arr, chunk.iter_rows(named=True)):
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
) -> Iterator[dict]:
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

        yield merged


def _streaming_sample_and_hold(
    anchor_name: str,
    anchor_iter: Iterator[tuple[int, dict]],
    non_anchor_iters: dict[str, Iterator[tuple[int, dict]]],
    *,
    max_buffer_messages: int,
) -> Iterator[dict]:
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

        yield merged


def _streaming_interpolate(
    anchor_name: str,
    anchor_iter: Iterator[tuple[int, dict]],
    non_anchor_iters: dict[str, Iterator[tuple[int, dict]]],
    *,
    boundary: str,
    max_buffer_messages: int,
) -> Iterator[dict]:
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
        yield merged

