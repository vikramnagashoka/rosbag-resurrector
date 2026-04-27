"""Tests for the bucketed min/max streaming aggregator.

The aggregator replaces eager view.to_polars() + LTTB downsampling on
the dashboard plot endpoint. It must:
- Produce a bounded number of points (~ 2 * num_buckets per column).
- Preserve the visual envelope of the input (global min and max land
  in the output).
- Handle empty inputs and out-of-range timestamps.
- Cope with cross-chunk schema drift gracefully.
"""

from __future__ import annotations

import math

import numpy as np
import polars as pl
import pytest

from resurrector.core.streaming import stream_bucketed_minmax


def _synth_chunks(n_total: int, n_chunks: int, value_fn) -> list[pl.DataFrame]:
    """Build n_chunks DataFrames each with ~n_total/n_chunks rows.

    timestamp_ns is monotonically increasing 1, 2, 3, .... value_fn(i)
    produces the value column.
    """
    rows_per_chunk = math.ceil(n_total / n_chunks)
    chunks = []
    for c in range(n_chunks):
        lo = c * rows_per_chunk
        hi = min(lo + rows_per_chunk, n_total)
        if lo >= hi:
            break
        ts = list(range(lo, hi))
        vals = [value_fn(i) for i in ts]
        chunks.append(pl.DataFrame({"timestamp_ns": ts, "v": vals}))
    return chunks


class TestBucketedMinmax:
    def test_output_size_bounded_by_buckets(self):
        chunks = _synth_chunks(10_000, n_chunks=20, value_fn=lambda i: float(i))
        out = stream_bucketed_minmax(
            chunks, num_buckets=50, time_range=(0, 9999),
        )
        # At most 2 points per bucket — but min and max can coincide
        # at the same timestamp (boundary), so the bound is loose.
        assert out.height <= 2 * 50
        assert out.height > 0
        # Output must be sorted by time.
        ts = out["timestamp_ns"].to_list()
        assert ts == sorted(ts)

    def test_global_min_and_max_preserved(self):
        # Inject a clear spike at t=5000 and a dip at t=8000.
        def vfn(i):
            if i == 5000:
                return 999.0
            if i == 8000:
                return -999.0
            return float(i % 10)
        chunks = _synth_chunks(10_000, n_chunks=20, value_fn=vfn)
        out = stream_bucketed_minmax(
            chunks, num_buckets=20, time_range=(0, 9999),
        )
        assert out["v"].max() == 999.0
        assert out["v"].min() == -999.0

    def test_single_bucket_returns_global_extremes(self):
        chunks = _synth_chunks(1000, n_chunks=4, value_fn=lambda i: float(i))
        out = stream_bucketed_minmax(
            chunks, num_buckets=1, time_range=(0, 999),
        )
        # 1 bucket -> at most 2 points (min and max)
        assert out.height <= 2
        assert out["v"].max() == 999.0
        assert out["v"].min() == 0.0

    def test_empty_chunks_return_empty_frame(self):
        out = stream_bucketed_minmax(
            iter([]), num_buckets=10, value_cols=["v"],
        )
        assert out.height == 0
        # Schema preserved when value_cols is supplied.
        assert "timestamp_ns" in out.columns
        assert "v" in out.columns

    def test_out_of_range_rows_dropped(self):
        # Bag spans ts 0-9999 but we request range 1000-2000.
        chunks = _synth_chunks(10_000, n_chunks=4, value_fn=lambda i: float(i))
        out = stream_bucketed_minmax(
            chunks, num_buckets=10, time_range=(1000, 2000),
        )
        # Every output ts must be inside [1000, 2000].
        ts_arr = out["timestamp_ns"].to_numpy()
        assert (ts_arr >= 1000).all()
        assert (ts_arr <= 2000).all()
        assert out["v"].max() <= 2000.0
        assert out["v"].min() >= 1000.0

    def test_schema_drift_tolerated(self):
        # First chunk has columns v + extra; second chunk only has v.
        c1 = pl.DataFrame({"timestamp_ns": [0, 1, 2], "v": [1.0, 2.0, 3.0], "extra": [0, 0, 0]})
        c2 = pl.DataFrame({"timestamp_ns": [3, 4, 5], "v": [4.0, 5.0, 6.0]})
        out = stream_bucketed_minmax(
            [c1, c2], num_buckets=2, value_cols=["v"], time_range=(0, 5),
        )
        assert out["v"].min() == 1.0
        assert out["v"].max() == 6.0

    def test_value_col_auto_detect(self):
        c = pl.DataFrame({"timestamp_ns": [0, 1, 2, 3], "a": [1.0, 2.0, 3.0, 4.0]})
        out = stream_bucketed_minmax(
            [c], num_buckets=1, time_range=(0, 3),
        )
        # Auto-detected the numeric "a" column.
        assert "a" in out.columns
        assert out["a"].max() == 4.0

    def test_invalid_num_buckets_raises(self):
        with pytest.raises(ValueError):
            stream_bucketed_minmax(iter([]), num_buckets=0)

    def test_memory_bound_independent_of_input_size(self):
        """Smoke test: feeding 100k rows uses the same state as 10k rows."""
        # Not a memory measurement — just verifies output size doesn't
        # scale with input.
        chunks_small = _synth_chunks(10_000, n_chunks=10, value_fn=lambda i: float(i))
        chunks_big = _synth_chunks(100_000, n_chunks=100, value_fn=lambda i: float(i))
        small = stream_bucketed_minmax(chunks_small, num_buckets=50, time_range=(0, 9999))
        big = stream_bucketed_minmax(chunks_big, num_buckets=50, time_range=(0, 99999))
        # Both bounded by 2 * num_buckets
        assert small.height <= 100
        assert big.height <= 100
