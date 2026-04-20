"""Tests for LTTB downsampling."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from resurrector.core.downsample import (
    _lttb_indices,
    downsample_dataframe,
    lttb,
)


class TestLttb:
    def test_returns_input_when_smaller_than_max(self):
        ts = np.array([1, 2, 3], dtype=np.int64)
        ys = np.array([1.0, 2.0, 3.0])
        out_ts, out_ys = lttb(ts, ys, max_points=10)
        np.testing.assert_array_equal(out_ts, ts)
        np.testing.assert_array_equal(out_ys, ys)

    def test_caps_at_max_points(self):
        ts = np.arange(10_000, dtype=np.int64)
        ys = np.sin(ts / 100.0)
        out_ts, out_ys = lttb(ts, ys, max_points=500)
        assert len(out_ts) == 500
        assert len(out_ys) == 500

    def test_preserves_endpoints(self):
        ts = np.arange(1000, dtype=np.int64)
        ys = np.random.default_rng(42).standard_normal(1000)
        out_ts, out_ys = lttb(ts, ys, max_points=100)
        assert out_ts[0] == ts[0]
        assert out_ts[-1] == ts[-1]
        assert out_ys[0] == ys[0]
        assert out_ys[-1] == ys[-1]

    def test_preserves_visual_shape_of_spike(self):
        """A spike in the middle of the series must survive downsampling."""
        ts = np.arange(2000, dtype=np.int64)
        ys = np.zeros(2000)
        ys[1000] = 100.0  # obvious spike
        out_ts, out_ys = lttb(ts, ys, max_points=100)
        assert out_ys.max() >= 50.0  # spike kept (possibly neighbour)

    def test_rejects_mismatched_lengths(self):
        with pytest.raises(ValueError, match="same length"):
            lttb([1, 2, 3], [1.0, 2.0], max_points=3)

    def test_rejects_tiny_max_points(self):
        with pytest.raises(ValueError, match=">= 3"):
            lttb([1, 2, 3], [1.0, 2.0, 3.0], max_points=2)

    def test_handles_nan_values(self):
        ts = np.arange(1000, dtype=np.int64)
        ys = np.ones(1000)
        ys[500] = np.nan
        # Must not crash.
        out_ts, out_ys = lttb(ts, ys, max_points=100)
        assert len(out_ts) == 100


class TestLttbIndices:
    def test_monotonic(self):
        ts = np.arange(1000, dtype=np.int64)
        ys = np.sin(ts / 50.0)
        idx = _lttb_indices(ts, ys, max_points=50)
        assert idx == sorted(idx)
        assert idx[0] == 0
        assert idx[-1] == 999


class TestDownsampleDataframe:
    def test_passthrough_when_smaller(self):
        df = pl.DataFrame({
            "timestamp_ns": [1, 2, 3],
            "x": [0.0, 1.0, 0.0],
        })
        out = downsample_dataframe(df, max_points=100)
        assert out.height == 3

    def test_reduces_to_max_points(self):
        ts = np.arange(5000, dtype=np.int64)
        df = pl.DataFrame({
            "timestamp_ns": ts,
            "x": np.sin(ts / 50.0),
            "y": np.cos(ts / 50.0),
        })
        out = downsample_dataframe(df, max_points=200)
        assert out.height == 200
        # Both columns still present
        assert "x" in out.columns and "y" in out.columns

    def test_rows_stay_aligned_across_columns(self):
        """LTTB picks one index set and slices all columns together,
        so the same row of the output has matching x and y."""
        ts = np.arange(1000, dtype=np.int64)
        df = pl.DataFrame({
            "timestamp_ns": ts,
            "x": ts * 1.0,
            "y": ts * 2.0,
        })
        out = downsample_dataframe(df, max_points=50)
        # y should always be exactly 2x of x in every output row.
        diff = (out["y"] - 2 * out["x"]).abs().max()
        assert diff == 0.0

    def test_drops_non_numeric_columns_gracefully(self):
        df = pl.DataFrame({
            "timestamp_ns": np.arange(500, dtype=np.int64),
            "x": np.arange(500, dtype=np.float64),
            "label": ["a"] * 500,  # non-numeric
        })
        # Should not crash; LTTB runs on the numeric column.
        out = downsample_dataframe(df, max_points=50)
        assert out.height == 50

    def test_raises_without_time_column(self):
        df = pl.DataFrame({"a": [1.0, 2.0, 3.0]})
        with pytest.raises(ValueError, match="time column"):
            downsample_dataframe(df, max_points=2)

    def test_no_numeric_columns_falls_back_to_stride(self):
        df = pl.DataFrame({
            "timestamp_ns": np.arange(1000, dtype=np.int64),
            "label": ["a"] * 1000,
        })
        out = downsample_dataframe(df, max_points=100)
        assert out.height <= 100
