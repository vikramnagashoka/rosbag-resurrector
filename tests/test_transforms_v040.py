"""Tests for v0.4.0 math/transform editor backend.

The existing tests/test_transforms.py covers the older quaternion / laser
helpers. This file covers the new apply_transform + apply_polars_expression.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from resurrector.core.transforms import (
    apply_polars_expression,
    apply_transform,
)


def linear_ramp_df(n: int = 100, slope: float = 2.0, dt_sec: float = 0.01):
    """Helper: timestamp_ns column + linear value column."""
    ts = np.arange(n, dtype=np.int64) * int(dt_sec * 1e9)
    values = np.arange(n, dtype=float) * slope * dt_sec  # slope * t
    return pl.DataFrame({"timestamp_ns": ts, "x": values})


class TestCommonOps:
    def test_abs(self):
        df = pl.DataFrame({"timestamp_ns": [0, 1, 2], "x": [-1.0, 0.0, 2.5]})
        out = apply_transform(df, "x", "abs")
        assert out.to_list() == [1.0, 0.0, 2.5]

    def test_scale(self):
        df = pl.DataFrame({"timestamp_ns": [0, 1], "x": [1.0, 2.0]})
        out = apply_transform(df, "x", "scale", factor=3.0)
        assert out.to_list() == [3.0, 6.0]

    def test_shift(self):
        df = pl.DataFrame({"timestamp_ns": [0, 1, 2], "x": [10.0, 20.0, 30.0]})
        out = apply_transform(df, "x", "shift", periods=1).to_list()
        assert out[0] is None
        assert out[1:] == [10.0, 20.0]

    def test_moving_average_smooths_a_step(self):
        # Step at index 5; window=3 should produce a transition over 3 points.
        x = [0.0] * 5 + [10.0] * 5
        df = pl.DataFrame({"timestamp_ns": list(range(10)), "x": x})
        out = apply_transform(df, "x", "moving_average", window=3).to_list()
        # Values at the step boundary must be intermediate, not 0 or 10.
        assert out[5] is not None
        assert 0.0 < out[5] < 10.0

    def test_low_pass_attenuates_step(self):
        x = [0.0] * 5 + [10.0] * 5
        df = pl.DataFrame({"timestamp_ns": list(range(10)), "x": x})
        out = apply_transform(df, "x", "low_pass", alpha=0.2).to_list()
        # First sample at the step should be far from 10.0 due to filtering.
        assert out[5] < 5.0

    def test_low_pass_rejects_bad_alpha(self):
        df = pl.DataFrame({"timestamp_ns": [0, 1], "x": [0.0, 1.0]})
        with pytest.raises(ValueError, match="alpha"):
            apply_transform(df, "x", "low_pass", alpha=1.5)
        with pytest.raises(ValueError, match="alpha"):
            apply_transform(df, "x", "low_pass", alpha=0.0)

    def test_derivative_of_linear_is_constant(self):
        # x = 2 * t  =>  dx/dt = 2 everywhere except the prepended first point
        df = linear_ramp_df(n=50, slope=2.0, dt_sec=0.01)
        out = apply_transform(df, "x", "derivative").to_numpy()
        # First sample is 0 (we prepend) — check only the steady state.
        assert out[0] == pytest.approx(0.0)
        np.testing.assert_allclose(out[1:], 2.0, atol=1e-6)

    def test_integral_of_constant_is_linear(self):
        # x = 5 everywhere, dt = 0.01s => integral at t = 5*t
        n = 50
        df = pl.DataFrame({
            "timestamp_ns": (np.arange(n) * int(0.01 * 1e9)).tolist(),
            "x": [5.0] * n,
        })
        out = apply_transform(df, "x", "integral").to_numpy()
        # Final value should be ~5 * (n-1) * 0.01 = 2.45
        assert out[-1] == pytest.approx(5.0 * (n - 1) * 0.01, abs=1e-6)

    def test_derivative_requires_timestamp(self):
        df = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
        with pytest.raises(ValueError, match="timestamp_ns"):
            apply_transform(df, "x", "derivative")

    def test_unknown_op(self):
        df = pl.DataFrame({"timestamp_ns": [0], "x": [1.0]})
        with pytest.raises(ValueError, match="Unknown"):
            apply_transform(df, "x", "magic")

    def test_unknown_column(self):
        df = pl.DataFrame({"timestamp_ns": [0], "x": [1.0]})
        with pytest.raises(ValueError, match="Column not in frame"):
            apply_transform(df, "ghost", "abs")


class TestPolarsExpressionSandbox:
    def test_simple_arithmetic(self):
        df = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
        out = apply_polars_expression(df, 'pl.col("x") * 2')
        assert out.to_list() == [2.0, 4.0, 6.0]

    def test_chained_method(self):
        df = pl.DataFrame({"x": [-1.0, 0.0, 2.0]})
        out = apply_polars_expression(df, 'pl.col("x").abs()')
        assert out.to_list() == [1.0, 0.0, 2.0]

    def test_pow_and_sqrt(self):
        df = pl.DataFrame({"x": [3.0], "y": [4.0]})
        out = apply_polars_expression(
            df, '(pl.col("x").pow(2) + pl.col("y").pow(2)).sqrt()',
        )
        assert out.to_list() == pytest.approx([5.0])

    def test_alias(self):
        df = pl.DataFrame({"x": [1.0]})
        out = apply_polars_expression(df, 'pl.col("x")', alias="renamed")
        assert out.name == "renamed"

    def test_rejects_empty(self):
        df = pl.DataFrame({"x": [1.0]})
        with pytest.raises(ValueError, match="empty"):
            apply_polars_expression(df, "")

    def test_rejects_bare_name(self):
        df = pl.DataFrame({"x": [1.0]})
        with pytest.raises(ValueError, match="Disallowed name"):
            apply_polars_expression(df, "x")

    def test_rejects_import(self):
        df = pl.DataFrame({"x": [1.0]})
        with pytest.raises(ValueError):
            # ast.parse rejects 'import' in expression mode, but 'eval'
            # mode disallows imports anyway. Use __import__ trick to cover.
            apply_polars_expression(df, '__import__("os").system("ls")')

    def test_rejects_underscore_dunder_attribute(self):
        df = pl.DataFrame({"x": [1.0]})
        with pytest.raises(ValueError):
            apply_polars_expression(df, '().__class__.__bases__')

    def test_rejects_unknown_pl_function(self):
        df = pl.DataFrame({"x": [1.0]})
        with pytest.raises(ValueError, match="not in the allowlist"):
            apply_polars_expression(df, 'pl.read_csv("evil.csv")')

    def test_multi_column_expression_raises(self):
        df = pl.DataFrame({"x": [1.0]})
        # An expression that produces 2 columns (impossible with our select(),
        # but a struct.unnest() etc. could in principle); just confirm the
        # single-column guard exists by passing a struct.
        with pytest.raises(ValueError):
            apply_polars_expression(
                df, 'pl.col("x").pow(2).struct.unnest()',
            )

    def test_runtime_error_wrapped(self):
        df = pl.DataFrame({"x": [1.0]})
        with pytest.raises(ValueError, match="expression error"):
            apply_polars_expression(df, 'pl.col("ghost_col") * 2')
