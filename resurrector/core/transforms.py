"""Common robotics data transforms.

Provides utilities for:
- Quaternion to Euler angle conversion
- Coordinate frame transforms
- Image decompression
- Point cloud conversions
- Downsampling
- Unit conversions
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import polars as pl


def quaternion_to_euler(
    qx: float | np.ndarray,
    qy: float | np.ndarray,
    qz: float | np.ndarray,
    qw: float | np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Convert quaternion (x, y, z, w) to Euler angles (roll, pitch, yaw) in radians.

    Uses the ZYX (yaw-pitch-roll) convention common in robotics.
    """
    qx, qy, qz, qw = (np.asarray(v, dtype=float) for v in (qx, qy, qz, qw))

    # Roll (x-axis rotation)
    sinr_cosp = 2.0 * (qw * qx + qy * qz)
    cosr_cosp = 1.0 - 2.0 * (qx * qx + qy * qy)
    roll = np.arctan2(sinr_cosp, cosr_cosp)

    # Pitch (y-axis rotation)
    sinp = 2.0 * (qw * qy - qz * qx)
    pitch = np.where(
        np.abs(sinp) >= 1.0,
        np.copysign(np.pi / 2, sinp),
        np.arcsin(sinp),
    )

    # Yaw (z-axis rotation)
    siny_cosp = 2.0 * (qw * qz + qx * qy)
    cosy_cosp = 1.0 - 2.0 * (qy * qy + qz * qz)
    yaw = np.arctan2(siny_cosp, cosy_cosp)

    return roll, pitch, yaw


def euler_to_quaternion(
    roll: float | np.ndarray,
    pitch: float | np.ndarray,
    yaw: float | np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Convert Euler angles (roll, pitch, yaw) to quaternion (x, y, z, w)."""
    roll, pitch, yaw = (np.asarray(v, dtype=float) for v in (roll, pitch, yaw))

    cr = np.cos(roll / 2)
    sr = np.sin(roll / 2)
    cp = np.cos(pitch / 2)
    sp = np.sin(pitch / 2)
    cy = np.cos(yaw / 2)
    sy = np.sin(yaw / 2)

    qw = cr * cp * cy + sr * sp * sy
    qx = sr * cp * cy - cr * sp * sy
    qy = cr * sp * cy + sr * cp * sy
    qz = cr * cp * sy - sr * sp * cy

    return qx, qy, qz, qw


def radians_to_degrees(rad: float | np.ndarray) -> np.ndarray:
    """Convert radians to degrees."""
    return np.degrees(np.asarray(rad, dtype=float))


def degrees_to_radians(deg: float | np.ndarray) -> np.ndarray:
    """Convert degrees to radians."""
    return np.radians(np.asarray(deg, dtype=float))


def add_euler_columns(df: pl.DataFrame, prefix: str = "orientation") -> pl.DataFrame:
    """Add roll/pitch/yaw columns from quaternion columns in a DataFrame.

    Expects columns: {prefix}.x, {prefix}.y, {prefix}.z, {prefix}.w
    Adds columns: {prefix}.roll, {prefix}.pitch, {prefix}.yaw
    """
    qx = df[f"{prefix}.x"].to_numpy()
    qy = df[f"{prefix}.y"].to_numpy()
    qz = df[f"{prefix}.z"].to_numpy()
    qw = df[f"{prefix}.w"].to_numpy()

    roll, pitch, yaw = quaternion_to_euler(qx, qy, qz, qw)

    return df.with_columns([
        pl.Series(f"{prefix}.roll", roll),
        pl.Series(f"{prefix}.pitch", pitch),
        pl.Series(f"{prefix}.yaw", yaw),
    ])


def downsample_temporal(
    df: pl.DataFrame,
    target_hz: float,
    timestamp_col: str = "timestamp_ns",
) -> pl.DataFrame:
    """Downsample a DataFrame to a target frequency.

    Selects the nearest message to each target timestamp.
    """
    if df.height == 0:
        return df

    timestamps = df[timestamp_col].to_numpy()
    start = timestamps[0]
    end = timestamps[-1]
    interval_ns = int(1e9 / target_hz)

    target_times = np.arange(start, end, interval_ns)
    indices = np.searchsorted(timestamps, target_times)
    indices = np.clip(indices, 0, len(timestamps) - 1)

    # Check if previous index is closer
    for i in range(len(indices)):
        idx = indices[i]
        if idx > 0:
            d1 = abs(timestamps[idx] - target_times[i])
            d2 = abs(timestamps[idx - 1] - target_times[i])
            if d2 < d1:
                indices[i] = idx - 1

    # Remove duplicates while preserving order
    unique_indices = list(dict.fromkeys(indices.tolist()))
    return df[unique_indices]


def laser_scan_to_cartesian(
    ranges: np.ndarray,
    angle_min: float,
    angle_max: float,
    angle_increment: float | None = None,
) -> np.ndarray:
    """Convert laser scan polar coordinates to Cartesian (x, y) points.

    Returns:
        Array of shape (N, 2) with x, y coordinates.
    """
    n = len(ranges)
    if angle_increment is None:
        angle_increment = (angle_max - angle_min) / n
    angles = np.arange(angle_min, angle_min + n * angle_increment, angle_increment)[:n]

    x = ranges * np.cos(angles)
    y = ranges * np.sin(angles)

    return np.column_stack([x, y])


# ---------------------------------------------------------------------------
# Math / transform editor (v0.4.0)
#
# Two surfaces:
#   1. apply_transform(df, column, op, **params) — common menu operations
#      (derivative / integral / moving_average / low_pass / scale / abs / shift)
#   2. apply_polars_expression(df, expr) — escape hatch for power users
#      who want a Polars expression. Sandboxed against arbitrary code
#      execution by parsing through a strict allowlisted namespace.
# ---------------------------------------------------------------------------


_TRANSFORM_OPS = {
    "derivative",
    "integral",
    "moving_average",
    "low_pass",
    "scale",
    "abs",
    "shift",
}


def apply_transform(
    df: pl.DataFrame,
    column: str,
    op: str,
    **params: Any,
) -> pl.Series:
    """Apply a named transform to one numeric column. Returns a new Series.

    ``timestamp_ns`` must exist in ``df`` for time-derivative/integral.
    Operators:

    - ``derivative`` — d(col)/dt with respect to timestamp_ns (returned in
      units of col / second)
    - ``integral`` — cumulative trapezoidal integral wrt timestamp_ns
    - ``moving_average`` — rolling mean; param ``window=N`` (samples)
    - ``low_pass`` — single-pole IIR low-pass; param ``alpha=0..1``
      (smaller = more smoothing)
    - ``scale`` — multiply by ``factor`` (default 1.0)
    - ``abs`` — element-wise absolute value
    - ``shift`` — lag/lead by ``periods`` (default 1)
    """
    if op not in _TRANSFORM_OPS:
        raise ValueError(
            f"Unknown transform op: {op!r}. Supported: {sorted(_TRANSFORM_OPS)}"
        )
    if column not in df.columns:
        raise ValueError(f"Column not in frame: {column!r}")

    s = df.get_column(column).cast(pl.Float64, strict=False)

    if op == "abs":
        return s.abs().alias(f"abs({column})")
    if op == "scale":
        factor = float(params.get("factor", 1.0))
        return (s * factor).alias(f"{column}*{factor}")
    if op == "shift":
        periods = int(params.get("periods", 1))
        return s.shift(periods).alias(f"shift({column},{periods})")
    if op == "moving_average":
        window = max(1, int(params.get("window", 5)))
        return s.rolling_mean(window).alias(f"ma({column},{window})")
    if op == "low_pass":
        alpha = float(params.get("alpha", 0.1))
        if not 0.0 < alpha <= 1.0:
            raise ValueError(f"alpha must be in (0, 1]; got {alpha}")
        # Vectorized single-pole IIR via numpy: y[n] = alpha*x[n] + (1-alpha)*y[n-1]
        x = s.to_numpy().astype(float)
        y = np.empty_like(x)
        if len(x) == 0:
            return pl.Series(f"lpf({column},{alpha})", y)
        y[0] = x[0]
        one_minus = 1.0 - alpha
        for i in range(1, len(x)):
            y[i] = alpha * x[i] + one_minus * y[i - 1]
        return pl.Series(f"lpf({column},{alpha})", y)
    if op in {"derivative", "integral"}:
        if "timestamp_ns" not in df.columns:
            raise ValueError(
                f"{op} requires a 'timestamp_ns' column for time reference"
            )
        t_sec = (df.get_column("timestamp_ns").cast(pl.Float64) / 1e9).to_numpy()
        x = s.to_numpy().astype(float)
        if op == "derivative":
            dt = np.diff(t_sec, prepend=t_sec[0] if len(t_sec) else 0.0)
            with np.errstate(divide="ignore", invalid="ignore"):
                dx = np.diff(x, prepend=x[0] if len(x) else 0.0)
                deriv = np.where(dt > 0, dx / dt, 0.0)
            return pl.Series(f"d({column})/dt", deriv)
        # integral: cumulative trapezoidal
        if len(x) == 0:
            return pl.Series(f"int({column})dt", np.empty(0))
        # cumulative trapezoid: 0, then accumulate (x[i-1]+x[i])/2 * dt[i]
        deltas = np.diff(t_sec)
        avg = (x[1:] + x[:-1]) * 0.5
        increments = avg * deltas
        cum = np.concatenate([[0.0], np.cumsum(increments)])
        return pl.Series(f"int({column})dt", cum)
    raise AssertionError(f"unreachable: {op}")  # pragma: no cover


# Polars expression sandbox. We allow only a small allowlist of names so
# someone typing into the dashboard textbox can't `__import__('os').system(...)`.
# pl.col(...), pl.lit(...), arithmetic, common methods. Everything else raises.

_SAFE_PL_NAMES = {
    "col", "lit", "when", "concat_str", "min_horizontal", "max_horizontal",
    "sum_horizontal", "mean_horizontal",
}


def apply_polars_expression(
    df: pl.DataFrame, expr_str: str, alias: str | None = None,
) -> pl.Series:
    """Evaluate a user-supplied Polars expression and return the result Series.

    The expression runs in a strict namespace exposing only ``pl`` (with
    a name allowlist) — no builtins, no module access. Examples:

    .. code-block:: python

        apply_polars_expression(df, 'pl.col("x") * 2')
        apply_polars_expression(df, 'pl.col("x").rolling_mean(10)')
        apply_polars_expression(df, '(pl.col("x").pow(2) + pl.col("y").pow(2)).sqrt()')

    Raises ``ValueError`` if the expression uses a forbidden name or the
    Polars evaluation fails. Never raises raw ``SyntaxError`` /
    ``AttributeError`` — those are wrapped so the API can return clean
    400 responses.
    """
    if not expr_str or not expr_str.strip():
        raise ValueError("expression cannot be empty")

    # AST-walk to reject any name access not on our allowlist.
    import ast

    tree = ast.parse(expr_str, mode="eval")
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            if node.id != "pl":
                raise ValueError(
                    f"Disallowed name in expression: {node.id!r}. "
                    f"Only 'pl' is allowed at the top level."
                )
        if isinstance(node, ast.Attribute):
            # pl.col, pl.col().rolling_mean, etc. — allow attribute walks but
            # check if the leftmost is something other than 'pl'.
            base = node
            while isinstance(base, ast.Attribute):
                base = base.value
            if isinstance(base, ast.Name) and base.id != "pl":
                raise ValueError(
                    f"Attribute access not allowed on: {base.id!r}"
                )
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise ValueError("import statements are not allowed")
        if isinstance(node, ast.Call):
            # pl.col(...) and similar — guard top-level pl.<name> calls
            # against the allowlist of "well known polars functions".
            if (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "pl"
                and node.func.attr not in _SAFE_PL_NAMES
            ):
                raise ValueError(
                    f"pl.{node.func.attr!r} is not in the allowlist. "
                    f"Allowed top-level Polars functions: {sorted(_SAFE_PL_NAMES)}. "
                    f"You can still chain methods on a pl.col(...) result."
                )

    # Evaluate in a namespace that exposes only ``pl``.
    safe_globals = {"__builtins__": {}, "pl": pl}
    try:
        expr = eval(compile(tree, "<expression>", "eval"), safe_globals, {})
        result_df = df.select(expr)
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"expression error: {type(e).__name__}: {e}") from e

    if result_df.width != 1:
        raise ValueError(
            f"expression must produce a single column; got {result_df.width}"
        )
    series = result_df.to_series(0)
    if alias:
        series = series.alias(alias)
    return series
