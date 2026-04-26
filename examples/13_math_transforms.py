"""Math/transform editor — common ops + Polars expression escape hatch.

Demonstrates: ``resurrector/core/transforms.py`` :func:`apply_transform`
and :func:`apply_polars_expression`.

Run:
    python examples/03_math_transforms.py

What you'll see: original IMU acceleration, then derivative, then
moving average, then a custom Polars expression. Each shown as a
sparkline so the effect of the transform is visible without a chart.
"""

from __future__ import annotations

from _common import ensure_sample_bag, header, section, sparkline

from resurrector.core.bag_frame import BagFrame
from resurrector.core.transforms import (
    apply_polars_expression,
    apply_transform,
)


def main() -> None:
    header("03 — Math/transform editor")
    bag_path = ensure_sample_bag()
    bf = BagFrame(bag_path)
    df = bf["/imu/data"].to_polars()
    col = "linear_acceleration.x"

    print(f"  Topic: /imu/data  Column: {col}  Rows: {df.height}\n")

    section("Original")
    print(f"  {sparkline(df[col].to_list())}")

    section("apply_transform: derivative (d/dt)")
    deriv = apply_transform(df, col, "derivative")
    print(f"  {sparkline(deriv.to_list())}")

    section("apply_transform: moving_average (window=20)")
    ma = apply_transform(df, col, "moving_average", window=20)
    # Drop nulls at the start of the window.
    ma_vals = [v for v in ma.to_list() if v is not None]
    print(f"  {sparkline(ma_vals)}")

    section("apply_transform: low_pass (alpha=0.05)")
    lp = apply_transform(df, col, "low_pass", alpha=0.05)
    print(f"  {sparkline(lp.to_list())}")

    section("apply_polars_expression: magnitude of accel vector")
    expr = (
        '(pl.col("linear_acceleration.x").pow(2) '
        '+ pl.col("linear_acceleration.y").pow(2) '
        '+ pl.col("linear_acceleration.z").pow(2)).sqrt()'
    )
    print(f"  Expression: {expr}")
    mag = apply_polars_expression(df, expr, alias="accel_magnitude")
    print(f"  {sparkline(mag.to_list())}")

    section("Sandbox: rejected unsafe expression")
    try:
        apply_polars_expression(df, '__import__("os").system("ls")')
    except ValueError as e:
        print(f"  ✓ Caught: {e}")

    print(
        "\n  ✓ Same primitives power the dashboard's Transform editor. The\n"
        "    Common tab calls apply_transform; the Expression tab calls\n"
        "    apply_polars_expression with the user's text.\n"
    )


if __name__ == "__main__":
    main()
