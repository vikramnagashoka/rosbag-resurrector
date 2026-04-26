"""Jupyter-friendly Parquet export — drop the data into a notebook.

Demonstrates: ``trim_to_format(format='parquet')`` + the snippet pattern
the dashboard's JupyterButton uses.

Run:
    python examples/07_jupyter_export.py

What you'll see: a Parquet file written to a temp dir, plus a Python
snippet you can paste into Jupyter to load and inspect it.
"""

from __future__ import annotations

from _common import ensure_output_dir, ensure_sample_bag, header

from resurrector.core.bag_frame import BagFrame
from resurrector.core.trim import trim_to_format


def main() -> None:
    header("07 — Jupyter-friendly Parquet export")
    src = ensure_sample_bag()
    out = ensure_output_dir()

    topic = "/imu/data"
    pq_dir = out / "jupyter_export"
    pq_dir.mkdir(exist_ok=True)

    # Export the FULL bag, not a trimmed slice — same code path though.
    bf = BagFrame(src)
    full_duration = bf.duration_sec
    print(f"  Source: {src}")
    print(f"  Topic:  {topic}")
    print(f"  Window: full bag ({full_duration:.2f}s)\n")

    trim_to_format(
        src, pq_dir, start_sec=0.0, end_sec=full_duration,
        topics=[topic], format="parquet",
    )

    safe_name = topic.lstrip("/").replace("/", "_")
    parquet_file = pq_dir / f"{safe_name}.parquet"
    print(f"  ✓ Wrote {parquet_file}\n")

    # Print the snippet exactly the way the dashboard's JupyterButton
    # copies it to the user's clipboard.
    snippet = (
        "import polars as pl\n"
        f'df = pl.read_parquet("{parquet_file}")\n'
        "df.head()"
    )
    print("  ── COPY-PASTE THIS INTO A JUPYTER CELL ──")
    print()
    for line in snippet.splitlines():
        print(f"    {line}")
    print()
    print(
        "  ✓ Same flow lives behind the dashboard's 'Open in Jupyter' button.\n"
        "    The button writes a Parquet to ~/.resurrector/, copies the\n"
        "    snippet to the clipboard, and opens localhost:8888.\n"
    )


if __name__ == "__main__":
    main()
