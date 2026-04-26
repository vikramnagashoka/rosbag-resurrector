"""Trim a time range from a bag and export to multiple formats.

Demonstrates: ``resurrector/core/trim.py`` :func:`trim_to_format`.

Run:
    python examples/02_trim_to_mcap.py

What you'll see: a [1.0s, 3.0s] slice of the demo bag exported four
ways into ``./_exploration_output/``. The MCAP output is byte-identical
to a recording over the same window — re-openable by any MCAP tool.
"""

from __future__ import annotations

from _common import ensure_output_dir, ensure_sample_bag, header

from resurrector.core.bag_frame import BagFrame
from resurrector.core.trim import trim_to_format


def main() -> None:
    header("02 — Trim a time range to MCAP / Parquet / CSV / NumPy")
    src = ensure_sample_bag()
    out = ensure_output_dir()

    start_sec, end_sec = 1.0, 3.0
    topics = ["/imu/data", "/joint_states"]

    print(f"  Source: {src}")
    print(f"  Window: [{start_sec}s, {end_sec}s]  Topics: {topics}\n")

    # MCAP: lossless slice. Re-openable by any MCAP-aware tool.
    mcap_out = out / "trimmed.mcap"
    trim_to_format(src, mcap_out, start_sec, end_sec, topics, format="mcap")
    bf = BagFrame(mcap_out)
    print(f"  ✓ MCAP    -> {mcap_out.name}  ({bf.message_count} messages)")

    # Parquet: per-topic columnar files in a directory.
    pq_dir = out / "parquet_trim"
    pq_dir.mkdir(exist_ok=True)
    trim_to_format(src, pq_dir, start_sec, end_sec, topics, format="parquet")
    print(f"  ✓ Parquet -> {pq_dir.name}/  ({len(list(pq_dir.glob('*.parquet')))} files)")

    # CSV: same shape, plain text.
    csv_dir = out / "csv_trim"
    csv_dir.mkdir(exist_ok=True)
    trim_to_format(src, csv_dir, start_sec, end_sec, topics, format="csv")
    print(f"  ✓ CSV     -> {csv_dir.name}/  ({len(list(csv_dir.glob('*.csv')))} files)")

    # NumPy archive.
    npz_dir = out / "numpy_trim"
    npz_dir.mkdir(exist_ok=True)
    trim_to_format(src, npz_dir, start_sec, end_sec, topics, format="numpy")
    print(f"  ✓ NumPy   -> {npz_dir.name}/  ({len(list(npz_dir.glob('*.npz')))} files)\n")

    print(
        "  ✓ All four outputs in ./_exploration_output/. Re-open the MCAP with\n"
        "    `resurrector info ./_exploration_output/trimmed.mcap` to confirm\n"
        "    the message count and topic types are preserved.\n"
    )


if __name__ == "__main__":
    main()
