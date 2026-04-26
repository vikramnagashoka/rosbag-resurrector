"""ML-ready export — Parquet / HDF5 / NumPy + LeRobot / RLDS formats.

Demonstrates: ``BagFrame.export()`` across every supported format.

Run:
    python examples/05_ml_export_formats.py

What you'll see: the same IMU + joint-state slice exported into 5
formats. Each is the on-disk layout a different ML pipeline expects.
LeRobot and RLDS in particular let you drop the output directly into
RT-2 / OpenX / LeRobot training code.
"""

from __future__ import annotations

from _common import ensure_output_dir, ensure_sample_bag, header, section

from resurrector import BagFrame


def main() -> None:
    header("05 — ML-ready export formats")
    bag_path = ensure_sample_bag()
    out = ensure_output_dir()
    bf = BagFrame(bag_path)

    topics = ["/imu/data", "/joint_states"]
    print(f"  Source: {bag_path.name}")
    print(f"  Topics: {topics}\n")

    section("Parquet — columnar, fastest to read in Polars/Pandas/Spark")
    pq_dir = out / "ml_parquet"
    bf.export(topics=topics, format="parquet", output=str(pq_dir))
    for p in sorted(pq_dir.glob("*.parquet")):
        print(f"    {p.name}  ({p.stat().st_size // 1024} KB)")

    section("HDF5 — mixed numeric/image, MATLAB compatibility")
    h5_dir = out / "ml_hdf5"
    bf.export(topics=topics, format="hdf5", output=str(h5_dir))
    for p in sorted(h5_dir.glob("*.h5")):
        print(f"    {p.name}  ({p.stat().st_size // 1024} KB)")

    section("NumPy — single .npz archive, Jupyter friendly")
    np_dir = out / "ml_numpy"
    bf.export(topics=topics, format="numpy", output=str(np_dir))
    for p in sorted(np_dir.glob("*.npz")):
        print(f"    {p.name}  ({p.stat().st_size // 1024} KB)")

    section("LeRobot — Hugging Face training format")
    try:
        lerobot_dir = out / "ml_lerobot"
        bf.export(topics=topics, format="lerobot", output=str(lerobot_dir),
                  sync=True, sync_method="nearest")
        files = sorted(lerobot_dir.rglob("*"))
        print(f"  Wrote {len(files)} files; structure:")
        for p in files[:8]:
            print(f"    {p.relative_to(lerobot_dir)}")
        if len(files) > 8:
            print(f"    ... and {len(files) - 8} more")
    except (ImportError, Exception) as e:
        print(f"  [INFO] LeRobot export needs the optional [lerobot] extra: {e}")

    section("RLDS — Reinforcement Learning Dataset Standard (RT-2, OpenX)")
    try:
        rlds_dir = out / "ml_rlds"
        bf.export(topics=topics, format="rlds", output=str(rlds_dir),
                  sync=True, sync_method="nearest")
        files = sorted(rlds_dir.rglob("*"))
        print(f"  Wrote {len(files)} files; structure:")
        for p in files[:8]:
            print(f"    {p.relative_to(rlds_dir)}")
        if len(files) > 8:
            print(f"    ... and {len(files) - 8} more")
    except (ImportError, Exception) as e:
        print(f"  [INFO] RLDS export needs the optional [rlds] extra: {e}")

    print(
        "\n  ✓ Synced exports collapse multi-topic streams into one row per\n"
        "    aligned timestamp. RLDS / LeRobot are pre-shaped for the\n"
        "    standard ML training loops; Parquet / HDF5 / NumPy are\n"
        "    general-purpose.\n"
    )


if __name__ == "__main__":
    main()
