"""Versioned dataset collections — the bridge between bags and ML pipelines.

Demonstrates: ``DatasetManager`` (resurrector/core/dataset.py).

Run:
    python examples/08_datasets_versioning.py

What you'll see: create a named dataset, add a version that bundles
specific bag refs + sync config + topic selection, export it. The
output directory has a manifest.json (SHA256 per file), a
dataset_config.json (full re-creation config), and an auto-generated
README — full reproducibility for ML training pipelines.
"""

from __future__ import annotations

from _common import ensure_output_dir, ensure_sample_bag, header, section

from resurrector.core.dataset import (
    BagRef,
    DatasetManager,
    DatasetMetadata,
    SyncConfig,
)


def main() -> None:
    header("08 — Versioned datasets")
    bag_path = ensure_sample_bag()
    out = ensure_output_dir()

    db_path = out / "scratch_datasets.db"
    if db_path.exists():
        db_path.unlink()

    section("Create a dataset")
    mgr = DatasetManager(db_path=db_path)
    ds_id = mgr.create(
        name="manipulation-demos-v1",
        description="Pick-and-place demos for the toy 6-DOF arm",
    )
    print(f"  Created 'manipulation-demos-v1' (id={ds_id})")

    section("Add a version with bag refs + sync config + metadata")
    mgr.create_version(
        dataset_name="manipulation-demos-v1",
        version="1.0",
        bag_refs=[
            BagRef(
                path=str(bag_path),
                topics=["/imu/data", "/joint_states"],
                start_time="0s",
                end_time="3s",
            ),
        ],
        sync_config=SyncConfig(method="nearest", tolerance_ms=25),
        export_format="parquet",
        downsample_hz=50,
        metadata=DatasetMetadata(
            description="3-second slice for sanity tests",
            license="MIT",
            robot_type="6-DOF arm (synthetic)",
            task="pick_and_place",
            tags=["manipulation", "imitation-learning", "test-fixture"],
        ),
    )
    print(f"  Added version 1.0")

    section("List versions")
    ds = mgr.get_dataset("manipulation-demos-v1")
    print(f"  Dataset: {ds['name']}")
    print(f"  Description: {ds['description']}")
    print(f"  Versions: {[v['version'] for v in ds['versions']]}")

    section("Export the version")
    output_dir = out / "exported_dataset"
    output_path = mgr.export_version(
        "manipulation-demos-v1", "1.0", output_dir=str(output_dir),
    )
    print(f"  Exported to: {output_path}")

    section("Inspect the output structure")
    files = sorted(p for p in output_path.rglob("*") if p.is_file())
    for p in files:
        rel = p.relative_to(output_path)
        size = p.stat().st_size
        size_str = f"{size:,} B" if size < 1024 else f"{size // 1024} KB"
        print(f"    {rel}   ({size_str})")

    section("Look at the auto-generated README")
    readme = output_path / "README.md"
    if readme.exists():
        text = readme.read_text(encoding="utf-8")
        for line in text.splitlines()[:20]:
            print(f"    {line}")
        if len(text.splitlines()) > 20:
            print(f"    ... ({len(text.splitlines()) - 20} more lines)")

    mgr.close()
    print(
        "\n  ✓ Every version produces:\n"
        "    • the data files (per export_format)\n"
        "    • manifest.json with SHA256 hashes for every file\n"
        "    • dataset_config.json with the full recreation config\n"
        "    • README.md with sources, config, and a load snippet\n"
        "    Reproducibility you can drop into a training pipeline.\n"
    )


if __name__ == "__main__":
    main()
