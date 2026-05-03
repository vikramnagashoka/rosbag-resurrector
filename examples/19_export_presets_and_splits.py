"""Guided export presets + dataset splits (v0.5.0).

Demonstrates:
  - ``BagFrame.export(preset=...)`` — five named bundles for ML pipelines
  - ``BagFrame.export(split={...}, split_strategy=...)`` — train/val/test
    output with a per-run manifest
  - Combining the two — preset chooses the format/sync, split slices the
    output into reproducible chunks

Run:
    python examples/19_export_presets_and_splits.py

What you'll see: the synthetic bag exported under three preset+split
combinations into ``_exploration_output/v05_presets_*``. Each split
folder lands as a real, ML-ready directory you can inspect.
"""

from __future__ import annotations

import json

from _common import ensure_output_dir, ensure_sample_bag, header, section

from resurrector import BagFrame
from resurrector.core.export import PRESETS, resolve_preset


def main() -> None:
    header("19 — v0.5.0: export presets + dataset splits")
    bag_path = ensure_sample_bag()
    out_root = ensure_output_dir()
    bf = BagFrame(bag_path)

    section("Available presets")
    for name, preset in PRESETS.items():
        print(f"  {name:<18} → format={preset.format:<10} sync={str(preset.sync):<5} "
              f"hz={preset.downsample_hz}  filter={preset.topic_filter}")
        print(f"  {' ' * 18}   {preset.description}")

    section("Resolved config: 'training-tabular' with no overrides")
    cfg = resolve_preset("training-tabular")
    print(f"  format={cfg['format']}  sync={cfg['sync']}  hz={cfg['downsample_hz']}")

    section("Resolved config: 'training-tabular' with downsample override")
    cfg = resolve_preset("training-tabular", downsample_hz=25)
    print(f"  format={cfg['format']}  sync={cfg['sync']}  hz={cfg['downsample_hz']}")
    print("  (user override beats the preset default — same pattern in CLI/dashboard)")

    section("Export 1 — 'training-tabular' preset, no split")
    out1 = out_root / "v05_presets_tabular"
    bf.export(preset="training-tabular", output=str(out1))
    files = sorted(out1.rglob("*"))
    print(f"  {out1.relative_to(out_root.parent)}/  ({len(files)} files)")
    for p in files[:5]:
        print(f"    {p.relative_to(out1)}")

    section("Export 2 — 'training-tabular' preset + 80/10/10 time split")
    out2 = out_root / "v05_presets_tabular_split"
    bf.export(
        preset="training-tabular",
        output=str(out2),
        split={"train": 0.8, "val": 0.1, "test": 0.1},
        split_strategy="time",
    )
    print(f"  {out2.relative_to(out_root.parent)}/  →")
    for sub in sorted(out2.iterdir()):
        if sub.is_dir():
            n = len(list(sub.rglob("*")))
            print(f"    {sub.name}/   ({n} files)")
        else:
            print(f"    {sub.name}")
    manifest_path = out2 / "split_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        print(f"  manifest.json: strategy={manifest['strategy']!r}, "
              f"duration_sec={manifest.get('duration_sec', 'n/a'):.2f}")
        for name, info in manifest.get("splits", {}).items():
            print(f"    {name}: {info['start_sec']:.2f}s → {info['end_sec']:.2f}s "
                  f"(ratio={info['ratio']})")

    section("Export 3 — random split for row-independent training")
    out3 = out_root / "v05_presets_random_split"
    bf.export(
        topics=["/imu/data", "/joint_states"],
        format="parquet",
        output=str(out3),
        sync=True,
        sync_method="nearest",
        split={"train": 0.7, "holdout": 0.3},
        split_strategy="random",
    )
    manifest = json.loads((out3 / "split_manifest.json").read_text())
    print(f"  strategy={manifest['strategy']!r} "
          f"(seed={manifest.get('seed', 'n/a')}; deterministic across runs)")
    for name, info in manifest.get("splits", {}).items():
        # Random splits report row counts per topic, not a time window
        print(f"    {name}: ratio={info.get('ratio')}  rows={info.get('rows', 'n/a')}")

    print(
        "\n  ✓ Time strategy preserves temporal locality — best for time-series\n"
        "    forecasting and avoiding train/val leakage. Random strategy is\n"
        "    deterministic (seed=42) — safe for row-independent setups.\n"
        "    'stratified' raises NotImplementedError pointing to v0.6+.\n"
    )


if __name__ == "__main__":
    main()
