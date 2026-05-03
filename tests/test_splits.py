"""Tests for the dataset-split system (Option 1.4).

- validate_split_ratios() catches malformed specs
- _cumulative_time_boundaries() math
- split_export(strategy="time") writes per-split subdirs + manifest
- split_export(strategy="random") preserves total row count + manifest
- split_export(strategy="stratified") raises NotImplementedError
- BagFrame.export(split=..., split_strategy=...) end-to-end
- CLI `--split train=0.8 --split val=0.1 --split test=0.1`
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from resurrector.cli.main import app
from resurrector.core.splits import (
    _cumulative_time_boundaries,
    split_export,
    validate_split_ratios,
)


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def synth_bag(tmp_dir: Path) -> Path:
    """Synthetic bag: 4 seconds, useful for time-strategy boundary checks."""
    from resurrector.demo.sample_bag import generate_bag, BagConfig
    bag_path = tmp_dir / "synth.mcap"
    generate_bag(bag_path, BagConfig(duration_sec=4.0))
    return bag_path


# ---------------------------------------------------------------------------
# validate_split_ratios
# ---------------------------------------------------------------------------

class TestValidateSplitRatios:
    def test_standard_train_val_test_passes(self):
        validate_split_ratios({"train": 0.8, "val": 0.1, "test": 0.1})

    def test_two_way_passes(self):
        validate_split_ratios({"a": 0.5, "b": 0.5})

    def test_single_split_passes(self):
        validate_split_ratios({"train": 1.0})

    def test_floating_tolerance(self):
        # 0.1 + 0.1 + ... + 0.1 (×10) doesn't quite equal 1.0 in float
        ratios = {f"s{i}": 0.1 for i in range(10)}
        validate_split_ratios(ratios)  # should not raise

    def test_empty_dict_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            validate_split_ratios({})

    def test_ratios_not_summing_to_one_raises(self):
        with pytest.raises(ValueError, match="sum to 1.0"):
            validate_split_ratios({"train": 0.7, "val": 0.2})  # = 0.9

    def test_negative_ratio_raises(self):
        with pytest.raises(ValueError, match="out of range"):
            validate_split_ratios({"train": 0.8, "val": -0.1, "test": 0.3})

    def test_ratio_over_one_raises(self):
        with pytest.raises(ValueError, match="out of range"):
            validate_split_ratios({"a": 1.5, "b": -0.5})

    def test_zero_ratio_raises(self):
        with pytest.raises(ValueError, match="out of range"):
            validate_split_ratios({"train": 0.0, "val": 1.0})

    def test_non_string_key_raises(self):
        with pytest.raises(ValueError, match="non-empty strings"):
            validate_split_ratios({None: 1.0})  # type: ignore

    def test_non_numeric_value_raises(self):
        with pytest.raises(ValueError, match="numeric"):
            validate_split_ratios({"train": "0.8"})  # type: ignore


# ---------------------------------------------------------------------------
# _cumulative_time_boundaries
# ---------------------------------------------------------------------------

class TestCumulativeBoundaries:
    def test_three_way_split(self):
        b = _cumulative_time_boundaries(
            {"train": 0.8, "val": 0.1, "test": 0.1}, duration=10.0,
        )
        assert b == [
            ("train", 8.0),
            ("val", 9.0),
            ("test", 10.0),
        ]

    def test_last_boundary_pins_to_duration(self):
        # Floating drift fix
        b = _cumulative_time_boundaries(
            {"a": 1/3, "b": 1/3, "c": 1/3}, duration=10.0,
        )
        assert b[-1][1] == 10.0  # not 9.999999...

    def test_two_way(self):
        b = _cumulative_time_boundaries({"train": 0.7, "test": 0.3}, duration=20.0)
        assert b == [("train", 14.0), ("test", 20.0)]

    def test_preserves_insertion_order(self):
        # Same ratios, different orders → different first-name
        b1 = _cumulative_time_boundaries({"a": 0.5, "b": 0.5}, duration=10.0)
        b2 = _cumulative_time_boundaries({"b": 0.5, "a": 0.5}, duration=10.0)
        assert b1[0][0] == "a"
        assert b2[0][0] == "b"


# ---------------------------------------------------------------------------
# split_export — time strategy
# ---------------------------------------------------------------------------

class TestSplitExportTime:
    def test_writes_three_subdirs(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        out = tmp_dir / "split_out"
        split_export(
            bag_frame=bf,
            topics=["/imu/data"],
            output=out,
            split={"train": 0.5, "val": 0.25, "test": 0.25},
            strategy="time",
            format="parquet",
        )
        assert (out / "train").is_dir()
        assert (out / "val").is_dir()
        assert (out / "test").is_dir()

    def test_writes_manifest(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        out = tmp_dir / "manifest_test"
        split_export(
            bag_frame=bf,
            topics=["/imu/data"],
            output=out,
            split={"train": 0.6, "val": 0.4},
            strategy="time",
            format="parquet",
        )
        manifest_path = out / "split_manifest.json"
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text())
        assert manifest["strategy"] == "time"
        assert manifest["duration_sec"] == bf.duration_sec
        assert "train" in manifest["splits"]
        assert "val" in manifest["splits"]
        assert manifest["splits"]["train"]["start_sec"] == 0.0
        assert manifest["splits"]["val"]["end_sec"] == bf.duration_sec

    def test_each_subdir_has_parquet_output(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        out = tmp_dir / "parquet_check"
        split_export(
            bag_frame=bf,
            topics=["/imu/data"],
            output=out,
            split={"train": 0.5, "test": 0.5},
            strategy="time",
            format="parquet",
        )
        for split_name in ("train", "test"):
            parquet_files = list((out / split_name).rglob("*.parquet"))
            assert parquet_files, f"no parquet found in {out / split_name}"


# ---------------------------------------------------------------------------
# split_export — random strategy
# ---------------------------------------------------------------------------

class TestSplitExportRandom:
    def test_writes_subdirs_with_data(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        out = tmp_dir / "random_out"
        split_export(
            bag_frame=bf,
            topics=["/imu/data"],
            output=out,
            split={"train": 0.7, "val": 0.15, "test": 0.15},
            strategy="random",
            format="parquet",
        )
        # All three should have at least one parquet
        for split_name in ("train", "val", "test"):
            assert list((out / split_name).rglob("*.parquet"))

    def test_random_manifest_records_seed_and_row_counts(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        out = tmp_dir / "random_manifest"
        split_export(
            bag_frame=bf,
            topics=["/imu/data"],
            output=out,
            split={"a": 0.6, "b": 0.4},
            strategy="random",
            format="parquet",
        )
        manifest = json.loads((out / "split_manifest.json").read_text())
        assert manifest["strategy"] == "random"
        assert "seed" in manifest
        assert manifest["splits"]["a"]["rows"] > 0
        assert manifest["splits"]["b"]["rows"] > 0
        # Total rows roughly correct (within some randomness)
        # Synth /imu/data at 200 Hz × 4s = 800 rows (approx)
        total = manifest["splits"]["a"]["rows"] + manifest["splits"]["b"]["rows"]
        assert 600 < total < 1000

    def test_random_is_deterministic_with_fixed_seed(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)

        out_a = tmp_dir / "run_a"
        out_b = tmp_dir / "run_b"
        for out in (out_a, out_b):
            split_export(
                bag_frame=bf,
                topics=["/imu/data"],
                output=out,
                split={"x": 0.5, "y": 0.5},
                strategy="random",
                format="parquet",
            )
        ma = json.loads((out_a / "split_manifest.json").read_text())
        mb = json.loads((out_b / "split_manifest.json").read_text())
        # Same seed (42) → same row counts per split
        assert ma["splits"]["x"]["rows"] == mb["splits"]["x"]["rows"]
        assert ma["splits"]["y"]["rows"] == mb["splits"]["y"]["rows"]


# ---------------------------------------------------------------------------
# Stratified — should raise NotImplementedError
# ---------------------------------------------------------------------------

class TestSplitExportStratified:
    def test_stratified_raises(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        with pytest.raises(NotImplementedError, match="stratified"):
            split_export(
                bag_frame=bf,
                topics=["/imu/data"],
                output=tmp_dir / "should_not_create",
                split={"a": 1.0},
                strategy="stratified",
            )

    def test_unknown_strategy_raises_valueerror(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        with pytest.raises(ValueError, match="Unknown split strategy"):
            split_export(
                bag_frame=bf,
                topics=["/imu/data"],
                output=tmp_dir / "x",
                split={"a": 1.0},
                strategy="nonsense",
            )


# ---------------------------------------------------------------------------
# BagFrame.export(split=...) end-to-end
# ---------------------------------------------------------------------------

class TestBagFrameExportSplit:
    def test_split_arg_writes_subdirs(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        out = tmp_dir / "bf_split"
        bf.export(
            topics=["/imu/data"],
            output=str(out),
            format="parquet",
            split={"train": 0.8, "val": 0.1, "test": 0.1},
            split_strategy="time",
        )
        for s in ("train", "val", "test"):
            assert (out / s).is_dir()

    def test_invalid_ratios_raise_via_export(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        with pytest.raises(ValueError, match="sum to 1.0"):
            bf.export(
                topics=["/imu/data"],
                output=str(tmp_dir / "bad"),
                format="parquet",
                split={"train": 0.5, "val": 0.3},  # sums to 0.8
            )

    def test_split_with_preset(self, synth_bag, tmp_dir):
        """Preset + split should compose: preset's format/sync flow into split_export."""
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        out = tmp_dir / "preset_split"
        bf.export(
            preset="training-tabular",
            output=str(out),
            split={"train": 0.7, "test": 0.3},
            split_strategy="time",
        )
        assert (out / "train").is_dir()
        assert (out / "test").is_dir()
        # training-tabular preset uses parquet
        assert list((out / "train").rglob("*.parquet"))


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------

class TestCLISplit:
    def test_cli_split_runs(self, synth_bag, tmp_dir):
        runner = CliRunner()
        result = runner.invoke(app, [
            "export", str(synth_bag),
            "-t", "/imu/data",
            "-o", str(tmp_dir / "cli_split"),
            "--split", "train=0.8",
            "--split", "val=0.1",
            "--split", "test=0.1",
        ])
        assert result.exit_code == 0, f"stdout: {result.stdout}"
        assert "Split (time)" in result.stdout

    def test_cli_split_random_strategy(self, synth_bag, tmp_dir):
        runner = CliRunner()
        result = runner.invoke(app, [
            "export", str(synth_bag),
            "-t", "/imu/data",
            "-o", str(tmp_dir / "cli_random"),
            "--split", "a=0.5",
            "--split", "b=0.5",
            "--split-strategy", "random",
        ])
        assert result.exit_code == 0
        assert "Split (random)" in result.stdout

    def test_cli_split_invalid_format_exits(self, synth_bag, tmp_dir):
        """--split without `=` should fail with exit code 2."""
        runner = CliRunner()
        result = runner.invoke(app, [
            "export", str(synth_bag),
            "-t", "/imu/data",
            "-o", str(tmp_dir / "x"),
            "--split", "no_equals_sign",
        ])
        assert result.exit_code != 0
        assert "Invalid --split" in result.stdout or "expected NAME=RATIO" in result.stdout

    def test_cli_split_bad_ratio_exits(self, synth_bag, tmp_dir):
        runner = CliRunner()
        result = runner.invoke(app, [
            "export", str(synth_bag),
            "-t", "/imu/data",
            "-o", str(tmp_dir / "x2"),
            "--split", "train=notanumber",
        ])
        assert result.exit_code != 0

    def test_cli_split_stratified_exits_with_clear_message(self, synth_bag, tmp_dir):
        runner = CliRunner()
        result = runner.invoke(app, [
            "export", str(synth_bag),
            "-t", "/imu/data",
            "-o", str(tmp_dir / "no_strat"),
            "--split", "train=1.0",
            "--split-strategy", "stratified",
        ])
        assert result.exit_code != 0
        assert "stratified" in result.stdout.lower()
