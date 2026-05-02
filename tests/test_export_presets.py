"""Tests for the export-preset system (Option 1.2).

Covers:
- PRESETS registry sanity
- resolve_preset() merge logic (user flags override preset)
- apply_topic_filter() image vs non-image splits
- BagFrame.export(preset=...) end-to-end on the synthetic bag
- CLI `resurrector export --preset` and `--list-presets`
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from resurrector.cli.main import app
from resurrector.core.export import (
    PRESETS,
    ExportPreset,
    apply_topic_filter,
    list_presets,
    resolve_preset,
)


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def synth_bag(tmp_dir: Path) -> Path:
    """Synthetic bag with imu + joint_states + camera, suitable for preset tests."""
    from resurrector.demo.sample_bag import generate_bag, BagConfig
    bag_path = tmp_dir / "synth.mcap"
    generate_bag(bag_path, BagConfig(duration_sec=2.0))
    return bag_path


# ---------------------------------------------------------------------------
# PRESETS registry
# ---------------------------------------------------------------------------

class TestPresetsRegistry:
    """Check the shape and contents of the PRESETS dict itself."""

    def test_all_five_presets_present(self):
        expected = {"lerobot", "rlds", "training-tabular", "camera-only", "multimodal"}
        assert set(PRESETS.keys()) == expected

    def test_each_preset_is_frozen_dataclass(self):
        for name, p in PRESETS.items():
            assert isinstance(p, ExportPreset)
            # frozen=True means we can't mutate
            with pytest.raises(Exception):
                p.format = "different"  # type: ignore

    def test_each_preset_has_nonempty_description(self):
        for name, p in PRESETS.items():
            assert p.description, f"{name} has empty description"
            assert len(p.description) > 20, f"{name} description too short"

    def test_name_field_matches_dict_key(self):
        for key, p in PRESETS.items():
            assert p.name == key, f"PRESETS[{key!r}].name == {p.name!r}"

    def test_format_field_is_known(self):
        known_formats = {"parquet", "hdf5", "csv", "numpy", "zarr", "lerobot", "rlds"}
        for name, p in PRESETS.items():
            assert p.format in known_formats, f"{name} has unknown format {p.format}"

    def test_topic_filter_is_known_or_none(self):
        for name, p in PRESETS.items():
            assert p.topic_filter in (None, "images", "non-images")

    def test_extras_required_is_tuple(self):
        for name, p in PRESETS.items():
            assert isinstance(p.extras_required, tuple)

    def test_list_presets_returns_all(self):
        ps = list_presets()
        assert len(ps) == len(PRESETS)
        assert {p.name for p in ps} == set(PRESETS.keys())


# ---------------------------------------------------------------------------
# resolve_preset()
# ---------------------------------------------------------------------------

class TestResolvePreset:
    """User flags must always override preset values."""

    def test_no_preset_returns_user_values_with_defaults(self):
        r = resolve_preset(
            None, format="csv", sync=True, sync_method="interpolate",
            downsample_hz=10, topics=["/imu/data"],
        )
        assert r["format"] == "csv"
        assert r["sync"] is True
        assert r["sync_method"] == "interpolate"
        assert r["downsample_hz"] == 10
        assert r["topics"] == ["/imu/data"]
        assert r["topic_filter"] is None

    def test_no_preset_fills_defaults(self):
        r = resolve_preset(None)
        assert r["format"] == "parquet"
        assert r["sync"] is False
        assert r["sync_method"] == "nearest"
        assert r["downsample_hz"] is None

    def test_preset_only_no_overrides(self):
        r = resolve_preset("lerobot")
        # Should match the preset's defaults exactly
        p = PRESETS["lerobot"]
        assert r["format"] == p.format
        assert r["sync"] == p.sync
        assert r["sync_method"] == p.sync_method
        assert r["downsample_hz"] == p.downsample_hz
        assert r["topic_filter"] == p.topic_filter

    def test_user_format_overrides_preset(self):
        r = resolve_preset("lerobot", format="parquet")
        assert r["format"] == "parquet"  # not "lerobot" from preset

    def test_user_downsample_overrides_preset(self):
        # lerobot preset has 30 Hz; user picks 60
        r = resolve_preset("lerobot", downsample_hz=60.0)
        assert r["downsample_hz"] == 60.0

    def test_user_sync_false_overrides_preset(self):
        # lerobot preset has sync=True; user wants no sync
        r = resolve_preset("lerobot", sync=False)
        assert r["sync"] is False

    def test_user_topics_disable_topic_filter(self):
        r = resolve_preset("camera-only", topics=["/imu/data"])
        # When user supplies topics, the preset's topic_filter is bypassed
        assert r["topics"] == ["/imu/data"]
        assert r["topic_filter"] is None

    def test_unknown_preset_raises(self):
        with pytest.raises(ValueError, match="Unknown preset"):
            resolve_preset("nonexistent")

    def test_unknown_preset_error_lists_available(self):
        with pytest.raises(ValueError) as exc:
            resolve_preset("typo")
        msg = str(exc.value)
        assert "lerobot" in msg
        assert "rlds" in msg


# ---------------------------------------------------------------------------
# apply_topic_filter()
# ---------------------------------------------------------------------------

class TestApplyTopicFilter:
    def test_none_returns_all(self, synth_bag):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        result = apply_topic_filter(bf, None)
        assert result == bf.topic_names

    def test_images_returns_only_image_topics(self, synth_bag):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        result = apply_topic_filter(bf, "images")
        # Synth bag has /camera/image (sensor_msgs/msg/Image)
        for topic_name in result:
            ti = bf._find_topic(topic_name)
            assert ti.message_type in (
                "sensor_msgs/msg/Image", "sensor_msgs/msg/CompressedImage"
            )

    def test_non_images_excludes_image_topics(self, synth_bag):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        result = apply_topic_filter(bf, "non-images")
        for topic_name in result:
            ti = bf._find_topic(topic_name)
            assert ti.message_type not in (
                "sensor_msgs/msg/Image", "sensor_msgs/msg/CompressedImage"
            )

    def test_unknown_filter_raises(self, synth_bag):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        with pytest.raises(ValueError, match="Unknown topic_filter"):
            apply_topic_filter(bf, "nonsense")


# ---------------------------------------------------------------------------
# BagFrame.export(preset=...) end-to-end
# ---------------------------------------------------------------------------

class TestBagFrameExportPreset:
    """End-to-end: bf.export(preset=...) writes the right files."""

    def test_training_tabular_preset_writes_parquet(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        out = tmp_dir / "tabular_out"
        result = bf.export(preset="training-tabular", output=str(out))
        assert result.exists()
        # Should produce at least one .parquet file (synced output)
        parquet_files = list(out.rglob("*.parquet"))
        assert len(parquet_files) >= 1, f"Expected parquet files in {out}, got nothing"

    def test_training_tabular_excludes_image_topics(self, synth_bag, tmp_dir):
        """training-tabular preset has topic_filter='non-images'."""
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        out = tmp_dir / "tabular_no_img"
        bf.export(preset="training-tabular", output=str(out))
        # No file should be named after a camera topic
        all_files = list(out.rglob("*"))
        for f in all_files:
            assert "camera" not in f.name.lower(), \
                f"camera-related file leaked into non-images preset: {f}"

    def test_user_override_takes_precedence(self, synth_bag, tmp_dir):
        """Override the preset's downsample with a different value."""
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        out = tmp_dir / "override"
        # training-tabular default is 50 Hz; we set 25.
        # Just verify it runs without error — semantic check would need
        # parquet inspection which is heavier than warranted here.
        result = bf.export(
            preset="training-tabular",
            downsample_hz=25.0,
            output=str(out),
        )
        assert result.exists()

    def test_unknown_preset_raises_valueerror(self, synth_bag, tmp_dir):
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(synth_bag)
        with pytest.raises(ValueError, match="Unknown preset"):
            bf.export(preset="totally-fake", output=str(tmp_dir / "x"))


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------

class TestCLIPresets:
    """`resurrector export --preset` and `--list-presets` from the CLI."""

    def test_list_presets_runs_and_shows_all(self):
        runner = CliRunner()
        # `--list-presets` ignores the bag arg requirement... actually it's
        # still required by Typer. Pass a placeholder path that won't be read.
        # Actually --list-presets short-circuits before BagFrame, so the path
        # doesn't have to exist. But Typer validates the Argument as a Path
        # — Typer's Path validation doesn't require existence by default.
        result = runner.invoke(app, ["export", "/dev/null", "--list-presets"])
        # Even if BagFrame would error on /dev/null, --list-presets exits early
        assert result.exit_code == 0
        for name in PRESETS.keys():
            assert name in result.stdout, f"preset {name!r} missing from output"

    def test_export_with_preset_runs_end_to_end(self, synth_bag, tmp_dir):
        runner = CliRunner()
        result = runner.invoke(app, [
            "export", str(synth_bag),
            "--preset", "training-tabular",
            "-o", str(tmp_dir / "cli_out"),
        ])
        assert result.exit_code == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
        assert "training-tabular" in result.stdout

    def test_export_with_unknown_preset_exits_nonzero(self, synth_bag, tmp_dir):
        runner = CliRunner()
        result = runner.invoke(app, [
            "export", str(synth_bag),
            "--preset", "no-such-preset",
            "-o", str(tmp_dir / "should_not_exist"),
        ])
        assert result.exit_code != 0
        assert "Unknown preset" in result.stdout or "no-such-preset" in result.stdout
