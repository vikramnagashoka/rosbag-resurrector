"""Tests for the RLDS ML-pipeline export format.

RLDS auto-skips when tensorflow isn't installed. LeRobot export is covered
in test_lerobot_export.py (round-trips through the real LeRobot library).
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import polars as pl
import pytest

from resurrector.core.bag_frame import BagFrame
from resurrector.core.export import Exporter
from tests.fixtures.generate_test_bags import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_bag(tmp_dir):
    return generate_bag(tmp_dir / "sample.mcap", BagConfig(duration_sec=2.0))


tf_available = False
try:
    import tensorflow  # noqa: F401
    tf_available = True
except ImportError:
    pass


@pytest.mark.skipif(not tf_available, reason="tensorflow not installed")
class TestRLDSExport:
    def test_writes_tfrecord(self, tmp_dir, sample_bag):
        bf = BagFrame(sample_bag)
        out = tmp_dir / "rlds_out"
        bf.export(
            topics=["/imu/data", "/joint_states"],
            format="rlds",
            output=str(out),
            sync=True,
        )
        assert (out / "synced.tfrecord").exists()

    def test_tfrecord_has_step_features(self, tmp_dir, sample_bag):
        import tensorflow as tf
        bf = BagFrame(sample_bag)
        out = tmp_dir / "rlds_out"
        bf.export(
            topics=["/imu/data", "/joint_states"],
            format="rlds",
            output=str(out),
            sync=True,
        )
        ds = tf.data.TFRecordDataset(str(out / "synced.tfrecord"))
        first = next(iter(ds))
        example = tf.train.Example()
        example.ParseFromString(first.numpy())
        keys = set(example.features.feature.keys())
        # Required RLDS step features
        assert "step/reward" in keys
        assert "step/discount" in keys
        assert "step/is_first" in keys
        assert "step/is_last" in keys
        assert "step/is_terminal" in keys
        # First step should be is_first=True, is_last=False
        assert example.features.feature["step/is_first"].int64_list.value[0] == 1
        assert example.features.feature["step/is_last"].int64_list.value[0] == 0


def test_rlds_raises_helpful_error_when_tf_missing(tmp_dir, sample_bag):
    """If tensorflow isn't available, the user gets a clear install hint."""
    if tf_available:
        pytest.skip("tensorflow IS installed; skipping the missing-deps test")
    bf = BagFrame(sample_bag)
    with pytest.raises(ImportError, match="tensorflow"):
        bf.export(
            topics=["/imu/data", "/joint_states"],
            format="rlds",
            output=str(tmp_dir / "rlds_out"),
            sync=True,
        )


def test_unknown_format_lists_lerobot_and_rlds(tmp_dir, sample_bag):
    """The error message should mention the new formats so users know they exist."""
    bf = BagFrame(sample_bag)
    with pytest.raises(ValueError, match="lerobot.*rlds|rlds.*lerobot"):
        bf.export(topics=["/imu/data"], format="bogus")
