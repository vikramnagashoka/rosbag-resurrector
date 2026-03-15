"""Tests for multi-stream temporal synchronization."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.core.bag_frame import BagFrame


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def healthy_bag(tmp_dir):
    return generate_bag(tmp_dir / "healthy.mcap", BagConfig(duration_sec=2.0))


class TestSync:
    def test_sync_nearest(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        df = bf.sync(["/imu/data", "/joint_states"], method="nearest", tolerance_ms=50)
        assert isinstance(df, pl.DataFrame)
        assert df.height > 0
        assert "timestamp_ns" in df.columns
        # Should have columns from both topics
        cols = set(df.columns)
        assert any("linear_acceleration" in c for c in cols)
        assert any("position" in c for c in cols)

    def test_sync_interpolate(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        df = bf.sync(["/imu/data", "/joint_states"], method="interpolate")
        assert isinstance(df, pl.DataFrame)
        assert df.height > 0

    def test_sync_sample_and_hold(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        df = bf.sync(["/imu/data", "/joint_states"], method="sample_and_hold")
        assert isinstance(df, pl.DataFrame)
        assert df.height > 0

    def test_sync_invalid_method(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        with pytest.raises(ValueError):
            bf.sync(["/imu/data", "/joint_states"], method="invalid")

    def test_sync_single_topic(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        df = bf.sync(["/imu/data"], method="nearest")
        assert isinstance(df, pl.DataFrame)
        assert df.height > 0

    def test_sync_anchor_topic(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        # Use the lower-frequency topic as anchor
        df = bf.sync(
            ["/imu/data", "/joint_states"],
            method="nearest",
            anchor="/joint_states",
        )
        assert df.height > 0
        # Should have roughly as many rows as joint_states
        joint_count = bf["/joint_states"].message_count
        assert abs(df.height - joint_count) < joint_count * 0.1
