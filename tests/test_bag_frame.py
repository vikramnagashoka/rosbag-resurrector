"""Tests for BagFrame — the core data abstraction."""

import tempfile
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.core.bag_frame import BagFrame, TopicView, _parse_time


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def healthy_bag(tmp_dir):
    return generate_bag(tmp_dir / "healthy.mcap", BagConfig(duration_sec=2.0))


class TestBagFrame:
    def test_create(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        assert bf.path == healthy_bag

    def test_metadata(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        assert bf.duration_sec > 0
        assert bf.message_count > 0
        assert len(bf.topics) >= 4

    def test_topic_names(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        assert "/imu/data" in bf.topic_names
        assert "/joint_states" in bf.topic_names

    def test_getitem(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        view = bf["/imu/data"]
        assert isinstance(view, TopicView)
        assert view.name == "/imu/data"

    def test_getitem_invalid(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        with pytest.raises(KeyError):
            bf["/nonexistent"]

    def test_to_polars(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        df = bf["/imu/data"].to_polars()
        assert isinstance(df, pl.DataFrame)
        assert "timestamp_ns" in df.columns
        assert "linear_acceleration.x" in df.columns
        assert df.height > 0

    def test_to_pandas(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        pd_df = bf["/imu/data"].to_pandas()
        assert "timestamp_ns" in pd_df.columns
        assert len(pd_df) > 0

    def test_to_numpy(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        arrays = bf["/imu/data"].to_numpy()
        assert "timestamp_ns" in arrays
        assert "linear_acceleration.x" in arrays
        assert isinstance(arrays["linear_acceleration.x"], np.ndarray)

    def test_time_slice(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        sliced = bf.time_slice("0.5s", "1.5s")
        df = sliced["/imu/data"].to_polars()
        assert df.height > 0
        # Should have fewer messages than full bag
        full_df = bf["/imu/data"].to_polars()
        assert df.height < full_df.height

    def test_info(self, healthy_bag, capsys):
        bf = BagFrame(healthy_bag)
        result = bf.info()
        assert "RosBag Resurrector" in result
        assert "/imu/data" in result

    def test_health_report(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        report = bf.health_report()
        assert report.score >= 0
        assert report.score <= 100

    def test_repr(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        r = repr(bf)
        assert "BagFrame" in r
        assert "healthy.mcap" in r

    def test_file_not_found(self, tmp_dir):
        with pytest.raises(FileNotFoundError):
            BagFrame(tmp_dir / "nonexistent.mcap")


class TestParseTime:
    def test_float(self):
        assert _parse_time(10.5) == 10.5

    def test_seconds(self):
        assert _parse_time("10s") == 10.0

    def test_minutes(self):
        assert _parse_time("2min") == 120.0

    def test_minutes_short(self):
        assert _parse_time("2m") == 120.0

    def test_hours(self):
        assert _parse_time("1h") == 3600.0

    def test_plain_number(self):
        assert _parse_time("10") == 10.0

    def test_milliseconds(self):
        assert _parse_time("500ms") == 0.5
