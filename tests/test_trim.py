"""Tests for time-range trim of bag files."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from resurrector.core.bag_frame import BagFrame
from resurrector.core.trim import trim_to_format, trim_to_mcap
from tests.fixtures.generate_test_bags import BagConfig, generate_bag


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_bag(tmp_dir):
    return generate_bag(tmp_dir / "sample.mcap", BagConfig(duration_sec=4.0))


class TestTrimToMcap:
    def test_creates_output_file(self, sample_bag, tmp_dir):
        out = tmp_dir / "trimmed.mcap"
        trim_to_mcap(sample_bag, out, start_sec=1.0, end_sec=2.0)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_round_trips_via_bagframe(self, sample_bag, tmp_dir):
        out = tmp_dir / "trimmed.mcap"
        trim_to_mcap(sample_bag, out, start_sec=1.0, end_sec=2.0)
        bf = BagFrame(out)
        # The trimmed window is 1 second; topic counts must be lower than
        # the source.
        src = BagFrame(sample_bag)
        assert bf.message_count > 0
        assert bf.message_count < src.message_count

    def test_topic_filter_excludes_others(self, sample_bag, tmp_dir):
        out = tmp_dir / "trimmed.mcap"
        trim_to_mcap(
            sample_bag, out, start_sec=0.5, end_sec=1.5, topics=["/imu/data"],
        )
        bf = BagFrame(out)
        names = {t.name for t in bf.topics}
        assert names == {"/imu/data"}

    def test_rejects_invalid_range(self, sample_bag, tmp_dir):
        with pytest.raises(ValueError, match="end_sec"):
            trim_to_mcap(sample_bag, tmp_dir / "x.mcap", start_sec=2.0, end_sec=1.0)

    def test_creates_parent_dir(self, sample_bag, tmp_dir):
        out = tmp_dir / "nested" / "deep" / "trimmed.mcap"
        trim_to_mcap(sample_bag, out, start_sec=0.0, end_sec=0.5)
        assert out.exists()


class TestTrimToFormat:
    def test_mcap_dispatch(self, sample_bag, tmp_dir):
        out = tmp_dir / "out.mcap"
        result = trim_to_format(
            sample_bag, out, 0.5, 1.5, ["/imu/data"], format="mcap",
        )
        assert result == out
        assert out.exists()

    def test_parquet_dispatch(self, sample_bag, tmp_dir):
        out_dir = tmp_dir / "parquet_out"
        out_dir.mkdir()
        trim_to_format(
            sample_bag, out_dir, 0.5, 1.5, ["/imu/data"], format="parquet",
        )
        assert (out_dir / "imu_data.parquet").exists()

    def test_csv_dispatch(self, sample_bag, tmp_dir):
        out_dir = tmp_dir / "csv_out"
        out_dir.mkdir()
        trim_to_format(
            sample_bag, out_dir, 0.5, 1.5, ["/imu/data"], format="csv",
        )
        assert (out_dir / "imu_data.csv").exists()

    def test_unsupported_format_raises(self, sample_bag, tmp_dir):
        with pytest.raises(ValueError, match="Unsupported"):
            trim_to_format(
                sample_bag, tmp_dir / "x", 0.0, 1.0, ["/imu/data"], format="bogus",
            )

    def test_mp4_requires_one_image_topic(self, sample_bag, tmp_dir):
        with pytest.raises(ValueError, match="one image topic"):
            trim_to_format(
                sample_bag, tmp_dir / "v.mp4", 0.0, 1.0,
                ["/imu/data", "/joint_states"], format="mp4",
            )

    def test_mp4_rejects_non_image_topic(self, sample_bag, tmp_dir):
        with pytest.raises(ValueError, match="not an image"):
            trim_to_format(
                sample_bag, tmp_dir / "v.mp4", 0.0, 1.0,
                ["/imu/data"], format="mp4",
            )

    def test_invalid_range_caught_at_dispatch(self, sample_bag, tmp_dir):
        with pytest.raises(ValueError, match="end_sec"):
            trim_to_format(
                sample_bag, tmp_dir / "out.mcap", 2.0, 1.0,
                ["/imu/data"], format="mcap",
            )
