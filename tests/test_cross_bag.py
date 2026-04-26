"""Tests for cross-bag overlay alignment."""

from __future__ import annotations

import tempfile
from pathlib import Path

import polars as pl
import pytest

from resurrector.core.cross_bag import align_bags_by_offset
from tests.fixtures.generate_test_bags import BagConfig, generate_bag


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def two_bags(tmp_dir):
    a = generate_bag(tmp_dir / "run_a.mcap", BagConfig(duration_sec=2.0))
    b = generate_bag(tmp_dir / "run_b.mcap", BagConfig(duration_sec=2.0))
    return a, b


class TestAlignBagsByOffset:
    def test_returns_long_format_with_bag_label(self, two_bags):
        a, b = two_bags
        df = align_bags_by_offset([a, b], topic="/imu/data")
        assert "bag_label" in df.columns
        assert "relative_t_sec" in df.columns
        labels = set(df.get_column("bag_label").unique().to_list())
        assert labels == {"run_a", "run_b"}

    def test_relative_alignment_starts_at_zero(self, two_bags):
        a, b = two_bags
        df = align_bags_by_offset([a, b], topic="/imu/data")
        for label in ("run_a", "run_b"):
            sub = df.filter(pl.col("bag_label") == label)
            assert sub.get_column("relative_t_sec").min() == pytest.approx(
                0.0, abs=1e-3,
            )

    def test_offset_shifts_one_bag(self, two_bags):
        a, b = two_bags
        df = align_bags_by_offset([a, b], topic="/imu/data", offsets_sec=[0.0, 1.5])
        b_min = (
            df.filter(pl.col("bag_label") == "run_b")
            .get_column("relative_t_sec")
            .min()
        )
        assert b_min == pytest.approx(1.5, abs=0.05)

    def test_custom_labels(self, two_bags):
        a, b = two_bags
        df = align_bags_by_offset(
            [a, b], topic="/imu/data", labels=["baseline", "experiment"],
        )
        assert set(df.get_column("bag_label").unique().to_list()) == {
            "baseline", "experiment",
        }

    def test_missing_topic_raises(self, two_bags):
        a, b = two_bags
        with pytest.raises(ValueError, match="does not contain topic"):
            align_bags_by_offset([a, b], topic="/ghost/topic")

    def test_empty_bag_paths_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            align_bags_by_offset([], topic="/imu/data")

    def test_offsets_length_mismatch_raises(self, two_bags):
        a, b = two_bags
        with pytest.raises(ValueError, match="offsets_sec length"):
            align_bags_by_offset([a, b], topic="/imu/data", offsets_sec=[0.0])

    def test_labels_length_mismatch_raises(self, two_bags):
        a, b = two_bags
        with pytest.raises(ValueError, match="labels length"):
            align_bags_by_offset([a, b], topic="/imu/data", labels=["only-one"])

    def test_caps_at_max_points_per_bag(self, two_bags):
        a, b = two_bags
        df = align_bags_by_offset(
            [a, b], topic="/imu/data", max_points_per_bag=50,
        )
        for label in ("run_a", "run_b"):
            sub = df.filter(pl.col("bag_label") == label)
            assert sub.height <= 50
