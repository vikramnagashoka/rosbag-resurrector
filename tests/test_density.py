"""Tests for per-topic message-density histogram."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from resurrector.ingest.density import compute_density
from tests.fixtures.generate_test_bags import BagConfig, generate_bag


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_bag(tmp_dir):
    return generate_bag(tmp_dir / "sample.mcap", BagConfig(duration_sec=2.0))


class TestComputeDensity:
    def test_returns_per_topic_dict(self, sample_bag):
        result = compute_density(sample_bag, bins=20)
        # Fixture has 6 topics; at least the well-known ones must appear
        assert "/imu/data" in result
        assert "/joint_states" in result
        assert all("bins" in v for v in result.values())

    def test_bins_have_requested_length(self, sample_bag):
        result = compute_density(sample_bag, bins=50)
        for topic, info in result.items():
            if info["total"] > 0:
                assert len(info["bins"]) == 50

    def test_total_matches_bin_sum(self, sample_bag):
        result = compute_density(sample_bag, bins=30)
        for topic, info in result.items():
            assert info["total"] == sum(info["bins"])

    def test_topic_filter(self, sample_bag):
        result = compute_density(sample_bag, topics=["/imu/data"], bins=10)
        assert set(result.keys()) == {"/imu/data"}

    def test_rejects_zero_bins(self, sample_bag):
        with pytest.raises(ValueError, match=">= 1"):
            compute_density(sample_bag, bins=0)

    def test_unknown_topic_returns_empty_histogram(self, sample_bag):
        # Topic not in bag — explicit allowlist returns empty bins, not error.
        result = compute_density(sample_bag, topics=["/ghost"], bins=10)
        assert result["/ghost"]["total"] == 0
        assert all(c == 0 for c in result["/ghost"]["bins"])

    def test_dense_topic_distributes_across_bins(self, sample_bag):
        # /imu/data is the highest-rate topic in the fixture (200Hz over 2s
        # = 400 messages). With 20 bins, every bin should be non-empty.
        result = compute_density(sample_bag, topics=["/imu/data"], bins=20)
        info = result["/imu/data"]
        nonzero = sum(1 for c in info["bins"] if c > 0)
        assert nonzero >= 18  # allow 1-2 empty edge bins for slight rounding
