"""Tests for the health check engine."""

import tempfile
from pathlib import Path

import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.health_check import Severity


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


class TestHealthCheck:
    def test_healthy_bag_high_score(self, tmp_dir):
        bag = generate_bag(tmp_dir / "healthy.mcap", BagConfig(duration_sec=3.0))
        bf = BagFrame(bag)
        report = bf.health_report()
        assert report.score >= 80

    def test_dropped_messages_detected(self, tmp_dir):
        bag = generate_bag(tmp_dir / "dropped.mcap", BagConfig(
            duration_sec=5.0,
            drop_messages=True,
            drop_topic="/lidar/scan",
            drop_start_sec=1.5,
            drop_duration_sec=2.0,
            drop_rate=0.8,
        ))
        bf = BagFrame(bag)
        report = bf.health_report()
        # Should have issues for the lidar topic
        lidar_health = report.topic_scores.get("/lidar/scan")
        assert lidar_health is not None
        assert lidar_health.score < 100

    def test_time_gap_detected(self, tmp_dir):
        bag = generate_bag(tmp_dir / "gap.mcap", BagConfig(
            duration_sec=5.0,
            time_gap=True,
            gap_topic="/imu/data",
            gap_start_sec=2.0,
            gap_duration_sec=1.0,
        ))
        bf = BagFrame(bag)
        report = bf.health_report()
        imu_health = report.topic_scores.get("/imu/data")
        assert imu_health is not None
        # Should detect the gap
        gap_issues = [i for i in imu_health.issues if i.check_name == "time_gaps"]
        assert len(gap_issues) > 0

    def test_partial_topic_detected(self, tmp_dir):
        bag = generate_bag(tmp_dir / "partial.mcap", BagConfig(
            duration_sec=5.0,
            partial_topic=True,
            partial_topic_name="/lidar/scan",
            partial_start_delay_sec=1.0,
            partial_end_early_sec=1.5,
        ))
        bf = BagFrame(bag)
        report = bf.health_report()
        lidar_health = report.topic_scores.get("/lidar/scan")
        assert lidar_health is not None
        completeness_issues = [i for i in lidar_health.issues if i.check_name == "topic_completeness"]
        assert len(completeness_issues) > 0

    def test_recommendations_generated(self, tmp_dir):
        bag = generate_bag(tmp_dir / "gap.mcap", BagConfig(
            duration_sec=5.0,
            time_gap=True,
            gap_topic="/imu/data",
            gap_start_sec=2.0,
            gap_duration_sec=1.0,
        ))
        bf = BagFrame(bag)
        report = bf.health_report()
        assert len(report.recommendations) > 0

    def test_per_topic_scores(self, tmp_dir):
        bag = generate_bag(tmp_dir / "healthy.mcap", BagConfig(duration_sec=3.0))
        bf = BagFrame(bag)
        report = bf.health_report()
        assert len(report.topic_scores) > 0
        for topic, th in report.topic_scores.items():
            assert 0 <= th.score <= 100

    def test_health_report_cached(self, tmp_dir):
        bag = generate_bag(tmp_dir / "healthy.mcap", BagConfig(duration_sec=2.0))
        bf = BagFrame(bag)
        r1 = bf.health_report()
        r2 = bf.health_report()
        assert r1 is r2  # Same object — cached
