"""Tests for configurable health check thresholds."""

import tempfile
from pathlib import Path

import numpy as np
import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.health_check import HealthChecker, HealthConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


class TestHealthConfig:
    def test_default_config(self):
        config = HealthConfig()
        assert config.rate_drop_threshold == 0.25
        assert config.gap_multiplier == 2.0
        assert config.completeness_threshold == 0.05

    def test_custom_config(self):
        config = HealthConfig(
            rate_drop_threshold=0.5,
            gap_multiplier=3.0,
            completeness_threshold=0.1,
        )
        checker = HealthChecker(config)
        assert checker.config.rate_drop_threshold == 0.5
        assert checker.config.gap_multiplier == 3.0

    def test_stricter_thresholds_catch_more(self, tmp_dir):
        """Stricter thresholds should produce lower health scores."""
        bag = generate_bag(tmp_dir / "test.mcap", BagConfig(duration_sec=3.0))
        bf = BagFrame(bag)

        # Get timestamps
        topic_timestamps: dict[str, list[int]] = {}
        for msg in bf._parser.read_messages():
            topic_timestamps.setdefault(msg.topic, []).append(msg.timestamp_ns)

        # Default checker
        default_checker = HealthChecker()
        default_report = default_checker.run_all_checks(
            topic_timestamps=topic_timestamps,
            bag_start_ns=bf.metadata.start_time_ns,
            bag_end_ns=bf.metadata.end_time_ns,
        )

        # Strict checker — very tight thresholds
        strict_config = HealthConfig(
            gap_multiplier=1.1,  # Flag gaps > 1.1x expected (very strict)
            completeness_threshold=0.001,  # Even tiny start delays flagged
        )
        strict_checker = HealthChecker(strict_config)
        strict_report = strict_checker.run_all_checks(
            topic_timestamps=topic_timestamps,
            bag_start_ns=bf.metadata.start_time_ns,
            bag_end_ns=bf.metadata.end_time_ns,
        )

        # Strict should find at least as many issues
        assert len(strict_report.issues) >= len(default_report.issues)

    def test_zero_messages_no_crash(self):
        """Health checker should not crash on topics with 0 or 1 messages."""
        checker = HealthChecker()
        report = checker.run_all_checks(
            topic_timestamps={"/empty": [], "/single": [1000000000]},
            bag_start_ns=0,
            bag_end_ns=5000000000,
        )
        # Should succeed, not crash
        assert report.score >= 0
        assert "/empty" in report.topic_scores
        assert "/single" in report.topic_scores

    def test_custom_weights(self):
        """Custom scoring weights should be respected."""
        config = HealthConfig(
            weights={
                "message_rate_stability": 100,
                "time_gaps": 0,
                "timestamp_ordering": 0,
                "topic_completeness": 0,
                "message_size_anomalies": 0,
            }
        )
        checker = HealthChecker(config)
        assert checker.WEIGHTS["message_rate_stability"] == 100
        assert checker.WEIGHTS["time_gaps"] == 0
