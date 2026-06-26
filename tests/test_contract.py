"""Tests for Bag Contracts — v0.7.1 Feature.

Covers:
- infer_contract: required topics (intersection), rate ranges, types
- check_contract: pass on good bag, violations on missing topic / wrong
  rate; collects all violations
- load/save round-trip in both JSON and YAML
- Rainy day: empty input raises, unreadable bag is a violation not a crash
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from resurrector.core.contract import (
    Contract,
    TopicContract,
    check_contract,
    infer_contract,
    load_contract,
    save_contract,
)
from resurrector.demo.sample_bag import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def good_bags(tmp_dir):
    paths = []
    for i in range(3):
        p = tmp_dir / f"good{i}.mcap"
        generate_bag(p, BagConfig(duration_sec=2.0, imu_hz=100.0))
        paths.append(p)
    return paths


class TestInfer:
    def test_requires_no_bags_raises(self):
        with pytest.raises(ValueError):
            infer_contract([])

    def test_infers_topics_and_rate(self, good_bags):
        c = infer_contract(good_bags)
        assert "/imu/data" in c.topics
        imu = c.topics["/imu/data"]
        assert imu.message_type == "sensor_msgs/msg/Imu"
        # Rate range brackets the true 100 Hz.
        assert imu.min_rate_hz is not None and imu.max_rate_hz is not None
        assert imu.min_rate_hz < 100.0 < imu.max_rate_hz

    def test_required_topics_are_intersection(self, tmp_dir):
        # One bag missing the compressed-camera topic -> not required.
        a = tmp_dir / "a.mcap"
        b = tmp_dir / "b.mcap"
        generate_bag(a, BagConfig(duration_sec=1.0, include_compressed=True))
        generate_bag(b, BagConfig(duration_sec=1.0, include_compressed=False))
        c = infer_contract([a, b])
        assert "/camera/compressed" not in c.topics
        # But a topic in both is required.
        assert "/imu/data" in c.topics

    def test_rate_tolerance_widens_range(self, good_bags):
        tight = infer_contract(good_bags, rate_tolerance=0.0)
        wide = infer_contract(good_bags, rate_tolerance=0.5)
        assert wide.topics["/imu/data"].max_rate_hz >= tight.topics["/imu/data"].max_rate_hz


class TestCheck:
    def test_good_bag_passes(self, good_bags):
        c = infer_contract(good_bags)
        res = check_contract(good_bags[0], c)
        assert res.passed, [v.to_dict() for v in res.violations]

    def test_missing_topic_violation(self, tmp_dir, good_bags):
        c = infer_contract(good_bags)
        # Add a required topic the candidate won't have.
        c.topics["/totally_missing"] = TopicContract(message_type="std_msgs/msg/Bool")
        res = check_contract(good_bags[0], c)
        assert not res.passed
        assert any(v.code == "missing_topic" and v.topic == "/totally_missing"
                   for v in res.violations)

    def test_rate_violation(self, tmp_dir, good_bags):
        c = infer_contract(good_bags)
        slow = tmp_dir / "slow.mcap"
        generate_bag(slow, BagConfig(duration_sec=2.0, imu_hz=30.0))
        res = check_contract(slow, c)
        assert not res.passed
        assert any(v.code == "rate_low" and v.topic == "/imu/data"
                   for v in res.violations)

    def test_wrong_type_violation(self, good_bags):
        c = infer_contract(good_bags)
        c.topics["/imu/data"].message_type = "nonsense_msgs/msg/Wrong"
        res = check_contract(good_bags[0], c)
        assert any(v.code == "wrong_type" for v in res.violations)

    def test_unreadable_bag_is_violation_not_crash(self, tmp_dir, good_bags):
        c = infer_contract(good_bags)
        res = check_contract(tmp_dir / "does_not_exist.mcap", c)
        assert not res.passed
        assert res.violations  # bag_unreadable rather than an exception

    def test_collects_all_violations(self, tmp_dir, good_bags):
        c = infer_contract(good_bags)
        c.topics["/missing_a"] = TopicContract(message_type="std_msgs/msg/Bool")
        c.topics["/missing_b"] = TopicContract(message_type="std_msgs/msg/Bool")
        res = check_contract(good_bags[0], c)
        codes = [v.topic for v in res.violations]
        assert "/missing_a" in codes and "/missing_b" in codes


class TestRoundTrip:
    def test_json_round_trip(self, tmp_dir, good_bags):
        c = infer_contract(good_bags)
        p = tmp_dir / "contract.json"
        save_contract(c, p)
        c2 = load_contract(p)
        assert set(c.topics) == set(c2.topics)
        assert sorted(c.tf_frames) == sorted(c2.tf_frames)

    def test_yaml_round_trip(self, tmp_dir, good_bags):
        pytest.importorskip("yaml")
        c = infer_contract(good_bags)
        p = tmp_dir / "contract.yaml"
        save_contract(c, p)
        c2 = load_contract(p)
        assert set(c.topics) == set(c2.topics)
        imu = c2.topics["/imu/data"]
        assert imu.message_type == "sensor_msgs/msg/Imu"

    def test_to_from_dict_stable(self, good_bags):
        c = infer_contract(good_bags)
        c2 = Contract.from_dict(c.to_dict())
        assert set(c.topics) == set(c2.topics)
        assert c.version == c2.version
