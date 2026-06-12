"""Tests for semantic bag diff — v0.7 Feature F.

Covers:
- Identical bags → no changes
- Topic added / removed (via include_tf toggle on the synth bag)
- Rate shift detection (imu_hz change beyond the 10% threshold)
- Message-count + duration deltas
- Rainy day: missing operand raises FileNotFoundError
- JSON round-trip + API endpoint
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from resurrector.core.bag_diff import BagDiff, TopicChange, diff_bags
from resurrector.demo.sample_bag import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


class TestDiffBags:
    def test_identical_bags_no_changes(self, tmp_dir):
        a = tmp_dir / "a.mcap"
        b = tmp_dir / "b.mcap"
        generate_bag(a, BagConfig(duration_sec=1.0, imu_hz=100.0))
        generate_bag(b, BagConfig(duration_sec=1.0, imu_hz=100.0))
        d = diff_bags(a, b)
        assert isinstance(d, BagDiff)
        assert not d.has_changes
        assert d.n_changes == 0
        assert d.topics_added == []
        assert d.topics_removed == []

    def test_topic_added(self, tmp_dir):
        # A without /tf, B with /tf → /tf is "added" in B.
        a = tmp_dir / "a.mcap"
        b = tmp_dir / "b.mcap"
        generate_bag(a, BagConfig(duration_sec=1.0, include_tf=False))
        generate_bag(b, BagConfig(duration_sec=1.0, include_tf=True))
        d = diff_bags(a, b)
        assert "/tf" in d.topics_added
        assert d.has_changes

    def test_topic_removed(self, tmp_dir):
        # A with /tf, B without → /tf is "removed".
        a = tmp_dir / "a.mcap"
        b = tmp_dir / "b.mcap"
        generate_bag(a, BagConfig(duration_sec=1.0, include_tf=True))
        generate_bag(b, BagConfig(duration_sec=1.0, include_tf=False))
        d = diff_bags(a, b)
        assert "/tf" in d.topics_removed

    def test_rate_shift_detected(self, tmp_dir):
        a = tmp_dir / "a.mcap"
        b = tmp_dir / "b.mcap"
        generate_bag(a, BagConfig(duration_sec=2.0, imu_hz=100.0))
        generate_bag(b, BagConfig(duration_sec=2.0, imu_hz=200.0))
        d = diff_bags(a, b)
        rate_changes = [c for c in d.topic_changes if c.kind == "rate" and c.topic == "/imu/data"]
        assert rate_changes, "expected a rate change on /imu/data"
        assert rate_changes[0].a_value < rate_changes[0].b_value

    def test_small_rate_wobble_not_flagged(self, tmp_dir):
        # A ~5% rate difference is below the 10% threshold — should be ignored.
        a = tmp_dir / "a.mcap"
        b = tmp_dir / "b.mcap"
        generate_bag(a, BagConfig(duration_sec=2.0, imu_hz=100.0))
        generate_bag(b, BagConfig(duration_sec=2.0, imu_hz=104.0))
        d = diff_bags(a, b)
        rate_changes = [c for c in d.topic_changes if c.kind == "rate" and c.topic == "/imu/data"]
        assert not rate_changes

    def test_duration_and_count_deltas(self, tmp_dir):
        a = tmp_dir / "a.mcap"
        b = tmp_dir / "b.mcap"
        generate_bag(a, BagConfig(duration_sec=1.0, imu_hz=100.0))
        generate_bag(b, BagConfig(duration_sec=2.0, imu_hz=100.0))
        d = diff_bags(a, b)
        # B is ~1s longer and has more messages.
        assert d.duration_delta_sec > 0.5
        assert d.message_count_delta > 0

    def test_missing_baseline_raises(self, tmp_dir):
        b = tmp_dir / "b.mcap"
        generate_bag(b, BagConfig(duration_sec=1.0))
        with pytest.raises(FileNotFoundError):
            diff_bags(tmp_dir / "nope.mcap", b)

    def test_missing_candidate_raises(self, tmp_dir):
        a = tmp_dir / "a.mcap"
        generate_bag(a, BagConfig(duration_sec=1.0))
        with pytest.raises(FileNotFoundError):
            diff_bags(a, tmp_dir / "nope.mcap")

    def test_to_dict_and_json_round_trip(self, tmp_dir):
        a = tmp_dir / "a.mcap"
        b = tmp_dir / "b.mcap"
        generate_bag(a, BagConfig(duration_sec=1.0, imu_hz=100.0))
        generate_bag(b, BagConfig(duration_sec=2.0, imu_hz=200.0))
        d = diff_bags(a, b)
        parsed = json.loads(d.to_json())
        assert parsed["bag_a"].endswith("a.mcap")
        assert "summary" in parsed
        assert isinstance(parsed["topic_changes"], list)


class TestDiffAPI:
    def test_diff_endpoint(self, tmp_dir, monkeypatch):
        # Index two bags then hit /api/diff.
        from resurrector.ingest.indexer import BagIndex
        from resurrector.ingest.scanner import scan_path
        from resurrector.ingest.parser import parse_bag

        a = tmp_dir / "a.mcap"
        b = tmp_dir / "b.mcap"
        generate_bag(a, BagConfig(duration_sec=1.0, imu_hz=100.0))
        generate_bag(b, BagConfig(duration_sec=2.0, imu_hz=200.0))

        db = tmp_dir / "index.db"
        index = BagIndex(db)
        ids = {}
        for p in (a, b):
            for sf in scan_path(p):
                meta = parse_bag(sf.path).get_metadata()
                ids[p.name] = index.upsert_bag(sf, meta)
        index.close()

        # Point the API at our temp index.
        monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db))
        import importlib
        from resurrector.dashboard import api as api_mod
        importlib.reload(api_mod)
        from fastapi.testclient import TestClient
        client = TestClient(api_mod.app)

        r = client.get("/api/diff", params={"bag_a_id": ids["a.mcap"], "bag_b_id": ids["b.mcap"]})
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["bag_a"].endswith("a.mcap")
        assert "summary" in data

    def test_diff_endpoint_404_for_missing_bag(self, tmp_dir, monkeypatch):
        from resurrector.ingest.indexer import BagIndex
        db = tmp_dir / "index.db"
        BagIndex(db).close()
        monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db))
        import importlib
        from resurrector.dashboard import api as api_mod
        importlib.reload(api_mod)
        from fastapi.testclient import TestClient
        client = TestClient(api_mod.app)
        r = client.get("/api/diff", params={"bag_a_id": 999, "bag_b_id": 998})
        assert r.status_code == 404
