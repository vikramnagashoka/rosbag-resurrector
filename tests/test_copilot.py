"""Tests for the 'Ask your bag' grounded copilot — v0.7 Feature B.

All offline. The LLM path is exercised with an injected fake client so no
network or API key is needed. Covers:
- Evidence gathering (window clamping, per-topic activity, totals)
- Rule-based summary content (grounded in evidence)
- LLM narrative via injected client
- Graceful fallback when the LLM client raises
- Grounding guarantee: the rule-based summary references only evidence facts
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from resurrector.core.copilot import (
    ExplainResult,
    explain_time_range,
    gather_evidence,
    summarize_rule_based,
)
from resurrector.demo.sample_bag import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def bag(tmp_dir):
    p = tmp_dir / "b.mcap"
    generate_bag(p, BagConfig(duration_sec=4.0, imu_hz=100.0))
    return p


class TestGatherEvidence:
    def test_basic_shape(self, bag):
        ev = gather_evidence(bag, 1.0, 2.0)
        assert ev["window"]["start_sec"] == 1.0
        assert ev["window"]["end_sec"] == 2.0
        assert ev["window"]["duration_sec"] == pytest.approx(1.0, abs=0.01)
        assert ev["totals"]["messages_in_window"] > 0
        assert ev["totals"]["active_topics"] > 0
        assert isinstance(ev["topics"], list)
        assert isinstance(ev["health_issues"], list)

    def test_window_clamped_to_bag(self, bag):
        # Ask for a window past the end; it clamps to the bag duration.
        ev = gather_evidence(bag, 100.0, 200.0)
        assert ev["window"]["end_sec"] <= ev["window"]["bag_duration_sec"] + 0.01
        assert ev["window"]["start_sec"] <= ev["window"]["end_sec"]

    def test_topics_sorted_by_activity(self, bag):
        ev = gather_evidence(bag, 0.0, 4.0)
        counts = [t["count_in_window"] for t in ev["topics"]]
        assert counts == sorted(counts, reverse=True)

    def test_imu_is_active(self, bag):
        ev = gather_evidence(bag, 0.0, 4.0)
        names = {t["topic"] for t in ev["topics"]}
        assert "/imu/data" in names
        imu = next(t for t in ev["topics"] if t["topic"] == "/imu/data")
        # ~100 Hz over 4s ≈ 400 messages.
        assert imu["count_in_window"] > 100

    def test_narrow_window_has_fewer_messages(self, bag):
        wide = gather_evidence(bag, 0.0, 4.0)["totals"]["messages_in_window"]
        narrow = gather_evidence(bag, 1.0, 1.5)["totals"]["messages_in_window"]
        assert narrow < wide


class TestRuleBasedSummary:
    def test_mentions_window_and_counts(self, bag):
        ev = gather_evidence(bag, 1.0, 2.0)
        text = summarize_rule_based(ev)
        assert "1.00s" in text or "1.0" in text
        assert "messages were recorded" in text
        # The most-active topic should be named.
        assert ev["topics"][0]["topic"] in text

    def test_grounded_only_in_evidence(self, bag):
        # Every topic named in the summary must exist in the evidence — the
        # grounding guarantee. (Rule-based can't hallucinate by construction;
        # this pins it.)
        ev = gather_evidence(bag, 0.0, 4.0)
        text = summarize_rule_based(ev)
        evidence_topics = {t["topic"] for t in ev["topics"]}
        evidence_topics |= set(ev["totals"]["silent_topics"])
        import re
        mentioned = set(re.findall(r"/[\w/]+", text))
        # Any slash-prefixed token in the prose must be a real bag topic.
        assert mentioned <= evidence_topics

    def test_no_health_issues_line(self, bag):
        ev = gather_evidence(bag, 0.0, 4.0)
        ev["health_issues"] = []
        text = summarize_rule_based(ev)
        assert "No health findings" in text


class _FakeContentBlock:
    def __init__(self, text):
        self.text = text


class _FakeResponse:
    def __init__(self, text):
        self.content = [_FakeContentBlock(text)]


class _FakeAnthropic:
    """Minimal stand-in for anthropic.Anthropic that records the prompt."""
    def __init__(self):
        self.last_system = None
        self.last_user = None
        self.messages = self

    def create(self, model, max_tokens, system, messages):
        self.last_system = system
        self.last_user = messages[0]["content"]
        return _FakeResponse("The robot's IMU slowed in this window.")


class TestExplainTimeRange:
    def test_rule_based_when_no_llm(self, bag):
        result = explain_time_range(bag, 1.0, 2.0, use_llm=False)
        assert isinstance(result, ExplainResult)
        assert result.source == "rule_based"
        assert result.evidence["totals"]["messages_in_window"] > 0
        assert result.narrative

    def test_llm_path_with_injected_client(self, bag):
        fake = _FakeAnthropic()
        result = explain_time_range(bag, 1.0, 2.0, use_llm=True, client=fake)
        assert result.source == "llm"
        assert "IMU slowed" in result.narrative
        # The evidence JSON must have been handed to the model (grounding).
        assert "Evidence JSON" in fake.last_user
        assert "only state facts present" in fake.last_system

    def test_graceful_fallback_when_client_raises(self, bag):
        class Boom:
            def __init__(self):
                self.messages = self

            def create(self, **kwargs):
                raise RuntimeError("api down")

        result = explain_time_range(bag, 1.0, 2.0, use_llm=True, client=Boom())
        # Falls back to the deterministic summary; never raises.
        assert result.source == "rule_based"
        assert result.narrative

    def test_result_to_dict_round_trips(self, bag):
        result = explain_time_range(bag, 0.0, 4.0, use_llm=False)
        d = result.to_dict()
        assert set(d.keys()) >= {"narrative", "source", "start_sec", "end_sec", "evidence"}


class TestExplainAPI:
    def test_explain_endpoint(self, tmp_dir, monkeypatch):
        from resurrector.ingest.indexer import BagIndex
        from resurrector.ingest.scanner import scan_path
        from resurrector.ingest.parser import parse_bag

        p = tmp_dir / "b.mcap"
        generate_bag(p, BagConfig(duration_sec=3.0, imu_hz=100.0))
        db = tmp_dir / "index.db"
        index = BagIndex(db)
        bag_id = None
        for sf in scan_path(p):
            bag_id = index.upsert_bag(sf, parse_bag(sf.path).get_metadata())
        index.close()

        monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db))
        # Ensure no real API key is picked up — force rule-based.
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        import importlib
        from resurrector.dashboard import api as api_mod
        importlib.reload(api_mod)
        from fastapi.testclient import TestClient
        client = TestClient(api_mod.app)

        r = client.get(
            f"/api/bags/{bag_id}/explain",
            params={"start_sec": 0.5, "end_sec": 1.5},
        )
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["source"] == "rule_based"
        assert data["evidence"]["totals"]["messages_in_window"] > 0
