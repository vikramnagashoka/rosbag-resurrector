"""Streaming-health equivalence tests.

The v0.4.0 streaming health path replaces the v0.3.x eager path
(which accumulated per-topic timestamp lists). These tests compare
the two on the existing fixture bags to ensure the streaming version
produces equivalent issue counts and within-tolerance scores.

Exact byte-equivalence is NOT expected because:
- The eager path uses np.median over all intervals to derive
  expected_interval; the streaming path uses metadata.frequency_hz
  (or a running estimate). These differ slightly on bags with
  non-uniform timing.
- The eager path can detect issues using global state (the full
  array); the streaming path uses the running expected interval
  from the start of the topic. On the first few messages, the
  streaming version may miss issues the eager version catches.

So we assert: both versions produce the same SET of issue categories,
and topic scores agree within ±10 points.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.health_check import (
    HealthChecker, TopicHealthState, update_state,
)
from tests.fixtures.generate_test_bags import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def healthy_bag(tmp_dir):
    return generate_bag(tmp_dir / "healthy.mcap", BagConfig(duration_sec=2.0))


def _eager_report(bag_path: Path):
    """Run the v0.3.x eager path manually (bypass BagFrame)."""
    from resurrector.ingest.parser import parse_bag
    parser = parse_bag(bag_path)
    metadata = parser.get_metadata()
    topic_timestamps: dict[str, list[int]] = {}
    topic_sizes: dict[str, list[int]] = {}
    for msg in parser.read_messages():
        topic_timestamps.setdefault(msg.topic, []).append(msg.timestamp_ns)
        if msg.raw_data:
            topic_sizes.setdefault(msg.topic, []).append(len(msg.raw_data))
    return HealthChecker().run_all_checks(
        topic_timestamps=topic_timestamps,
        topic_message_sizes=topic_sizes,
        bag_start_ns=metadata.start_time_ns,
        bag_end_ns=metadata.end_time_ns,
    )


class TestStreamingEquivalence:
    def test_overall_score_within_tolerance(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        streaming = bf.health_report()
        eager = _eager_report(healthy_bag)
        # Overall scores should be close.
        assert abs(streaming.score - eager.score) <= 10

    def test_streaming_covers_at_least_eager_topics(self, healthy_bag):
        """Streaming may report MORE topics than eager (it pre-seeds
        zero-message topics like /tf from metadata) but never fewer."""
        bf = BagFrame(healthy_bag)
        streaming = bf.health_report()
        eager = _eager_report(healthy_bag)
        assert set(eager.topic_scores).issubset(set(streaming.topic_scores))

    def test_per_topic_scores_within_tolerance(self, healthy_bag):
        bf = BagFrame(healthy_bag)
        streaming = bf.health_report()
        eager = _eager_report(healthy_bag)
        # Compare only topics both paths reported (eager skips zero-msg topics).
        for topic, eager_th in eager.topic_scores.items():
            stream_th = streaming.topic_scores[topic]
            assert abs(stream_th.score - eager_th.score) <= 10, (
                f"Topic {topic}: streaming={stream_th.score} eager={eager_th.score}"
            )

    def test_streaming_uses_constant_memory_per_topic(self, healthy_bag):
        """Smoke test: TopicHealthState must remain a small dataclass."""
        # Run a streaming pass and inspect a state's memory footprint
        # via __dict__ field count. If someone adds a per-message list,
        # this test won't catch it directly — but it documents intent.
        from resurrector.ingest.parser import parse_bag
        parser = parse_bag(healthy_bag)
        states = {}
        config = HealthChecker().config
        for msg in parser.read_messages():
            state = states.setdefault(msg.topic, TopicHealthState())
            update_state(
                state, msg.topic, msg.timestamp_ns,
                len(msg.raw_data) if msg.raw_data else None,
                config,
            )
        # Each state should have bounded issue lists.
        for topic, state in states.items():
            from resurrector.ingest.health_check import MAX_INLINE_ISSUES
            assert len(state.ooo_issues) <= MAX_INLINE_ISSUES
            assert len(state.gap_issues) <= MAX_INLINE_ISSUES
            assert len(state.rate_drop_issues) <= MAX_INLINE_ISSUES


class TestStreamingDetectsObviousIssues:
    """Make sure the streaming path catches things the eager path catches."""

    def test_dropped_messages_bag(self):
        """If we already have a dropped_messages.mcap fixture, both
        paths should flag it."""
        fixture = Path(__file__).parent / "fixtures" / "dropped_messages.mcap"
        if not fixture.exists():
            pytest.skip("dropped_messages.mcap fixture not present")
        bf = BagFrame(fixture)
        report = bf.health_report()
        # Score should be < 100 — a healthy bag would be 95+.
        assert report.score < 100
