"""Tests for MultiBagPlayback (Sub-feature 2.1).

Covers:
- BagPlaybackConfig validation (duplicates, empty IDs, negative offsets)
- Topic namespacing (bag_id prefix on every emitted message)
- play / pause / stop / seek / set_speed coherent across engines
- Discovery info (bag entries + namespaced topics)
- Per-bag offset staggers start times
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from resurrector.bridge.multibag import (
    BagPlaybackConfig,
    MultiBagPlayback,
    _validate_configs,
)
from resurrector.bridge.playback import PlaybackState


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def bag_a(tmp_dir):
    from resurrector.demo.sample_bag import generate_bag, BagConfig
    bag_path = tmp_dir / "bag_a.mcap"
    generate_bag(bag_path, BagConfig(duration_sec=1.0))
    return bag_path


@pytest.fixture
def bag_b(tmp_dir):
    from resurrector.demo.sample_bag import generate_bag, BagConfig
    bag_path = tmp_dir / "bag_b.mcap"
    generate_bag(bag_path, BagConfig(duration_sec=1.0))
    return bag_path


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestConfigValidation:
    def test_empty_configs_raises(self):
        with pytest.raises(ValueError, match="at least one bag config"):
            _validate_configs([])

    def test_empty_bag_id_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            _validate_configs([
                BagPlaybackConfig(bag_path="/x.mcap", bag_id=""),
            ])

    def test_duplicate_bag_id_raises(self):
        with pytest.raises(ValueError, match="Duplicate bag_id"):
            _validate_configs([
                BagPlaybackConfig(bag_path="/a.mcap", bag_id="a"),
                BagPlaybackConfig(bag_path="/b.mcap", bag_id="a"),
            ])

    def test_negative_offset_raises(self):
        with pytest.raises(ValueError, match="must be >= 0"):
            _validate_configs([
                BagPlaybackConfig(bag_path="/a.mcap", bag_id="a", offset_sec=-1.0),
            ])

    def test_valid_configs_pass(self):
        _validate_configs([
            BagPlaybackConfig(bag_path="/a.mcap", bag_id="a"),
            BagPlaybackConfig(bag_path="/b.mcap", bag_id="b", offset_sec=2.5),
        ])


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConstruction:
    def test_constructs_one_engine_per_config(self, bag_a, bag_b):
        mp = MultiBagPlayback(configs=[
            BagPlaybackConfig(bag_path=bag_a, bag_id="a"),
            BagPlaybackConfig(bag_path=bag_b, bag_id="b"),
        ])
        assert len(mp._engines) == 2

    def test_state_is_stopped_initially(self, bag_a):
        mp = MultiBagPlayback(configs=[
            BagPlaybackConfig(bag_path=bag_a, bag_id="a"),
        ])
        assert mp.state == PlaybackState.STOPPED


# ---------------------------------------------------------------------------
# Discovery info
# ---------------------------------------------------------------------------

class TestDiscoveryInfo:
    def test_lists_one_entry_per_bag(self, bag_a, bag_b):
        mp = MultiBagPlayback(configs=[
            BagPlaybackConfig(bag_path=bag_a, bag_id="a", label="Run A"),
            BagPlaybackConfig(bag_path=bag_b, bag_id="b", offset_sec=2.5),
        ])
        info = mp.get_discovery_info()
        assert len(info["bags"]) == 2
        assert info["bags"][0]["id"] == "a"
        assert info["bags"][0]["label"] == "Run A"
        assert info["bags"][1]["id"] == "b"
        assert info["bags"][1]["offset_sec"] == 2.5
        # Empty label falls back to bag_id
        assert info["bags"][1]["label"] == "b"

    def test_namespaces_topics(self, bag_a, bag_b):
        mp = MultiBagPlayback(configs=[
            BagPlaybackConfig(bag_path=bag_a, bag_id="a"),
            BagPlaybackConfig(bag_path=bag_b, bag_id="b"),
        ])
        info = mp.get_discovery_info()
        ns = info["namespaced_topics"]
        # Every topic should be prefixed with bag_id:
        assert all(":" in t and t.startswith(("a:", "b:")) for t in ns)
        # Both bags contribute (synthetic bag has /imu/data, /joint_states, etc.)
        assert any(t.startswith("a:") for t in ns)
        assert any(t.startswith("b:") for t in ns)

    def test_each_bag_entry_has_duration(self, bag_a):
        mp = MultiBagPlayback(configs=[
            BagPlaybackConfig(bag_path=bag_a, bag_id="a"),
        ])
        info = mp.get_discovery_info()
        assert info["bags"][0]["duration_sec"] == pytest.approx(1.0, rel=0.1)


# ---------------------------------------------------------------------------
# Topic namespacing in callbacks
# ---------------------------------------------------------------------------

class TestTopicNamespacing:
    @pytest.mark.asyncio
    async def test_messages_arrive_with_namespaced_topic(self, bag_a, bag_b):
        seen: list[tuple[str, str]] = []  # (bag_id, topic)
        mp = MultiBagPlayback(
            configs=[
                BagPlaybackConfig(bag_path=bag_a, bag_id="a"),
                BagPlaybackConfig(bag_path=bag_b, bag_id="b"),
            ],
            speed=20.0,  # max speed; finishes the 1-second bag in ~50ms
            message_callback=lambda bid, msg: seen.append((bid, msg.topic)),
        )
        await mp.play()
        # Wait for bags to finish playing (1.0 sec / 20x speed = 50ms; pad with margin)
        await asyncio.sleep(0.5)
        await mp.stop()
        # Both bag_ids should appear; every topic should be namespaced
        bids = {bid for bid, _ in seen}
        assert "a" in bids and "b" in bids
        for bid, topic in seen:
            assert topic.startswith(f"{bid}:"), \
                f"topic {topic!r} should start with {bid!r}: prefix"


# ---------------------------------------------------------------------------
# Playback control
# ---------------------------------------------------------------------------

class TestPlaybackControl:
    @pytest.mark.asyncio
    async def test_play_then_stop(self, bag_a):
        mp = MultiBagPlayback(
            configs=[BagPlaybackConfig(bag_path=bag_a, bag_id="a")],
            speed=20.0,
        )
        await mp.play()
        await asyncio.sleep(0.05)
        await mp.stop()
        assert mp.state == PlaybackState.STOPPED

    @pytest.mark.asyncio
    async def test_set_speed_propagates_to_all_engines(self, bag_a, bag_b):
        mp = MultiBagPlayback(
            configs=[
                BagPlaybackConfig(bag_path=bag_a, bag_id="a"),
                BagPlaybackConfig(bag_path=bag_b, bag_id="b"),
            ],
            speed=1.0,
        )
        await mp.set_speed(5.0)
        assert mp.speed == 5.0
        for engine in mp._engines:
            assert engine.speed == 5.0


# ---------------------------------------------------------------------------
# Per-bag offset staggers start times
# ---------------------------------------------------------------------------

class TestOffsetStaggering:
    @pytest.mark.asyncio
    async def test_offset_delays_bag_start(self, bag_a, bag_b):
        """Bag B with offset=0.3s should not emit ANY messages until ~0.3s after start.

        We use a low speed so the offset is observable in real time.
        """
        first_seen_at: dict[str, float] = {}
        loop = asyncio.get_event_loop()
        mp = MultiBagPlayback(
            configs=[
                BagPlaybackConfig(bag_path=bag_a, bag_id="a", offset_sec=0.0),
                BagPlaybackConfig(bag_path=bag_b, bag_id="b", offset_sec=0.3),
            ],
            speed=1.0,  # real-time so offset semantics are meaningful
            message_callback=lambda bid, msg: first_seen_at.setdefault(
                bid, loop.time(),
            ),
        )
        start = loop.time()
        await mp.play()
        # Run for 0.5s — long enough to observe both bags starting
        await asyncio.sleep(0.5)
        await mp.stop()

        assert "a" in first_seen_at, "bag a never emitted a message"
        assert "b" in first_seen_at, "bag b never emitted a message"
        a_start = first_seen_at["a"] - start
        b_start = first_seen_at["b"] - start
        # The invariant that actually matters is RELATIVE: bag a (offset 0)
        # starts well before bag b (offset 0.3s). That's jitter-proof. An
        # absolute "a_start < 0.1" bound is flaky on loaded CI runners —
        # thread startup + asyncio scheduling can eat >100ms before the
        # first callback fires (observed: 0.103s on a 3.10 runner).
        assert a_start < b_start, (
            f"bag a ({a_start:.3f}s) should start before bag b ({b_start:.3f}s)"
        )
        # bag a should still start promptly — not delayed by the 0.3s offset.
        # 0.25 keeps it clearly below b's offset while tolerating scheduling slack.
        assert a_start < 0.25, f"bag a started after {a_start:.3f}s, expected ~0"
        # bag b should start at ~0.3s (offset) + scheduling slack.
        assert 0.2 < b_start < 0.6, f"bag b started after {b_start:.3f}s, expected ~0.3"
