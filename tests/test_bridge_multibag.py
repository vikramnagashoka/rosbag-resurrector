"""Tests for MultiBagPlayback (Sub-feature 2.1).

Covers:
- BagPlaybackConfig validation (duplicates, empty IDs, negative offsets)
- Topic namespacing (bag_id prefix on every emitted message)
- play / pause / stop / seek / set_speed coherent across engines
- Discovery info (bag entries + namespaced topics)
- Per-bag offset staggers start times
- Pause / resume keeps offsets served exactly once per session
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


@pytest.fixture
def long_bag(tmp_dir):
    """10 s bag: at 1x it can't finish before a sub-second offset elapses
    unless the whole process stalls for ~10 s."""
    from resurrector.demo.sample_bag import generate_bag, BagConfig
    bag_path = tmp_dir / "long_bag.mcap"
    generate_bag(bag_path, BagConfig(duration_sec=10.0))
    return bag_path


# asyncio may fire a timer up to the monotonic clock's resolution early
# (under a microsecond on macOS/Linux); this absorbs that, so a delay
# lower bound stays exact.
_TIMER_SLACK_SEC = 0.02


async def _wait_until(predicate, timeout_sec: float = 15.0) -> bool:
    """Poll until ``predicate()`` holds; False on timeout.

    The deadline is a hang detector, not a timing assertion: nothing a
    correct engine does on a starved runner comes close to it.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_sec
    while not predicate():
        if loop.time() > deadline:
            return False
        await asyncio.sleep(0.01)
    return True


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
    async def test_offset_delays_bag_start(self, long_bag, bag_b):
        """Bag b (offset 0.3 s) starts after its offset; bag a (offset 0) goes first.

        Would catch: offsets ignored (b emits before 0.3 s), bag a held
        back behind b, or b only starting once bag a has finished.

        Each assertion is a bound a correct engine meets however starved the
        runner is: a delay never ends early, a's start is queued ahead of
        b's timer, and a's 10 s bag can't finish inside b's 0.3 s offset.
        The old version gave both bags a fixed 0.5 s window and asserted
        absolute start times, which flaked on a CPU-starved machine
        (bag b's first message landing after the window closed).
        """
        offset = 0.3
        loop = asyncio.get_running_loop()
        first_seen_at: dict[str, float] = {}
        a_emitted = 0
        a_emitted_when_b_started: int | None = None

        def on_message(bid: str, _msg) -> None:
            nonlocal a_emitted, a_emitted_when_b_started
            if bid == "a":
                a_emitted += 1
            elif a_emitted_when_b_started is None:
                a_emitted_when_b_started = a_emitted
            first_seen_at.setdefault(bid, loop.time())

        mp = MultiBagPlayback(
            configs=[
                BagPlaybackConfig(bag_path=long_bag, bag_id="a", offset_sec=0.0),
                BagPlaybackConfig(bag_path=bag_b, bag_id="b", offset_sec=offset),
            ],
            speed=1.0,  # real-time so offset semantics are meaningful
            message_callback=on_message,
        )
        a_total = sum(t["count"] for t in mp.get_discovery_info()["bags"][0]["topics"])
        start = loop.time()
        await mp.play()
        try:
            await _wait_until(lambda: len(first_seen_at) == 2)
        finally:
            await mp.stop()

        assert "a" in first_seen_at, "bag a never emitted a message"
        assert "b" in first_seen_at, "bag b never emitted a message"
        a_start = first_seen_at["a"] - start
        b_start = first_seen_at["b"] - start
        assert b_start >= offset - _TIMER_SLACK_SEC, (
            f"bag b started {b_start:.3f}s after play(), before its {offset}s offset"
        )
        assert a_start < b_start, (
            f"bag a ({a_start:.3f}s) should start before bag b ({b_start:.3f}s)"
        )
        assert a_emitted_when_b_started < a_total, (
            "bag b only started after bag a played all "
            f"{a_total} messages; it should start {offset}s in"
        )


# ---------------------------------------------------------------------------
# Pause / resume across per-bag offsets
# ---------------------------------------------------------------------------

class TestPauseResume:
    """Each bag's offset is served once per session (play() after stop()).

    loop=True in these tests keeps every engine alive, so no bag can reach
    end-of-bag and change state on its own however slow the runner is.
    """

    @pytest.mark.asyncio
    async def test_resume_continues_started_bags_without_their_offset(self, bag_a, bag_b):
        """Resume must continue every bag that had already started, at once.

        Would catch: play() re-applying start offsets on resume, so bag b
        (already playing when paused) sat paused for its offset again and
        fell a further `offset` behind bag a on every pause/resume.

        Asserted on engine state straight after play() returns, which takes
        no timing: resuming a paused engine is synchronous, while the bug
        leaves b paused behind a fresh delay task.
        """
        seen: list[str] = []
        mp = MultiBagPlayback(
            configs=[
                BagPlaybackConfig(bag_path=bag_a, bag_id="a", offset_sec=0.0),
                BagPlaybackConfig(bag_path=bag_b, bag_id="b", offset_sec=0.1),
            ],
            speed=1.0,
            loop=True,
            message_callback=lambda bid, _msg: seen.append(bid),
        )
        await mp.play()
        try:
            assert await _wait_until(lambda: "b" in seen), "bag b never started"
            await mp.pause()
            assert [e.state for e in mp._engines] == [PlaybackState.PAUSED] * 2

            await mp.play()
            states = [e.state for e in mp._engines]
            assert states == [PlaybackState.PLAYING] * 2, (
                f"engine states after resume: {states}; bag b is waiting "
                "out its 0.1 s offset a second time"
            )
            resumed_at = len(seen)
            assert await _wait_until(lambda: "b" in seen[resumed_at:]), (
                "bag b emitted nothing after resume"
            )
        finally:
            await mp.stop()

    @pytest.mark.asyncio
    async def test_pause_before_offset_elapses_holds_the_bag(self, bag_a, bag_b):
        """A bag still inside its offset stays held through a pause, then
        waits out only the unserved rest of the offset after resume.

        Would catch: pause() leaving the start delay running, so bag b began
        playing in the middle of a pause; and resume starting a held bag at
        once, dropping the rest of its offset.
        """
        offset = 0.3
        loop = asyncio.get_running_loop()
        first_seen_at: dict[str, float] = {}
        mp = MultiBagPlayback(
            configs=[
                BagPlaybackConfig(bag_path=bag_a, bag_id="a", offset_sec=0.0),
                BagPlaybackConfig(bag_path=bag_b, bag_id="b", offset_sec=offset),
            ],
            speed=1.0,
            loop=True,
            message_callback=lambda bid, _msg: first_seen_at.setdefault(bid, loop.time()),
        )
        t_play = loop.time()
        await mp.play()
        await mp.pause()
        # At most this much of b's offset was served before pause() froze it.
        served_at_most = loop.time() - t_play
        try:
            # Outlast b's original start time. Its delay timer is due before
            # this sleep's, so a delay pause() left running has already called
            # engine.play() by the time this returns.
            await asyncio.sleep(2 * offset)
            b_state = mp._engines[1].state
            assert b_state == PlaybackState.STOPPED and "b" not in first_seen_at, (
                f"bag b started while the session was paused (state={b_state})"
            )

            t_resume = loop.time()
            await mp.play()
            assert await _wait_until(lambda: "b" in first_seen_at), (
                "bag b never started after resume"
            )
            waited = first_seen_at["b"] - t_resume
            unserved = offset - served_at_most
            assert waited >= unserved - _TIMER_SLACK_SEC, (
                f"bag b started {waited:.3f}s after resume, with at least "
                f"{unserved:.3f}s of its offset still to wait"
            )
        finally:
            await mp.stop()

    @pytest.mark.asyncio
    async def test_play_after_stop_reapplies_offsets(self, bag_a, bag_b):
        """stop() ends the session, so the next play() serves offsets again.

        Would catch: resume bookkeeping surviving stop(), which would start
        bag b alongside bag a on the next play().
        """
        seen: list[str] = []
        mp = MultiBagPlayback(
            configs=[
                BagPlaybackConfig(bag_path=bag_a, bag_id="a", offset_sec=0.0),
                BagPlaybackConfig(bag_path=bag_b, bag_id="b", offset_sec=0.1),
            ],
            speed=1.0,
            loop=True,
            message_callback=lambda bid, _msg: seen.append(bid),
        )
        await mp.play()
        try:
            assert await _wait_until(lambda: "b" in seen), "bag b never started"
            await mp.stop()

            await mp.play()
            states = [e.state for e in mp._engines]
            assert states == [PlaybackState.PLAYING, PlaybackState.STOPPED], (
                f"engine states after stop() then play(): {states}; "
                "bag b should be waiting out its offset again"
            )
            restarted_at = len(seen)
            assert await _wait_until(lambda: "b" in seen[restarted_at:]), (
                "bag b never started after the restart"
            )
        finally:
            await mp.stop()
