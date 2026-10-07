"""Tests for MultiBagPlayback (Sub-feature 2.1).

Covers:
- BagPlaybackConfig validation (duplicates, empty IDs, negative offsets)
- Topic namespacing (bag_id prefix on every emitted message)
- play / pause / stop / seek / set_speed coherent across engines
- Discovery info (bag entries + namespaced topics)
- Per-bag offset staggers start times
- Pause / resume keeps offsets served exactly once per session
- Offset bookkeeping across pause / set_speed / stop / seek, on state alone
- Concurrent pause() + play() leave one coherent session
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Callable

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


# A hang detector, not a timing bound. The wait returns as soon as the
# predicate holds, so the limit costs nothing on a healthy run. It is
# generous because a CPU-starved runner can stall the event loop for many
# seconds, and that alone must not fail a test; only a real hang should.
_HANG_TIMEOUT_SEC = 120.0

# Bounds below compare loop.time() values (monotonic seconds, up to ~1e7
# on a long-running host), so float rounding in deadline arithmetic is
# ~1e-9 s; this only absorbs that.
_FLOAT_EPS = 1e-6

# An offset no state-based test can see fire: even at the 20x speed cap it
# is a 3-minute wait, against at most a 0.05 s sleep inside those tests.
_FAR_OFFSET_SEC = 3600.0


async def _wait_until(
    predicate: Callable[[], bool],
    failure: str | Callable[[], str],
    timeout_sec: float = _HANG_TIMEOUT_SEC,
) -> float:
    """Poll until ``predicate()`` holds and return the seconds waited.

    The deadline is a hang detector, not a timing assertion. On timeout
    the test fails with ``failure`` (called first if it's a callable, so
    it can describe state at that moment) plus the time waited.
    """
    loop = asyncio.get_running_loop()
    start = loop.time()
    while not predicate():
        elapsed = loop.time() - start
        if elapsed > timeout_sec:
            msg = failure() if callable(failure) else failure
            pytest.fail(f"{msg} (still not true after {elapsed:.1f}s)")
        await asyncio.sleep(0.01)
    return loop.time() - start


def _two_bags(bag_a, bag_b, offset_b: float, **kwargs) -> MultiBagPlayback:
    """Bag a starts at once, bag b after ``offset_b``. loop=True keeps both
    engines alive, so neither can reach end-of-bag and change state alone."""
    return MultiBagPlayback(
        configs=[
            BagPlaybackConfig(bag_path=bag_a, bag_id="a", offset_sec=0.0),
            BagPlaybackConfig(bag_path=bag_b, bag_id="b", offset_sec=offset_b),
        ],
        loop=True,
        **kwargs,
    )


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
        try:
            # Polled, not a fixed window: a starved runner can take far
            # longer than the ~50 ms the bags need at 20x.
            await _wait_until(
                lambda: {bid for bid, _ in seen} == {"a", "b"},
                lambda: "bag(s) {} never emitted a message".format(
                    sorted({"a", "b"} - {bid for bid, _ in seen})),
            )
        finally:
            await mp.stop()
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
            # Timing-free: a starts now, b's start sits behind its offset.
            states = [e.state for e in mp._engines]
            assert states == [PlaybackState.PLAYING, PlaybackState.STOPPED], (
                f"engine states straight after play(): {states}"
            )
            await _wait_until(
                lambda: len(first_seen_at) == 2,
                lambda: "bag(s) {} never emitted a message".format(
                    sorted({"a", "b"} - first_seen_at.keys())),
            )
        finally:
            await mp.stop()

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
            await _wait_until(lambda: "b" in seen, "bag b never started")
            await mp.pause()
            assert [e.state for e in mp._engines] == [PlaybackState.PAUSED] * 2

            await mp.play()
            states = [e.state for e in mp._engines]
            assert states == [PlaybackState.PLAYING] * 2, (
                f"engine states after resume: {states}; bag b is waiting "
                "out its 0.1 s offset a second time"
            )
            resumed_at = len(seen)
            await _wait_until(
                lambda: "b" in seen[resumed_at:], "bag b emitted nothing after resume",
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
            await _wait_until(
                lambda: "b" in first_seen_at, "bag b never started after resume",
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
            await _wait_until(lambda: "b" in seen, "bag b never started")
            await mp.stop()

            await mp.play()
            states = [e.state for e in mp._engines]
            assert states == [PlaybackState.PLAYING, PlaybackState.STOPPED], (
                f"engine states after stop() then play(): {states}; "
                "bag b should be waiting out its offset again"
            )
            restarted_at = len(seen)
            await _wait_until(
                lambda: "b" in seen[restarted_at:], "bag b never started after the restart",
            )
        finally:
            await mp.stop()


# ---------------------------------------------------------------------------
# Offset bookkeeping, asserted on state (no timing)
# ---------------------------------------------------------------------------

class TestOffsetBookkeeping:
    """Offsets are bag-time seconds, waited out at ``offset / speed`` wall s.

    Every test here brackets a control call between two ``loop.time()``
    reads and checks the recorded remainder / deadline against bounds that
    hold however long the runner stalls. Offsets are ``_FAR_OFFSET_SEC``
    (except the one test that waits for a rescheduled offset to fire), so
    no wait can fire mid-test.
    """

    @pytest.mark.asyncio
    async def test_pause_inside_offset_keeps_the_unserved_remainder(self, bag_a, bag_b):
        """pause() holds bag b and records how much of its offset is left.

        Would catch: pause() storing the remainder in wall seconds instead
        of bag seconds (at 2x it would keep half), or not shrinking it at all.
        """
        speed = 2.0
        loop = asyncio.get_running_loop()
        mp = _two_bags(bag_a, bag_b, _FAR_OFFSET_SEC, speed=speed)
        t0 = loop.time()
        await mp.play()
        t_played = loop.time()
        try:
            # Serve a measurable slice of the offset. The sleep's length is
            # irrelevant: the bounds below use the times actually observed.
            await asyncio.sleep(0.05)
            t_pausing = loop.time()
            await mp.pause()
            t1 = loop.time()
            pending = mp._pending[1]
            assert pending.task is None, "bag b's offset wait still running after pause()"
            assert mp._engines[1].state == PlaybackState.STOPPED
            # The wait was scheduled inside play() and held inside pause(),
            # so the wall time it ran is between (t_pausing - t_played) and
            # (t1 - t0); times speed, that's the bag-time served.
            served_at_least = (t_pausing - t_played) * speed
            served_at_most = (t1 - t0) * speed
            assert (
                _FAR_OFFSET_SEC - served_at_most - _FLOAT_EPS
                <= pending.remaining_sec
                <= _FAR_OFFSET_SEC - served_at_least + _FLOAT_EPS
            ), (
                f"remaining {pending.remaining_sec!r} bag-s; expected the "
                f"{_FAR_OFFSET_SEC} s offset minus {served_at_least:.6f} to "
                f"{served_at_most:.6f} bag-s served at {speed}x"
            )
        finally:
            await mp.stop()

    @pytest.mark.asyncio
    async def test_set_speed_while_paused_rescales_the_remainder_on_resume(
        self, bag_a, bag_b,
    ):
        """After pause() -> set_speed(2) -> play(), b waits remaining / 2.

        Would catch: set_speed() starting a held wait during a pause, or
        play() scheduling the remainder at the old speed.
        """
        loop = asyncio.get_running_loop()
        mp = _two_bags(bag_a, bag_b, _FAR_OFFSET_SEC, speed=1.0)
        await mp.play()
        try:
            await mp.pause()
            pending = mp._pending[1]
            remaining = pending.remaining_sec
            await mp.set_speed(2.0)
            assert pending.task is None, "set_speed() restarted a held offset wait"
            assert pending.remaining_sec == remaining

            t0 = loop.time()
            await mp.play()
            t1 = loop.time()
            assert pending.task is not None
            assert pending.speed == 2.0
            assert (
                t0 + remaining / 2 - _FLOAT_EPS
                <= pending.deadline
                <= t1 + remaining / 2 + _FLOAT_EPS
            ), (
                f"deadline {pending.deadline - t0:.6f}s after play(); expected "
                f"{remaining / 2:.6f}s (remaining {remaining:.6f} bag-s at 2x)"
            )
            assert [e.state for e in mp._engines] == [
                PlaybackState.PLAYING, PlaybackState.STOPPED,
            ]
        finally:
            await mp.stop()

    @pytest.mark.asyncio
    async def test_play_stop_play_inside_offset_reapplies_the_full_offset(
        self, bag_a, bag_b,
    ):
        """stop() inside b's offset ends the session; the next play() waits
        the whole offset again.

        Would catch: stop() keeping the pending bookkeeping, so the restart
        served only what was left (or started b at once).
        """
        speed = 2.0
        loop = asyncio.get_running_loop()
        mp = _two_bags(bag_a, bag_b, _FAR_OFFSET_SEC, speed=speed)
        await mp.play()
        try:
            await mp.stop()
            assert mp._pending == {}
            assert [e.state for e in mp._engines] == [PlaybackState.STOPPED] * 2

            t0 = loop.time()
            await mp.play()
            t1 = loop.time()
            pending = mp._pending[1]
            assert pending.remaining_sec == _FAR_OFFSET_SEC
            wait = _FAR_OFFSET_SEC / speed
            assert t0 + wait - _FLOAT_EPS <= pending.deadline <= t1 + wait + _FLOAT_EPS, (
                f"deadline {pending.deadline - t0:.6f}s after play(); expected {wait}s"
            )
            assert [e.state for e in mp._engines] == [
                PlaybackState.PLAYING, PlaybackState.STOPPED,
            ]
        finally:
            await mp.stop()

    @pytest.mark.asyncio
    async def test_set_speed_during_a_running_offset_wait_rescales_it(self, bag_a, bag_b):
        """set_speed(4) while b is waiting out its offset at 1x cuts the
        rest of the wait to a quarter.

        Would catch: set_speed() leaving the running wait on its old
        schedule, so at 4x bag b started a full ``offset`` wall seconds in
        and lagged bag a by ~4x its configured offset.
        """
        new_speed = 4.0
        loop = asyncio.get_running_loop()
        mp = _two_bags(bag_a, bag_b, _FAR_OFFSET_SEC, speed=1.0)
        await mp.play()
        try:
            pending = mp._pending[1]
            old_task, old_deadline = pending.task, pending.deadline
            # Serve a measurable slice first, so keeping the full offset
            # can't pass for keeping the unserved part.
            await asyncio.sleep(0.05)
            t0 = loop.time()
            await mp.set_speed(new_speed)
            t1 = loop.time()

            assert old_task.cancelled(), "old offset wait still running"
            assert pending.task is not None and pending.task is not old_task
            assert pending.speed == new_speed
            # set_speed() ran between t0 and t1, so the bag-time still
            # unserved at 1x is old_deadline minus a moment in [t0, t1] ...
            assert (
                old_deadline - t1 - _FLOAT_EPS
                <= pending.remaining_sec
                <= old_deadline - t0 + _FLOAT_EPS
            ), (
                f"remaining {pending.remaining_sec!r} bag-s; expected the "
                f"{old_deadline - t1:.6f}-{old_deadline - t0:.6f} s left at 1x"
            )
            # ... and the new wait is that remainder at 4x, also scheduled
            # from a moment in [t0, t1].
            scheduled_at = pending.deadline - pending.remaining_sec / new_speed
            assert t0 - _FLOAT_EPS <= scheduled_at <= t1 + _FLOAT_EPS, (
                f"deadline {pending.deadline - t0:.6f}s after set_speed(); expected "
                f"remaining / {new_speed} = {pending.remaining_sec / new_speed:.6f}s "
                f"(was {old_deadline - t0:.6f}s at 1x)"
            )
            assert mp._engines[1].state == PlaybackState.STOPPED
        finally:
            await mp.stop()

    @pytest.mark.asyncio
    async def test_rescaled_offset_wait_still_starts_the_bag(self, bag_a, bag_b):
        """The wait set_speed() reschedules is the one that starts bag b.

        Would catch: set_speed() cancelling the running wait without
        scheduling a replacement, leaving bag b stopped for good.
        """
        mp = _two_bags(bag_a, bag_b, 0.2, speed=1.0)
        await mp.play()
        try:
            await mp.set_speed(2.0)
            await _wait_until(
                lambda: mp._engines[1].state == PlaybackState.PLAYING,
                lambda: f"bag b never started (state={mp._engines[1].state})",
            )
            assert mp._pending == {}
        finally:
            await mp.stop()

    @pytest.mark.asyncio
    async def test_offset_wait_uses_the_speed_the_engines_play_at(self, bag_a, bag_b):
        """At speed=100 the engines play at their 20x cap, so b's offset is
        waited out at 20x too.

        Would catch: dividing the offset by the requested speed, so bag b
        started 5x too early and sat offset * 4/5 bag-s behind where it
        should be relative to bag a.
        """
        loop = asyncio.get_running_loop()
        mp = _two_bags(bag_a, bag_b, _FAR_OFFSET_SEC, speed=100.0)
        engine_speed = mp._engines[1].speed
        assert engine_speed == 20.0
        t0 = loop.time()
        await mp.play()
        t1 = loop.time()
        try:
            pending = mp._pending[1]
            wait = _FAR_OFFSET_SEC / engine_speed
            assert t0 + wait - _FLOAT_EPS <= pending.deadline <= t1 + wait + _FLOAT_EPS, (
                f"deadline {pending.deadline - t0:.6f}s after play(); expected "
                f"{wait}s ({_FAR_OFFSET_SEC} bag-s at the engines' {engine_speed}x)"
            )
        finally:
            await mp.stop()


# ---------------------------------------------------------------------------
# Concurrent control calls
# ---------------------------------------------------------------------------

class TestConcurrentControl:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("first", ["pause", "play"])
    async def test_concurrent_pause_and_play_inside_offset_end_consistent(
        self, bag_a, bag_b, first,
    ):
        """gather(pause(), play()) while b waits out its offset must leave
        one coherent session: either paused (a paused, b's wait held) or
        playing (a playing, b's wait running).

        Would catch: pause() yielding before it paused the engines, so a
        play() slipping in rescheduled b's wait and pause() then paused
        only a, leaving a paused while b went on to start.
        """
        mp = _two_bags(bag_a, bag_b, _FAR_OFFSET_SEC, speed=1.0)
        await mp.play()
        try:
            calls = [mp.pause(), mp.play()]
            if first == "play":
                calls.reverse()
            await asyncio.gather(*calls)

            a_state = mp._engines[0].state
            b_waiting = mp._pending[1].task is not None
            assert mp._engines[1].state == PlaybackState.STOPPED
            assert (a_state, b_waiting) in {
                (PlaybackState.PAUSED, False),
                (PlaybackState.PLAYING, True),
            }, (
                f"bag a is {a_state.value} but bag b's offset wait is "
                f"{'running' if b_waiting else 'held'}"
            )
        finally:
            await mp.stop()


# ---------------------------------------------------------------------------
# Seek ends the session
# ---------------------------------------------------------------------------

class TestSeek:
    """seek() stops every bag at the target and drops offset bookkeeping;
    the next play() is a fresh session that applies every offset again."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("before_seek", ["waiting", "held", "started"])
    async def test_seek_ends_the_session(self, bag_a, bag_b, before_seek):
        """Would catch: seek() restarting engines that were playing (so the
        next play() was a resume that skipped offsets), leaving a pending
        wait running (b starting on the old schedule), or keeping a held
        remainder (the next play() serving only part of b's offset)."""
        speed = 2.0
        loop = asyncio.get_running_loop()
        offset = 0.05 if before_seek == "started" else _FAR_OFFSET_SEC
        mp = _two_bags(bag_a, bag_b, offset, speed=speed)
        await mp.play()
        try:
            old_task = mp._pending[1].task
            if before_seek == "held":
                await mp.pause()
            elif before_seek == "started":
                await _wait_until(
                    lambda: mp._engines[1].state == PlaybackState.PLAYING,
                    "bag b never started",
                )

            start_sec = mp._engines[0].metadata.start_time_ns / 1e9
            target = start_sec + 0.5
            await mp.seek(target)

            states = [e.state for e in mp._engines]
            assert states == [PlaybackState.STOPPED] * 2, (
                f"engine states after seek(): {states}"
            )
            assert mp._pending == {}
            assert old_task.done(), "bag b's old offset wait outlived seek()"
            for e in mp._engines:
                assert e.current_timestamp_sec == pytest.approx(target, abs=1e-6)

            t0 = loop.time()
            await mp.play()
            t1 = loop.time()
            assert [e.state for e in mp._engines] == [
                PlaybackState.PLAYING, PlaybackState.STOPPED,
            ]
            pending = mp._pending[1]
            assert pending.remaining_sec == offset
            wait = offset / speed
            assert t0 + wait - _FLOAT_EPS <= pending.deadline <= t1 + wait + _FLOAT_EPS
        finally:
            await mp.stop()
