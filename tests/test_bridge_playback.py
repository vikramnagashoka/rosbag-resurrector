"""Tests for the bag playback engine."""

import asyncio
import tempfile
import threading
import time
from pathlib import Path

import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.bridge.playback import PlaybackEngine, PlaybackState


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def test_bag(tmp_dir):
    return generate_bag(tmp_dir / "test.mcap", BagConfig(duration_sec=5.0))


class TestPlaybackEngine:
    def test_create_from_bag(self, test_bag):
        engine = PlaybackEngine(test_bag)
        assert engine.state == PlaybackState.STOPPED
        assert engine.metadata.duration_sec > 0
        assert len(engine.get_topics_info()) >= 4

    def test_progress_starts_at_zero(self, test_bag):
        engine = PlaybackEngine(test_bag)
        assert engine.progress == 0.0

    @pytest.mark.asyncio
    async def test_play_and_receive_messages(self, test_bag):
        received = []

        def on_msg(msg):
            received.append(msg)

        engine = PlaybackEngine(test_bag, speed=10.0, message_callback=on_msg)
        await engine.play()
        assert engine.state == PlaybackState.PLAYING

        # Wait for some messages
        await asyncio.sleep(0.5)
        await engine.stop()

        assert len(received) > 0
        assert received[0].topic in ("/imu/data", "/joint_states", "/camera/rgb", "/lidar/scan", "/camera/compressed")

    @pytest.mark.asyncio
    async def test_pause_resume(self, test_bag):
        received = []

        engine = PlaybackEngine(test_bag, speed=2.0, message_callback=lambda m: received.append(m))
        await engine.play()
        await asyncio.sleep(0.1)

        count_before_pause = len(received)
        await engine.pause()
        assert engine.state == PlaybackState.PAUSED

        await asyncio.sleep(0.2)
        count_during_pause = len(received)
        # Should not receive many more messages while paused
        assert count_during_pause - count_before_pause <= 1

        await engine.play()
        await asyncio.sleep(0.2)
        await engine.stop()

        # Should have received more messages after resume
        assert len(received) > count_during_pause

    @pytest.mark.asyncio
    async def test_resume_does_not_burst_through_bag(self, test_bag):
        """Regression: after a long pause, the loop used to interpret the
        pause as 'we're behind' and emit every remaining message in one
        tight burst. Verifies bag-time advances proportionally to wall
        time spent playing, not to wall time spent paused.
        """
        # Play at 1x so bag-time-progress ≈ wall-time-playing.
        engine = PlaybackEngine(test_bag, speed=1.0)
        await engine.play()
        await asyncio.sleep(0.1)  # ~0.1s of bag content
        await engine.pause()
        bag_ts_at_pause = engine.current_timestamp_sec

        # Long pause — without the rebase fix, wall_start vs bag_start_ns
        # would diverge by this whole interval and the resume would emit
        # ~PAUSE_SEC of bag content at max speed before catching up.
        PAUSE_SEC = 0.6
        await asyncio.sleep(PAUSE_SEC)

        # Resume and let it play for a small wall window.
        PLAY_AFTER_RESUME = 0.15
        await engine.play()
        await asyncio.sleep(PLAY_AFTER_RESUME)
        bag_ts_after_resume = engine.current_timestamp_sec
        await engine.stop()

        bag_progress_after_resume = bag_ts_after_resume - bag_ts_at_pause

        # With the fix: bag progress ≈ PLAY_AFTER_RESUME (within timing noise).
        # Without the fix: bag progress would be ≥ PAUSE_SEC + PLAY_AFTER_RESUME,
        # because the loop rushes through the "missed" pause window.
        # Allow generous slack for asyncio scheduling: 3x the real window.
        assert bag_progress_after_resume < PLAY_AFTER_RESUME * 3, (
            f"Bag advanced by {bag_progress_after_resume:.3f}s in "
            f"{PLAY_AFTER_RESUME:.3f}s of wall time — engine is bursting "
            f"through messages after pause. (Was {bag_ts_at_pause:.3f}s, now {bag_ts_after_resume:.3f}s)"
        )

    @pytest.mark.asyncio
    async def test_behind_schedule_playback_does_not_starve_event_loop(self, test_bag):
        """Pause must take effect even when playback can't keep up.

        Would catch: _playback_loop never yielding while behind schedule
        (no asyncio.sleep, and an already-set Event.wait() doesn't suspend),
        which froze the whole event loop until end-of-bag. In the server
        that silently ignored Pause and stalled WebSocket sends; in CI it
        made test_playback_play_pause flaky on loaded runners.

        A 1 ms-per-message callback at 20x keeps the engine permanently
        behind (~1.75 s of work for a 0.25 s schedule), so the outcome
        doesn't depend on machine speed.
        """
        emitted = 0

        def slow_callback(_msg):
            nonlocal emitted
            emitted += 1
            end = time.perf_counter() + 0.001
            while time.perf_counter() < end:
                pass

        engine = PlaybackEngine(test_bag, speed=20.0, message_callback=slow_callback)
        total = sum(t["count"] for t in engine.get_topics_info())
        # Precondition: 1 ms/message must exceed the per-message budget at
        # 20x, or the engine isn't behind and the test proves nothing.
        assert engine.duration_sec / 20.0 / total < 0.001, "bag too sparse to fall behind"

        await engine.play()
        await asyncio.sleep(0.05)  # starved loop: this wouldn't return until end-of-bag
        await engine.pause()
        try:
            assert engine.state == PlaybackState.PAUSED, (
                f"pause had no effect (state={engine.state}); the playback "
                "loop starved the event loop and ran to end-of-bag"
            )
            assert engine.progress < 1.0, "bag already finished at pause"
            at_pause = emitted
            await asyncio.sleep(0.1)
            assert emitted - at_pause <= 1, f"{emitted - at_pause} messages emitted while paused"
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_loop_with_no_matching_messages_does_not_spin(self, test_bag):
        """--loop with a topic filter that matches nothing must not spin.

        Would catch: each pass yielding zero messages, so the per-message
        yield never runs and the outer while re-opens the bag forever with
        no await — event loop frozen, HTTP dead, Ctrl+C ignored (only
        SIGKILL stops the server). Hit by a typo'd --topic, the demo bag's
        declared-but-empty /tf, or a multibag topic missing from one bag.
        A watchdog thread forces a stop so a regression fails instead of
        hanging pytest.
        """
        engine = PlaybackEngine(test_bag, speed=1.0, topics=["/nonexistent"], loop=True)
        watchdog = threading.Timer(5.0, lambda: setattr(engine, "_stop_requested", True))
        watchdog.start()
        try:
            await engine.play()
            t0 = time.monotonic()
            await asyncio.sleep(0.05)
            stalled = time.monotonic() - t0
            assert stalled < 2.0, f"event loop frozen for {stalled:.2f}s by an empty --loop pass"
            await asyncio.sleep(0.1)
            assert engine.state == PlaybackState.STOPPED, "empty selection should stop, not 'play' forever"
        finally:
            watchdog.cancel()
            await engine.stop()

    @pytest.mark.asyncio
    async def test_play_after_pause_on_final_message_restarts(self, test_bag):
        """A pause that lands on the last message must not wedge the next play().

        Would catch: pause() clearing _pause_event while the loop is on the
        final message; the run then ends STOPPED with the event still
        cleared, and the fresh-start play() never set it, so the new task
        waited forever while /api/status said 'playing'.

        The precondition is constructed directly (the final message's
        callback does exactly what pause() does) rather than by scheduling a
        pause task and hoping it runs before end-of-bag: that version flaked
        when a delayed timer let the run finish first and the stale pause
        then hit the replay.
        """
        topic = "/lidar/scan"
        engine = PlaybackEngine(test_bag, speed=20.0, topics=[topic])
        total = next(t["count"] for t in engine.get_topics_info() if t["name"] == topic)
        seen = 0

        def on_msg(_msg):
            nonlocal seen
            seen += 1
            if seen == total:  # what pause() does when it lands on the last message
                engine._pause_event.clear()
                engine._state = PlaybackState.PAUSED

        engine._callback = on_msg
        await engine.play()
        await asyncio.wait_for(engine._task, timeout=10)
        assert engine.state == PlaybackState.STOPPED
        assert not engine._pause_event.is_set(), "precondition: run ended with the pause event cleared"

        replayed = []
        engine._callback = replayed.append
        engine._current_timestamp_ns = engine._metadata.start_time_ns
        await engine.play()
        try:
            for _ in range(200):  # up to ~2 s; the first message is due within ~5 ms
                if replayed:
                    break
                await asyncio.sleep(0.01)
            assert replayed, "play() after end-of-bag emitted nothing — wedged on a cleared pause event"
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_speed_change_mid_play_keeps_pace(self, test_bag):
        """Changing speed mid-play must not burst (speed-up) or stall (slow-down).

        Would catch: set_speed() not re-zeroing the timing reference, so a
        speed-up replays the whole elapsed bag span at once (a starvation
        trigger before the yield fix) and a slow-down sleeps until wall time
        "catches up" with the bag time accumulated at the higher speed.

        Both checks are sized so the bug's signal (~1.2 s) dwarfs scheduling
        jitter, and bag-time advance is compared with the wall time actually
        elapsed, so a stalled runner can't fake a failure.
        """
        engine = PlaybackEngine(test_bag, speed=1.0, topics=["/imu/data"])
        await engine.play()
        await asyncio.sleep(0.4)
        try:
            await engine.set_speed(4.0)
            bag0, wall0 = engine.current_timestamp_sec, time.monotonic()
            await asyncio.sleep(0.4)
            advance, wall = engine.current_timestamp_sec - bag0, time.monotonic() - wall0
            # Paced: ~4x wall. The bug adds the elapsed span (~3 * 0.4 s) at once.
            assert advance < 4.0 * wall + 0.6, (
                f"bag advanced {advance:.2f}s in {wall:.2f}s at 4x: catch-up burst after speed-up"
            )

            # Bug: the next message waits until wall time reaches the bag time
            # accumulated at 4x — ~1.2 s with no progress at all. Fixed: steady 1x.
            await engine.set_speed(1.0)
            bag0 = engine.current_timestamp_sec
            await asyncio.sleep(0.5)
            advance = engine.current_timestamp_sec - bag0
            assert advance >= 0.05, (
                f"bag advanced only {advance:.3f}s in 0.5s after 4x->1x: stalled after slow-down"
            )
        finally:
            await engine.stop()

    @pytest.mark.asyncio
    async def test_speed_change(self, test_bag):
        engine = PlaybackEngine(test_bag, speed=1.0)
        assert engine.speed == 1.0

        await engine.set_speed(4.0)
        assert engine.speed == 4.0

        # Clamp to bounds
        await engine.set_speed(100.0)
        assert engine.speed == 20.0

        await engine.set_speed(0.001)
        assert engine.speed == 0.1

    @pytest.mark.asyncio
    async def test_topic_filter(self, test_bag):
        received = []

        engine = PlaybackEngine(
            test_bag, speed=20.0,
            topics=["/imu/data"],
            message_callback=lambda m: received.append(m),
        )
        await engine.play()
        await asyncio.sleep(0.5)
        await engine.stop()

        assert len(received) > 0
        assert all(m.topic == "/imu/data" for m in received)
