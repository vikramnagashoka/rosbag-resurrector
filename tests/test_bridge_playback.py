"""Tests for the bag playback engine."""

import asyncio
import tempfile
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
