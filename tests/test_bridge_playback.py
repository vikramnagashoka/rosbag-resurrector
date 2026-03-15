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
