"""Tests for the LargeTopicError guards on eager conversion APIs.

The v0.4.0 performance contract refuses ``to_polars()`` / ``to_pandas()``
/ ``to_numpy()`` on topics above ``LARGE_TOPIC_THRESHOLD`` unless the
caller passes ``force=True``. These tests monkeypatch the threshold
down to verify the guard fires (without having to build a real 1 M-row
synthetic bag here — that lives in tests/test_streaming_oom.py).
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from resurrector.core import bag_frame as bag_frame_module
from resurrector.core.bag_frame import BagFrame
from resurrector.core.exceptions import LargeTopicError
from tests.fixtures.generate_test_bags import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def small_bag(tmp_dir):
    """Bag with /imu/data ~ 400 messages (2 s × 200 Hz)."""
    return generate_bag(tmp_dir / "small.mcap", BagConfig(duration_sec=2.0))


@pytest.fixture
def low_threshold(monkeypatch):
    """Lower the threshold so the small bag's IMU topic exceeds it."""
    monkeypatch.setattr(bag_frame_module, "LARGE_TOPIC_THRESHOLD", 100)


class TestLargeTopicError:
    def test_to_polars_raises_when_over_threshold(self, small_bag, low_threshold):
        bf = BagFrame(small_bag)
        with pytest.raises(LargeTopicError) as exc:
            bf["/imu/data"].to_polars()
        # Error fields are populated for callers that want to handle
        # programmatically.
        assert exc.value.topic_name == "/imu/data"
        assert exc.value.message_count > 100
        assert exc.value.threshold == 100
        # And the human-readable message should mention the alternatives.
        assert "iter_chunks" in str(exc.value)
        assert "materialize_ipc_cache" in str(exc.value)

    def test_to_polars_succeeds_with_force(self, small_bag, low_threshold):
        bf = BagFrame(small_bag)
        df = bf["/imu/data"].to_polars(force=True)
        assert df.height > 100  # we forced through the guard

    def test_to_pandas_raises_when_over_threshold(self, small_bag, low_threshold):
        bf = BagFrame(small_bag)
        with pytest.raises(LargeTopicError):
            bf["/imu/data"].to_pandas()

    def test_to_pandas_succeeds_with_force(self, small_bag, low_threshold):
        bf = BagFrame(small_bag)
        pdf = bf["/imu/data"].to_pandas(force=True)
        assert pdf.shape[0] > 100

    def test_to_numpy_raises_when_over_threshold(self, small_bag, low_threshold):
        bf = BagFrame(small_bag)
        with pytest.raises(LargeTopicError):
            bf["/imu/data"].to_numpy()

    def test_to_numpy_succeeds_with_force(self, small_bag, low_threshold):
        bf = BagFrame(small_bag)
        result = bf["/imu/data"].to_numpy(force=True)
        assert "timestamp_ns" in result
        assert len(result["timestamp_ns"]) > 100

    def test_default_threshold_lets_small_topics_through(self, small_bag):
        """Without monkeypatch, the small fixture is well under 1M."""
        bf = BagFrame(small_bag)
        df = bf["/imu/data"].to_polars()  # no force needed
        assert df.height > 0

    def test_iter_chunks_never_raises(self, small_bag, low_threshold):
        """The streaming primitive must remain available regardless of size."""
        bf = BagFrame(small_bag)
        chunks = list(bf["/imu/data"].iter_chunks(chunk_size=50))
        assert sum(c.height for c in chunks) > 100

    def test_materialize_ipc_cache_never_raises(self, small_bag, low_threshold):
        """The lazy alternative must remain available regardless of size."""
        bf = BagFrame(small_bag)
        with bf["/imu/data"].materialize_ipc_cache() as cache:
            df = cache.scan().head(5).collect()
            assert df.height == 5
