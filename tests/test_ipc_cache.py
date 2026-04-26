"""Tests for the explicit-lifecycle IpcCache.

Replaces the v0.3.x ``to_lazy_polars()`` which leaked temp files.
The tests cover:

- Context-manager use deletes the file on exit.
- Explicit close() is idempotent.
- scan() after close() raises a clear error.
- An empty topic returns a working but empty cache (no file on disk).
- __del__ on a non-closed cache emits a ResourceWarning so the leak
  is visible rather than silent.
"""

from __future__ import annotations

import gc
import tempfile
import warnings
from pathlib import Path

import polars as pl
import pytest

from resurrector.core.bag_frame import BagFrame, IpcCache
from tests.fixtures.generate_test_bags import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_bag(tmp_dir):
    return generate_bag(tmp_dir / "sample.mcap", BagConfig(duration_sec=1.5))


class TestContextManager:
    def test_file_exists_inside_block(self, sample_bag):
        bf = BagFrame(sample_bag)
        with bf["/imu/data"].materialize_ipc_cache() as cache:
            assert cache.path is not None
            assert cache.path.exists()

    def test_file_deleted_after_exit(self, sample_bag):
        bf = BagFrame(sample_bag)
        with bf["/imu/data"].materialize_ipc_cache() as cache:
            path = cache.path
            assert path.exists()
        assert not path.exists()

    def test_scan_inside_block(self, sample_bag):
        bf = BagFrame(sample_bag)
        with bf["/imu/data"].materialize_ipc_cache() as cache:
            df = cache.scan().head(5).collect()
            assert df.height == 5
            assert "timestamp_ns" in df.columns


class TestExplicitClose:
    def test_close_deletes_file(self, sample_bag):
        bf = BagFrame(sample_bag)
        cache = bf["/imu/data"].materialize_ipc_cache()
        path = cache.path
        assert path.exists()
        cache.close()
        assert not path.exists()

    def test_close_is_idempotent(self, sample_bag):
        bf = BagFrame(sample_bag)
        cache = bf["/imu/data"].materialize_ipc_cache()
        cache.close()
        cache.close()  # must not raise

    def test_scan_after_close_raises(self, sample_bag):
        bf = BagFrame(sample_bag)
        cache = bf["/imu/data"].materialize_ipc_cache()
        cache.close()
        with pytest.raises(RuntimeError, match="after close"):
            cache.scan()


class TestEmptyTopic:
    def test_empty_topic_no_file(self, tmp_dir):
        # A 1-ms bag has effectively zero IMU samples for some configurations.
        # Build an empty cache directly via the constructor to test the
        # empty-path explicitly.
        cache = IpcCache(path=None, _empty=True)
        assert cache.path is None
        # scan() returns a working LazyFrame even with no data
        df = cache.scan().collect()
        assert df.height == 0
        # close() is a no-op
        cache.close()


class TestDelWarning:
    def test_unclosed_cache_warns_on_del(self, sample_bag):
        bf = BagFrame(sample_bag)
        cache = bf["/imu/data"].materialize_ipc_cache()
        path = cache.path

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", ResourceWarning)
            del cache
            gc.collect()

        # At least one ResourceWarning mentioning "not closed".
        matched = [w for w in captured if issubclass(w.category, ResourceWarning)]
        assert any("not closed" in str(w.message) for w in matched)
        # And the file should still get cleaned up.
        assert not path.exists()
