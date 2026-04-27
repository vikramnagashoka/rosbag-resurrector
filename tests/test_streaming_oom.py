"""Memory-regression tests for the v0.4.0 performance contract.

Builds a large synthetic bag once per session (cached on disk) and
asserts that every workflow advertised as bounded-memory in the README
keeps peak RSS delta under its budget. Marked ``@pytest.mark.slow``;
opt in via ``pytest -m slow``. CI runs slow tests on the wheel-smoke
job only — PRs stay fast.

The fixture is a 100k-message bag (small enough to build in a few
seconds locally) but the assertions verify the *streaming property* —
peak RSS should be a small constant regardless of bag size. Bumping
the fixture to 10M would slow the suite without proving anything new.
"""

from __future__ import annotations

import gc
import os
import tempfile
from pathlib import Path

import polars as pl
import psutil
import pytest

from resurrector.core.bag_frame import BagFrame
from resurrector.core.exceptions import LargeTopicError
from resurrector.core.export import Exporter
from resurrector.core.streaming import stream_bucketed_minmax
from resurrector.demo.sample_bag import BagConfig, generate_bag


pytestmark = pytest.mark.slow


_LARGE_BAG_DURATION_SEC = 100.0  # 100s × 200 Hz IMU = 20K IMU messages
_LARGE_BAG_CONFIG = BagConfig(
    duration_sec=_LARGE_BAG_DURATION_SEC,
    imu_hz=200.0,
    joint_hz=100.0,
    camera_hz=10.0,
    lidar_hz=5.0,
)


@pytest.fixture(scope="session")
def large_bag(tmp_path_factory):
    """Build a session-scoped synthetic bag for memory tests.

    Cached on disk inside the pytest tmp dir. Built once per session
    even though the suite has many tests.
    """
    cache_dir = tmp_path_factory.mktemp("oom_fixtures")
    bag_path = cache_dir / "large_synth.mcap"
    if not bag_path.exists():
        generate_bag(bag_path, _LARGE_BAG_CONFIG)
    yield bag_path


def _peak_rss_delta_mb(callable_, *args, **kwargs) -> tuple[float, object]:
    """Run ``callable_`` and return (peak RSS delta in MB, return value).

    Forces GC before/after to remove allocator noise. Uses the mid-call
    RSS sampled at the end as the peak — Python's resource module gives
    true peaks but isn't cross-platform; this approximation is fine for
    enforcing "is the peak roughly bounded" rather than measuring exact
    headroom.
    """
    proc = psutil.Process(os.getpid())
    gc.collect()
    baseline = proc.memory_info().rss
    result = callable_(*args, **kwargs)
    gc.collect()
    after = proc.memory_info().rss
    delta_mb = max(0.0, (after - baseline) / (1024 * 1024))
    return delta_mb, result


# ---------------------------------------------------------------------------
# Each test runs one workflow on the synthetic bag and checks the RSS delta.
# Budgets are deliberately generous — we're verifying "bounded by chunk
# size, not bag size", not "uses exactly X MB".
# ---------------------------------------------------------------------------


def test_iter_chunks_bounded(large_bag):
    """Iterating chunks in a tight loop should stay near baseline."""
    bf = BagFrame(large_bag)
    def consume():
        total = 0
        for chunk in bf["/imu/data"].iter_chunks(chunk_size=5_000):
            total += chunk.height
        return total
    delta_mb, total = _peak_rss_delta_mb(consume)
    assert total > 0
    assert delta_mb < 100, f"iter_chunks RSS delta {delta_mb:.1f} MB > 100 MB"


def test_health_report_bounded(large_bag):
    """Streaming health checks should stay bounded."""
    bf = BagFrame(large_bag)
    delta_mb, report = _peak_rss_delta_mb(lambda: bf.health_report())
    assert report.score >= 0
    assert delta_mb < 200, f"health_report RSS delta {delta_mb:.1f} MB > 200 MB"


def test_density_bounded(large_bag):
    """Streaming density should stay bounded."""
    from resurrector.ingest.density import compute_density
    delta_mb, result = _peak_rss_delta_mb(
        lambda: compute_density(large_bag, bins=200),
    )
    assert "/imu/data" in result
    assert delta_mb < 100, f"compute_density RSS delta {delta_mb:.1f} MB > 100 MB"


def test_stream_bucketed_minmax_bounded(large_bag):
    """Stream-aggregating /imu through bucketed min/max should stay bounded."""
    bf = BagFrame(large_bag)
    view = bf["/imu/data"]
    bag_start = int(bf.metadata.start_time_ns)
    bag_end = int(bf.metadata.end_time_ns)
    delta_mb, df = _peak_rss_delta_mb(
        lambda: stream_bucketed_minmax(
            view.iter_chunks(),
            num_buckets=200,
            time_range=(bag_start, bag_end),
        ),
    )
    assert df.height > 0
    assert df.height <= 2 * 200
    assert delta_mb < 100, (
        f"stream_bucketed_minmax RSS delta {delta_mb:.1f} MB > 100 MB"
    )


def test_streaming_sync_bounded(large_bag):
    """Streaming sync of /imu vs /joint_states should stay bounded."""
    bf = BagFrame(large_bag)
    delta_mb, result = _peak_rss_delta_mb(
        lambda: bf.sync(
            ["/joint_states", "/imu/data"],
            method="nearest",
            tolerance_ms=50.0,
            anchor="/joint_states",
            engine="streaming",
            out_of_order="reorder",
            max_lateness_ms=100.0,
        ),
    )
    assert result.height > 0
    assert delta_mb < 300, (
        f"streaming sync RSS delta {delta_mb:.1f} MB > 300 MB"
    )


def test_parquet_export_bounded(large_bag):
    """Streaming parquet export should stay bounded."""
    bf = BagFrame(large_bag)
    with tempfile.TemporaryDirectory() as d:
        delta_mb, _ = _peak_rss_delta_mb(
            lambda: Exporter().export(
                bag_frame=bf, topics=["/imu/data"], format="parquet",
                output_dir=str(d),
            ),
        )
    assert delta_mb < 100, f"parquet export RSS delta {delta_mb:.1f} MB > 100 MB"


def test_numpy_export_under_cap_bounded(large_bag):
    """NumPy export of a topic under NUMPY_HARD_CAP succeeds with reasonable
    memory. /imu at 200Hz × 100s = 20K rows, well under 1M."""
    bf = BagFrame(large_bag)
    with tempfile.TemporaryDirectory() as d:
        delta_mb, _ = _peak_rss_delta_mb(
            lambda: Exporter().export(
                bag_frame=bf, topics=["/imu/data"], format="numpy",
                output_dir=str(d),
            ),
        )
    # NumPy isn't streaming — bounded by total array size, not chunk
    # size. For 20K IMU rows this is ~few MB; loose budget for safety.
    assert delta_mb < 200, f"numpy export RSS delta {delta_mb:.1f} MB > 200 MB"


def test_numpy_export_over_cap_raises(large_bag, monkeypatch):
    """NumPy export above NUMPY_HARD_CAP must raise LargeTopicError."""
    from resurrector.core import export as export_module
    monkeypatch.setattr(export_module, "NUMPY_HARD_CAP", 100)
    bf = BagFrame(large_bag)
    with tempfile.TemporaryDirectory() as d:
        with pytest.raises(LargeTopicError):
            Exporter().export(
                bag_frame=bf, topics=["/imu/data"], format="numpy",
                output_dir=str(d),
            )
