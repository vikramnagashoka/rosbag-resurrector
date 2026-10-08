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
import json
import os
import subprocess
import sys
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


def test_lerobot_grid_resample_bounded(large_bag):
    """LeRobot's as-of resampler streams chunks onto the frame grid.

    Runs with a small chunk size so the topic spans many chunks; the
    streaming path must not accumulate them. Doesn't need LeRobot itself.
    """
    from resurrector.core.lerobot_export import asof_on_grid, build_grid, numeric_columns

    bf = BagFrame(large_bag)
    view = bf["/imu/data"]
    cols = numeric_columns(next(iter(view.iter_chunks(1_000))).schema)
    grid = build_grid(int(bf.metadata.start_time_ns), int(bf.metadata.end_time_ns), 30)
    delta_mb, df = _peak_rss_delta_mb(
        lambda: asof_on_grid(view.iter_chunks(1_000), grid, cols),
    )
    assert df.height == len(grid)
    assert delta_mb < 100, f"asof_on_grid RSS delta {delta_mb:.1f} MB > 100 MB"


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


_SYNC_EXPORT_BAG_CONFIG = BagConfig(
    duration_sec=100.0,
    imu_hz=1000.0,   # 100K anchor rows: enough that the whole synced table
    joint_hz=100.0,  # costs several hundred MB, far past the budget below
    camera_hz=0.0,
    lidar_hz=0.0,
    include_tf=False,
    include_compressed=False,
)

# Runs in a fresh interpreter so ru_maxrss (the true peak, which
# _peak_rss_delta_mb can't see: it samples RSS after the call) belongs
# to this export alone.
_SYNC_EXPORT_CHILD = """
import json, resource, sys
from resurrector.core import export as export_module
from resurrector.core import sync as sync_module
from resurrector.core.bag_frame import BagFrame
from resurrector.core.export import ExportError, Exporter

bag, out, downsample, fmt = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
# Small chunks, so the chunk-bounded part is a sliver of the budget.
export_module.CHUNK_SIZE = 5_000
# Route engine='auto' to the streaming engine, the one big bags get.
sync_module.LARGE_TOPIC_THRESHOLD = 0

def peak_bytes():
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return ru if sys.platform == "darwin" else ru * 1024

bf = BagFrame(bag)
bf.metadata
before = peak_bytes()
failed = []
try:
    Exporter().export(
        bag_frame=bf, topics=["/imu/data", "/joint_states"], format=fmt,
        output_dir=out, sync=True,
        downsample_hz=float(downsample) if downsample != "none" else None,
    )
except ExportError as e:
    failed = sorted(f.column for f in e.failures)
delta_mb = (peak_bytes() - before) / 2**20
strings = "joint_states__header.frame_id"
if fmt == "parquet":
    import pyarrow.parquet as pq
    rows = string_rows = pq.read_metadata(f"{out}/synced.parquet").num_rows
elif fmt == "hdf5":
    import h5py
    with h5py.File(f"{out}/synced.h5", "r") as f:
        rows = f["synced/timestamp_ns"].shape[0]
        string_rows = f["synced"][strings].shape[0]
else:
    import zarr
    group = zarr.open_group(f"{out}/synced.zarr", mode="r")
    rows = group["timestamp_ns"].shape[0]
    string_rows = group[strings].shape[0]
print(json.dumps({
    "delta_mb": delta_mb, "rows": rows, "string_rows": string_rows, "failed": failed,
}))
"""


@pytest.fixture(scope="session")
def sync_export_bag(tmp_path_factory):
    path = tmp_path_factory.mktemp("oom_sync_export") / "sync_export.mcap"
    generate_bag(path, _SYNC_EXPORT_BAG_CONFIG)
    yield path


@pytest.mark.skipif(sys.platform == "win32", reason="ru_maxrss is POSIX-only")
@pytest.mark.parametrize("fmt,downsample", [
    ("parquet", "none"), ("parquet", "50"), ("hdf5", "none"), ("zarr", "none"),
])
def test_synced_export_bounded(sync_export_bag, tmp_path, fmt, downsample):
    """Synced export (CLI --sync, the rlds / training-tabular / multimodal
    presets) streams, so its peak RSS doesn't grow with the bag.

    Would catch: the synced table built in memory before writing. On
    this bag that path peaked around 500 MB (and grows with the bag);
    the streamed path stays near 70 MB, the same at 3x the rows. HDF5
    and Zarr (the multimodal preset) go through the same stream plus a
    per-chunk dtype conversion, the header.frame_id strings included.
    """
    import resurrector

    if fmt == "zarr":
        pytest.importorskip("zarr")

    src_root = str(Path(resurrector.__file__).resolve().parents[1])
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (src_root, env.get("PYTHONPATH")) if p
    )
    proc = subprocess.run(
        [sys.executable, "-c", _SYNC_EXPORT_CHILD,
         str(sync_export_bag), str(tmp_path / "out"), downsample, fmt],
        capture_output=True, text=True, env=env, check=False,
    )
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.strip().splitlines()[-1])

    expected_rows = 100_000 if downsample == "none" else 5_000
    assert abs(result["rows"] - expected_rows) <= 1
    assert result["failed"] == []
    assert result["string_rows"] == result["rows"]
    assert result["delta_mb"] < 200, (
        f"synced export peak RSS delta {result['delta_mb']:.1f} MB > 200 MB"
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


def test_hdf5_export_bounded(large_bag):
    """Streaming HDF5 export uses resizable datasets (append per chunk).

    Peak RSS should stay near baseline like parquet. h5py's gzip
    compression adds some allocator noise; budget is generous.
    """
    bf = BagFrame(large_bag)
    with tempfile.TemporaryDirectory() as d:
        delta_mb, _ = _peak_rss_delta_mb(
            lambda: Exporter().export(
                bag_frame=bf, topics=["/imu/data"], format="hdf5",
                output_dir=str(d),
            ),
        )
    assert delta_mb < 150, f"hdf5 export RSS delta {delta_mb:.1f} MB > 150 MB"


def test_zarr_export_bounded(large_bag):
    """Streaming Zarr export appends to chunked arrays per chunk, the
    header.frame_id strings (variable-length UTF-8) included.

    Skips if zarr (in [all-exports]) isn't installed in this venv.
    """
    zarr = pytest.importorskip("zarr")
    bf = BagFrame(large_bag)
    with tempfile.TemporaryDirectory() as d:
        delta_mb, _ = _peak_rss_delta_mb(
            lambda: Exporter().export(
                bag_frame=bf, topics=["/imu/data"], format="zarr",
                output_dir=str(d),
            ),
        )
        frame_ids = zarr.open(f"{d}/imu_data.zarr", mode="r")["header.frame_id"]
        assert frame_ids.shape[0] == bf["/imu/data"].message_count
    assert delta_mb < 150, f"zarr export RSS delta {delta_mb:.1f} MB > 150 MB"


def test_csv_export_bounded(large_bag):
    """Streaming CSV export writes per-chunk; only one chunk in memory at a time."""
    bf = BagFrame(large_bag)
    with tempfile.TemporaryDirectory() as d:
        delta_mb, _ = _peak_rss_delta_mb(
            lambda: Exporter().export(
                bag_frame=bf, topics=["/imu/data"], format="csv",
                output_dir=str(d),
            ),
        )
    assert delta_mb < 100, f"csv export RSS delta {delta_mb:.1f} MB > 100 MB"


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


_NPZ_TEXT_CHILD = """
import json, resource, sys
from pathlib import Path
import polars as pl
from resurrector.core.export import _stream_numpy

out, rows, chunk, width = Path(sys.argv[1]), 150_000, 50_000, 500

def chunks():
    for start in range(0, rows, chunk):
        s = ["base_link"] * chunk
        s[0] = "x" * width
        yield pl.DataFrame({"timestamp_ns": range(start, start + chunk), "s": s})

def peak_bytes():
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return ru if sys.platform == "darwin" else ru * 1024

before = peak_bytes()
_stream_numpy(chunks(), out, "t")
print(json.dumps({
    "delta_mb": (peak_bytes() - before) / 2**20,
    "fixed_mb": rows * width * 4 / 2**20,
}))
"""


@pytest.mark.skipif(sys.platform == "win32", reason="ru_maxrss is POSIX-only")
def test_npz_text_column_holds_one_fixed_width_copy(tmp_path):
    """``.npz`` text is fixed width, so a column costs rows x its longest
    value, unavoidably. Writing it must not cost that twice: per-chunk
    ``<U`` arrays plus their concatenation peaked near 2x here (each chunk
    holds one 500-character value); the column filled part by part from
    object arrays stays near 1x.
    """
    import resurrector

    src_root = str(Path(resurrector.__file__).resolve().parents[1])
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (src_root, env.get("PYTHONPATH")) if p
    )
    proc = subprocess.run(
        [sys.executable, "-c", _NPZ_TEXT_CHILD, str(tmp_path)],
        capture_output=True, text=True, env=env, check=False,
    )
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    assert result["delta_mb"] < 1.5 * result["fixed_mb"], (
        f".npz text export peak RSS delta {result['delta_mb']:.0f} MB for a "
        f"{result['fixed_mb']:.0f} MB fixed-width column"
    )


_WIDE_TYPE = "resurrector_test/msg/Wide"
_WIDE_ROWS, _WIDE_LATE_FROM, _WIDE_COLS = 120_000, 100_000, 50

_IPC_WIDEN_CHILD = f"""
import json, resource, struct, sys
import polars as pl
from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.parser import register_decoder

def decode(data):
    (i,) = struct.unpack_from("<I", data, 4)
    row = {{f"f{{k}}": float(i + k) for k in range({_WIDE_COLS})}}
    if i >= {_WIDE_LATE_FROM}:
        row["late"] = float(i)
    return row

register_decoder("{_WIDE_TYPE}", decode)

def peak_bytes():
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return ru if sys.platform == "darwin" else ru * 1024

view = BagFrame(sys.argv[1])["/wide"]
view.message_count
before = peak_bytes()
with view.materialize_ipc_cache(chunk_size=2_000) as cache:
    delta_mb = (peak_bytes() - before) / 2**20
    late = cache.scan().select(pl.col("late").drop_nulls().len()).collect().item()
    rows = cache.scan().select(pl.len()).collect().item()
print(json.dumps({{"delta_mb": delta_mb, "rows": rows, "late": late}}))
"""


@pytest.fixture(scope="session")
def wide_bag(tmp_path_factory):
    """/wide: 50 float fields per message, plus ``late`` from message
    100 000 on, so the IPC cache is widened after 100k rows."""
    import struct

    from mcap.writer import Writer

    path = tmp_path_factory.mktemp("oom_wide") / "wide.mcap"
    with open(path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="resurrector-test")
        sid = writer.register_schema(name=_WIDE_TYPE, encoding="ros2msg", data=b"uint32 i\n")
        cid = writer.register_channel(topic="/wide", message_encoding="cdr", schema_id=sid)
        for i in range(_WIDE_ROWS):
            t = 1_700_000_000_000_000_000 + i * 1_000_000
            writer.add_message(cid, log_time=t, publish_time=t,
                               data=b"\x00\x01\x00\x00" + struct.pack("<I", i))
        writer.finish()
    yield path


@pytest.mark.skipif(sys.platform == "win32", reason="ru_maxrss is POSIX-only")
def test_ipc_cache_widening_bounded(wide_bag):
    """A column that first appears after 100k rows widens the cached IPC
    file, which rewrites those rows. The rewrite goes a batch at a time:
    about 59 MB here against 48 MB with no widening, the same at 300k
    rows. Reading the cached rows back whole peaked at 125 MB here and
    251 MB at 300k rows."""
    import resurrector

    src_root = str(Path(resurrector.__file__).resolve().parents[1])
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (src_root, env.get("PYTHONPATH")) if p
    )
    proc = subprocess.run(
        [sys.executable, "-c", _IPC_WIDEN_CHILD, str(wide_bag)],
        capture_output=True, text=True, env=env, check=False,
    )
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    assert result["rows"] == _WIDE_ROWS
    assert result["late"] == _WIDE_ROWS - _WIDE_LATE_FROM
    assert result["delta_mb"] < 90, (
        f"IPC cache widening peak RSS delta {result['delta_mb']:.1f} MB > 90 MB"
    )


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
