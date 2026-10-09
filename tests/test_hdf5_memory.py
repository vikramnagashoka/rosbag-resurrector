"""HDF5 export peak memory must not grow with the number of rows written.

HDF5 keeps a chunk cache per dataset (8 MiB by default since HDF5 2.0,
the library h5py 3.16 bundles). The HDF5 writer only appends, so with
that default every column parked up to 8 MiB of written chunks in
memory: a 31-column stream peaked around 285 MB at 1M rows against
45 MB at 100k.

Two tests:

- A fast guard, in every PR's test run on any h5py: every dataset the
  writer creates gets ``chunks=(_HDF5_CHUNK_ROWS,)`` and the bounded
  chunk cache, read back from the dataset's own access property list.
- The slow RSS test (``pytest -m slow``, CI's memory-regression job):
  the same stream written at two lengths in fresh interpreters (so
  ``ru_maxrss`` is the true peak of that export alone, as in
  ``test_streaming_oom.py``), checking the peak barely moves. It only
  sees the regression where h5py bundles HDF5 2.0 or newer: with HDF5
  1.12's 1 MiB default the unfixed writer passes it.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest

from resurrector.core import export as export_module

_MAX_CACHE_BYTES = 64 * 1024


def test_every_hdf5_dataset_gets_fixed_chunks_and_a_bounded_cache(tmp_path, monkeypatch):
    """Would catch: a merge dropping ``**_HDF5_CHUNK_CACHE`` from the
    ``h5py.File`` call or ``chunks=`` from either ``create_dataset``
    call, or the cache constant raised back towards HDF5's default.
    Covers every way a dataset gets created: numeric, text, a column
    back-filled after it first appears, one null in every chunk, and one
    seen only in a 0-row chunk."""
    h5py = pytest.importorskip("h5py")
    created: dict[str, tuple] = {}
    real = h5py.Group.create_dataset

    def spy(self, name, *args, **kwargs):
        ds = real(self, name, *args, **kwargs)
        created[name] = (kwargs.get("chunks"), ds.chunks, ds.id.get_access_plist().get_chunk_cache())
        return ds

    monkeypatch.setattr(h5py.Group, "create_dataset", spy)
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2], "x": [1.0, 2.0], "s": ["a", None], "n": [None, None]}),
        pl.DataFrame({"timestamp_ns": [3], "x": [3.0], "late": [3], "n": [None]}),
        pl.DataFrame(schema={"timestamp_ns": pl.Int64, "empty": pl.Float64}),
    ]
    export_module._stream_hdf5(iter(chunks), tmp_path, "t")

    rows = export_module._HDF5_CHUNK_ROWS
    nbytes = export_module._HDF5_CHUNK_CACHE["rdcc_nbytes"]
    assert nbytes <= _MAX_CACHE_BYTES
    assert set(created) == {"timestamp_ns", "x", "s", "n", "late", "empty"}
    for name, (asked, chunk_shape, (_slots, cache_bytes, _w0)) in created.items():
        assert asked == (rows,), name
        assert chunk_shape == (rows,), name
        assert cache_bytes == nbytes, (name, cache_bytes)


def test_ci_memory_regression_job_runs_the_rss_test():
    """Would catch: the slow test below running in no CI job. The default
    run deselects ``slow``; only the memory-regression job selects it."""
    ci = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"
    job = ci.read_text().split("\n  memory-regression:\n", 1)[1].split("\n  frontend-build:\n", 1)[0]
    assert re.search(r"\n\s*pytest [^\n]*tests/test_hdf5_memory\.py[^\n]* -m slow", job), job

_SMALL_ROWS = 100_000
# Past this, each float64 column has outgrown an 8 MiB per-dataset cache,
# so a cache-bound writer shows its full growth here.
_LARGE_ROWS = 1_000_000
# With the cache bounded the growth measured 3-37 MB on macOS (the high
# end on a heavily loaded machine), about what the same stream costs with
# a writer that discards every column. The default cache added 240-270 MB.
_GROWTH_BUDGET_MB = 80

_CHILD = """
import json, resource, sys
from pathlib import Path

import h5py
import numpy as np
import polars as pl

from resurrector.core import export as export_module

out, rows, strings = Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3] == "1"
chunk_rows = export_module.CHUNK_SIZE


def chunks():
    rng = np.random.default_rng(0)
    for start in range(0, rows, chunk_rows):
        n = min(chunk_rows, rows - start)
        cols = {"timestamp_ns": np.arange(start, start + n, dtype=np.int64)}
        for j in range(30):
            cols[f"f{j}"] = rng.random(n)
        if strings:
            cols["header.frame_id"] = ["imu_link"] * n
            cols["child_frame_id"] = [None if k % 3 else "base" for k in range(n)]
        yield pl.DataFrame(cols)


def peak_bytes():
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return ru if sys.platform == "darwin" else ru * 1024


# One chunk through polars first, so its buffers sit in the baseline.
next(chunks())
before = peak_bytes()
result = export_module._stream_hdf5(chunks(), out, "t")
delta_mb = (peak_bytes() - before) / 2**20
with h5py.File(result.path, "r") as f:
    lengths = sorted({ds.shape[0] for ds in f["t"].values()})
    columns = len(f["t"])
print(json.dumps({"delta_mb": delta_mb, "lengths": lengths, "columns": columns}))
"""


def _export_peak(out: Path, rows: int, strings: bool) -> dict:
    import resurrector

    out.mkdir()
    src_root = str(Path(resurrector.__file__).resolve().parents[1])
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (src_root, env.get("PYTHONPATH")) if p
    )
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD, str(out), str(rows), "1" if strings else "0"],
        capture_output=True, text=True, env=env, check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1])


@pytest.mark.slow
@pytest.mark.skipif(sys.platform == "win32", reason="ru_maxrss is POSIX-only")
@pytest.mark.parametrize("strings", [False, True], ids=["numeric", "with-strings"])
def test_hdf5_export_peak_rss_flat_in_rows(tmp_path, strings):
    """Would catch: h5py datasets opened with the library's default chunk
    cache, which holds written chunks in memory up to 8 MiB per column."""
    pytest.importorskip("h5py")
    small = _export_peak(tmp_path / "small", _SMALL_ROWS, strings)
    large = _export_peak(tmp_path / "large", _LARGE_ROWS, strings)

    expected_columns = 33 if strings else 31
    assert small["columns"] == large["columns"] == expected_columns
    assert small["lengths"] == [_SMALL_ROWS]
    assert large["lengths"] == [_LARGE_ROWS]
    growth = large["delta_mb"] - small["delta_mb"]
    assert growth < _GROWTH_BUDGET_MB, (
        f"HDF5 export peak RSS grew {growth:.1f} MB from {_SMALL_ROWS:,} rows "
        f"({small['delta_mb']:.1f} MB) to {_LARGE_ROWS:,} rows "
        f"({large['delta_mb']:.1f} MB); budget {_GROWTH_BUDGET_MB} MB"
    )
