"""HDF5 export peak memory must not grow with the number of rows written.

HDF5 keeps a chunk cache per dataset (8 MiB by default since HDF5 2.0,
the library h5py 3.16 bundles). The HDF5 writer only appends, so with
that default every column parked up to 8 MiB of written chunks in
memory: a 31-column stream peaked around 285 MB at 1M rows against
45 MB at 100k. This test writes the same stream at two lengths in fresh
interpreters (so ``ru_maxrss`` is the true peak of that export alone,
as in ``test_streaming_oom.py``) and checks the peak barely moves.

Marked ``@pytest.mark.slow``; run with ``pytest -m slow``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(sys.platform == "win32", reason="ru_maxrss is POSIX-only"),
]

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
