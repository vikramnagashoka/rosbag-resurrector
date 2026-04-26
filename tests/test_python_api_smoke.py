"""End-to-end smoke test mirroring README "Path C — Python / Jupyter".

This is the exact flow a new user would hit when copy-pasting from the
README. It runs against an installed wheel (no editable install, no
test-tree imports) so the wheel-smoke CI job catches packaging
regressions like the v0.3.2 demo-import bug.

Marked ``@pytest.mark.smoke`` so CI's wheel-smoke job can run only this
file (`pytest -m smoke`) on the freshly-installed wheel without pulling
in fixtures from the dev tree.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import polars as pl
import pytest

# Use the installed package only — never import from tests.fixtures here.
# This file deliberately mirrors the README snippets verbatim.
from resurrector import BagFrame
from resurrector.demo.sample_bag import BagConfig, generate_bag


pytestmark = pytest.mark.smoke


@pytest.fixture(scope="module")
def smoke_bag():
    """Build a tiny bag once for the whole module."""
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "smoke.mcap"
        generate_bag(path, BagConfig(duration_sec=2.0))
        yield path


def test_open_and_info(smoke_bag, capsys):
    """README Path C — open a bag, call info()."""
    bf = BagFrame(smoke_bag)
    bf.info()
    out = capsys.readouterr().out
    assert "/imu/data" in out


def test_to_polars(smoke_bag):
    """README — to_polars conversion produces a real DataFrame."""
    bf = BagFrame(smoke_bag)
    df = bf["/imu/data"].to_polars()
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert "timestamp_ns" in df.columns


def test_to_pandas(smoke_bag):
    """README — to_pandas conversion works for sklearn/matplotlib pipelines."""
    bf = BagFrame(smoke_bag)
    pdf = bf["/imu/data"].to_pandas()
    assert pdf.shape[0] > 0


def test_iter_chunks(smoke_bag):
    """README — chunked iteration yields multiple non-empty chunks."""
    bf = BagFrame(smoke_bag)
    chunk_count = 0
    for chunk in bf["/imu/data"].iter_chunks(chunk_size=100):
        assert chunk.height > 0
        chunk_count += 1
    assert chunk_count > 1


def test_materialize_ipc_cache_filter(smoke_bag):
    """README — materialize_ipc_cache().scan() supports filter pushdown."""
    bf = BagFrame(smoke_bag)
    with bf["/imu/data"].materialize_ipc_cache() as cache:
        filtered = (
            cache.scan()
            .filter(pl.col("linear_acceleration.x").abs() > 0.0)
            .collect()
        )
        assert filtered.height >= 0
        # File should exist while inside the with-block
        assert cache.path is not None and cache.path.exists()
    # And be cleaned up after exit
    assert cache.path is None or not cache.path.exists()


def test_health_report(smoke_bag):
    """README — health_report() returns a usable score."""
    bf = BagFrame(smoke_bag)
    report = bf.health_report()
    assert 0 <= report.score <= 100


def test_sync(smoke_bag):
    """README — sync() across multiple topics returns one aligned frame."""
    bf = BagFrame(smoke_bag)
    synced = bf.sync(
        ["/imu/data", "/joint_states"],
        method="nearest",
        tolerance_ms=50,
    )
    assert synced.height > 0
    assert "timestamp_ns" in synced.columns
