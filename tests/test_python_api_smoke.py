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


def test_bridge_runtime_deps_ship_with_the_base_install():
    """The bridge's runtime dependencies come with a plain ``pip install``.

    Would catch: httpx and websockets living only in the [dev] extra. Every
    CI job but this one installs [dev], which hid the gap: on a base install
    the dashboard's bridge proxy (Play/Pause/Seek) raised ModuleNotFoundError
    -> HTTP 500, and the bridge's /ws returned 404 because uvicorn found no
    WebSocket implementation. The wheel-smoke job installs only the wheel and
    pytest, so this test is the one that sees a base install.
    """
    import httpx  # noqa: F401  (dashboard -> bridge proxy)
    import websockets  # noqa: F401  (bridge /ws)
    from uvicorn.protocols.websockets.auto import AutoWebSocketsProtocol

    assert AutoWebSocketsProtocol is not None, (
        "uvicorn found no WebSocket library; the bridge's /ws would return 404"
    )


def test_doctor_core_checks_pass_on_base_install():
    """`resurrector doctor` finds every core dependency in a base install.

    Would catch: doctor listing a module as core (where a miss counts as
    a warning) that only an extra installs, or a base dependency such as
    Pillow dropping out of pyproject.toml.
    """
    from resurrector.cli.doctor import run_all_checks

    core = {r.name: r for r in run_all_checks() if r.tier == "core"}
    assert "Image decoding" in core
    # The index directory only appears after the first scan.
    core.pop("Index location", None)
    failing = {n: r.detail for n, r in core.items() if r.status != "pass"}
    assert not failing, f"core doctor checks not passing: {failing}"


def test_demo_camera_frames_decode_at_configured_size(smoke_bag, tmp_path):
    """Demo JPEG frames decode to full-size RGB on a base install.

    Would catch: Pillow living only in the extras. Without it the demo
    generator wrote hard-coded 1x1 grayscale JPEGs (LeRobot export then
    crashed on them) and decoding any CompressedImage raised ImportError.
    The second bag uses a non-default size, so a generator that ignored
    BagConfig and always wrote 64x48 frames would fail too.
    """
    import numpy as np

    config = BagConfig()
    bf = BagFrame(smoke_bag)
    _, frame = next(iter(bf["/camera/compressed"].iter_images()))
    assert frame.shape == (config.image_height, config.image_width, 3)

    # Same colour as the raw frame at that time, up to JPEG loss: a
    # placeholder frame of the right size would still fail here.
    _, raw = next(iter(bf["/camera/rgb"].iter_images()))
    drift = np.abs(frame.reshape(-1, 3).mean(axis=0) - raw[0, 0].astype(float))
    assert drift.max() < 10, f"compressed frame colour drifted by {drift}"

    odd = BagConfig(duration_sec=0.5, image_width=40, image_height=30)
    small = BagFrame(generate_bag(tmp_path / "small.mcap", odd))
    for topic in ("/camera/compressed", "/camera/rgb"):
        _, frame = next(iter(small[topic].iter_images()))
        assert frame.shape == (30, 40, 3), topic


def test_dashboard_serves_camera_frames(smoke_bag, tmp_path, monkeypatch):
    """Frame and Library-thumbnail endpoints return JPEGs on a base install.

    Would catch: the same Pillow gap on the dashboard side, where every
    camera frame and thumbnail returned HTTP 500 (ModuleNotFoundError:
    PIL) after a plain ``pip install``.
    """
    import io

    from fastapi.testclient import TestClient

    from resurrector.dashboard.api import app
    from resurrector.ingest.indexer import BagIndex
    from resurrector.ingest.parser import parse_bag
    from resurrector.ingest.scanner import scan_path

    db_path = tmp_path / "index.db"
    monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db_path))
    index = BagIndex(db_path)
    try:
        bag_id = index.upsert_bag(
            scan_path(smoke_bag)[0], parse_bag(smoke_bag).get_metadata(),
        )
    finally:
        index.close()

    config = BagConfig()
    client = TestClient(app, raise_server_exceptions=False)
    for topic in ("camera/compressed", "camera/rgb"):
        for route, size in (
            ("frame/0", (config.image_width, config.image_height)),
            ("thumbnail", None),
        ):
            r = client.get(f"/api/bags/{bag_id}/topics/{topic}/{route}")
            assert r.status_code == 200, (
                f"{topic}/{route}: HTTP {r.status_code} {r.text[:200]}"
            )
            assert r.headers["content-type"] == "image/jpeg"
            # Imported only after the request so a missing Pillow shows
            # up as the endpoint's 500, not as this test's ImportError.
            from PIL import Image

            img = Image.open(io.BytesIO(r.content))
            assert img.format == "JPEG"
            if size is not None:
                assert img.size == size
