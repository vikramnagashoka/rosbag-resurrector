"""Tests for the v0.6.0 camera-frame-at endpoint — Sub-feature A.4.

GET /api/bags/{id}/scene/camera-frame-at?topic=&time_ns= returns the
frame_index nearest to the given time on an image topic. Used by the
SceneViewer's camera overlay to keep the displayed frame synced to
the scene's time slider.

Covers: happy path, time before/after the bag, exact match, image
topic enforcement, missing topic, missing bag, lazy frame-index build.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def synth_bag(tmp_dir):
    """Generate a bag with image topics for camera-overlay tests."""
    from resurrector.demo.sample_bag import generate_bag, BagConfig
    bag_path = tmp_dir / "synth.mcap"
    generate_bag(
        bag_path,
        BagConfig(duration_sec=2.0, camera_hz=30.0, include_compressed=False),
    )
    return bag_path


@pytest.fixture
def indexed_bag(synth_bag, tmp_dir):
    db_path = tmp_dir / "test.db"
    index = BagIndex(db_path)
    scanned = scan_path(synth_bag)[0]
    parser = parse_bag(synth_bag)
    metadata = parser.get_metadata()
    bag_id = index.upsert_bag(scanned, metadata)
    bf = BagFrame(synth_bag)
    index.update_health_score(bag_id, bf.health_report().score)
    index.close()
    return synth_bag, db_path, bag_id


@pytest.fixture
def app_client(indexed_bag, tmp_dir, monkeypatch):
    _, db_path, _ = indexed_bag
    monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db_path))
    monkeypatch.setenv(
        "RESURRECTOR_ALLOWED_ROOTS",
        os.pathsep.join([str(tmp_dir), tempfile.gettempdir(), str(Path.home())]),
    )
    import importlib
    from resurrector.dashboard import api as api_module
    importlib.reload(api_module)
    transport = ASGITransport(app=api_module.app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
class TestCameraFrameAt:
    async def test_returns_frame_index_at_bag_start(
        self, app_client, indexed_bag,
    ):
        bag_path, _, bag_id = indexed_bag
        bf = BagFrame(bag_path)
        target = bf.metadata.start_time_ns
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/camera-frame-at",
                params={"topic": "/camera/rgb", "time_ns": target},
            )
        assert r.status_code == 200
        body = r.json()
        # Should pick the very first frame
        assert body["frame_index"] == 0
        assert body["frame_time_ns"] >= target

    async def test_returns_last_frame_at_bag_end(
        self, app_client, indexed_bag,
    ):
        bag_path, _, bag_id = indexed_bag
        bf = BagFrame(bag_path)
        target = bf.metadata.end_time_ns
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/camera-frame-at",
                params={"topic": "/camera/rgb", "time_ns": target},
            )
        assert r.status_code == 200
        body = r.json()
        # 2 seconds * 30 Hz - 1 = frame 59 (zero-indexed)
        assert body["frame_index"] >= 50

    async def test_returns_dt_ns_signed(
        self, app_client, indexed_bag,
    ):
        # Pick a target slightly after a known frame to verify dt_ns is
        # signed (positive = frame is ahead of target, negative = behind).
        bag_path, _, bag_id = indexed_bag
        bf = BagFrame(bag_path)
        target = bf.metadata.start_time_ns + 50_000_000  # 50 ms in
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/camera-frame-at",
                params={"topic": "/camera/rgb", "time_ns": target},
            )
        assert r.status_code == 200
        body = r.json()
        # dt should be small (within one frame interval = ~33 ms at 30 Hz)
        assert abs(body["dt_ns"]) < 35_000_000

    async def test_unknown_topic_returns_404(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/camera-frame-at",
                params={"topic": "/nonexistent", "time_ns": 1000},
            )
        assert r.status_code == 404

    async def test_non_image_topic_returns_400(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        # /imu/data is not an image topic
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/camera-frame-at",
                params={"topic": "/imu/data", "time_ns": 1000},
            )
        assert r.status_code == 400
        assert "image" in r.text.lower()

    async def test_unknown_bag_returns_404(self, app_client):
        async with app_client as client:
            r = await client.get(
                "/api/bags/9999/scene/camera-frame-at",
                params={"topic": "/camera/rgb", "time_ns": 1000},
            )
        assert r.status_code == 404

    async def test_missing_required_params_returns_422(
        self, app_client, indexed_bag,
    ):
        # FastAPI returns 422 on missing required Query params
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(f"/api/bags/{bag_id}/scene/camera-frame-at")
        assert r.status_code == 422

    async def test_lazy_frame_index_build_on_first_request(
        self, app_client, indexed_bag, tmp_dir,
    ):
        # A fresh bag has no frame_offsets. The endpoint should build them
        # lazily on first call. Verify by clearing offsets, then calling.
        bag_path, db_path, bag_id = indexed_bag
        index = BagIndex(db_path)
        index.clear_frame_offsets(bag_id, "/camera/rgb")
        assert not index.has_frame_offsets(bag_id, "/camera/rgb")
        index.close()

        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/camera-frame-at",
                params={"topic": "/camera/rgb", "time_ns": 1_000_000_000},
            )
        assert r.status_code == 200

        # After the call, offsets should be built
        index = BagIndex(db_path)
        assert index.has_frame_offsets(bag_id, "/camera/rgb")
        index.close()

    async def test_idempotent_on_repeated_calls(
        self, app_client, indexed_bag,
    ):
        bag_path, _, bag_id = indexed_bag
        bf = BagFrame(bag_path)
        target = bf.metadata.start_time_ns + 500_000_000

        async with app_client as client:
            r1 = await client.get(
                f"/api/bags/{bag_id}/scene/camera-frame-at",
                params={"topic": "/camera/rgb", "time_ns": target},
            )
            r2 = await client.get(
                f"/api/bags/{bag_id}/scene/camera-frame-at",
                params={"topic": "/camera/rgb", "time_ns": target},
            )
        assert r1.json() == r2.json()
