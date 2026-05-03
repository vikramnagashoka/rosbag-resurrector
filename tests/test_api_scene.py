"""Tests for the dashboard's 3D scene endpoints (Sub-feature 3.1 + 3.2).

- GET /api/bags/{id}/scene/topics categorizes topics by 3D-relevance
- GET /api/bags/{id}/scene/tf-tree resolves the TF tree at a timestamp
- GET /api/bags/{id}/scene/pointcloud decodes a PointCloud2 message
- 404 on unknown bag IDs / topic names
- max_points caps the response payload size
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
from tests.fixtures.scene_bag import generate_scene_bag


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def indexed_scene_bag(tmp_dir):
    bag_path = generate_scene_bag(tmp_dir / "scene.mcap")
    db_path = tmp_dir / "scene.db"
    index = BagIndex(db_path)
    scanned = scan_path(bag_path)[0]
    parser = parse_bag(bag_path)
    metadata = parser.get_metadata()
    bag_id = index.upsert_bag(scanned, metadata)
    bf = BagFrame(bag_path)
    index.update_health_score(bag_id, bf.health_report().score)
    index.close()
    return bag_path, db_path, bag_id


@pytest.fixture
def app_client(indexed_scene_bag, monkeypatch):
    _, db_path, _ = indexed_scene_bag
    monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db_path))
    monkeypatch.setenv(
        "RESURRECTOR_ALLOWED_ROOTS",
        os.pathsep.join([tempfile.gettempdir(), str(Path.home())]),
    )
    import importlib
    from resurrector.dashboard import api as api_module
    importlib.reload(api_module)
    transport = ASGITransport(app=api_module.app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
class TestSceneTopicsEndpoint:
    async def test_categorizes_known_topics(self, app_client, indexed_scene_bag):
        async with app_client as client:
            r = await client.get(f"/api/bags/{indexed_scene_bag[2]}/scene/topics")
        assert r.status_code == 200
        body = r.json()
        assert body["tf"] == ["/tf"]
        assert body["tf_static"] == ["/tf_static"]
        assert body["pointclouds"] == ["/lidar/points"]
        assert body["images"] == []
        assert body["markers"] == []

    async def test_unknown_bag_returns_404(self, app_client):
        async with app_client as client:
            r = await client.get("/api/bags/9999/scene/topics")
        assert r.status_code == 404


@pytest.mark.asyncio
class TestSceneTFTreeEndpoint:
    async def test_default_time_returns_initial_state(self, app_client, indexed_scene_bag):
        async with app_client as client:
            r = await client.get(f"/api/bags/{indexed_scene_bag[2]}/scene/tf-tree")
        assert r.status_code == 200
        body = r.json()
        assert "frames" in body
        # All four frames should appear
        assert set(body["frames"]) >= {"world", "base_link", "camera_link"}
        assert "world" in body["roots"]
        # 1 static + 1 (latest) dynamic edge resolved
        assert body["static_count"] == 1
        # All 5 dynamic samples loaded
        assert body["dynamic_count"] == 5

    async def test_specific_time_resolves_correct_dynamic(
        self, app_client, indexed_scene_bag,
    ):
        # The dynamic TFs are 100 ms apart starting at base_ns.
        # Asking at t=base_ns+250ms should resolve to the 3rd sample (index 2).
        bag_path, _, bag_id = indexed_scene_bag
        bf = BagFrame(bag_path)
        target = bf.metadata.start_time_ns + 250_000_000
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/tf-tree?time_ns={target}",
            )
        assert r.status_code == 200
        body = r.json()
        # Find the world→base_link edge
        edges = [
            e for e in body["edges"]
            if e["parent_frame"] == "world" and e["child_frame"] == "base_link"
        ]
        assert len(edges) == 1
        # i=2 → translation x = 0.2; i=3 → x = 0.3. nearest at 250 ms is i=2 (200ms) or i=3 (300ms);
        # tie-break of bisect_left picks the earlier when equidistant
        assert edges[0]["translation"][0] in (0.2, 0.3)

    async def test_unknown_bag_returns_404(self, app_client):
        async with app_client as client:
            r = await client.get("/api/bags/9999/scene/tf-tree")
        assert r.status_code == 404


@pytest.mark.asyncio
class TestScenePointCloudEndpoint:
    async def test_decodes_first_message(self, app_client, indexed_scene_bag):
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{indexed_scene_bag[2]}/scene/pointcloud"
                "?topic=/lidar/points",
            )
        assert r.status_code == 200
        body = r.json()
        assert body["frame_id"] == "lidar_link"
        assert body["n_points"] == 100  # we wrote 100 per frame
        assert len(body["points"]) == 100
        # Each point is [x, y, z]
        assert all(len(p) == 3 for p in body["points"])

    async def test_max_points_caps_payload(self, app_client, indexed_scene_bag):
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{indexed_scene_bag[2]}/scene/pointcloud"
                "?topic=/lidar/points&max_points=20",
            )
        assert r.status_code == 200
        body = r.json()
        # Decimation gives ~20 points (could be slightly more or fewer)
        assert 10 <= body["n_points"] <= 25

    async def test_specific_time_picks_nearest(self, app_client, indexed_scene_bag):
        bag_path, _, bag_id = indexed_scene_bag
        bf = BagFrame(bag_path)
        # Second pointcloud is at start_ns + 100 ms
        target = bf.metadata.start_time_ns + 100_000_000
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/pointcloud"
                f"?topic=/lidar/points&time_ns={target}",
            )
        assert r.status_code == 200
        body = r.json()
        # The chosen message timestamp should match (or be very close to) target
        assert abs(body["time_ns"] - target) < 100_000_000

    async def test_unknown_topic_returns_404(self, app_client, indexed_scene_bag):
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{indexed_scene_bag[2]}/scene/pointcloud"
                "?topic=/nonexistent",
            )
        assert r.status_code == 404

    async def test_unknown_bag_returns_404(self, app_client):
        async with app_client as client:
            r = await client.get(
                "/api/bags/9999/scene/pointcloud?topic=/lidar/points",
            )
        assert r.status_code == 404
