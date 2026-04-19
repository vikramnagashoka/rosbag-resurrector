"""Tests for v0.3.0 API additions: annotations, datasets, bridge.

Bridge lifecycle is tested in `test_bridge_api.py` because it needs
subprocess orchestration that's isolated from the other API tests.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path
from resurrector.core.bag_frame import BagFrame
from tests.fixtures.generate_test_bags import BagConfig, generate_bag


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def indexed_bag(tmp_dir):
    bag_path = generate_bag(tmp_dir / "test.mcap", BagConfig(duration_sec=2.0))
    db_path = tmp_dir / "test.db"
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
def app_client(indexed_bag, monkeypatch):
    _, db_path, _ = indexed_bag
    monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db_path))
    monkeypatch.setenv(
        "RESURRECTOR_ALLOWED_ROOTS",
        os.pathsep.join([tempfile.gettempdir(), str(Path.home())]),
    )
    # Reload the module so env vars are picked up
    import importlib
    from resurrector.dashboard import api as api_module
    importlib.reload(api_module)
    transport = ASGITransport(app=api_module.app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
class TestAnnotations:
    async def test_list_empty(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(f"/api/bags/{bag_id}/annotations")
            assert r.status_code == 200
            assert r.json() == {"annotations": []}

    async def test_create_list_delete(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{bag_id}/annotations",
                json={"timestamp_ns": 1_000_000_000, "text": "robot fell"},
            )
            assert r.status_code == 200
            aid = r.json()["id"]

            r = await client.get(f"/api/bags/{bag_id}/annotations")
            assert len(r.json()["annotations"]) == 1
            assert r.json()["annotations"][0]["text"] == "robot fell"

            r = await client.patch(
                f"/api/annotations/{aid}", json={"text": "robot tipped"},
            )
            assert r.status_code == 200
            r = await client.get(f"/api/bags/{bag_id}/annotations")
            assert r.json()["annotations"][0]["text"] == "robot tipped"

            r = await client.delete(f"/api/annotations/{aid}")
            assert r.status_code == 200
            r = await client.get(f"/api/bags/{bag_id}/annotations")
            assert r.json()["annotations"] == []

    async def test_create_rejects_empty_text(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{bag_id}/annotations",
                json={"timestamp_ns": 1, "text": "   "},
            )
            assert r.status_code == 400

    async def test_create_rejects_bad_timestamp(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{bag_id}/annotations",
                json={"timestamp_ns": "not-a-number", "text": "hi"},
            )
            assert r.status_code == 400

    async def test_create_on_unknown_bag(self, app_client):
        async with app_client as client:
            r = await client.post(
                "/api/bags/999/annotations",
                json={"timestamp_ns": 1, "text": "ghost"},
            )
            assert r.status_code == 404

    async def test_update_nonexistent(self, app_client):
        async with app_client as client:
            r = await client.patch(
                "/api/annotations/99999", json={"text": "ghost"},
            )
            assert r.status_code == 404


@pytest.mark.asyncio
class TestDatasets:
    async def test_list_initially_empty(self, app_client):
        async with app_client as client:
            r = await client.get("/api/datasets")
            assert r.status_code == 200
            assert r.json() == {"datasets": []}

    async def test_create_and_fetch(self, app_client):
        async with app_client as client:
            r = await client.post(
                "/api/datasets",
                json={"name": "pick-and-place", "description": "UR5 demos"},
            )
            assert r.status_code == 200
            r = await client.get("/api/datasets")
            names = [d["name"] for d in r.json()["datasets"]]
            assert "pick-and-place" in names

            r = await client.get("/api/datasets/pick-and-place")
            assert r.status_code == 200
            assert r.json()["description"] == "UR5 demos"

    async def test_create_duplicate(self, app_client):
        async with app_client as client:
            await client.post("/api/datasets", json={"name": "dup"})
            r = await client.post("/api/datasets", json={"name": "dup"})
            assert r.status_code == 409

    async def test_create_missing_name(self, app_client):
        async with app_client as client:
            r = await client.post("/api/datasets", json={})
            assert r.status_code == 400

    async def test_get_unknown(self, app_client):
        async with app_client as client:
            r = await client.get("/api/datasets/ghost")
            assert r.status_code == 404

    async def test_delete(self, app_client):
        async with app_client as client:
            await client.post("/api/datasets", json={"name": "deleteme"})
            r = await client.delete("/api/datasets/deleteme")
            assert r.status_code == 200
            r = await client.get("/api/datasets/deleteme")
            assert r.status_code == 404

    async def test_delete_unknown(self, app_client):
        async with app_client as client:
            r = await client.delete("/api/datasets/ghost")
            assert r.status_code == 404

    async def test_create_version_bad_bag_refs(self, app_client):
        async with app_client as client:
            await client.post("/api/datasets", json={"name": "v1"})
            r = await client.post(
                "/api/datasets/v1/versions",
                json={"version": "1.0", "bag_refs": [{"not_path": "x"}]},
            )
            assert r.status_code == 400

    async def test_create_version_unknown_dataset(self, app_client):
        async with app_client as client:
            r = await client.post(
                "/api/datasets/ghost/versions",
                json={"version": "1.0", "bag_refs": []},
            )
            assert r.status_code == 404

    async def test_create_version_missing_fields(self, app_client):
        async with app_client as client:
            await client.post("/api/datasets", json={"name": "v2"})
            r = await client.post("/api/datasets/v2/versions", json={})
            assert r.status_code == 400


@pytest.mark.asyncio
class TestFrameEndpointWithOffsetCache:
    async def test_first_request_builds_cache(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/topics/camera/rgb/frame/0?width=64",
            )
            assert r.status_code == 200
            assert r.headers["content-type"] == "image/jpeg"
            assert len(r.content) > 100  # non-trivial JPEG

    async def test_rejects_non_image_topic(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/topics/imu/data/frame/0",
            )
            assert r.status_code == 400

    async def test_unknown_topic(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/topics/camera/ghost/frame/0",
            )
            assert r.status_code == 404

    async def test_out_of_range_frame(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/topics/camera/rgb/frame/999999",
            )
            assert r.status_code == 404
            assert "has" in r.json()["detail"].lower()


@pytest.mark.asyncio
class TestDownsampledTopicData:
    async def test_downsample_caps_points(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/topics/imu/data?max_points=50",
            )
            assert r.status_code == 200
            body = r.json()
            assert body["downsampled"] is True
            assert body["max_points"] == 50
            assert len(body["data"]) <= 50

    async def test_without_max_points_uses_pagination(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/topics/imu/data?limit=10",
            )
            assert r.status_code == 200
            body = r.json()
            assert body["downsampled"] is False
            assert len(body["data"]) <= 10

    async def test_cache_hit_returns_same_shape(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            url = f"/api/bags/{bag_id}/topics/imu/data?max_points=100"
            r1 = await client.get(url)
            r2 = await client.get(url)
            assert r1.status_code == 200
            assert r2.status_code == 200
            assert r1.json()["total"] == r2.json()["total"]
            assert len(r1.json()["data"]) == len(r2.json()["data"])
