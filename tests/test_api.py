"""Tests for the FastAPI dashboard backend."""

import os
import tempfile
from pathlib import Path

import pytest
from httpx import AsyncClient, ASGITransport

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.ingest.scanner import scan_path
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.indexer import BagIndex
from resurrector.core.bag_frame import BagFrame


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def indexed_bag(tmp_dir):
    """Create and index a bag file, returning (bag_path, db_path, bag_id)."""
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
def app_client(indexed_bag):
    """Create a test client with the DB path set."""
    _, db_path, _ = indexed_bag
    os.environ["RESURRECTOR_DB_PATH"] = str(db_path)
    from resurrector.dashboard.api import app
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
class TestBagsAPI:
    async def test_list_bags(self, app_client, indexed_bag):
        async with app_client as client:
            response = await client.get("/api/bags")
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
            assert len(data) >= 1

    async def test_get_bag(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            response = await client.get(f"/api/bags/{bag_id}")
            assert response.status_code == 200
            data = response.json()
            assert data["id"] == bag_id
            assert "topics" in data

    async def test_get_bag_not_found(self, app_client):
        async with app_client as client:
            response = await client.get("/api/bags/99999")
            assert response.status_code == 404

    async def test_get_health(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            response = await client.get(f"/api/bags/{bag_id}/health")
            assert response.status_code == 200
            data = response.json()
            assert "score" in data
            assert 0 <= data["score"] <= 100
            assert "issues" in data
            assert "recommendations" in data

    async def test_get_topic_data(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            response = await client.get(
                f"/api/bags/{bag_id}/topics/imu/data",
                params={"limit": 10},
            )
            assert response.status_code == 200
            data = response.json()
            assert data["topic"] == "/imu/data"
            assert data["total"] > 0
            assert len(data["data"]) <= 10

    async def test_get_topic_not_found(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            response = await client.get(f"/api/bags/{bag_id}/topics/nonexistent")
            assert response.status_code == 404

    async def test_get_synced_data(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            response = await client.get(
                f"/api/bags/{bag_id}/sync",
                params={"topics": "/imu/data,/joint_states", "limit": 10},
            )
            assert response.status_code == 200
            data = response.json()
            assert data["total"] > 0

    async def test_get_timeline(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            response = await client.get(f"/api/bags/{bag_id}/timeline")
            assert response.status_code == 200
            data = response.json()
            assert "duration_sec" in data
            assert len(data["topics"]) > 0


@pytest.mark.asyncio
class TestSearchAPI:
    async def test_search(self, app_client, indexed_bag):
        async with app_client as client:
            response = await client.get("/api/search", params={"q": "topic:/imu/data"})
            assert response.status_code == 200
            data = response.json()
            assert len(data) >= 1

    async def test_search_no_results(self, app_client):
        async with app_client as client:
            response = await client.get("/api/search", params={"q": "topic:/nonexistent"})
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 0


@pytest.mark.asyncio
class TestScanAPI:
    async def test_scan_nonexistent_path(self, app_client):
        async with app_client as client:
            response = await client.post(
                "/api/scan",
                params={"path": "/definitely/not/a/real/path"},
            )
            assert response.status_code == 400

    async def test_scan_blocking(self, app_client, indexed_bag, tmp_dir):
        """Test non-streaming scan."""
        bag_dir = tmp_dir / "scan_test"
        bag_dir.mkdir()
        generate_bag(bag_dir / "new.mcap", BagConfig(duration_sec=1.0))
        async with app_client as client:
            response = await client.post(
                "/api/scan",
                params={"path": str(bag_dir)},
            )
            assert response.status_code == 200
            data = response.json()
            assert data["scanned"] == 1
            assert data["indexed"] == 1


@pytest.mark.asyncio
class TestExportAPI:
    async def test_export(self, app_client, indexed_bag, tmp_dir):
        _, _, bag_id = indexed_bag
        output = tmp_dir / "api_export"
        async with app_client as client:
            response = await client.post(
                f"/api/bags/{bag_id}/export",
                params={
                    "topics": "/imu/data",
                    "format": "parquet",
                    "output_dir": str(output),
                },
            )
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "completed"
