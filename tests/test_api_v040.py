"""Tests for v0.4.0 API additions: density, trim, transform preview, compare."""

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
from tests.fixtures.generate_test_bags import BagConfig, generate_bag


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


def _index_bag(bag_path: Path, db_path: Path) -> int:
    index = BagIndex(db_path)
    scanned = scan_path(bag_path)[0]
    parser = parse_bag(bag_path)
    metadata = parser.get_metadata()
    bag_id = index.upsert_bag(scanned, metadata)
    bf = BagFrame(bag_path)
    index.update_health_score(bag_id, bf.health_report().score)
    index.close()
    return bag_id


@pytest.fixture
def indexed_bag(tmp_dir):
    bag_path = generate_bag(tmp_dir / "test.mcap", BagConfig(duration_sec=2.0))
    db_path = tmp_dir / "test.db"
    bag_id = _index_bag(bag_path, db_path)
    return bag_path, db_path, bag_id


@pytest.fixture
def two_indexed_bags(tmp_dir):
    db_path = tmp_dir / "test.db"
    a_path = generate_bag(tmp_dir / "run_a.mcap", BagConfig(duration_sec=2.0))
    b_path = generate_bag(tmp_dir / "run_b.mcap", BagConfig(duration_sec=2.0))
    a_id = _index_bag(a_path, db_path)
    b_id = _index_bag(b_path, db_path)
    return (a_path, a_id), (b_path, b_id), db_path


@pytest.fixture
def app_client(indexed_bag, monkeypatch):
    _, db_path, _ = indexed_bag
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


@pytest.fixture
def app_client_two_bags(two_indexed_bags, monkeypatch):
    _, _, db_path = two_indexed_bags
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
class TestDensityEndpoint:
    async def test_returns_density_per_topic(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(f"/api/bags/{bag_id}/density?bins=20")
            assert r.status_code == 200
            body = r.json()
            assert body["bag_id"] == bag_id
            assert body["bins"] == 20
            assert "/imu/data" in body["density"]

    async def test_topic_filter(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(
                f"/api/bags/{bag_id}/density?topic=/imu/data&bins=10",
            )
            assert r.status_code == 200
            body = r.json()
            assert set(body["density"].keys()) == {"/imu/data"}

    async def test_unknown_bag_404(self, app_client):
        async with app_client as client:
            r = await client.get("/api/bags/9999/density")
            assert r.status_code == 404

    async def test_invalid_bins(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.get(f"/api/bags/{bag_id}/density?bins=2")
            assert r.status_code == 422  # FastAPI validates ge=10


@pytest.mark.asyncio
class TestTrimEndpoint:
    async def test_trim_to_mcap(self, app_client, indexed_bag, tmp_dir):
        _, _, bag_id = indexed_bag
        out = tmp_dir / "trimmed.mcap"
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{bag_id}/trim",
                json={
                    "start_sec": 0.5,
                    "end_sec": 1.5,
                    "topics": ["/imu/data"],
                    "format": "mcap",
                    "output_path": str(out),
                },
            )
            assert r.status_code == 200, r.text
            assert out.exists()

    async def test_trim_to_csv(self, app_client, indexed_bag, tmp_dir):
        _, _, bag_id = indexed_bag
        out_dir = tmp_dir / "csv_out"
        out_dir.mkdir()
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{bag_id}/trim",
                json={
                    "start_sec": 0.5,
                    "end_sec": 1.5,
                    "topics": ["/imu/data"],
                    "format": "csv",
                    "output_path": str(out_dir),
                },
            )
            assert r.status_code == 200, r.text

    async def test_invalid_range_400(self, app_client, indexed_bag, tmp_dir):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{bag_id}/trim",
                json={
                    "start_sec": 2.0,
                    "end_sec": 1.0,
                    "topics": ["/imu/data"],
                    "format": "mcap",
                    "output_path": str(tmp_dir / "x.mcap"),
                },
            )
            assert r.status_code == 400

    async def test_missing_field_400(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{bag_id}/trim",
                json={"start_sec": 0.0},
            )
            assert r.status_code == 400
            assert "Missing" in r.json()["detail"]

    async def test_unknown_bag_404(self, app_client, tmp_dir):
        async with app_client as client:
            r = await client.post(
                "/api/bags/9999/trim",
                json={
                    "start_sec": 0.0,
                    "end_sec": 1.0,
                    "topics": ["/imu/data"],
                    "format": "mcap",
                    "output_path": str(tmp_dir / "x.mcap"),
                },
            )
            assert r.status_code == 404


@pytest.mark.asyncio
class TestTransformPreviewEndpoint:
    async def test_menu_op(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                "/api/transforms/preview",
                json={
                    "bag_id": bag_id,
                    "topic": "/imu/data",
                    "column": "linear_acceleration.x",
                    "op": "abs",
                    "max_points": 100,
                },
            )
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["label"].startswith("abs")
            assert len(body["data"]) <= 100

    async def test_expression_mode(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                "/api/transforms/preview",
                json={
                    "bag_id": bag_id,
                    "topic": "/imu/data",
                    "expression": 'pl.col("linear_acceleration.x") * 2',
                    "max_points": 100,
                },
            )
            assert r.status_code == 200, r.text
            assert r.json()["label"] == "result"

    async def test_unsafe_expression_400(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                "/api/transforms/preview",
                json={
                    "bag_id": bag_id,
                    "topic": "/imu/data",
                    "expression": '__import__("os").system("ls")',
                },
            )
            assert r.status_code == 400

    async def test_missing_op_or_expression_400(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                "/api/transforms/preview",
                json={"bag_id": bag_id, "topic": "/imu/data"},
            )
            assert r.status_code == 400

    async def test_unknown_topic_404(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                "/api/transforms/preview",
                json={
                    "bag_id": bag_id,
                    "topic": "/ghost",
                    "op": "abs",
                    "column": "x",
                },
            )
            assert r.status_code == 404

    async def test_menu_requires_column(self, app_client, indexed_bag):
        _, _, bag_id = indexed_bag
        async with app_client as client:
            r = await client.post(
                "/api/transforms/preview",
                json={
                    "bag_id": bag_id,
                    "topic": "/imu/data",
                    "op": "abs",
                },
            )
            assert r.status_code == 400


@pytest.mark.asyncio
class TestCompareTopicsEndpoint:
    async def test_overlay_two_bags(self, app_client_two_bags, two_indexed_bags):
        (_, a_id), (_, b_id), _ = two_indexed_bags
        async with app_client_two_bags as client:
            r = await client.post(
                "/api/compare/topics",
                json={"bag_ids": [a_id, b_id], "topic": "/imu/data"},
            )
            assert r.status_code == 200, r.text
            body = r.json()
            assert "bag_label" in body["columns"]
            assert "relative_t_sec" in body["columns"]
            labels = {row["bag_label"] for row in body["data"]}
            assert labels == set(body["labels"])

    async def test_offsets_passed_through(self, app_client_two_bags, two_indexed_bags):
        (_, a_id), (_, b_id), _ = two_indexed_bags
        async with app_client_two_bags as client:
            r = await client.post(
                "/api/compare/topics",
                json={
                    "bag_ids": [a_id, b_id],
                    "topic": "/imu/data",
                    "offsets_sec": [0.0, 1.0],
                },
            )
            assert r.status_code == 200

    async def test_unknown_topic_400(self, app_client_two_bags, two_indexed_bags):
        (_, a_id), (_, b_id), _ = two_indexed_bags
        async with app_client_two_bags as client:
            r = await client.post(
                "/api/compare/topics",
                json={"bag_ids": [a_id, b_id], "topic": "/ghost"},
            )
            assert r.status_code == 400

    async def test_empty_bag_ids_400(self, app_client):
        async with app_client as client:
            r = await client.post(
                "/api/compare/topics",
                json={"bag_ids": [], "topic": "/imu/data"},
            )
            assert r.status_code == 400

    async def test_unknown_bag_404(self, app_client):
        async with app_client as client:
            r = await client.post(
                "/api/compare/topics",
                json={"bag_ids": [9999], "topic": "/imu/data"},
            )
            assert r.status_code == 404
