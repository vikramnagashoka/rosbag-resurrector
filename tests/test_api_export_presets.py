"""Tests for the export-preset API surface (v0.5.0).

- GET /api/export-presets returns the registered presets
- POST /api/bags/{id}/export accepts ?preset=NAME
- Preset + override params produce the expected merged config
- Unknown preset → 400
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from resurrector.core.bag_frame import BagFrame
from resurrector.core.export import PRESETS
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path
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
    import importlib
    from resurrector.dashboard import api as api_module
    importlib.reload(api_module)
    transport = ASGITransport(app=api_module.app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
class TestListPresetsEndpoint:
    async def test_returns_all_registered_presets(self, app_client):
        async with app_client as client:
            r = await client.get("/api/export-presets")
        assert r.status_code == 200
        body = r.json()
        names = {p["name"] for p in body}
        assert names == set(PRESETS.keys())

    async def test_each_entry_has_required_fields(self, app_client):
        async with app_client as client:
            r = await client.get("/api/export-presets")
        body = r.json()
        required_fields = {
            "name", "format", "sync", "sync_method", "downsample_hz",
            "topic_filter", "description", "extras_required", "available",
        }
        for entry in body:
            missing = required_fields - set(entry.keys())
            assert not missing, f"{entry['name']} missing fields: {missing}"

    async def test_available_flag_reflects_extras_install(self, app_client):
        """`all-exports` extras (zarr) may or may not be installed in this venv."""
        async with app_client as client:
            r = await client.get("/api/export-presets")
        body = r.json()
        # camera-only / training-tabular have NO extras_required → always available
        for entry in body:
            if not entry["extras_required"]:
                assert entry["available"] is True, \
                    f"{entry['name']} has no extras but is marked unavailable"


@pytest.mark.asyncio
class TestExportWithPreset:
    async def test_preset_runs_export(self, app_client, indexed_bag, tmp_dir):
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{indexed_bag[2]}/export",
                params={
                    "preset": "training-tabular",
                    "output_dir": str(tmp_dir / "preset_out"),
                },
            )
        assert r.status_code == 200, f"body: {r.text}"
        body = r.json()
        assert body["status"] == "completed"
        assert "preset_out" in body["output_path"]

    async def test_unknown_preset_returns_400(self, app_client, indexed_bag, tmp_dir):
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{indexed_bag[2]}/export",
                params={
                    "preset": "no-such-preset",
                    "output_dir": str(tmp_dir / "should_not_exist"),
                },
            )
        assert r.status_code == 400
        assert "Unknown preset" in r.text or "no-such-preset" in r.text

    async def test_format_override_takes_precedence(self, app_client, indexed_bag, tmp_dir):
        """User-supplied format param must override preset's format."""
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{indexed_bag[2]}/export",
                params={
                    "preset": "training-tabular",  # default format=parquet
                    "format": "csv",                # override
                    "output_dir": str(tmp_dir / "override_csv"),
                },
            )
        assert r.status_code == 200
        # Should produce .csv not .parquet
        out = Path(tmp_dir / "override_csv")
        csv_files = list(out.rglob("*.csv"))
        parquet_files = list(out.rglob("*.parquet"))
        assert len(csv_files) >= 1, "user-supplied format should win — expected csv"
        assert len(parquet_files) == 0, \
            "preset's parquet should NOT have been used when user passed format=csv"

    async def test_no_preset_works_as_before(self, app_client, indexed_bag, tmp_dir):
        """Backward-compat: existing non-preset workflow still works."""
        async with app_client as client:
            r = await client.post(
                f"/api/bags/{indexed_bag[2]}/export",
                params={
                    "format": "parquet",
                    "output_dir": str(tmp_dir / "no_preset"),
                },
            )
        assert r.status_code == 200, f"body: {r.text}"
        out = Path(tmp_dir / "no_preset")
        assert len(list(out.rglob("*.parquet"))) >= 1
