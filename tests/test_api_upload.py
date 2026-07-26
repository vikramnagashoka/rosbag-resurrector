"""Tests for POST /api/bags/upload — the notebook 'Import a new bag' flow.

The browser can't hand the server a filesystem path, so it POSTs the raw
bag bytes. The endpoint streams them to a uuid subdir under an uploads base
(RESURRECTOR_UPLOADS_DIR), scans + indexes the one file, and returns the
indexed bag record.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from tests.fixtures.generate_test_bags import BagConfig, generate_bag


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def app_client(tmp_dir, monkeypatch):
    db_path = tmp_dir / "test.db"
    uploads = tmp_dir / "uploads"
    monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db_path))
    monkeypatch.setenv("RESURRECTOR_UPLOADS_DIR", str(uploads))
    # Uploads live under tmp; allow it for the later path validation.
    monkeypatch.setenv(
        "RESURRECTOR_ALLOWED_ROOTS",
        os.pathsep.join([tempfile.gettempdir(), str(tmp_dir), str(Path.home())]),
    )
    import importlib
    from resurrector.dashboard import api as api_module
    importlib.reload(api_module)
    transport = ASGITransport(app=api_module.app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
async def test_upload_indexes_bag_and_returns_record(app_client, tmp_dir):
    bag_path = generate_bag(tmp_dir / "uploaded.mcap", BagConfig(duration_sec=2.0))
    data = bag_path.read_bytes()

    async with app_client as client:
        resp = await client.post(
            "/api/bags/upload",
            params={"filename": "uploaded.mcap"},
            content=data,
        )
        assert resp.status_code == 200, resp.text
        bag = resp.json()
        assert bag["id"] >= 1
        assert bag["path"].endswith("uploaded.mcap")
        assert len(bag["topics"]) > 0
        assert bag["message_count"] > 0

        # It shows up in the library listing too.
        listing = await client.get("/api/bags")
        assert any(b["id"] == bag["id"] for b in listing.json())


@pytest.mark.asyncio
async def test_upload_rejects_non_bag_extension(app_client):
    async with app_client as client:
        resp = await client.post(
            "/api/bags/upload",
            params={"filename": "notes.txt"},
            content=b"hello",
        )
        assert resp.status_code == 400
        assert "Unsupported file type" in resp.text


@pytest.mark.asyncio
async def test_upload_rejects_empty_body(app_client):
    async with app_client as client:
        resp = await client.post(
            "/api/bags/upload",
            params={"filename": "empty.mcap"},
            content=b"",
        )
        assert resp.status_code == 400
        assert "Empty upload" in resp.text


@pytest.mark.asyncio
async def test_upload_rejects_garbage_bytes_as_unreadable(app_client):
    async with app_client as client:
        resp = await client.post(
            "/api/bags/upload",
            params={"filename": "corrupt.mcap"},
            content=b"not a real mcap file" * 10,
        )
        # Written + scanned, but parse fails -> 422 (or 400 if scanner
        # rejects it outright). Either way, not a 500 and not a 200.
        assert resp.status_code in (400, 422), resp.text
