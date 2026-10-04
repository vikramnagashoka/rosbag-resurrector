"""Tests for the bridge WebSocket server."""

import asyncio
import json
import tempfile
from pathlib import Path

import pytest
from httpx import AsyncClient, ASGITransport

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.bridge.server import create_bridge_app


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def test_bag(tmp_dir):
    return generate_bag(tmp_dir / "test.mcap", BagConfig(duration_sec=2.0))


@pytest.fixture
def bridge_app(test_bag):
    return create_bridge_app(mode="playback", bag_path=test_bag, speed=10.0)


@pytest.fixture
def client(bridge_app):
    transport = ASGITransport(app=bridge_app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
class TestBridgeREST:
    async def test_get_topics(self, client):
        async with client as c:
            resp = await c.get("/api/topics")
            assert resp.status_code == 200
            data = resp.json()
            assert "available" in data
            assert len(data["available"]) >= 4
            names = [t["name"] for t in data["available"]]
            assert "/imu/data" in names

    async def test_get_metadata(self, client):
        async with client as c:
            resp = await c.get("/api/metadata")
            assert resp.status_code == 200
            data = resp.json()
            assert data["mode"] == "playback"
            assert data["duration_sec"] > 0
            assert data["topic_count"] >= 4

    async def test_get_status(self, client):
        async with client as c:
            resp = await c.get("/api/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["type"] == "status"
            assert data["mode"] == "playback"
            assert data["state"] == "stopped"

    async def test_playback_play_pause(self, client):
        async with client as c:
            # This test checks the play/pause state machine over HTTP, not
            # timing. At the fixture's 10x the 2 s bag ends ~0.2 s after
            # play, leaving ~0.1 s of slack; 1x gives ~1.9 s. The CI flake
            # ('stopped' on 3.13) was really event-loop starvation in
            # PlaybackEngine, fixed there and pinned by
            # test_behind_schedule_playback_does_not_starve_event_loop.
            resp = await c.post("/api/playback/speed", params={"v": 1.0})
            assert resp.status_code == 200

            resp = await c.post("/api/playback/play")
            assert resp.status_code == 200
            assert resp.json()["status"] == "playing"

            await asyncio.sleep(0.1)
            resp = await c.get("/api/status")
            assert resp.json()["state"] == "playing"

            resp = await c.post("/api/playback/pause")
            assert resp.status_code == 200
            resp = await c.get("/api/status")
            assert resp.json()["state"] == "paused"

    async def test_playback_speed(self, client):
        async with client as c:
            resp = await c.post("/api/playback/speed", params={"v": 4.0})
            assert resp.status_code == 200
            assert resp.json()["speed"] == 4.0

    async def test_root_page(self, client):
        async with client as c:
            resp = await c.get("/")
            assert resp.status_code == 200
