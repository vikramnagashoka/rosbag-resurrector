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


def _ws_scope() -> dict:
    return {
        "type": "websocket",
        "asgi": {"version": "3.0"},
        "scheme": "ws",
        "path": "/ws",
        "raw_path": b"/ws",
        "root_path": "",
        "query_string": b"",
        "headers": [],
        "server": ("testserver", 80),
        "client": ("testclient", 50000),
        "subprotocols": [],
    }


class TestWebSocketHandlerLifecycle:
    async def test_handler_returns_after_client_disconnects(self, test_bag):
        # Regression: the handler ran asyncio.gather(send_loop, receive_loop,
        # event_loop). Only receive_loop notices a disconnect; the other two
        # loop forever, so the handler never returned, its cleanup never ran,
        # and uvicorn's graceful shutdown waited on it indefinitely.
        # Driven at the raw ASGI level so the test owns both ends and can
        # bound the wait instead of hanging.
        from resurrector.bridge.server import BridgeServer

        bridge = BridgeServer(mode="playback", bag_path=test_bag, speed=10.0)
        app = bridge.create_app()
        to_app: asyncio.Queue = asyncio.Queue()
        from_app: asyncio.Queue = asyncio.Queue()
        await to_app.put({"type": "websocket.connect"})
        handler = asyncio.create_task(app(_ws_scope(), to_app.get, from_app.put))
        try:
            accept = await asyncio.wait_for(from_app.get(), timeout=30)
            assert accept["type"] == "websocket.accept"
            assert len(bridge._event_subscribers) == 1

            await to_app.put({"type": "websocket.disconnect", "code": 1001})
            await asyncio.wait_for(handler, timeout=30)
        finally:
            if not handler.done():
                handler.cancel()

        assert bridge._event_subscribers == []
