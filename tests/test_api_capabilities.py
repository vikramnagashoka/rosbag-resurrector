"""Tests for the unified capabilities endpoint and the install-banner
wiring it powers (bridge live pre-check + scan error classification).

Covers the API surface only — frontend integration is exercised by
vitest in resurrector/dashboard/app.
"""
from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient


def _client() -> TestClient:
    from resurrector.dashboard.api import app
    return TestClient(app)


class TestCapabilitiesEndpoint:
    def test_returns_all_four_known_capabilities(self):
        r = _client().get("/api/system/capabilities")
        assert r.status_code == 200
        d = r.json()
        assert set(d.keys()) == {
            "vision", "bridge_live", "ros1_convert", "all_exports",
            "publish", "copilot",
        }

    def test_each_capability_has_required_fields(self):
        d = _client().get("/api/system/capabilities").json()
        for name, cap in d.items():
            assert cap["name"] == name
            assert isinstance(cap["available"], bool)
            assert isinstance(cap["install_command"], str)
            assert cap["install_command"]  # not empty
            assert isinstance(cap["description"], str)

    def test_vision_install_command_mentions_pip(self):
        d = _client().get("/api/system/capabilities").json()
        assert "pip install" in d["vision"]["install_command"]

    def test_ros1_install_command_mentions_mcap(self):
        d = _client().get("/api/system/capabilities").json()
        assert "mcap" in d["ros1_convert"]["install_command"].lower()


class TestBridgeStartLiveModePreCheck:
    def test_live_mode_without_rclpy_returns_503_with_structured_detail(self):
        # rclpy isn't installed in the test env; if it ever is, skip.
        from resurrector.core.capabilities import _bridge_live_available
        if _bridge_live_available():
            import pytest
            pytest.skip("rclpy is installed in this environment")

        c = _client()
        r = c.post("/api/bridge/start", json={"mode": "live", "topics": ["/imu/data"]})
        assert r.status_code == 503
        detail = r.json()["detail"]
        assert detail["kind"] == "capability_unavailable"
        assert detail["capability"] == "bridge_live"
        assert "install_command" in detail
        assert "ROS 2" in detail["install_command"]

    def test_playback_mode_doesnt_pre_check_rclpy(self):
        # Playback should hit a downstream check (missing bag_path or busy
        # port) — anything BUT the rclpy capability gate. That proves we
        # only check rclpy when mode=live.
        import socket
        # Pick an unused port so we land on the bag_path check, not the
        # port-in-use check that might fire in environments running a
        # dev bridge.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            free_port = s.getsockname()[1]
        c = _client()
        r = c.post("/api/bridge/start", json={"mode": "playback", "port": free_port})
        # 400 = bag_path missing (the path we actually want). 409/400
        # for other reasons also acceptable — we just need NOT 503 with
        # bridge_live capability detail.
        assert r.status_code != 503 or r.json().get("detail", {}).get("capability") != "bridge_live"


class TestScanErrorClassification:
    def test_ros1_bag_without_mcap_cli_gets_kind(self, tmp_path: Path, monkeypatch):
        # Make `mcap` CLI unavailable so the convert path fails predictably.
        import shutil
        original_which = shutil.which
        monkeypatch.setattr(
            shutil, "which",
            lambda name, *a, **kw: None if name == "mcap" else original_which(name, *a, **kw),
        )

        # Drop a fake .bag in tmp_path so scan_path picks it up.
        fake_bag = tmp_path / "legacy.bag"
        fake_bag.write_bytes(b"\x00")  # not a real bag, will fail downstream too

        # Point allowed roots at our tmp dir so the scan endpoint accepts it.
        monkeypatch.setenv("RESURRECTOR_ALLOWED_ROOTS", str(tmp_path))

        # Re-import the api to pick up the new env var.
        import importlib
        from resurrector.dashboard import api as api_mod
        importlib.reload(api_mod)
        c = TestClient(api_mod.app)

        r = c.post("/api/scan", params={"path": str(tmp_path)})
        assert r.status_code == 200
        d = r.json()
        # Either kind=ros1_convert_unavailable (mcap missing) or unknown
        # (fake-bag content fails). Both kinds should appear as classified
        # objects with a 'kind' field, never as raw strings.
        for err in d["errors"]:
            assert "kind" in err
            assert "file" in err
            assert "error" in err
