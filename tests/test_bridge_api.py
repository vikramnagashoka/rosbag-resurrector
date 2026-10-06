"""Dashboard-side bridge lifecycle: /api/bridge/start, /stop, /proxy.

The start/stop tests spawn the real bridge subprocess through the
dashboard API, the same path the Bridge page uses. The ``bridge_env``
fixture kills whatever bridge a test left behind so a failing test
can't leak a process that holds a port.
"""

from __future__ import annotations

import asyncio
import signal
import socket
import subprocess
import sys
import threading
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from resurrector.dashboard import api as dash_api
from resurrector.demo.sample_bag import BagConfig, generate_bag


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _reset_bridge_state() -> None:
    dash_api._get_bridge_state().update(process=None, port=None, mode=None)


class _AliveProc:
    """Stands in for a running bridge process in proxy tests."""

    pid = 0

    def poll(self):
        return None


@pytest.fixture
def bridge_env(tmp_path, monkeypatch):
    """Allowed roots + bridge log confined to tmp_path; bridge reaped on exit."""
    monkeypatch.setenv("RESURRECTOR_ALLOWED_ROOTS", str(tmp_path))
    monkeypatch.setenv("RESURRECTOR_BRIDGE_LOG_DIR", str(tmp_path / "logs"))
    _reset_bridge_state()
    yield tmp_path
    proc = dash_api._get_bridge_state()["process"]
    if proc is not None and proc.poll() is None:
        proc.kill()
        proc.wait(timeout=10)
    _reset_bridge_state()


def _start_playback_bridge(client: TestClient, bag: Path, port: int):
    r = client.post(
        "/api/bridge/start",
        json={"mode": "playback", "bag_path": str(bag), "port": port},
    )
    assert r.status_code == 200, r.text
    return dash_api._get_bridge_state()["process"]


class TestBridgeOutputCannotBlockBridge:
    def test_bridge_keeps_serving_past_a_pipe_buffer_of_access_log(self, bridge_env):
        # Regression: the dashboard spawned the bridge with stdout=PIPE and
        # never read it. uvicorn writes one access-log line per request, so
        # after ~64 KB the bridge's write() blocked its event loop for good
        # (1053 polls of /api/status, ~9 min of viewer.js at 500 ms).
        # A 4 KB query string makes each access-log line ~4 KB, so 300
        # requests write ~1.2 MB: past a default 64 KB pipe and past
        # Linux's 1 MB pipe-max-size. The old code hung on request ~16.
        bag = generate_bag(bridge_env / "b.mcap", BagConfig(duration_sec=1.0))
        port = _free_port()
        client = TestClient(dash_api.app)
        _start_playback_bridge(client, bag, port)

        pad = "x" * 4000
        with httpx.Client(timeout=30.0) as h:
            for i in range(300):
                resp = h.get(
                    f"http://127.0.0.1:{port}/api/status", params={"pad": pad},
                )
                assert resp.status_code == 200, f"request {i}: {resp.status_code}"

        assert client.post("/api/bridge/stop").json() == {"stopped": True}

    def test_startup_crash_reports_the_cause_and_log_path(self, bridge_env):
        # The bridge's output now goes to a log file instead of a pipe;
        # a crash during startup must still produce an actionable error.
        # The old message was the first 500 chars of stderr, which for a
        # Rich traceback is the top of the box, never the exception.
        bad = bridge_env / "not_a_bag.mcap"
        bad.write_bytes(b"this is not an mcap file")
        port = _free_port()

        r = TestClient(dash_api.app).post(
            "/api/bridge/start",
            json={"mode": "playback", "bag_path": str(bad), "port": port},
        )

        assert r.status_code == 500, r.text
        detail = r.json()["detail"]
        assert "exited during startup" in detail
        assert "InvalidMagic" in detail
        log_path = bridge_env / "logs" / f"bridge-{port}.log"
        assert str(log_path) in detail
        assert "InvalidMagic" in log_path.read_text()

    def test_startup_error_line_skips_uvicorn_shutdown_chatter(self, tmp_path):
        # uvicorn logs INFO shutdown lines after a bind failure; the
        # error shown to the user must be the ERROR line, not the last line.
        log = tmp_path / "bridge.log"
        log.write_text(
            "INFO:     Started server process [3579]\n"
            "INFO:     Application startup complete.\n"
            "ERROR:    [Errno 48] error while attempting to bind on address "
            "('0.0.0.0', 9090): address already in use\n"
            "INFO:     Waiting for application shutdown.\n"
            "INFO:     Application shutdown complete.\n\n"
        )
        line = dash_api._last_error_line(log)
        assert line.startswith("ERROR:")
        assert "address already in use" in line

        assert dash_api._last_error_line(tmp_path / "missing.log") == ""


class TestBridgeStop:
    async def test_stop_does_not_block_the_dashboard_event_loop(self, monkeypatch):
        # Regression: stop called proc.wait(timeout=5) inside an async def,
        # so every other dashboard request stalled while the bridge shut
        # down. The child here ignores SIGTERM and only exits when its
        # stdin closes, and the grace period is far longer than the test,
        # so the stop is guaranteed to still be in flight when the status
        # request runs. No wall-clock assertion needed.
        monkeypatch.setattr(dash_api, "_BRIDGE_STOP_GRACE_S", 120.0, raising=False)
        child = subprocess.Popen(
            [
                sys.executable, "-c",
                "import signal, sys\n"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                "print('ready', flush=True)\n"
                "sys.stdin.read()\n",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        state = dash_api._get_bridge_state()
        try:
            assert child.stdout.readline().strip() == b"ready"
            terminate_sent = asyncio.Event()
            real_terminate = child.terminate

            def terminate() -> None:
                real_terminate()
                terminate_sent.set()

            child.terminate = terminate
            state.update(process=child, port=1, mode="playback")

            transport = httpx.ASGITransport(app=dash_api.app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://dashboard",
            ) as c:
                stop = asyncio.create_task(c.post("/api/bridge/stop"))
                await asyncio.wait_for(terminate_sent.wait(), timeout=30)

                status = await asyncio.wait_for(
                    c.get("/api/bridge/status"), timeout=30,
                )
                stop_finished_first = stop.done()
                child.stdin.close()  # now let the child exit
                resp = await asyncio.wait_for(stop, timeout=30)

            assert not stop_finished_first, (
                "/api/bridge/stop finished before a concurrent request was "
                "served: it blocked the event loop while waiting for the bridge"
            )
            assert status.json()["running"] is True
            assert resp.json() == {"stopped": True}
            assert state["process"] is None
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
            child.stdin.close()
            child.stdout.close()
            _reset_bridge_state()

    def test_stop_after_ws_client_left_exits_on_sigterm(self, bridge_env, monkeypatch):
        # Regression: the bridge's /ws handler gathered three loops, two of
        # which never return once the client is gone. uvicorn's graceful
        # shutdown waits for that handler, so SIGTERM never finished and
        # every Stop escalated to SIGKILL after the grace period. With the
        # grace period at 60 s, only a SIGTERM exit avoids the SIGKILL.
        websockets = pytest.importorskip("websockets")
        monkeypatch.setattr(dash_api, "_BRIDGE_STOP_GRACE_S", 60.0, raising=False)
        bag = generate_bag(bridge_env / "b.mcap", BagConfig(duration_sec=1.0))
        port = _free_port()
        client = TestClient(dash_api.app)
        proc = _start_playback_bridge(client, bag, port)

        async def visit_and_leave() -> None:
            async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as ws:
                await asyncio.wait_for(ws.recv(), timeout=30)  # topic list

        asyncio.run(visit_and_leave())

        assert client.post("/api/bridge/stop").json() == {"stopped": True}
        assert proc.returncode is not None
        assert proc.returncode != -signal.SIGKILL, (
            "bridge ignored SIGTERM and had to be SIGKILLed"
        )


class TestBridgeProxyErrors:
    def test_proxy_returns_504_when_bridge_never_answers(self, monkeypatch):
        # Regression: a hung bridge made proxied Play/Pause wait 30 s and
        # then 500 (only httpx.ConnectError was caught). The listening
        # socket below completes the TCP handshake in the kernel but never
        # answers, which is exactly what a frozen bridge looks like.
        monkeypatch.setattr(dash_api, "_BRIDGE_PROXY_TIMEOUT_S", 0.5, raising=False)
        with socket.socket() as srv:
            srv.bind(("127.0.0.1", 0))
            srv.listen(8)
            dash_api._get_bridge_state().update(
                process=_AliveProc(), port=srv.getsockname()[1], mode="playback",
            )
            try:
                r = TestClient(dash_api.app, raise_server_exceptions=False).post(
                    "/api/bridge/proxy/api/playback/pause",
                )
            finally:
                _reset_bridge_state()

        assert r.status_code == 504, r.text
        detail = r.json()["detail"]
        assert "did not respond" in detail
        assert "restart" in detail.lower()

    def test_proxy_returns_502_when_bridge_drops_the_connection(self):
        # A bridge that dies mid-request closes the socket without a
        # response (httpx.RemoteProtocolError). Same opaque-500 class.
        with socket.socket() as srv:
            srv.bind(("127.0.0.1", 0))
            srv.listen(8)

            def accept_and_hang_up() -> None:
                conn, _ = srv.accept()
                conn.close()

            t = threading.Thread(target=accept_and_hang_up, daemon=True)
            t.start()
            dash_api._get_bridge_state().update(
                process=_AliveProc(), port=srv.getsockname()[1], mode="playback",
            )
            try:
                r = TestClient(dash_api.app, raise_server_exceptions=False).post(
                    "/api/bridge/proxy/api/playback/pause",
                )
            finally:
                _reset_bridge_state()
            t.join(timeout=10)

        assert r.status_code == 502, r.text
        assert "bridge" in r.json()["detail"].lower()
