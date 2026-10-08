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
import tempfile
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


def _block_log_dir(bridge_env: Path, monkeypatch) -> Path:
    """Point the bridge log dir somewhere mkdir can't create; return the
    temp dir the fallback log should land in.

    The log dir's parent is a regular file, so mkdir raises
    NotADirectoryError even for root (a chmod-based block wouldn't).
    """
    blocker = bridge_env / "not_a_dir"
    blocker.write_text("")
    monkeypatch.setenv("RESURRECTOR_BRIDGE_LOG_DIR", str(blocker / "logs"))
    temp_dir = bridge_env / "tmp"
    temp_dir.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp_dir))
    return temp_dir


class TestBridgeLogFallback:
    def test_start_works_when_log_dir_cannot_be_created(self, bridge_env, monkeypatch):
        # Regression: sending stderr to ~/.resurrector/logs made Start
        # depend on that directory. A read-only home or a bad
        # RESURRECTOR_BRIDGE_LOG_DIR turned Start into a 500, where the
        # old PIPE-based Start worked. The log must fall back to a file
        # in the temp dir (still a file, never a pipe).
        temp_dir = _block_log_dir(bridge_env, monkeypatch)
        bag = generate_bag(bridge_env / "b.mcap", BagConfig(duration_sec=1.0))
        port = _free_port()
        client = TestClient(dash_api.app)

        _start_playback_bridge(client, bag, port)

        logs = list(temp_dir.glob(f"resurrector-bridge-{port}-*.log"))
        assert len(logs) == 1, logs
        # uvicorn logs this to stderr before it binds, and the dashboard
        # only reports ready once the port accepts connections.
        assert "Started server process" in logs[0].read_text()
        assert client.post("/api/bridge/stop").json() == {"stopped": True}

    def test_startup_crash_names_the_fallback_log(self, bridge_env, monkeypatch):
        temp_dir = _block_log_dir(bridge_env, monkeypatch)
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
        [log] = temp_dir.glob(f"resurrector-bridge-{port}-*.log")
        assert str(log) in detail


class TestBridgeCliFatalErrors:
    def test_live_mode_without_rclpy_reports_the_cli_message(self, bridge_env, monkeypatch):
        # Regression: the bridge CLI printed fatal errors through the
        # shared stdout console. The dashboard discards the bridge's
        # stdout, so the early-exit detail said "no error output". The
        # dashboard's own rclpy check is forced to pass here, as when the
        # dashboard and the bridge interpreter disagree; the child still
        # has no rclpy and exits through the CLI's error print.
        from resurrector.bridge.live import is_rclpy_available
        from resurrector.core import capabilities

        if is_rclpy_available():
            pytest.skip("rclpy is installed, so the live bridge would start")
        monkeypatch.setattr(capabilities, "_bridge_live_available", lambda: True)
        port = _free_port()

        r = TestClient(dash_api.app).post(
            "/api/bridge/start",
            json={"mode": "live", "topics": ["/imu/data"], "port": port},
        )

        assert r.status_code == 500, r.text
        detail = r.json()["detail"]
        assert "exit code 1" in detail
        assert "Live mode requires rclpy" in detail
        assert "no error output" not in detail


class _StoppableProc:
    """A bridge process whose SIGTERM exit the test releases by hand."""

    pid = 0

    def __init__(self) -> None:
        self.terminate_sent = threading.Event()
        self.exit_now = threading.Event()
        self.returncode = None

    def poll(self):
        return self.returncode

    def terminate(self) -> None:
        self.terminate_sent.set()

    def kill(self) -> None:
        self.exit_now.set()

    def wait(self, timeout=None):
        if not self.exit_now.wait(timeout):
            raise subprocess.TimeoutExpired("fake-bridge", timeout)
        self.returncode = -signal.SIGTERM
        return self.returncode


class TestBridgeStop:
    async def test_stop_leaves_a_bridge_started_during_the_wait_alone(self, monkeypatch):
        # Stop awaits the old bridge's exit off the event loop, so a Start
        # can register a new bridge before Stop resumes. Stop must then
        # clear the state only if it still points at the process it
        # stopped; clearing it unconditionally orphans the new bridge
        # (still running, but the dashboard reports "not running" and
        # can't stop it).
        monkeypatch.setattr(dash_api, "_BRIDGE_STOP_GRACE_S", 120.0, raising=False)
        state = dash_api._get_bridge_state()
        old, new = _StoppableProc(), _AliveProc()
        state.update(process=old, port=1, mode="playback")
        try:
            transport = httpx.ASGITransport(app=dash_api.app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://dashboard",
            ) as c:
                stop = asyncio.create_task(c.post("/api/bridge/stop"))
                assert await asyncio.to_thread(old.terminate_sent.wait, 30)
                state.update(process=new, port=2, mode="live")
                old.exit_now.set()
                resp = await asyncio.wait_for(stop, timeout=30)

            assert resp.json() == {"stopped": True}
            assert state["process"] is new
            assert (state["port"], state["mode"]) == (2, "live")
        finally:
            old.exit_now.set()
            _reset_bridge_state()

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


class TestBridgeCommand:
    """How the dashboard launches ``resurrector bridge``.

    Would catch: the frozen DMG/DEB app spawning ``<binary> -m
    resurrector.cli.main bridge ...``. There sys.executable is the
    resurrector CLI, so Typer exited with "No such option: -m" and the
    Bridge page's Start returned 500.
    """

    def test_frozen_app_calls_its_own_cli(self, monkeypatch):
        import sys
        from resurrector.dashboard.api import _bridge_command
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        assert _bridge_command("playback") == [sys.executable, "bridge", "playback"]

    def test_python_install_runs_the_module(self, monkeypatch):
        import sys
        from resurrector.dashboard.api import _bridge_command
        monkeypatch.delattr(sys, "frozen", raising=False)
        assert _bridge_command("live") == [
            sys.executable, "-m", "resurrector.cli.main", "bridge", "live",
        ]
