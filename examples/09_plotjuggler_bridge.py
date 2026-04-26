"""PlotJuggler bridge — stream bag data over WebSocket for live viz.

Demonstrates: ``resurrector bridge playback`` (the WebSocket bridge
behind the dashboard's Bridge page).

Run:
    python examples/09_plotjuggler_bridge.py

What you'll see: a bridge subprocess starts in the background streaming
the demo bag at 1x. Print the connection URL for PlotJuggler. After 10
seconds of streaming, terminate the subprocess and print stats.

If you have PlotJuggler installed:
  PlotJuggler -> WebSocket Client -> ws://localhost:9090/ws

Or just open http://localhost:9090/ in any browser for the built-in
Plotly viewer.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time

from _common import ensure_sample_bag, header, section


PORT = 9090


def port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.2)
        try:
            s.connect(("127.0.0.1", port))
            return True
        except OSError:
            return False


def main() -> None:
    header("09 — PlotJuggler-compatible WebSocket bridge")
    bag_path = ensure_sample_bag()

    if port_open(PORT):
        print(f"\n  [SKIP] Port {PORT} is already in use. Stop the existing\n"
              f"         server (or change the port) and re-run.\n")
        return

    section("Start the bridge subprocess")
    cmd = [
        sys.executable, "-m", "resurrector.cli.main", "bridge", "playback",
        str(bag_path),
        "--port", str(PORT),
        "--speed", "1.0",
        "--no-browser",
    ]
    print(f"  Command: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    print(f"  PID: {proc.pid}")

    section("Wait for the WebSocket port to come up (max 8s)")
    deadline = time.time() + 8
    ready = False
    while time.time() < deadline:
        if proc.poll() is not None:
            stderr = (proc.stderr.read() if proc.stderr else b"").decode(
                errors="replace",
            )
            print(f"\n  [FATAL] Bridge subprocess exited (code {proc.returncode}).")
            print("  ── stderr ──")
            for line in stderr.splitlines()[-15:]:
                print(f"    {line}")
            return
        if port_open(PORT):
            ready = True
            break
        time.sleep(0.2)

    if not ready:
        print(f"\n  [FATAL] Port {PORT} never opened. Killing subprocess.\n")
        proc.terminate()
        return
    print(f"  [OK] Bridge listening on port {PORT}")

    section("Connection details")
    print(f"  PlotJuggler:    ws://localhost:{PORT}/ws")
    print(f"  Built-in viewer:  http://localhost:{PORT}/")
    print(f"  REST control:    http://localhost:{PORT}/api/playback/{{play,pause,seek,speed}}")

    section("Stream for 10 seconds, then stop")
    for sec in range(10, 0, -1):
        print(f"  streaming... {sec:>2}s remaining", end="\r", flush=True)
        time.sleep(1)
    print(f"  streaming... done           ")

    section("Shutdown")
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)
    print(f"  Bridge stopped (exit code {proc.returncode})")

    print(
        "\n  ✓ The same subprocess flow lives behind the dashboard's Bridge\n"
        "    page. The bridge supports playback (replay any MCAP) and live\n"
        "    mode (relay live ROS 2 topics — needs rclpy installed).\n"
    )


if __name__ == "__main__":
    main()
