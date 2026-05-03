"""Record-while-streaming via the bridge --record flag (v0.5.0).

Demonstrates: ``resurrector bridge playback bag.mcap --record out.mcap``
— write every relayed message to a fresh MCAP in parallel with the
WebSocket stream, then verify the recording round-trips cleanly.

Run:
    python examples/21_record_while_streaming.py

What you'll see: a bridge subprocess plays the demo bag at 5× speed
while writing the same messages to a sidecar MCAP. After 4 wall-clock
seconds we shut down and re-open the recorded file to confirm
topics + message counts.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
import urllib.request

from _common import ensure_output_dir, ensure_sample_bag, header, section

from resurrector.ingest.parser import parse_bag


PORT = 9095


def port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.2)
        try:
            s.connect(("127.0.0.1", port))
            return True
        except OSError:
            return False


def main() -> None:
    header("21 — v0.5.0: bridge --record (record-while-streaming)")
    bag_path = ensure_sample_bag()
    out_dir = ensure_output_dir()
    record_path = out_dir / "v05_recorded.mcap"
    if record_path.exists():
        record_path.unlink()
    print(f"  Source bag: {bag_path}")
    print(f"  Will record to: {record_path}\n")

    if port_open(PORT):
        print(f"  [SKIP] Port {PORT} already in use; pick a free port.\n")
        return

    section("Start bridge with --record")
    cmd = [
        sys.executable, "-m", "resurrector.cli.main", "bridge", "playback",
        str(bag_path),
        "--port", str(PORT),
        "--speed", "5.0",
        "--record", str(record_path),
        "--no-browser",
    ]
    print(f"  $ {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    section("Wait for the WebSocket port to open")
    deadline = time.time() + 8
    while time.time() < deadline:
        if proc.poll() is not None:
            err = (proc.stderr.read() if proc.stderr else b"").decode(errors="replace")
            print(f"  [FATAL] subprocess exited (code {proc.returncode}). stderr:")
            for line in err.splitlines()[-15:]:
                print(f"    {line}")
            return
        if port_open(PORT):
            break
        time.sleep(0.2)
    print(f"  [OK] Bridge listening on {PORT}; recorder open on {record_path.name}")

    section("Trigger playback (the bridge waits for play() before streaming)")
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}/api/playback/play", method="POST",
    )
    with urllib.request.urlopen(req, timeout=2) as r:
        print(f"  POST /api/playback/play → {r.status}")

    section("Stream + record for 4 wall-clock seconds")
    for sec in range(4, 0, -1):
        print(f"  recording... {sec:>2}s left", end="\r", flush=True)
        time.sleep(1)
    print(f"  recording... done           ")

    section("Shutdown bridge cleanly (so the recorder can finalize)")
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)
    print(f"  Bridge stopped (exit code {proc.returncode})")

    if not record_path.exists() or record_path.stat().st_size < 200:
        print(f"\n  [WARN] recorded file missing or tiny ({record_path}).")
        return

    section("Verify the recorded MCAP")
    print(f"  size on disk: {record_path.stat().st_size // 1024} KB")
    parser = parse_bag(record_path)
    meta = parser.get_metadata()
    print(f"  duration: {meta.duration_sec:.3f}s   total messages: {meta.message_count}")
    print(f"  topics in the recording:")
    for ti in meta.topics[:8]:
        print(f"    {ti.name:<28} ({ti.message_type})  msgs={ti.message_count}")

    print(
        "\n  ✓ The recorder writes a real, re-openable MCAP — schemas + channels\n"
        "    are registered per topic so downstream tools (resurrector info,\n"
        "    PlotJuggler, mcap CLI) treat it as a first-class bag.\n"
    )


if __name__ == "__main__":
    main()
