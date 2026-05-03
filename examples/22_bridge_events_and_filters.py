"""Bridge protocol extras — event markers + server-side filter (v0.5.0).

Two new pieces of the WebSocket bridge protocol:

  1. **Time-anchored events.** ``POST /api/events`` broadcasts
     ``{type:"event", topic, timestamp_ns, text, kind}`` to every
     connected WS client. Useful for annotating "spike here" moments
     during live demos or for fan-out from a separate scoring pipeline.

  2. **Server-side filter language.** WS subscribe payload accepts
     ``{filters: {topic: polars_expr_str}}``. Each expression is a
     Polars predicate evaluated per-message — only matching messages
     get forwarded. AST-sandboxed (no ``__import__``, no module access).

Run:
    python examples/22_bridge_events_and_filters.py

What you'll see: connect a WS client to the bridge with a filter that
keeps only IMU messages where ``|linear_acceleration.x| > 0.05``,
post one event marker, then count what came through vs. an unfiltered
client running side-by-side.
"""

from __future__ import annotations

import asyncio
import json
import socket
import subprocess
import sys
import time
import urllib.request
from collections import Counter

from _common import ensure_sample_bag, header, section


PORT = 9096


def port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.2)
        try:
            s.connect(("127.0.0.1", port))
            return True
        except OSError:
            return False


async def consumer(label: str, filters: dict[str, str], counts: Counter, events: list,
                   stop_at: float) -> None:
    """Connect a WS client, subscribe, then count messages until stop_at (wall-clock)."""
    try:
        import websockets
    except ImportError:
        print(f"  [{label}] websockets package not installed; skipping client.")
        return

    url = f"ws://127.0.0.1:{PORT}/ws"
    async with websockets.connect(url) as ws:
        sub = {"type": "subscribe", "topics": ["/imu/data"], "filters": filters}
        await ws.send(json.dumps(sub))
        while asyncio.get_event_loop().time() < stop_at:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=0.3)
            except asyncio.TimeoutError:
                continue
            payload = json.loads(raw)
            if payload.get("type") == "event":
                events.append((label, payload))
            else:
                counts[label] += 1


async def run() -> None:
    header("22 — v0.5.0: bridge events + server-side filters")
    bag_path = ensure_sample_bag()
    print(f"  Source bag: {bag_path}\n")

    if port_open(PORT):
        print(f"  [SKIP] Port {PORT} in use; pick a free port.\n")
        return

    section("Start bridge")
    cmd = [
        sys.executable, "-m", "resurrector.cli.main", "bridge", "playback",
        str(bag_path), "--port", str(PORT), "--speed", "5.0",
        "--no-browser", "--loop",
    ]
    print(f"  $ {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
    print(f"  [OK] Bridge listening on {PORT}")

    # Trigger playback so messages flow
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}/api/playback/play", method="POST",
    )
    with urllib.request.urlopen(req, timeout=2) as r:
        print(f"  POST /api/playback/play → {r.status}")

    try:
        section("Connect 2 WS clients side-by-side: 1 unfiltered, 1 filtered")
        counts: Counter = Counter()
        events: list = []
        loop_now = asyncio.get_event_loop().time()
        stop_at = loop_now + 3.0
        # Filter expression — only keep IMU messages with |x acceleration| > 0.05
        filtered_expr = "pl.col('linear_acceleration.x').abs() > 0.05"
        await asyncio.gather(
            consumer("unfiltered", {}, counts, events, stop_at),
            consumer("filtered", {"/imu/data": filtered_expr}, counts, events, stop_at),
            broadcast_event_after(0.4),
        )
        print(f"  unfiltered client: {counts['unfiltered']} messages")
        print(f"  filtered client:   {counts['filtered']} messages "
              f"(expression: {filtered_expr})")
        if counts["unfiltered"]:
            ratio = counts["filtered"] / counts["unfiltered"]
            print(f"  filter passes ~{ratio * 100:.1f}% of IMU messages")

        section("Event marker delivery")
        print(f"  events received across both clients: {len(events)}")
        for label, ev in events[:4]:
            print(f"    [{label}] kind={ev['kind']!r}  text={ev['text']!r}  "
                  f"ts_ns={ev['timestamp_ns']}")

        print(
            "\n  ✓ Filtered client sees only the messages that match the\n"
            "    Polars predicate. Bad expressions raise ValueError at\n"
            "    subscribe time so the bridge can NACK with a structured\n"
            "    error (test_bridge_filter.py covers the full sandbox).\n"
            "  ✓ Events fan out to ALL connected clients regardless of their\n"
            "    topic subscription — they're protocol-level annotations.\n"
        )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)


async def broadcast_event_after(delay_sec: float) -> None:
    """POST one annotation event after a short delay."""
    await asyncio.sleep(delay_sec)
    payload = {
        "topic": "/imu/data",
        "timestamp_ns": 1_700_000_000_500_000_000,
        "text": "spike here",
        "kind": "alert",
    }
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}/api/events",
        data=body, headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=2) as r:
        print(f"  POST /api/events → {r.status} "
              f"(subscribers reached: {json.loads(r.read())['subscribers']})")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
