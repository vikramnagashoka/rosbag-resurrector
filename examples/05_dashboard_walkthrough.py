"""Boot the dashboard, open the right page, and tell the user what to click.

Demonstrates: end-to-end use of the v0.3.1 dashboard.

Run:
    python examples/05_dashboard_walkthrough.py

What you'll see: the dashboard server starts in a subprocess, the
sample bag gets indexed, your browser opens to the bag's Explorer page,
and the script prints a step-by-step list of v0.3.1 features to try in
the UI. Press Ctrl+C to stop the server when done.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import webbrowser

from _common import ensure_sample_bag, header

from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path


def main() -> None:
    header("05 — Dashboard walkthrough")
    bag_path = ensure_sample_bag()

    # Index the sample bag so it appears in the Library.
    print(f"  Indexing {bag_path.name}...")
    index = BagIndex()
    scanned = scan_path(bag_path)[0]
    parser = parse_bag(bag_path)
    metadata = parser.get_metadata()
    bag_id = index.upsert_bag(scanned, metadata)
    bf = BagFrame(bag_path)
    index.update_health_score(bag_id, bf.health_report().score)
    index.close()
    print(f"  ✓ Indexed as bag id {bag_id}\n")

    # Pre-build frame offsets so the ImageViewer is fast.
    print(f"  Pre-building image frame offsets...")
    from resurrector.ingest.frame_index import build_frame_offsets, image_topics
    img_topics = image_topics(bag_path)
    if img_topics:
        idx = BagIndex()
        build_frame_offsets(idx, bag_id, bag_path, topics=img_topics)
        idx.close()
        print(f"  ✓ Pre-built offsets for {img_topics}\n")

    # Boot the dashboard.
    print(f"  Starting dashboard subprocess (port 8080)...")
    env = {**os.environ}
    # Make sure the temp dirs (and home) are allowed roots so scan/etc work.
    import tempfile
    from pathlib import Path
    env["RESURRECTOR_ALLOWED_ROOTS"] = os.pathsep.join(
        [tempfile.gettempdir(), str(Path.home())]
    )
    # Capture stderr so we can quote it back to the user if the
    # subprocess dies during startup (e.g. SyntaxError on older Python
    # versions). Without piping, the traceback would still print to the
    # terminal but the script would oblivously claim "server up" later.
    proc = subprocess.Popen(
        [sys.executable, "-m", "resurrector.cli.main", "dashboard", "--port", "8080"],
        env=env,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    print(f"  [OK] Dashboard PID {proc.pid}\n")

    # Wait for the server to come up. We poll BOTH the subprocess
    # liveness (proc.poll() returning non-None means it crashed during
    # startup and we should surface stderr) and the port. Without the
    # liveness check the script used to lie "server up" when the subprocess
    # had actually exited with a SyntaxError.
    print(f"  Waiting for the server to listen...")
    import socket
    deadline = time.time() + 10
    server_up = False
    while time.time() < deadline:
        if proc.poll() is not None:
            # Subprocess exited during startup. Capture stderr and abort.
            try:
                _, stderr_bytes = proc.communicate(timeout=2)
                stderr = (stderr_bytes or b"").decode("utf-8", errors="replace")
            except Exception:
                stderr = "(could not read subprocess stderr)"
            print(f"\n  [FATAL] Dashboard subprocess exited (code {proc.returncode}).")
            print(f"  ──── subprocess stderr ────")
            for line in stderr.splitlines()[-30:]:
                print(f"  {line}")
            print(f"  ──────────────────────────\n")
            sys.exit(1)
        try:
            with socket.create_connection(("127.0.0.1", 8080), timeout=0.2):
                server_up = True
                break
        except OSError:
            time.sleep(0.2)

    if not server_up:
        print(f"\n  [FATAL] Server did not start listening within 10 seconds.")
        proc.terminate()
        sys.exit(1)

    explorer_url = f"http://localhost:8080/bag/{bag_id}"
    compare_url = f"http://localhost:8080/compare-runs"
    print(f"  [OK] Server up at http://localhost:8080\n")
    print(f"  Opening {explorer_url}...\n")
    webbrowser.open(explorer_url)

    print("  ─" * 35)
    print("  TRY THESE v0.3.1 FEATURES IN THE EXPLORER:")
    print("  ─" * 35)
    print(f"")
    print(f"   1. Density ribbon (above the chart)")
    print(f"      → look for the Plotly heatmap; click any cell to jump there")
    print(f"")
    print(f"   2. Bookmarks (right rail)")
    print(f"      → click anywhere on the main chart to add an annotation")
    print(f"      → the new bookmark appears in the right rail; click to jump")
    print(f"")
    print(f"   3. Transform editor (header button)")
    print(f"      → click 'Transform...'  → pick 'Derivative' → 'Add to plot'")
    print(f"      → the derived series appears as a dashed purple subplot")
    print(f"      → switch to Expression tab, try:")
    print(f'        pl.col("linear_acceleration.x").abs()')
    print(f"")
    print(f"   4. Trim & export (shift-drag selection)")
    print(f"      → hold SHIFT and drag horizontally on the chart")
    print(f"      → trim popover appears; pick MCAP, click Export")
    print(f"")
    print(f"   5. Open in Jupyter (header button)")
    print(f"      → writes a Parquet under ~/.resurrector/, copies a snippet")
    print(f"")
    print(f"   6. Compare runs page")
    print(f"      → {compare_url}")
    print(f"      → after running 04_cross_bag_overlay.py first, you'll see")
    print(f"        two bags to overlay")
    print(f"")
    print("  ─" * 35)
    print(f"\n  Press Ctrl+C in this terminal to stop the dashboard.\n")

    try:
        proc.wait()
    except KeyboardInterrupt:
        print(f"\n  Stopping dashboard...")
        proc.terminate()
        proc.wait(timeout=5)
        print(f"  ✓ Stopped.\n")


if __name__ == "__main__":
    main()
