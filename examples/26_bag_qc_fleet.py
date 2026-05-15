"""Bag-side QC tool — single bag + cross-bag fleet (v0.6.0 — Bundle B.3).

Demonstrates ``resurrector.core.qc.run_qc()`` and the ``resurrector qc``
CLI. Distinct lane from ``lerobot-doctor`` — that QCs already-converted
LeRobot datasets; this catches problems BEFORE conversion, including
bag-collection issues (drift across recording sessions, missing
sensors on one robot, etc.) that a single-dataset check wouldn't surface.

Run:
    python examples/26_bag_qc_fleet.py

What you'll see:
  1. A small synth fleet — three healthy bags + one with a deliberately
     anomalous IMU rate + one missing the /tf topic
  2. ``run_qc()`` returns per-bag scores + cross-bag findings
  3. JSON output suitable for CI gating
  4. Equivalent ``resurrector qc`` CLI invocation
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from _common import ensure_output_dir, header, section

from resurrector.core.qc import run_qc
from resurrector.demo.sample_bag import generate_bag, BagConfig


def main() -> None:
    header("26 — v0.6.0: bag-side QC tool (single + fleet)")
    out = ensure_output_dir()

    section("Generate a synthetic bag fleet with deliberate drift")
    bags = []

    # 3 healthy bags
    for i in range(3):
        p = out / f"v06_qc_healthy_{i}.mcap"
        if p.exists():
            p.unlink()
        generate_bag(p, BagConfig(duration_sec=0.5, imu_hz=200.0))
        bags.append(p)
        print(f"    {p.name}: 0.5s @ 200Hz IMU (healthy)")

    # One bag with anomalous IMU rate (10 Hz vs cluster median 200 Hz)
    slow = out / "v06_qc_slow_imu.mcap"
    if slow.exists():
        slow.unlink()
    generate_bag(slow, BagConfig(duration_sec=0.5, imu_hz=10.0))
    bags.append(slow)
    print(f"    {slow.name}: 0.5s @ 10Hz IMU (anomalous!)")

    # One bag missing /tf
    no_tf = out / "v06_qc_no_tf.mcap"
    if no_tf.exists():
        no_tf.unlink()
    generate_bag(no_tf, BagConfig(duration_sec=0.5, include_tf=False))
    bags.append(no_tf)
    print(f"    {no_tf.name}: 0.5s, /tf missing (drift!)")

    section("Run QC across the fleet")
    report = run_qc(bags)
    print(f"  {report.n_bags} bags · {report.n_errors} errors · {report.n_warnings} warnings")

    section("Per-bag scores")
    for b in report.bags:
        print(f"    {Path(b.bag_path).name:<28} score={b.health_score:>3}  "
              f"duration={b.duration_sec:.1f}s  topics={b.n_topics}  "
              f"issues={len(b.issues)}")

    section("Cross-bag fleet findings")
    if not report.fleet_issues:
        print("  (no fleet-level issues)")
    for issue in report.fleet_issues:
        topic_str = f" [{issue.topic}]" if issue.topic else ""
        bag_str = f" {Path(issue.bag).name}" if issue.bag else ""
        print(f"  {issue.severity.upper():<8} {issue.code}{topic_str}{bag_str}")
        print(f"           → {issue.message}")

    section("JSON output for CI")
    json_path = out / "v06_qc_report.json"
    json_path.write_text(report.to_json())
    print(f"  Wrote {json_path.relative_to(out.parent)}")
    parsed = json.loads(json_path.read_text())
    print(f"  Schema: keys={list(parsed.keys())}")
    print(f"  Summary: {parsed['summary']}")

    section("Same thing via the CLI: 'resurrector qc'")
    cli_args = [
        "/tmp/v060-build/bin/resurrector", "qc",
        *[str(p) for p in bags],
    ]
    print(f"  $ {' '.join(cli_args[:3])} ...")
    result = subprocess.run(cli_args, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        print(f"  [SKIP] CLI not in venv path; rc={result.returncode}")
    else:
        # Print just the head of the output to keep this script's output tight
        for line in result.stdout.splitlines()[:6]:
            print(f"  {line}")
        print("  ... (CLI output truncated; run `resurrector qc <bag>` to see full table)")

    print(
        "\n  ✓ Per-bag: wraps health_report() + adds empty / very-short detection.\n"
        "  ✓ Fleet: schema drift, topic-set divergence, rate anomaly, coverage.\n"
        "  ✓ Add --json out.json + --fail-on-error in CI to gate before training.\n"
        "  ✓ Distinct lane from lerobot-doctor — upstream-side, not LeRobot-side.\n"
    )


if __name__ == "__main__":
    main()
