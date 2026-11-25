#!/usr/bin/env python3
"""Compare two bench/run.py result folders (e.g. main vs a branch) as Markdown.

    python bench/compare.py ~/rosdata/bench-results/<before> ~/rosdata/bench-results/<after>

Prints one table per bag: median wall time and peak memory for each op in
both runs, the speedup, and how much of the bag each read decoded (for
``chunks``: the share of rows with any column besides the timestamp, so a
decoded topic whose fields don't flatten, like TF's transform lists, counts
as empty). Only runs with status ``ok`` count; anything else is listed
under the tables.
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from pathlib import Path


def load(run_dir: Path) -> tuple[dict, list[dict]]:
    meta = json.loads((run_dir / "run.json").read_text())
    with open(run_dir / "runs.csv", newline="") as f:
        return meta, list(csv.DictReader(f))


def medians(rows: list[dict]) -> dict[tuple[str, str], dict]:
    groups: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        if r["status"] == "ok":
            groups.setdefault((r["bag"], r["op"]), []).append(r)
    out = {}
    for key, rs in groups.items():
        def med(k: str):
            vals = [float(r[k]) for r in rs if r[k] != ""]
            return statistics.median(vals) if vals else None
        unparsed = [int(r["msgs_unparsed"]) for r in rs if r["msgs_unparsed"] != ""]
        msgs = int(rs[0]["msgs"]) if rs[0]["msgs"] else 0
        out[key] = {
            "n": len(rs), "wall": med("wall_s"), "peak": med("rss_peak_mb"),
            "decoded": (1 - unparsed[0] / msgs) if unparsed and msgs else None,
            "gb": float(rs[0]["bag_gb"]),
        }
    return out


def fmt_s(v):
    return "" if v is None else (f"{v:.2f} s" if v < 10 else f"{v:.1f} s")


def main(before_dir: str, after_dir: str) -> None:
    (mb, rb), (ma, ra) = load(Path(before_dir).expanduser()), load(Path(after_dir).expanduser())
    b, a = medians(rb), medians(ra)
    print(f"Before: `{mb['git']['commit'][:9]}` ({mb.get('label', '')}), "
          f"after: `{ma['git']['commit'][:9]}` ({ma.get('label', '')}). "
          f"{ma['machine']['model']} {ma['machine']['chip']}, "
          f"{ma['machine']['ram_bytes'] / 2**30:.0f} GB, warm cache, median of "
          f"{ma['args']['repeats']} runs.\n")
    ops = ["read", "mcap_read", "rosbags_read", "chunks", "health", "export_parquet"]
    bags = sorted({k[0] for k in a} | {k[0] for k in b}, key=lambda x: (a.get((x, "read")) or b.get((x, "read")) or {"gb": 0})["gb"])
    for bag in bags:
        gb = (a.get((bag, "read")) or b.get((bag, "read")) or {}).get("gb")
        print(f"**{bag}** ({gb:.1f} GB)\n" if gb else f"**{bag}**\n")
        print("| Op | Before | After | Speedup | Peak MB before | Peak MB after "
              "| Decoded before | Decoded after |")
        print("|---|---|---|---|---|---|---|---|")
        for op in ops:
            x, y = b.get((bag, op)), a.get((bag, op))
            if not x and not y:
                continue
            speed = f"{x['wall'] / y['wall']:.1f}x" if x and y and y["wall"] else ""
            dec = lambda r: "" if not r or r["decoded"] is None else f"{100 * r['decoded']:.0f}%"
            print(f"| {op} | {fmt_s(x and x['wall'])} | {fmt_s(y and y['wall'])} | {speed} | "
                  f"{'' if not x else round(x['peak'])} | {'' if not y else round(y['peak'])} | "
                  f"{dec(x)} | {dec(y)} |")
        print()
    print("Decoded: share of messages decoded (read) or of rows with columns (chunks).\n")
    bad = [r for r in rb + ra if r["status"] != "ok"]
    if bad:
        print("Runs that did not finish cleanly:\n")
        for r in bad:
            print(f"- {r['label']} {r['bag']} {r['op']} #{r['repeat']}: {r['status']} {r['error']}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
