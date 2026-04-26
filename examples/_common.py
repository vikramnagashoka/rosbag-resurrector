"""Shared helpers for the exploration scripts.

Every numbered script imports from here so we have one source of
truth for the demo bag location and basic console formatting.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Make the repo root importable so the test-fixture bag generator
# (which lives under tests/) is reachable when scripts are launched
# from the examples/ directory or anywhere else inside the repo.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Force UTF-8 on stdout so unicode (sparkline glyphs, box-drawing) renders
# on Windows cp1252 terminals as well as Linux/macOS.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass


SAMPLE_BAG = Path.home() / ".resurrector" / "explore_sample.mcap"
OUTPUT_DIR = Path.cwd() / "_exploration_output"


def ensure_sample_bag(duration_sec: float = 5.0) -> Path:
    """Create the demo bag if it doesn't already exist; return its path.

    Uses the same fixture generator the test suite uses, so the data
    is realistic (IMU 200Hz, joint states 100Hz, camera 30Hz, lidar
    10Hz, compressed image 10Hz, plus a TF tree).
    """
    SAMPLE_BAG.parent.mkdir(parents=True, exist_ok=True)
    if SAMPLE_BAG.exists():
        return SAMPLE_BAG
    print(f"  Generating demo bag at {SAMPLE_BAG} (~{int(duration_sec)}s)...")
    from resurrector.demo.sample_bag import BagConfig, generate_bag
    generate_bag(SAMPLE_BAG, BagConfig(duration_sec=duration_sec))
    print(f"  [OK] Created {SAMPLE_BAG.stat().st_size // 1024} KB bag\n")
    return SAMPLE_BAG


def header(title: str) -> None:
    """Print a section header."""
    bar = "=" * (len(title) + 4)
    print(f"\n{bar}\n  {title}\n{bar}")


def section(title: str) -> None:
    """Print a sub-section header."""
    print(f"\n--- {title} ---")


def ensure_output_dir() -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR


def sparkline(values: list[float], width: int = 50) -> str:
    """Render a list of values as a unicode sparkline."""
    if not values:
        return "(empty)"
    chars = " ▁▂▃▄▅▆▇█"
    if len(values) > width:
        # Bucket the values down to ``width`` cells.
        bucket_size = len(values) / width
        bucketed = [
            sum(values[int(i * bucket_size):int((i + 1) * bucket_size)])
            / max(1, int((i + 1) * bucket_size) - int(i * bucket_size))
            for i in range(width)
        ]
    else:
        bucketed = list(values)
    lo = min(bucketed)
    hi = max(bucketed)
    rng = hi - lo
    out = []
    if rng == 0:
        # Flat distribution — render solid mid-bar so the user sees
        # presence rather than empty cells.
        full = chars[len(chars) // 2 + 2]
        return full * len(bucketed) if hi > 0 else " " * len(bucketed)
    for v in bucketed:
        idx = int(((v - lo) / rng) * (len(chars) - 1))
        out.append(chars[idx])
    return "".join(out)
