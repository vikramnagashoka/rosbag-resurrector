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


def generate_bag_or_exit(path: Path, config=None) -> Path:
    """``generate_bag``, but a generator that can't run exits cleanly.

    On a partial install without Pillow the generator raises a one-line
    ImportError naming the fix; print that and exit 1 instead of dumping
    a traceback on someone running the examples as a smoke test.
    """
    from resurrector.demo.sample_bag import generate_bag

    try:
        return generate_bag(path, config)
    except ImportError as e:
        print(f"  [ERROR] Can't generate the sample bag: {e}", file=sys.stderr)
        raise SystemExit(1) from None


def ensure_bag(path: Path, duration_sec: float = 5.0) -> Path:
    """Reuse the synthetic bag at ``path`` unless it is stale; return its path.

    Stale means ``stale_sample_reason`` flags it: empty, cut off by an
    interrupted run, or holding the 1x1 placeholder frames that installs
    without Pillow used to write. Those are regenerated.
    """
    from resurrector.demo.sample_bag import BagConfig, stale_sample_reason

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        stale = stale_sample_reason(path)
        if stale is None:
            return path
        print(f"  Regenerating {path}: {stale}.")
    else:
        print(f"  Generating demo bag at {path} (~{int(duration_sec)}s)...")
    generate_bag_or_exit(path, BagConfig(duration_sec=duration_sec))
    print(f"  [OK] Created {path.stat().st_size // 1024} KB bag\n")
    return path


def ensure_sample_bag(duration_sec: float = 5.0) -> Path:
    """The shared demo bag at ``SAMPLE_BAG``, created or regenerated as needed.

    Uses the same generator the test suite uses, so the data is
    realistic (IMU 200Hz, joint states 100Hz, camera 30Hz, lidar 10Hz,
    compressed image 10Hz, plus a TF tree).
    """
    return ensure_bag(SAMPLE_BAG, duration_sec)


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
