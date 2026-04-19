"""`resurrector doctor` — one-command environment readiness check.

Verifies which features are available with the current install and flags
what's missing. Designed to be the first thing a new user runs.
"""

from __future__ import annotations

import importlib
import os
import platform
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.table import Table


@dataclass
class CheckResult:
    name: str
    status: str  # "pass" | "warn" | "fail"
    detail: str
    fix_hint: str = ""


def _style(status: str) -> str:
    return {
        "pass": "[green]OK[/green]",
        "warn": "[yellow]WARN[/yellow]",
        "fail": "[red]FAIL[/red]",
    }.get(status, status)


def _check_python() -> CheckResult:
    v = sys.version_info
    version = f"{v.major}.{v.minor}.{v.micro}"
    if v < (3, 10):
        return CheckResult(
            "Python", "fail", f"{version} — need 3.10+",
            "Upgrade Python to 3.10 or newer",
        )
    return CheckResult("Python", "pass", version)


def _check_module(name: str, feature: str, fix: str) -> CheckResult:
    try:
        importlib.import_module(name)
        return CheckResult(feature, "pass", f"{name} available")
    except ImportError:
        return CheckResult(feature, "warn", f"{name} not installed", fix)


def _check_index_path() -> CheckResult:
    home = Path.home()
    index_dir = home / ".resurrector"
    if not index_dir.exists():
        return CheckResult(
            "Index location", "warn", f"{index_dir} does not exist yet",
            "Will be created on first scan — no action needed",
        )
    try:
        db = index_dir / "index.db"
        if db.exists():
            size_mb = db.stat().st_size / (1024 * 1024)
            return CheckResult(
                "Index location", "pass", f"{db} ({size_mb:.1f} MB)",
            )
        return CheckResult(
            "Index location", "pass", f"{index_dir} exists (no index yet)",
        )
    except OSError as e:
        return CheckResult(
            "Index location", "fail", f"cannot access: {e}",
            "Check permissions on ~/.resurrector/",
        )


def _check_allowed_roots() -> CheckResult:
    raw = os.environ.get("RESURRECTOR_ALLOWED_ROOTS", "")
    if not raw:
        return CheckResult(
            "Dashboard allowed roots", "pass",
            f"default ({Path.home()})",
            "Set RESURRECTOR_ALLOWED_ROOTS to broaden dashboard scan scope",
        )
    roots = [r for r in raw.split(os.pathsep) if r]
    return CheckResult(
        "Dashboard allowed roots", "pass",
        f"{len(roots)} root(s): " + ", ".join(roots[:2]) + ("…" if len(roots) > 2 else ""),
    )


def _check_converter(name: str, feature: str) -> CheckResult:
    path = shutil.which(name)
    if path:
        return CheckResult(feature, "pass", f"{name} at {path}")
    return CheckResult(
        feature, "warn", f"{name} not on PATH",
        f"Install {name} to enable auto-convert for legacy bags",
    )


def run_all_checks() -> list[CheckResult]:
    """Run every check and return results."""
    return [
        _check_python(),
        CheckResult("OS", "pass", f"{platform.system()} {platform.release()}"),
        _check_module("mcap", "MCAP parser", "pip install mcap (should be bundled)"),
        _check_module("duckdb", "DuckDB index", "pip install duckdb (should be bundled)"),
        _check_module("polars", "Polars", "pip install polars (should be bundled)"),
        _check_module("fastapi", "Dashboard backend", "pip install fastapi uvicorn"),
        _check_index_path(),
        _check_allowed_roots(),
        _check_module(
            "PIL", "Image/frame parsing",
            "pip install rosbag-resurrector[vision-lite]",
        ),
        _check_module(
            "cv2", "Video export",
            "pip install rosbag-resurrector[vision-lite]",
        ),
        _check_module(
            "sentence_transformers", "CLIP semantic search (local)",
            "pip install rosbag-resurrector[vision]",
        ),
        _check_module(
            "openai", "CLIP semantic search (OpenAI)",
            "pip install rosbag-resurrector[vision-openai]",
        ),
        _check_module(
            "rclpy", "Live ROS 2 bridge",
            "pip install rosbag-resurrector[bridge-live] (requires ROS 2 install)",
        ),
        _check_module(
            "watchdog", "Watch mode (auto-index new bags)",
            "pip install rosbag-resurrector[watch]",
        ),
        _check_module(
            "zarr", "Zarr export",
            "pip install rosbag-resurrector[all-exports]",
        ),
        _check_converter("mcap", "mcap CLI (.bag -> .mcap conversion)"),
        _check_converter("ros2", "ros2 CLI (.db3 -> .mcap conversion)"),
    ]


def render(results: list[CheckResult]) -> tuple[int, int, int]:
    """Print a table and return (pass_count, warn_count, fail_count)."""
    console = Console()
    table = Table(title="resurrector doctor", show_header=True, header_style="bold")
    table.add_column("Check", style="cyan", no_wrap=True)
    table.add_column("Status", width=6)
    table.add_column("Detail", style="white")
    table.add_column("Fix hint", style="dim")

    passed = warned = failed = 0
    for r in results:
        table.add_row(r.name, _style(r.status), r.detail, r.fix_hint)
        if r.status == "pass":
            passed += 1
        elif r.status == "warn":
            warned += 1
        else:
            failed += 1

    console.print(table)
    console.print()
    summary = f"[green]{passed} passed[/green]"
    if warned:
        summary += f"  [yellow]{warned} warnings[/yellow]"
    if failed:
        summary += f"  [red]{failed} failed[/red]"
    console.print(summary)

    if failed:
        console.print(
            "\n[red]One or more core checks failed. "
            "Core features may not work until you fix them.[/red]"
        )
    elif warned:
        console.print(
            "\n[yellow]Everything essential works. "
            "Warnings indicate optional features you can enable later.[/yellow]"
        )
    else:
        console.print("\n[green]All checks passed. You're ready to go.[/green]")

    return passed, warned, failed
