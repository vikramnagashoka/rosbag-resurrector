"""Legacy-format auto-convert helper.

When a user opens a .bag or .db3 file we don't have a native parser for,
shell out to the official converter (`mcap convert` for ROS 1 bags,
`ros2 bag convert` for ROS 2 SQLite) and open the resulting MCAP.

Keeps one user-visible format (MCAP) without requiring us to maintain
two additional parsers.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger("resurrector.ingest.convert")


class ConversionError(RuntimeError):
    """Raised when a legacy bag file cannot be converted to MCAP."""


def needs_conversion(path: Path) -> bool:
    """True if this path is a legacy format we can convert but not natively read."""
    return path.suffix.lower() in {".bag", ".db3"}


def _tool_for(suffix: str) -> tuple[str, list[str]]:
    """Return (tool_name, argv_prefix) for converting this suffix."""
    if suffix == ".bag":
        return "mcap", ["mcap", "convert"]
    if suffix == ".db3":
        return "ros2", ["ros2", "bag", "convert", "-i"]
    raise ValueError(f"No converter registered for {suffix}")


def convert_to_mcap(path: Path, output: Path | None = None) -> Path:
    """Convert a .bag or .db3 file to MCAP and return the new path.

    Args:
        path: Source file (.bag or .db3).
        output: Destination .mcap path. Defaults to same dir, same stem + .mcap.

    Raises:
        ConversionError: If the converter is missing or fails.
        FileNotFoundError: If ``path`` doesn't exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"Source bag not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".mcap":
        return path

    output = output or path.with_suffix(".mcap")
    tool, argv = _tool_for(suffix)

    if shutil.which(tool) is None:
        raise ConversionError(
            f"Cannot convert {path.name}: '{tool}' CLI not found on PATH. "
            f"Install it to enable auto-convert for {suffix} files.\n"
            f"  .bag  → install the mcap CLI (https://mcap.dev/guides/cli)\n"
            f"  .db3  → install ROS 2 (provides `ros2 bag convert`)"
        )

    if suffix == ".bag":
        # mcap convert <input> <output>
        cmd = argv + [str(path), str(output)]
    else:
        # ros2 bag convert -i <input> -o <output>
        cmd = argv + [str(path), "-o", str(output)]

    logger.info("Converting %s → %s via %s", path, output, tool)
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=600,
        )
    except subprocess.TimeoutExpired:
        raise ConversionError(f"Conversion timed out after 10 minutes: {path}")

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()[:500]
        raise ConversionError(
            f"{tool} failed ({result.returncode}) converting {path.name}:\n{stderr}"
        )

    if not output.exists():
        raise ConversionError(
            f"{tool} reported success but no MCAP was produced at {output}"
        )

    return output
