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
    """True if this path is a legacy format we can convert but not natively read.

    Accepts either single files (``.bag``, ``.db3``) or directories
    that look like ROS 2 bag directories (containing ``metadata.yaml``).
    """
    if path.is_dir():
        from resurrector.ingest.scanner import is_ros2_bag_directory
        return is_ros2_bag_directory(path)
    return path.suffix.lower() in {".bag", ".db3"}


def _tool_for(path: Path) -> tuple[str, list[str], str]:
    """Return (tool_name, argv_prefix, kind) for converting this path.

    ``kind`` is "bag" (single ROS 1 .bag file) or "ros2" (either a
    standalone .db3 file or a ROS 2 bag directory).
    """
    if path.is_dir():
        # ROS 2 directory bag — directory IS the bag, pass it as-is.
        return "ros2", ["ros2", "bag", "convert", "-i"], "ros2"
    suffix = path.suffix.lower()
    if suffix == ".bag":
        return "mcap", ["mcap", "convert"], "bag"
    if suffix == ".db3":
        return "ros2", ["ros2", "bag", "convert", "-i"], "ros2"
    raise ValueError(f"No converter registered for {path}")


def convert_to_mcap(path: Path, output: Path | None = None) -> Path:
    """Convert a .bag, .db3 file, or ROS 2 bag directory to MCAP.

    Args:
        path: Source file or ROS 2 bag directory.
        output: Destination .mcap path. Defaults to same parent dir,
            same stem (or directory name) + .mcap.

    Raises:
        ConversionError: If the converter is missing or fails.
        FileNotFoundError: If ``path`` doesn't exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"Source bag not found: {path}")

    if path.is_file() and path.suffix.lower() == ".mcap":
        return path

    if output is None:
        if path.is_dir():
            # ROS 2 bag directory — output sibling named after the dir.
            output = path.parent / f"{path.name}.mcap"
        else:
            output = path.with_suffix(".mcap")

    tool, argv, kind = _tool_for(path)

    if shutil.which(tool) is None:
        raise ConversionError(
            f"Cannot convert {path.name}: '{tool}' CLI not found on PATH. "
            f"Install it to enable auto-convert.\n"
            f"  .bag                → install the mcap CLI "
            f"(https://mcap.dev/guides/cli)\n"
            f"  .db3 / ROS 2 bag dir → install ROS 2 "
            f"(provides `ros2 bag convert`)"
        )

    if kind == "bag":
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
