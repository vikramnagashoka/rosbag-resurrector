"""Recursively scan directories for rosbag/MCAP files."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path

BAG_EXTENSIONS = {".mcap", ".bag", ".db3"}


@dataclass
class ScannedFile:
    """Metadata about a discovered bag file."""
    path: Path
    extension: str
    size_bytes: int
    sha256: str
    mtime: float

    @property
    def format(self) -> str:
        if self.extension == ".mcap":
            return "mcap"
        elif self.extension == ".bag":
            return "ros1bag"
        elif self.extension == ".db3":
            return "ros2db3"
        return "unknown"


def _compute_sha256(path: Path, chunk_size: int = 8192) -> str:
    """Compute SHA256 hash of a file. Uses first 1MB for speed on large files."""
    h = hashlib.sha256()
    bytes_read = 0
    max_bytes = 1024 * 1024  # 1MB for fast hashing
    with open(path, "rb") as f:
        while bytes_read < max_bytes:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
            bytes_read += len(chunk)
    # Include file size in hash to reduce collisions from partial reads
    h.update(str(path.stat().st_size).encode())
    return h.hexdigest()


def scan_path(path: str | Path) -> list[ScannedFile]:
    """Scan a file or directory for bag files.

    Args:
        path: A file path or directory to scan recursively.

    Returns:
        List of ScannedFile objects for each discovered bag file.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")

    results: list[ScannedFile] = []

    if path.is_file():
        if path.suffix.lower() in BAG_EXTENSIONS:
            results.append(_scan_file(path))
    elif path.is_dir():
        for ext in BAG_EXTENSIONS:
            for file_path in path.rglob(f"*{ext}"):
                if file_path.is_file():
                    results.append(_scan_file(file_path))

    # Sort by path for deterministic ordering
    results.sort(key=lambda f: f.path)
    return results


def _scan_file(path: Path) -> ScannedFile:
    """Create a ScannedFile from a path."""
    stat = path.stat()
    return ScannedFile(
        path=path.resolve(),
        extension=path.suffix.lower(),
        size_bytes=stat.st_size,
        sha256=_compute_sha256(path),
        mtime=stat.st_mtime,
    )


def scan(path: str | Path) -> list[ScannedFile]:
    """Public API: scan a path for bag files. Alias for scan_path."""
    return scan_path(path)
