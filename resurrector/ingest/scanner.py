"""Recursively scan directories for rosbag/MCAP files."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

BAG_EXTENSIONS = {".mcap", ".bag", ".db3"}


@dataclass
class ScannedFile:
    """Metadata about a discovered bag file.

    The ``fingerprint`` field is a fast change-detection hash computed
    from the first 1 MB plus the file size — NOT a real cryptographic
    digest of the file's full contents. Bag files are typically large
    and write-once, so the fingerprint is enough for cache invalidation.
    Users who need a real SHA256 (reproducibility, supply-chain audit)
    must opt in via ``resurrector scan --full-hash``, which populates
    ``sha256_full`` in the index.
    """
    path: Path
    extension: str
    size_bytes: int
    fingerprint: str
    mtime: float
    sha256_full: str | None = None

    @property
    def format(self) -> str:
        if self.extension == ".mcap":
            return "mcap"
        elif self.extension == ".bag":
            return "ros1bag"
        elif self.extension == ".db3":
            return "ros2db3"
        return "unknown"


def _fingerprint_fast(path: Path, chunk_size: int = 8192) -> str:
    """Compute a fast change-detection fingerprint.

    Hashes the first 1 MB of the file plus the total file size. This is
    NOT a cryptographic digest — it cannot detect changes past the first
    megabyte if the size is unchanged. Use ``_compute_sha256_full()``
    when you need a real hash (e.g. for reproducibility manifests).

    The fingerprint is named honestly so the index doesn't claim to have
    a property it doesn't.
    """
    h = hashlib.sha256()
    bytes_read = 0
    max_bytes = 1024 * 1024  # 1 MB
    with open(path, "rb") as f:
        while bytes_read < max_bytes:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
            bytes_read += len(chunk)
    h.update(str(path.stat().st_size).encode())
    return h.hexdigest()


def _compute_sha256_full(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute a real SHA256 over every byte of the file.

    Used when the user passes ``--full-hash`` to ``resurrector scan``,
    or when computing dataset manifests where reproducibility matters.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def scan_path(path: str | Path, full_hash: bool = False) -> list[ScannedFile]:
    """Scan a file or directory for bag files.

    Args:
        path: A file path or directory to scan recursively.
        full_hash: If True, also compute a real SHA256 over every byte
            of each file and populate ``ScannedFile.sha256_full``. Slow
            on large bags — only enable when the index needs to be
            cryptographically reproducible.

    Returns:
        List of ScannedFile objects for each discovered bag file.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")

    results: list[ScannedFile] = []

    if path.is_file():
        if path.suffix.lower() in BAG_EXTENSIONS:
            results.append(_scan_file(path, full_hash=full_hash))
    elif path.is_dir():
        for ext in BAG_EXTENSIONS:
            for file_path in path.rglob(f"*{ext}"):
                if file_path.is_file():
                    results.append(_scan_file(file_path, full_hash=full_hash))

    results.sort(key=lambda f: f.path)
    return results


def _scan_file(path: Path, full_hash: bool = False) -> ScannedFile:
    """Create a ScannedFile from a path."""
    stat = path.stat()
    return ScannedFile(
        path=path.resolve(),
        extension=path.suffix.lower(),
        size_bytes=stat.st_size,
        fingerprint=_fingerprint_fast(path),
        mtime=stat.st_mtime,
        sha256_full=_compute_sha256_full(path) if full_hash else None,
    )


def scan(path: str | Path, full_hash: bool = False) -> list[ScannedFile]:
    """Public API: scan a path for bag files. Alias for scan_path."""
    return scan_path(path, full_hash=full_hash)
