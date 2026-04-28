"""Recursively scan directories for rosbag/MCAP files.

Two discovery modes:

1. **File-extension scan.** Any file ending in ``.mcap``, ``.bag``, or
   ``.db3`` is a bag candidate.

2. **ROS 2 directory-format scan.** Real ROS 2 bags are commonly
   *directories* containing a ``metadata.yaml`` plus one or more
   ``.db3`` shards. The directory itself is the bag — recursing into it
   and treating each shard as a standalone bag would index a single
   recording N times. We detect a ROS 2 bag directory by the presence
   of ``metadata.yaml`` and treat the directory as the canonical path.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

BAG_EXTENSIONS = {".mcap", ".bag", ".db3"}

# Marker filename inside a ROS 2 directory bag. Per the rosbag2 spec,
# the metadata.yaml at the bag-directory root describes the storage
# plugin and the .db3 shards.
ROS2_BAG_MARKER = "metadata.yaml"


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


def is_ros2_bag_directory(path: Path) -> bool:
    """True if ``path`` is a directory containing a rosbag2 metadata.yaml."""
    return path.is_dir() and (path / ROS2_BAG_MARKER).is_file()


def scan_path(path: str | Path, full_hash: bool = False) -> list[ScannedFile]:
    """Scan a file or directory for bag files.

    Args:
        path: A file path or directory to scan recursively.
        full_hash: If True, also compute a real SHA256 over every byte
            of each file and populate ``ScannedFile.sha256_full``. Slow
            on large bags — only enable when the index needs to be
            cryptographically reproducible.

    Returns:
        List of ScannedFile objects for each discovered bag file. ROS 2
        directory-format bags are returned as a single entry pointing
        to the directory itself (not the .db3 shards inside).
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")

    results: list[ScannedFile] = []

    if path.is_file():
        if path.suffix.lower() in BAG_EXTENSIONS:
            results.append(_scan_file(path, full_hash=full_hash))
    elif is_ros2_bag_directory(path):
        # The argument itself is a ROS 2 bag directory.
        results.append(_scan_ros2_directory(path, full_hash=full_hash))
    elif path.is_dir():
        # Directory containing zero or more bags. Walk it, but treat
        # any ROS 2 bag directory as a single unit — don't recurse
        # into one looking for its own .db3 shards.
        seen_ros2_roots: set[Path] = set()
        for entry in sorted(path.rglob("*")):
            if entry.is_dir() and is_ros2_bag_directory(entry):
                resolved = entry.resolve()
                if resolved in seen_ros2_roots:
                    continue
                seen_ros2_roots.add(resolved)
                results.append(_scan_ros2_directory(entry, full_hash=full_hash))
                continue
            if not entry.is_file():
                continue
            # Skip any file that lives inside a ROS 2 bag directory we
            # already indexed.
            if any(root in entry.resolve().parents for root in seen_ros2_roots):
                continue
            if entry.suffix.lower() in BAG_EXTENSIONS:
                results.append(_scan_file(entry, full_hash=full_hash))

    results.sort(key=lambda f: f.path)
    return results


def _scan_file(path: Path, full_hash: bool = False) -> ScannedFile:
    """Create a ScannedFile for a single bag file (.mcap, .bag, or .db3)."""
    stat = path.stat()
    return ScannedFile(
        path=path.resolve(),
        extension=path.suffix.lower(),
        size_bytes=stat.st_size,
        fingerprint=_fingerprint_fast(path),
        mtime=stat.st_mtime,
        sha256_full=_compute_sha256_full(path) if full_hash else None,
    )


def _scan_ros2_directory(path: Path, full_hash: bool = False) -> ScannedFile:
    """Create a ScannedFile for a ROS 2 directory bag.

    Aggregates size across the metadata.yaml + every .db3 shard.
    Fingerprint and (optional) full hash are computed over the
    metadata.yaml only — bag contents live in the .db3 shards but
    metadata.yaml changes whenever the bag is re-recorded, which is
    the change-detection signal we need.
    """
    metadata = path / ROS2_BAG_MARKER
    total_size = metadata.stat().st_size
    latest_mtime = metadata.stat().st_mtime
    for db3 in path.rglob("*.db3"):
        st = db3.stat()
        total_size += st.st_size
        if st.st_mtime > latest_mtime:
            latest_mtime = st.st_mtime
    return ScannedFile(
        path=path.resolve(),
        extension=".db3",  # canonical ROS 2 bag marker for downstream code
        size_bytes=total_size,
        fingerprint=_fingerprint_fast(metadata),
        mtime=latest_mtime,
        sha256_full=_compute_sha256_full(metadata) if full_hash else None,
    )


def scan(path: str | Path, full_hash: bool = False) -> list[ScannedFile]:
    """Discover bag files at a path. Returns a :class:`ScannedFile` per bag.

    Public API; an alias for :func:`scan_path`. Walks ``path`` recursively
    if it's a directory, identifies every supported bag (``.mcap``,
    ``.bag``, ROS 2 directory format), and builds a :class:`ScannedFile`
    record for each one. Does NOT touch the index database — pair with
    :class:`BagIndex.upsert_bag` if you want indexing.

    A ROS 2 directory bag (a directory containing ``metadata.yaml`` plus
    one or more ``.db3`` shards) is returned as a single entry, not one
    entry per shard.

    Args:
        path: File or directory to scan.
        full_hash: When True, also compute a real SHA256 over every byte
            and populate ``ScannedFile.sha256_full``. Slow for large bags
            — only set this when the index needs cryptographic
            reproducibility (matches ``resurrector scan --full-hash`` from
            the CLI). Default uses a fast first-1MB-plus-size fingerprint
            that's sufficient for change detection.

    Returns:
        List of :class:`ScannedFile` records, sorted by path. Empty list
        if no bags were found.

    Raises:
        FileNotFoundError: If ``path`` does not exist.

    Example::

        from resurrector import scan

        for found in scan("~/recordings"):
            print(f"{found.path}  ({found.size_bytes:,} bytes)")
    """
    return scan_path(path, full_hash=full_hash)
