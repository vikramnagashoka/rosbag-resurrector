"""Dataset — a named, versioned collection of bag slices for reproducible ML training.

A Dataset is the bridge between raw bag files and ML training pipelines.
It captures:
- Which bags (by path or ID)
- Which topics
- Time slices
- Sync configuration
- Export format and settings
- Metadata for publishing (HuggingFace dataset card, citation, license)
- A manifest of exact file hashes for reproducibility
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger("resurrector.core.dataset")


@dataclass
class BagRef:
    """Reference to a bag file or a time-slice of one, used inside a dataset version.

    Attributes:
        path: Filesystem path to the source bag.
        topics: Topics to include from this bag, or ``None`` for all topics.
        start_time: Time-slice start, e.g. ``"10s"`` or ``5.0``. ``None`` = bag start.
        end_time: Time-slice end, same format.

    Example::

        BagRef(path="session_001.mcap")
        BagRef(path="long_run.mcap", topics=["/imu/data"], start_time="10s", end_time="60s")
    """
    path: str
    topics: list[str] | None = None  # None = all topics
    start_time: str | float | None = None  # time slice start
    end_time: str | float | None = None  # time slice end


@dataclass
class SyncConfig:
    """Sync configuration for a dataset version. Mirrors :meth:`BagFrame.sync`.

    Attributes:
        method: ``"nearest"`` / ``"interpolate"`` / ``"sample_and_hold"``.
        tolerance_ms: Maximum time delta for a match, in milliseconds.
        anchor: Anchor topic name. ``None`` picks the highest-frequency topic.

    Example::

        SyncConfig(method="nearest", tolerance_ms=50.0, anchor="/joint_states")
    """
    method: str = "nearest"
    tolerance_ms: float = 50.0
    anchor: str | None = None


@dataclass
class DatasetMetadata:
    """Optional dataset-card fields. Surfaced in the auto-generated README.

    Designed to map cleanly onto a HuggingFace Hub dataset card if you
    later publish there.

    Attributes:
        description: Free-text description of the dataset.
        license: SPDX-style license id (default ``"MIT"``).
        citation: BibTeX or plain-text citation block.
        tags: Free-form tags for filtering / search.
        robot_type: e.g. ``"digit"``, ``"spot"``, ``"ur5"``.
        environment: e.g. ``"warehouse"``, ``"home"``, ``"lab"``.
        task: e.g. ``"pick_and_place"``, ``"navigation"``.

    Example::

        DatasetMetadata(
            description="Pick-and-place across April",
            license="CC-BY-4.0",
            tags=["manipulation", "real-world"],
            robot_type="digit",
            task="pick_and_place",
        )
    """
    description: str = ""
    license: str = "MIT"
    citation: str = ""
    tags: list[str] = field(default_factory=list)
    robot_type: str = ""
    environment: str = ""
    task: str = ""


@dataclass
class DatasetVersion:
    """A specific version of a dataset."""
    version: str
    created_at: str
    bag_refs: list[BagRef]
    topics: list[str]
    sync_config: SyncConfig | None
    export_format: str
    downsample_hz: float | None
    metadata: DatasetMetadata
    manifest: dict[str, str] = field(default_factory=dict)  # filename -> sha256


class DatasetManager:
    """Create, list, and export reproducible bag-derived datasets, stored in DuckDB.

    A dataset is a named container; each version pins a set of bags +
    sync/export config. On export, every output file is hashed into a
    ``manifest.json`` so the dataset is byte-for-byte reproducible.

    Args:
        db_path: Custom index DB. ``None`` uses ``~/.resurrector/index.db``.

    Example::

        from resurrector import DatasetManager, BagRef, SyncConfig
        mgr = DatasetManager()
        mgr.create("pick-place", "Pick-and-place runs April 2026")
        mgr.create_version(
            dataset_name="pick-place",
            version="1.0",
            bag_refs=[BagRef(path="session_001.mcap")],
            topics=["/imu/data", "/joint_states"],
            sync_config=SyncConfig(method="nearest", tolerance_ms=50),
            export_format="parquet",
        )
        mgr.export_version("pick-place", "1.0", "./out")
        mgr.close()
    """

    def __init__(self, db_path: str | Path | None = None):
        from resurrector.ingest.indexer import DEFAULT_INDEX_PATH
        self.db_path = Path(db_path) if db_path else DEFAULT_INDEX_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()

    def _init_schema(self):
        """Create dataset tables if they don't exist."""
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS dataset_id_seq START 1
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id INTEGER PRIMARY KEY,
                name VARCHAR UNIQUE NOT NULL,
                description VARCHAR DEFAULT '',
                created_at TIMESTAMP DEFAULT current_timestamp,
                updated_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS dataset_version_id_seq START 1
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dataset_versions (
                id INTEGER PRIMARY KEY,
                dataset_id INTEGER NOT NULL,
                version VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT current_timestamp,
                config_json VARCHAR NOT NULL,
                manifest_json VARCHAR DEFAULT '{}',
                metadata_json VARCHAR DEFAULT '{}',
                export_format VARCHAR DEFAULT 'parquet',
                UNIQUE(dataset_id, version)
            )
        """)

    def create(self, name: str, description: str = "") -> int:
        """Create a new (empty) dataset.

        Args:
            name: Unique dataset name.
            description: Free-text description shown in ``list_datasets``
                and the auto-generated README on export.

        Returns:
            The new dataset's integer id.

        Example::

            mgr.create("pick-place", "Pick-and-place runs April 2026")
        """
        did = self.conn.execute("SELECT nextval('dataset_id_seq')").fetchone()[0]
        self.conn.execute(
            "INSERT INTO datasets (id, name, description) VALUES (?, ?, ?)",
            [did, name, description],
        )
        logger.info("Created dataset '%s' (id=%d)", name, did)
        return did

    def create_version(
        self,
        dataset_name: str,
        version: str,
        bag_refs: list[BagRef],
        topics: list[str] | None = None,
        sync_config: SyncConfig | None = None,
        export_format: str = "parquet",
        downsample_hz: float | None = None,
        metadata: DatasetMetadata | None = None,
    ) -> int:
        """Pin a set of bags + sync / export settings to a named version.

        The dataset must already exist (call :meth:`create` first). The
        version captures everything needed to re-materialize the same
        data later: bag paths, topic filter, time slices, sync method,
        export format, downsampling. No data is written to disk yet —
        call :meth:`export_version` for that.

        Args:
            dataset_name: The dataset's name (must exist).
            version: Version label. Free-form (``"1.0"``, ``"2026-04-28"``).
            bag_refs: List of :class:`BagRef` — bags to include, with optional
                per-bag topic filter and time slice.
            topics: Default topic filter applied if a ``BagRef`` doesn't
                specify its own. ``None`` means all topics.
            sync_config: Optional :class:`SyncConfig` to time-align the
                included topics on export.
            export_format: Format used at materialization time. Supports
                anything :class:`Exporter` does — ``parquet``, ``hdf5``,
                ``lerobot``, ``rlds``, etc.
            downsample_hz: Resample rate applied at export time.
            metadata: Optional :class:`DatasetMetadata` published with
                the dataset (auto-README, citation, license, etc.).

        Returns:
            The new version's integer id.

        Raises:
            KeyError: If ``dataset_name`` does not exist.

        Example::

            mgr.create_version(
                dataset_name="pick-place",
                version="1.0",
                bag_refs=[
                    BagRef(path="session_001.mcap"),
                    BagRef(path="session_002.mcap", start_time="10s", end_time="60s"),
                ],
                topics=["/imu/data", "/joint_states"],
                sync_config=SyncConfig(method="nearest", tolerance_ms=50),
                export_format="parquet",
                downsample_hz=50,
            )
        """
        ds = self._get_dataset_by_name(dataset_name)
        if ds is None:
            raise KeyError(f"Dataset '{dataset_name}' not found. Create it first.")

        config = {
            "bag_refs": [
                {
                    "path": br.path,
                    "topics": br.topics,
                    "start_time": br.start_time,
                    "end_time": br.end_time,
                }
                for br in bag_refs
            ],
            "topics": topics,
            "sync_config": {
                "method": sync_config.method,
                "tolerance_ms": sync_config.tolerance_ms,
                "anchor": sync_config.anchor,
            } if sync_config else None,
            "export_format": export_format,
            "downsample_hz": downsample_hz,
        }

        meta_dict = {}
        if metadata:
            meta_dict = {
                "description": metadata.description,
                "license": metadata.license,
                "citation": metadata.citation,
                "tags": metadata.tags,
                "robot_type": metadata.robot_type,
                "environment": metadata.environment,
                "task": metadata.task,
            }

        vid = self.conn.execute("SELECT nextval('dataset_version_id_seq')").fetchone()[0]
        self.conn.execute("""
            INSERT INTO dataset_versions (id, dataset_id, version, config_json, metadata_json, export_format)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [vid, ds["id"], version, json.dumps(config), json.dumps(meta_dict), export_format])

        self.conn.execute(
            "UPDATE datasets SET updated_at = current_timestamp WHERE id = ?",
            [ds["id"]],
        )

        logger.info("Created version '%s' for dataset '%s'", version, dataset_name)
        return vid

    def export_version(
        self,
        dataset_name: str,
        version: str,
        output_dir: str = "./datasets",
    ) -> Path:
        """Materialize a dataset version to disk with manifest, README, and config.

        Reads each bag listed in the version (applying per-bag time slice
        if any), runs the recorded sync / downsample / format settings,
        and writes the result to ``<output_dir>/<dataset_name>/<version>/``.

        Side effects:
          - Writes data files in the requested format
          - Writes ``manifest.json`` (SHA256 of every output file) for
            reproducibility
          - Writes ``dataset_config.json`` (the exact config used)
          - Writes ``README.md`` (auto-generated, HuggingFace-card-friendly)

        Args:
            dataset_name: Existing dataset's name.
            version: Existing version label.
            output_dir: Parent directory for the export.

        Returns:
            ``Path`` to the version's output directory.

        Raises:
            KeyError: If the dataset or version doesn't exist.

        Example::

            out = mgr.export_version("pick-place", "1.0", "./datasets")
            print(out)            # ./datasets/pick-place/1.0
        """
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.export import Exporter

        ds = self._get_dataset_by_name(dataset_name)
        if ds is None:
            raise KeyError(f"Dataset '{dataset_name}' not found")

        ver = self._get_version(ds["id"], version)
        if ver is None:
            raise KeyError(f"Version '{version}' not found for dataset '{dataset_name}'")

        config = json.loads(ver["config_json"])
        metadata = json.loads(ver["metadata_json"]) if ver["metadata_json"] else {}

        output_path = Path(output_dir) / dataset_name / version
        output_path.mkdir(parents=True, exist_ok=True)

        exporter = Exporter()
        manifest: dict[str, str] = {}

        for ref_dict in config["bag_refs"]:
            bf = BagFrame(ref_dict["path"])
            if ref_dict.get("start_time") and ref_dict.get("end_time"):
                bf = bf.time_slice(ref_dict["start_time"], ref_dict["end_time"])

            topics = ref_dict.get("topics") or config.get("topics") or bf.topic_names
            sync_cfg = config.get("sync_config")
            do_sync = sync_cfg is not None and len(topics) > 1

            exporter.export(
                bag_frame=bf,
                topics=topics,
                format=config.get("export_format", "parquet"),
                output_dir=str(output_path),
                sync=do_sync,
                sync_method=sync_cfg["method"] if sync_cfg else "nearest",
                downsample_hz=config.get("downsample_hz"),
            )

        # Build manifest (hash all output files)
        for f in output_path.rglob("*"):
            if f.is_file() and f.name != "manifest.json" and f.name != "README.md":
                manifest[str(f.relative_to(output_path))] = _file_sha256(f)

        # Save manifest
        manifest_path = output_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))

        # Update manifest in DB
        self.conn.execute(
            "UPDATE dataset_versions SET manifest_json = ? WHERE id = ?",
            [json.dumps(manifest), ver["id"]],
        )

        # Save dataset config for reproducibility
        config_path = output_path / "dataset_config.json"
        config_path.write_text(json.dumps({
            "name": dataset_name,
            "version": version,
            "config": config,
            "metadata": metadata,
            "created_at": ver["created_at"],
        }, indent=2, default=str))

        # Generate README
        from resurrector.core.dataset_readme import generate_dataset_readme
        generate_dataset_readme(
            output_path=output_path,
            dataset_name=dataset_name,
            version=version,
            config=config,
            metadata=metadata,
            manifest=manifest,
        )

        logger.info("Exported dataset '%s' v%s to %s", dataset_name, version, output_path)
        return output_path

    def list_datasets(self) -> list[dict[str, Any]]:
        """Return every dataset in the index, with versions pre-populated.

        Returns:
            List of dicts with keys ``id``, ``name``, ``description``,
            ``created_at``, ``updated_at``, and ``versions`` (list of
            per-version dicts). Sorted by most-recently-updated first.

        Example::

            for ds in mgr.list_datasets():
                print(f"{ds['name']}: {len(ds['versions'])} version(s)")
        """
        rows = self.conn.execute(
            "SELECT * FROM datasets ORDER BY updated_at DESC"
        ).fetchall()
        cols = [desc[0] for desc in self.conn.description]
        results = []
        for row in rows:
            ds = dict(zip(cols, row))
            ds["versions"] = self._list_versions(ds["id"])
            results.append(ds)
        return results

    def get_dataset(self, name: str) -> dict[str, Any] | None:
        """Look up one dataset by name and return its versions.

        Returns:
            The dataset dict (with ``versions`` list) or ``None`` if not found.
        """
        ds = self._get_dataset_by_name(name)
        if ds is None:
            return None
        ds["versions"] = self._list_versions(ds["id"])
        return ds

    def delete_dataset(self, name: str) -> bool:
        """Remove a dataset and every version under it from the index.

        Does NOT touch any already-exported files on disk. Idempotent:
        deleting a non-existent dataset returns False rather than raising.

        Returns:
            True if the dataset was removed, False if it didn't exist.
        """
        ds = self._get_dataset_by_name(name)
        if ds is None:
            return False
        self.conn.execute("DELETE FROM dataset_versions WHERE dataset_id = ?", [ds["id"]])
        self.conn.execute("DELETE FROM datasets WHERE id = ?", [ds["id"]])
        return True

    def delete_version(self, name: str, version: str) -> bool:
        """Remove one version from a dataset (the dataset itself stays).

        Does NOT touch already-exported files on disk.

        Returns:
            True if the version was removed, False if dataset/version not found.
        """
        ds = self._get_dataset_by_name(name)
        if ds is None:
            return False
        v = self._get_version(ds["id"], version)
        if v is None:
            return False
        self.conn.execute(
            "DELETE FROM dataset_versions WHERE dataset_id = ? AND version = ?",
            [ds["id"], version],
        )
        return True

    def _get_dataset_by_name(self, name: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM datasets WHERE name = ?", [name]
        ).fetchone()
        if row is None:
            return None
        cols = [desc[0] for desc in self.conn.description]
        return dict(zip(cols, row))

    def _get_version(self, dataset_id: int, version: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM dataset_versions WHERE dataset_id = ? AND version = ?",
            [dataset_id, version],
        ).fetchone()
        if row is None:
            return None
        cols = [desc[0] for desc in self.conn.description]
        return dict(zip(cols, row))

    def _list_versions(self, dataset_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT version, created_at, export_format FROM dataset_versions WHERE dataset_id = ? ORDER BY created_at DESC",
            [dataset_id],
        ).fetchall()
        cols = [desc[0] for desc in self.conn.description]
        return [dict(zip(cols, row)) for row in rows]

    def close(self):
        """Close the underlying DuckDB connection.

        Safe to call multiple times. Recommended at the end of a script.
        """
        self.conn.close()


def _file_sha256(path: Path) -> str:
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
