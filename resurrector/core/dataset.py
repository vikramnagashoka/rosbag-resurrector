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
    """Reference to a bag file (or slice of one) in a dataset."""
    path: str
    topics: list[str] | None = None  # None = all topics
    start_time: str | float | None = None  # time slice start
    end_time: str | float | None = None  # time slice end


@dataclass
class SyncConfig:
    """Synchronization configuration for a dataset."""
    method: str = "nearest"
    tolerance_ms: float = 50.0
    anchor: str | None = None


@dataclass
class DatasetMetadata:
    """Metadata for publishing (HuggingFace-compatible)."""
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
    """Manage datasets stored in DuckDB."""

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
        """Create a new dataset. Returns dataset ID."""
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
        """Create a new version of a dataset. Returns version ID."""
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
        """Export a dataset version to disk.

        Creates the output directory with exported data files and a manifest.
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
        """List all datasets."""
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
        """Get a dataset by name with all versions."""
        ds = self._get_dataset_by_name(name)
        if ds is None:
            return None
        ds["versions"] = self._list_versions(ds["id"])
        return ds

    def delete_dataset(self, name: str) -> bool:
        """Delete a dataset and all its versions."""
        ds = self._get_dataset_by_name(name)
        if ds is None:
            return False
        self.conn.execute("DELETE FROM dataset_versions WHERE dataset_id = ?", [ds["id"]])
        self.conn.execute("DELETE FROM datasets WHERE id = ?", [ds["id"]])
        return True

    def delete_version(self, name: str, version: str) -> bool:
        """Delete a specific version of a dataset."""
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
        self.conn.close()


def _file_sha256(path: Path) -> str:
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
