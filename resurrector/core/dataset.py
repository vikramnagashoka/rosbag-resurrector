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
import re
import shlex
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

        For ``export_format="lerobot"`` the version exports as ONE LeRobot
        dataset with one episode per bag: ``topics`` applies to every
        episode (per-bag topic filters and ``sync_config`` are ignored), and
        ``downsample_hz`` is the integer fps (default 30).

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
        A version with several bags (other than ``lerobot``, which writes
        one episode per bag) gets one subdirectory per bag, named by
        :func:`bag_folder_names`, each holding what a single-bag export
        writes. A bag with no messages in its selected topics and range is
        skipped with a warning.

        On failure, directories this call created and left empty are
        removed; an existing version directory is never removed.

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
            ImportError: The export format's optional dependency is
                missing (raised before anything is written).
            TopicNotFoundError: A bag lacks one of the version's topics.
            EmptyExportError: No bag has any messages to export.

        Example::

            out = mgr.export_version("pick-place", "1.0", "./datasets")
            print(out)            # ./datasets/pick-place/1.0
        """
        from resurrector.core.export import (
            first_missing_dir,
            require_export_dependencies,
            restore_output_dir,
        )

        ds = self._get_dataset_by_name(dataset_name)
        if ds is None:
            raise KeyError(f"Dataset '{dataset_name}' not found")

        ver = self._get_version(ds["id"], version)
        if ver is None:
            raise KeyError(f"Version '{version}' not found for dataset '{dataset_name}'")

        config = json.loads(ver["config_json"])
        metadata = json.loads(ver["metadata_json"]) if ver["metadata_json"] else {}

        require_export_dependencies(config.get("export_format", "parquet"))
        output_path = Path(output_dir) / dataset_name / version
        created = first_missing_dir(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        try:
            self._write_version_data(config, metadata, dataset_name, output_path)
        except BaseException:
            restore_output_dir(output_path, created)
            raise

        # Build manifest (hash the data files). In a multi-bag version only
        # the per-bag folders hold this export's data: a file beside them
        # is left from an earlier export (0.8.6 wrote every bag flat, so
        # such a file holds only the last bag) and stays out of the manifest.
        folders = None if config.get("export_format") == "lerobot" else bag_folder_names(config["bag_refs"])
        manifest: dict[str, str] = {}
        stale: list[str] = []
        for f in sorted(output_path.rglob("*")):
            if not f.is_file():
                continue
            rel = f.relative_to(output_path)
            if str(rel) in ("manifest.json", "README.md", "dataset_config.json"):
                continue
            if folders and rel.parts[0] not in folders:
                stale.append(str(rel))
                continue
            manifest[str(rel)] = _file_sha256(f)
        if stale:
            logger.warning(
                "%s holds %d file(s) outside its per-bag folders, left from an "
                "earlier export; they are not in manifest.json: %s",
                output_path, len(stale), ", ".join(stale[:5]) + (" ..." if len(stale) > 5 else ""),
            )

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

    def _write_version_data(
        self,
        config: dict[str, Any],
        metadata: dict[str, Any],
        dataset_name: str,
        output_path: Path,
    ) -> None:
        """Write a version's data files into ``output_path`` (see
        :meth:`export_version` for the layout)."""
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.exceptions import EmptyExportError
        from resurrector.core.export import Exporter, check_topics

        bag_refs = config["bag_refs"]
        is_lerobot = config.get("export_format") == "lerobot"
        bags = []
        for ref_dict in bag_refs:
            bf = BagFrame(ref_dict["path"])
            if ref_dict.get("start_time") and ref_dict.get("end_time"):
                bf = bf.time_slice(ref_dict["start_time"], ref_dict["end_time"])
            bags.append(bf)

        def ref_topics(ref_dict: dict[str, Any], bf) -> list[str]:
            if is_lerobot:  # LeRobot applies the version's topics to every bag
                return list(config.get("topics") or bf.topic_names)
            return list(ref_dict.get("topics") or config.get("topics") or bf.topic_names)

        # Every bag's topics are checked before any bag is written, so a
        # topic missing from the last bag doesn't leave the first ones on disk.
        for ref_dict, bf in zip(bag_refs, bags):
            check_topics(bf, ref_topics(ref_dict, bf))

        if is_lerobot:
            # One LeRobot dataset, one episode per bag ref; per-bag
            # directories would make N separate datasets.
            from resurrector.core.lerobot_export import DEFAULT_FPS, export_lerobot

            export_lerobot(
                bags,
                config.get("topics"),
                output_path,
                fps=int(round(config.get("downsample_hz") or DEFAULT_FPS)),
                task=metadata.get("description") or dataset_name,
            )
            output_path.mkdir(parents=True, exist_ok=True)
            return

        # One directory per bag: exported into one directory, every bag
        # wrote the same file names and only the last bag's data survived.
        folders = bag_folder_names(bag_refs)
        sync_cfg = config.get("sync_config")
        exporter = Exporter()
        empty: list[EmptyExportError] = []
        for i, (ref_dict, bf) in enumerate(zip(bag_refs, bags)):
            topics = ref_topics(ref_dict, bf)
            try:
                exporter.export(
                    bag_frame=bf,
                    topics=topics,
                    format=config.get("export_format", "parquet"),
                    output_dir=str(output_path / folders[i] if folders else output_path),
                    sync=sync_cfg is not None and len(topics) > 1,
                    sync_method=sync_cfg["method"] if sync_cfg else "nearest",
                    downsample_hz=config.get("downsample_hz"),
                )
            except EmptyExportError as e:
                if len(bag_refs) == 1:
                    raise
                logger.warning("%s Skipping that bag.", e)
                empty.append(e)
        if empty and len(empty) == len(bag_refs):
            topics = sorted({t for e in empty for t in e.topics})
            raise EmptyExportError(topics, f"any of the {len(bag_refs)} bags")

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


def bag_folder_names(bag_refs: list[dict[str, Any]]) -> list[str] | None:
    """The subdirectory each bag of a multi-bag version exports into:
    ``000_<file stem>``, ``001_<file stem>``, ... in ``bag_refs`` order
    (the index keeps two bags with the same file name apart). None for a
    single bag, which exports straight into the version directory."""
    if len(bag_refs) < 2:
        return None
    return [
        f"{i:03d}_" + (re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(ref["path"]).stem) or "bag")
        for i, ref in enumerate(bag_refs)
    ]


def _file_sha256(path: Path) -> str:
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


_FORMAT_LABELS = {"parquet": "Parquet", "hdf5": "HDF5", "zarr": "Zarr"}


def version_format_hint(name: str, formats: list[str]) -> str:
    """What to do when exporting a version of dataset ``name`` failed with
    an :class:`~resurrector.core.export.ExportError` that suggests
    ``formats`` (its ``suggested_formats``, e.g. ``["parquet"]`` or
    ``["hdf5", "zarr"]``).

    ``resurrector dataset export`` and the dashboard's dataset-version
    export both show it. Neither can change the format: it is fixed when
    the version is added, so the way to another format is a new version.
    """
    label = " or ".join(_FORMAT_LABELS.get(f, f) for f in formats)
    flags = " or ".join(f"-f {f}" for f in formats)
    if name.startswith("-"):
        # Quoting doesn't stop a leading dash reading as an option; "--"
        # does, so the positionals go after it.
        command = (f"resurrector dataset add-version -b <bag> ... -f {formats[0]} "
                   f"-- {shlex.quote(name)} <new-version>")
    else:
        command = (f"resurrector dataset add-version {shlex.quote(name)} "
                   f"<new-version> -b <bag> ... -f {formats[0]}")
    return (
        f"A dataset version's format is set when the version is added. To get "
        f"{label}, add a version with the same bags and settings and {flags}, "
        f"then export that version: {command}"
    )


def parquet_version_hint(name: str) -> str:
    """:func:`version_format_hint` for Parquet."""
    return version_format_hint(name, ["parquet"])
