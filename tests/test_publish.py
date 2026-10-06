"""Tests for HuggingFace dataset publishing — v0.7 Feature A.

All offline: the card builder is a pure function and publish_dataset's
dry_run path does everything except the network upload. We never require
huggingface_hub to be installed for these tests.

Covers:
- Card YAML frontmatter + body structure
- Quality section derived from a QC summary (grade buckets)
- Reading manifest/config from the dataset dir
- dry_run writes README.md, counts files, skips upload
- Missing-directory error
- ImportError surfaced when huggingface_hub absent + not dry_run
- The nested dataset_config.json shape DatasetManager actually writes
  (card + CLI QC both read it), checked against a real export
- Format-aware Loading snippet (LeRobot datasets load via LeRobotDataset)
- LeRobot datasets get the codebase-version tag LeRobotDataset needs
"""

from __future__ import annotations

import json
import sys
import tempfile
import types
from pathlib import Path

import pytest
from typer.testing import CliRunner

from resurrector.core.publish import (
    PublishResult,
    build_dataset_card,
    publish_dataset,
)
from resurrector.demo.sample_bag import BagConfig, generate_bag


@pytest.fixture
def dataset_dir():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d)
        # A minimal materialized LeRobot dataset. dataset_config.json has the
        # nested shape DatasetManager.export_version writes: the version
        # config under "config", the card fields under "metadata".
        (p / "dataset_config.json").write_text(json.dumps({
            "name": "pick-place",
            "version": "1.0",
            "config": {
                "export_format": "lerobot",
                "topics": ["/imu/data", "/camera/rgb"],
                "bag_refs": [
                    {"path": "/data/run1.mcap", "topics": None,
                     "start_time": None, "end_time": None},
                    {"path": "/data/run2.mcap", "topics": None,
                     "start_time": None, "end_time": None},
                ],
                "sync_config": None,
                "downsample_hz": None,
            },
            "metadata": {"description": "Pick the red cube."},
            "created_at": "2026-10-01 12:00:00",
        }))
        (p / "meta").mkdir()
        (p / "meta" / "info.json").write_text(json.dumps({
            "codebase_version": "v3.0", "fps": 30,
        }))
        (p / "manifest.json").write_text(json.dumps({
            "data/episode_0.parquet": "abc",
            "data/episode_1.parquet": "def",
            "dataset_config.json": "ghi",
        }))
        (p / "data").mkdir()
        (p / "data" / "episode_0.parquet").write_bytes(b"x")
        (p / "data" / "episode_1.parquet").write_bytes(b"y")
        yield p


class TestBuildDatasetCard:
    def test_has_yaml_frontmatter(self, dataset_dir):
        card = build_dataset_card(dataset_dir, "me/pick-place")
        assert card.startswith("---\n")
        # frontmatter closes before the body
        assert card.count("---") >= 2
        assert "license: apache-2.0" in card
        assert "task_categories:" in card

    def test_includes_repo_name_and_overview(self, dataset_dir):
        card = build_dataset_card(dataset_dir, "me/pick-place")
        assert "# pick-place" in card
        assert "lerobot" in card
        assert "## Overview" in card
        # 2 source bags, 2 topics in the config
        assert "| Source bags | 2 |" in card
        assert "| Topics | 2 |" in card

    def test_lists_topics(self, dataset_dir):
        card = build_dataset_card(dataset_dir, "me/pick-place")
        assert "`/imu/data`" in card
        assert "`/camera/rgb`" in card

    def test_lerobot_load_snippet_uses_lerobot_dataset(self, dataset_dir):
        """Would catch: the card telling LeRobot users to call
        ``datasets.load_dataset``, which doesn't understand a LeRobot v3
        layout (videos, episode metadata) and isn't how LeRobot loads it."""
        card = build_dataset_card(dataset_dir, "me/pick-place")
        assert "from lerobot.datasets.lerobot_dataset import LeRobotDataset" in card
        assert 'ds = LeRobotDataset("me/pick-place")' in card
        assert "load_dataset" not in card

    def test_bare_lerobot_export_dir_gets_lerobot_snippet(self):
        """`resurrector export --preset lerobot` writes no dataset_config.json;
        the LeRobot layout itself (meta/info.json) identifies the format."""
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p / "meta").mkdir()
            (p / "meta" / "info.json").write_text(json.dumps({
                "codebase_version": "v3.0", "fps": 30,
            }))
            card = build_dataset_card(p, "me/raw")
        assert "| Format | `lerobot` |" in card
        assert 'ds = LeRobotDataset("me/raw")' in card
        assert "load_dataset" not in card

    def test_parquet_load_snippet_still_uses_datasets(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p / "dataset_config.json").write_text(json.dumps({
                "config": {"export_format": "parquet", "topics": ["/imu/data"],
                           "bag_refs": [{"path": "/data/a.mcap"}]},
                "metadata": {},
            }))
            card = build_dataset_card(p, "me/tabular")
        assert "from datasets import load_dataset" in card
        assert 'load_dataset("me/tabular")' in card
        assert "LeRobotDataset" not in card

    def test_description_read_from_metadata(self, dataset_dir):
        card = build_dataset_card(dataset_dir, "me/pick-place")
        assert "Pick the red cube." in card

    def test_flat_config_still_read(self):
        """A hand-written flat dataset_config.json keeps working."""
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p / "dataset_config.json").write_text(json.dumps({
                "export_format": "hdf5",
                "topics": ["/a", "/b", "/c"],
                "bag_refs": ["/data/run1.mcap"],
                "description": "flat shape",
            }))
            card = build_dataset_card(p, "me/flat")
        assert "| Format | `hdf5` |" in card
        assert "| Source bags | 1 |" in card
        assert "| Topics | 3 |" in card
        assert "flat shape" in card

    def test_quality_section_clean_grade(self, dataset_dir):
        qc = {"summary": {"n_bags": 2, "n_errors": 0, "n_warnings": 0}}
        card = build_dataset_card(dataset_dir, "me/pick-place", qc_summary=qc)
        assert "## Data quality" in card
        assert "Grade: A" in card

    def test_quality_section_error_grade(self, dataset_dir):
        qc = {"summary": {"n_bags": 2, "n_errors": 3, "n_warnings": 1}}
        card = build_dataset_card(dataset_dir, "me/pick-place", qc_summary=qc)
        assert "Grade: D" in card
        assert "Errors: 3" in card

    def test_custom_license_and_tasks(self, dataset_dir):
        card = build_dataset_card(
            dataset_dir, "me/x", license="mit",
            task_categories=["robotics", "reinforcement-learning"],
        )
        assert "license: mit" in card
        assert "- reinforcement-learning" in card

    def test_tolerates_missing_manifest_and_config(self):
        with tempfile.TemporaryDirectory() as d:
            card = build_dataset_card(d, "me/empty")
            assert "# empty" in card
            assert "| Source bags | 0 |" in card


class TestPublishDataset:
    def test_dry_run_writes_card_and_counts_files(self, dataset_dir):
        result = publish_dataset(dataset_dir, "me/pick-place", dry_run=True)
        assert isinstance(result, PublishResult)
        assert result.dry_run is True
        assert result.repo_id == "me/pick-place"
        assert result.url == "https://huggingface.co/datasets/me/pick-place"
        # README.md was written
        assert (dataset_dir / "README.md").exists()
        assert "# pick-place" in (dataset_dir / "README.md").read_text()
        # Counted the files present (config, manifest, 2 parquet, README)
        assert result.n_files >= 4

    def test_dry_run_embeds_qc_grade(self, dataset_dir):
        qc = {"summary": {"n_bags": 2, "n_errors": 0, "n_warnings": 0}}
        publish_dataset(dataset_dir, "me/pick-place", qc_summary=qc, dry_run=True)
        assert "Grade: A" in (dataset_dir / "README.md").read_text()

    def test_missing_directory_raises(self):
        with pytest.raises(FileNotFoundError):
            publish_dataset("/no/such/dir", "me/x", dry_run=True)

    def test_import_error_when_hub_missing_and_not_dry_run(self, dataset_dir, monkeypatch):
        # Simulate huggingface_hub not being importable.
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "huggingface_hub":
                raise ImportError("no hub")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(ImportError, match="huggingface_hub"):
            publish_dataset(dataset_dir, "me/x", dry_run=False)


_LEROBOT_INFO = {
    "codebase_version": "v3.0",
    "fps": 30,
    "total_episodes": 2,
    "total_frames": 118,
    "features": {
        "observation.state": {"dtype": "float32", "shape": [10],
                              "names": [f"imu/data/f{i}" for i in range(10)]},
        "action": {"dtype": "float32", "shape": [7],
                   "names": [f"joint_states/position.{i}" for i in range(7)]},
        "observation.images.camera_rgb": {"dtype": "video", "shape": [48, 64, 3],
                                          "names": ["height", "width", "channels"]},
        # LeRobot's own per-frame index columns, present in every dataset.
        "timestamp": {"dtype": "float32", "shape": [1], "names": None},
        "frame_index": {"dtype": "int64", "shape": [1], "names": None},
        "episode_index": {"dtype": "int64", "shape": [1], "names": None},
        "index": {"dtype": "int64", "shape": [1], "names": None},
        "task_index": {"dtype": "int64", "shape": [1], "names": None},
    },
}


@pytest.fixture
def bare_lerobot_dir():
    """What `resurrector export --preset lerobot` leaves: LeRobot's own v3
    layout and nothing else (no dataset_config.json, no manifest.json)."""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d)
        for rel in ("data/chunk-000/file-000.parquet",
                    "meta/episodes/chunk-000/file-000.parquet",
                    "meta/tasks.parquet",
                    "videos/observation.images.camera_rgb/chunk-000/file-000.mp4"):
            (p / rel).parent.mkdir(parents=True, exist_ok=True)
            (p / rel).write_bytes(b"x")
        (p / "meta" / "info.json").write_text(json.dumps(_LEROBOT_INFO))
        (p / "meta" / "stats.json").write_text("{}")
        yield p


class TestLeRobotCardFromInfoJson:
    """Would catch: the card for a bare `resurrector export --preset lerobot`
    dir reading only dataset_config.json / manifest.json, which that export
    doesn't write, so its Overview said 'Source bags 0 | Topics 0 | Data
    files 0' for a dataset with episodes, frames and cameras in it."""

    def test_bare_dir_overview_from_info_json(self, bare_lerobot_dir):
        card = build_dataset_card(bare_lerobot_dir, "me/raw")
        assert "| Format | `lerobot` |" in card
        assert "| Episodes | 2 |" in card
        assert "| Frames | 118 |" in card
        assert "| Frame rate | 30 fps |" in card
        # data parquet, 2 meta parquets, 1 mp4 (json metadata isn't data)
        assert "| Data files | 4 |" in card
        assert "| Source bags | 0 |" not in card
        assert "| Topics | 0 |" not in card

    def test_bare_dir_lists_features(self, bare_lerobot_dir):
        card = build_dataset_card(bare_lerobot_dir, "me/raw")
        assert "## Features" in card
        assert "| `observation.state` | float32 | 10 |" in card
        assert "| `action` | float32 | 7 |" in card
        assert "| `observation.images.camera_rgb` | video | 48x64x3 |" in card
        for default in ("timestamp", "frame_index", "episode_index", "index", "task_index"):
            assert f"`{default}`" not in card

    def test_dataset_manager_lerobot_card_adds_info_totals(self, dataset_dir):
        (dataset_dir / "meta" / "info.json").write_text(json.dumps(_LEROBOT_INFO))
        card = build_dataset_card(dataset_dir, "me/pick-place")
        assert "| Source bags | 2 |" in card
        assert "| Topics | 2 |" in card
        assert "| Episodes | 2 |" in card
        assert "| Frames | 118 |" in card
        assert "| `observation.state` | float32 | 10 |" in card

    def test_non_lerobot_card_has_no_lerobot_rows(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p / "dataset_config.json").write_text(json.dumps({
                "config": {"export_format": "parquet", "topics": ["/imu/data"],
                           "bag_refs": [{"path": "/data/a.mcap"}]},
                "metadata": {},
            }))
            card = build_dataset_card(p, "me/tabular")
        assert "| Episodes |" not in card
        assert "## Features" not in card


class TestTopicsNone:
    """Would catch: a DatasetManager version created with ``topics=None``
    ("every topic in each bag") showing 'Topics 0' on its card."""

    def test_topics_none_is_all_topics(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p / "dataset_config.json").write_text(json.dumps({
                "config": {"export_format": "parquet", "topics": None,
                           "bag_refs": [{"path": "/data/a.mcap"}]},
                "metadata": {},
            }))
            card = build_dataset_card(p, "me/all")
        assert "| Topics | all topics |" in card
        assert "| Topics | 0 |" not in card

    def test_empty_dir_still_reports_zero(self):
        """No config at all is 'unknown', not 'all topics'."""
        with tempfile.TemporaryDirectory() as d:
            card = build_dataset_card(d, "me/empty")
        assert "| Topics | 0 |" in card


class _FakeHub:
    """Stand-in ``huggingface_hub`` module that records HfApi calls."""

    def __init__(self, existing_tags=()):
        self.calls: list[tuple[str, dict]] = []
        self.existing_tags = list(existing_tags)
        hub = self

        class HfApi:
            def __init__(self, token=None):
                pass

            def create_repo(self, **kw):
                hub.calls.append(("create_repo", kw))

            def upload_folder(self, **kw):
                hub.calls.append(("upload_folder", kw))

            def list_repo_refs(self, repo_id, **kw):
                hub.calls.append(("list_repo_refs", {"repo_id": repo_id, **kw}))
                return types.SimpleNamespace(
                    tags=[types.SimpleNamespace(name=t) for t in hub.existing_tags],
                )

            def delete_tag(self, repo_id, **kw):
                hub.calls.append(("delete_tag", {"repo_id": repo_id, **kw}))

            def create_tag(self, repo_id, **kw):
                hub.calls.append(("create_tag", {"repo_id": repo_id, **kw}))

        self.module = types.ModuleType("huggingface_hub")
        self.module.HfApi = HfApi

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


class TestLeRobotVersionTag:
    """``LeRobotDataset(repo_id)`` refuses a Hub repo that has no
    codebase-version tag ("Your dataset must be tagged with a codebase
    version"), so the card's snippet only works if publish tags the repo
    the way LeRobot's own push_to_hub does."""

    def test_lerobot_publish_tags_codebase_version(self, dataset_dir, monkeypatch):
        hub = _FakeHub()
        monkeypatch.setitem(sys.modules, "huggingface_hub", hub.module)
        publish_dataset(dataset_dir, "me/pick-place", dry_run=False)
        tags = [kw for name, kw in hub.calls if name == "create_tag"]
        assert tags == [{"repo_id": "me/pick-place", "tag": "v3.0", "repo_type": "dataset"}]
        assert hub.names().index("create_tag") > hub.names().index("upload_folder")

    def test_republish_moves_existing_tag(self, dataset_dir, monkeypatch):
        """A stale tag would pin LeRobot to the previous upload's commit."""
        hub = _FakeHub(existing_tags=["v3.0"])
        monkeypatch.setitem(sys.modules, "huggingface_hub", hub.module)
        publish_dataset(dataset_dir, "me/pick-place", dry_run=False)
        names = hub.names()
        assert "delete_tag" in names
        assert names.index("upload_folder") < names.index("delete_tag") < names.index("create_tag")

    def test_non_lerobot_publish_does_not_tag(self, monkeypatch):
        hub = _FakeHub()
        monkeypatch.setitem(sys.modules, "huggingface_hub", hub.module)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p / "dataset_config.json").write_text(json.dumps({
                "config": {"export_format": "parquet"}, "metadata": {},
            }))
            publish_dataset(p, "me/tabular", dry_run=False)
        assert "upload_folder" in hub.names()
        assert "create_tag" not in hub.names()

    def test_dry_run_never_touches_the_hub(self, dataset_dir, monkeypatch):
        hub = _FakeHub()
        monkeypatch.setitem(sys.modules, "huggingface_hub", hub.module)
        publish_dataset(dataset_dir, "me/pick-place", dry_run=True)
        assert hub.calls == []


@pytest.fixture
def real_dataset_export():
    """A dataset exported by DatasetManager — the producer whose
    dataset_config.json the card builder and `resurrector publish` read."""
    from resurrector.core.dataset import BagRef, DatasetManager, DatasetMetadata

    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        bag = generate_bag(tmp / "run1.mcap", BagConfig(duration_sec=2.0))
        mgr = DatasetManager(tmp / "idx.db")
        try:
            mgr.create("pick-place")
            mgr.create_version(
                "pick-place", "1.0", [BagRef(path=str(bag))],
                topics=["/imu/data", "/joint_states"],
                export_format="parquet",
                metadata=DatasetMetadata(description="Pick the red cube."),
            )
            out = mgr.export_version("pick-place", "1.0", str(tmp / "datasets"))
        finally:
            mgr.close()
        yield out


class TestRealDatasetExport:
    """Would catch: the card and CLI reading export_format / topics /
    bag_refs / description from the top level of dataset_config.json, when
    DatasetManager nests them under "config" and "metadata". Every
    published dataset showed Format `unknown`, 0 bags, 0 topics, no
    description, and `resurrector publish` always skipped QC."""

    def test_card_reflects_dataset_config(self, real_dataset_export):
        card = build_dataset_card(real_dataset_export, "me/pick-place")
        assert "| Format | `parquet` |" in card
        assert "| Source bags | 1 |" in card
        assert "| Topics | 2 |" in card
        assert "`/imu/data`" in card and "`/joint_states`" in card
        assert "Pick the red cube." in card

    def test_cli_publish_runs_qc_on_recorded_bags(self, real_dataset_export):
        from resurrector.cli.main import app

        result = CliRunner().invoke(app, [
            "publish", str(real_dataset_export),
            "--repo-id", "me/pick-place", "--dry-run",
        ], env={"COLUMNS": "200"})
        assert result.exit_code == 0, result.stdout
        assert "QC skipped" not in result.stdout
        assert "across 1 bag(s)" in result.stdout
        card = (real_dataset_export / "README.md").read_text()
        assert "## Data quality" in card
        assert "Bags checked: 1" in card

    def test_card_for_all_topics_version(self, tmp_path):
        """DatasetManager stores topics=None for a version without a topic
        filter; the exported card says so instead of 'Topics 0'."""
        from resurrector.core.dataset import BagRef, DatasetManager

        bag = generate_bag(tmp_path / "run1.mcap", BagConfig(duration_sec=1.0))
        mgr = DatasetManager(tmp_path / "idx.db")
        try:
            mgr.create("everything")
            mgr.create_version("everything", "1.0", [BagRef(path=str(bag))],
                               export_format="parquet")
            out = mgr.export_version("everything", "1.0", str(tmp_path / "datasets"))
        finally:
            mgr.close()
        card = build_dataset_card(out, "me/everything")
        assert "| Topics | all topics |" in card
        assert "| Source bags | 1 |" in card
