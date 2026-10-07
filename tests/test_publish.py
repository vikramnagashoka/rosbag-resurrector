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
- Card topics follow what export_version exported (per-bag filters win)
- Card read/write is UTF-8 regardless of the locale encoding
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

    def test_card_for_per_bag_topic_filter(self, tmp_path):
        """Would catch: a version with topics=None and a per-bag filter
        exporting only that bag's topics while the card said 'all topics'.
        export_version gives a BagRef's own topics precedence."""
        from resurrector.core.dataset import BagRef, DatasetManager

        bag = generate_bag(tmp_path / "run1.mcap", BagConfig(duration_sec=1.0))
        mgr = DatasetManager(tmp_path / "idx.db")
        try:
            mgr.create("imu-only")
            mgr.create_version("imu-only", "1.0",
                               [BagRef(path=str(bag), topics=["/imu/data"])],
                               export_format="parquet")
            out = mgr.export_version("imu-only", "1.0", str(tmp_path / "datasets"))
        finally:
            mgr.close()
        assert sorted(f.name for f in out.glob("*.parquet")) == ["imu_data.parquet"]
        card = build_dataset_card(out, "me/imu-only")
        assert "all topics" not in card
        assert "| Topics | 1 |" in card
        assert "- `/imu/data`" in card


def _card_for(config: dict) -> str:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d)
        (p / "dataset_config.json").write_text(json.dumps({"config": config, "metadata": {}}))
        return build_dataset_card(p, "me/x")


def _ref(path: str, topics: list[str] | None = None) -> dict:
    return {"path": path, "topics": topics, "start_time": None, "end_time": None}


class TestCardTopicsFollowExport:
    """Would catch: the card describing the version-level ``topics`` while
    export_version exports each bag's own filter first (``ref.topics or
    config.topics or every topic in the bag``)."""

    def test_bag_filter_overrides_version_topics(self):
        card = _card_for({"export_format": "parquet", "topics": ["/joint_states"],
                          "bag_refs": [_ref("/data/a.mcap", ["/imu/data"])]})
        assert "| Topics | 1 |" in card
        assert "- `/imu/data`" in card
        assert "/joint_states" not in card

    def test_bag_filters_are_unioned(self):
        card = _card_for({"export_format": "parquet", "topics": None, "bag_refs": [
            _ref("/data/a.mcap", ["/imu/data"]),
            _ref("/data/b.mcap", ["/imu/data", "/joint_states"]),
        ]})
        assert "| Topics | 2 |" in card
        assert "- `/imu/data`" in card and "- `/joint_states`" in card
        assert "all topics" not in card

    def test_mixed_filtered_and_unfiltered_bags(self):
        card = _card_for({"export_format": "parquet", "topics": None, "bag_refs": [
            _ref("/data/a.mcap", ["/imu/data"]),
            _ref("/data/b.mcap"),
        ]})
        assert "| Topics | 1 listed + all topics from 1 of 2 bags |" in card
        assert "- `/imu/data`" in card
        assert "- plus every topic in 1 of 2 source bags (no topic filter)" in card

    def test_empty_version_list_is_all_topics(self):
        """export_version's ``or`` chain treats ``topics=[]`` like None."""
        card = _card_for({"export_format": "parquet", "topics": [],
                          "bag_refs": [_ref("/data/a.mcap")]})
        assert "| Topics | all topics |" in card

    def test_lerobot_ignores_bag_filters(self):
        """export_lerobot applies the version's topics to every episode."""
        card = _card_for({"export_format": "lerobot", "topics": None,
                          "bag_refs": [_ref("/data/a.mcap", ["/imu/data"])]})
        assert "| Topics | all topics |" in card
        assert "/imu/data" not in card


# Outside cp1252 (Windows' usual locale encoding) and ASCII.
_NON_ASCII = "Pick the red cube → 拾取红色方块"

_ASCII_LOCALE_PRELUDE = f"""
import locale, sys
TEXT = {_NON_ASCII!r}
try:
    TEXT.encode(locale.getpreferredencoding(False))
except UnicodeEncodeError:
    pass
else:
    sys.exit(3)  # this locale can encode TEXT, so the run proves nothing
"""


def _run_in_ascii_locale(body: str, *args: str) -> None:
    """Run ``body`` in a Python whose locale encoding can't encode
    ``_NON_ASCII`` (C locale, UTF-8 mode off), like a Windows cp1252 box."""
    import os
    import subprocess

    import resurrector

    root = str(Path(resurrector.__file__).resolve().parent.parent)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("LC_", "LANG", "PYTHONUTF8", "PYTHONIOENCODING"))}
    env.update(LC_ALL="C", LANG="C", PYTHONUTF8="0",
               PYTHONPATH=os.pathsep.join(filter(None, [root, env.get("PYTHONPATH")])))
    proc = subprocess.run(
        [sys.executable, "-X", "utf8=0", "-c", _ASCII_LOCALE_PRELUDE + body, *args],
        env=env, capture_output=True, text=True, errors="replace", timeout=120,
    )
    if proc.returncode == 3:
        pytest.skip("the C locale here can encode the test text; nothing to prove")
    assert proc.returncode == 0, proc.stderr


class TestCardEncoding:
    """Would catch: README.md written (and dataset_config.json read) in the
    locale encoding. User-written descriptions reach the card, so a
    non-cp1252 character crashed publishing on Windows."""

    def test_card_with_non_ascii_description_writes_utf8(self, tmp_path):
        _run_in_ascii_locale(
            "from resurrector.core.publish import publish_dataset\n"
            "publish_dataset(sys.argv[1], 'me/x', dry_run=True, extra_description=TEXT)\n",
            str(tmp_path),
        )
        assert _NON_ASCII in (tmp_path / "README.md").read_text(encoding="utf-8")

    def test_utf8_dataset_config_description_reaches_card(self, tmp_path):
        """A hand-written dataset_config.json is UTF-8 (JSON's encoding),
        not escaped ASCII like DatasetManager writes."""
        (tmp_path / "dataset_config.json").write_text(json.dumps({
            "config": {"export_format": "parquet", "topics": ["/imu/data"],
                       "bag_refs": [_ref("/data/a.mcap")]},
            "metadata": {"description": _NON_ASCII},
        }, ensure_ascii=False), encoding="utf-8")
        _run_in_ascii_locale(
            "from resurrector.core.publish import publish_dataset\n"
            "publish_dataset(sys.argv[1], 'me/x', dry_run=True)\n",
            str(tmp_path),
        )
        card = (tmp_path / "README.md").read_text(encoding="utf-8")
        assert _NON_ASCII in card
        assert "| Format | `parquet` |" in card

    def test_dataset_readme_with_non_ascii_description(self, tmp_path):
        """Guard for the dataset README writer, which already forces UTF-8."""
        _run_in_ascii_locale(
            "from pathlib import Path\n"
            "from resurrector.core.dataset_readme import generate_dataset_readme\n"
            "generate_dataset_readme(Path(sys.argv[1]), 'x', '1.0',\n"
            "    {'export_format': 'parquet', 'bag_refs': []},\n"
            "    {'description': TEXT}, {})\n",
            str(tmp_path),
        )
        assert _NON_ASCII in (tmp_path / "README.md").read_text(encoding="utf-8")
