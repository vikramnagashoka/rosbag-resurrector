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
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from resurrector.core.publish import (
    PublishResult,
    build_dataset_card,
    publish_dataset,
)


@pytest.fixture
def dataset_dir():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d)
        # A minimal materialized dataset: config + manifest + a data file.
        (p / "dataset_config.json").write_text(json.dumps({
            "export_format": "lerobot",
            "topics": ["/imu/data", "/camera/rgb"],
            "bag_refs": ["/data/run1.mcap", "/data/run2.mcap"],
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

    def test_load_snippet_uses_repo_id(self, dataset_dir):
        card = build_dataset_card(dataset_dir, "me/pick-place")
        assert 'load_dataset("me/pick-place")' in card

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
