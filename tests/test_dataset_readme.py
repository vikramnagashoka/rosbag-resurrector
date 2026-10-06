"""Tests for the auto-generated dataset README (``dataset_readme.py``).

LeRobot exports ignore ``sync_config`` (frames always sit on a causal
uniform fps grid) and treat ``downsample_hz`` as the fps (default 30). The
README used to describe them with the tabular-export lines anyway, so a
LeRobot dataset claimed a sync method/tolerance it never applied, said
nothing about its frame rate when ``downsample_hz`` was unset, and its
quick start was a generic "load your lerobot files" comment.
"""

from __future__ import annotations

import ast
import json
import re
import tempfile
from pathlib import Path

import pytest

from resurrector.core.dataset_readme import generate_dataset_readme

_SYNC = {"method": "nearest", "tolerance_ms": 50.0, "anchor": "/imu/data"}


@pytest.fixture
def out_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


def _config(fmt: str, *, sync=_SYNC, downsample_hz=None) -> dict:
    return {
        "bag_refs": [
            {"path": "/data/a.mcap", "topics": ["/imu/data"],
             "start_time": None, "end_time": None},
            {"path": "/data/b.mcap", "topics": None,
             "start_time": None, "end_time": None},
        ],
        "topics": ["/imu/data", "/joint_states"],
        "sync_config": sync,
        "export_format": fmt,
        "downsample_hz": downsample_hz,
    }


def _readme(out_dir: Path, config: dict, name: str = "pick-place") -> str:
    path = generate_dataset_readme(
        output_path=out_dir, dataset_name=name, version="1.0",
        config=config, metadata={}, manifest={},
    )
    return path.read_text(encoding="utf-8")


def _quick_start(readme: str) -> str:
    m = re.search(r"## Quick Start\n\n```python\n(.*?)```", readme, re.S)
    assert m, "README has no python quick-start block"
    return m.group(1)


class TestLeRobotReadme:
    def test_no_sync_or_downsample_lines(self, out_dir):
        readme = _readme(out_dir, _config("lerobot", downsample_hz=15))
        assert "Sync method" not in readme
        assert "Sync tolerance" not in readme
        assert "Anchor topic" not in readme
        assert "Downsampled to" not in readme

    def test_frame_rate_defaults_to_30(self, out_dir):
        readme = _readme(out_dir, _config("lerobot", downsample_hz=None))
        assert "**Frame rate**: `30 fps`" in readme

    def test_frame_rate_from_downsample_hz(self, out_dir):
        readme = _readme(out_dir, _config("lerobot", downsample_hz=14.6))
        assert "**Frame rate**: `15 fps`" in readme

    def test_frame_rate_prefers_written_metadata(self, out_dir):
        """meta/info.json is what LeRobot actually wrote; trust it."""
        (out_dir / "meta").mkdir()
        (out_dir / "meta" / "info.json").write_text(json.dumps({
            "codebase_version": "v3.0", "fps": 24,
            "total_episodes": 2, "total_frames": 120,
        }))
        readme = _readme(out_dir, _config("lerobot", downsample_hz=None))
        assert "**Frame rate**: `24 fps`" in readme
        assert "**Episodes**: 2 (120 frames)" in readme

    def test_describes_causal_grid_and_one_episode_per_bag(self, out_dir):
        readme = _readme(out_dir, _config("lerobot"))
        assert "at or before" in readme
        assert "- Episode 0: `/data/a.mcap`" in readme
        assert "- Episode 1: `/data/b.mcap`" in readme
        # Per-bag topic filters are ignored for LeRobot; don't advertise them.
        assert "topics: /imu/data" not in readme

    def test_quick_start_loads_with_lerobot_dataset(self, out_dir):
        readme = _readme(out_dir, _config("lerobot"))
        code = _quick_start(readme)
        ast.parse(code)
        assert "from lerobot.datasets.lerobot_dataset import LeRobotDataset" in code
        assert f"root={str(out_dir)!r}" in code
        assert "load your lerobot files" not in readme.lower()

    def test_quick_start_is_valid_python_for_awkward_paths(self, out_dir):
        """Windows-style backslashes and quotes must not break the snippet."""
        weird = out_dir / 'it\'s "here"'
        weird.mkdir()
        code = _quick_start(_readme(weird, _config("lerobot"), name="my set"))
        ast.parse(code)


class TestTabularReadmeUnchanged:
    def test_parquet_keeps_sync_and_downsample_lines(self, out_dir):
        readme = _readme(out_dir, _config("parquet", downsample_hz=50))
        assert "**Sync method**: `nearest`" in readme
        assert "**Sync tolerance**: `50.0ms`" in readme
        assert "**Anchor topic**: `/imu/data`" in readme
        assert "**Downsampled to**: `50Hz`" in readme
        assert "Frame rate" not in readme
        assert "pl.read_parquet" in _quick_start(readme)
        assert "topics: /imu/data" in readme
