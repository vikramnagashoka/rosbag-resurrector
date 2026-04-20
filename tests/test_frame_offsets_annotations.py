"""Tests for frame_offsets cache and annotations tables."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from resurrector.ingest.indexer import BagIndex


@pytest.fixture
def idx():
    with tempfile.TemporaryDirectory() as d:
        index = BagIndex(Path(d) / "test.db")
        yield index
        index.close()


class TestFrameOffsets:
    def test_starts_empty(self, idx):
        assert not idx.has_frame_offsets(1, "/cam")
        assert idx.count_frames(1, "/cam") == 0

    def test_insert_and_lookup(self, idx):
        idx.insert_frame_offsets(1, "/cam", [(0, 100), (1, 200), (2, 300)])
        assert idx.has_frame_offsets(1, "/cam")
        assert idx.count_frames(1, "/cam") == 3
        assert idx.get_frame_timestamp(1, "/cam", 1) == 200

    def test_missing_frame_returns_none(self, idx):
        idx.insert_frame_offsets(1, "/cam", [(0, 100)])
        assert idx.get_frame_timestamp(1, "/cam", 99) is None

    def test_idempotent_insert(self, idx):
        idx.insert_frame_offsets(1, "/cam", [(0, 100), (1, 200)])
        idx.insert_frame_offsets(1, "/cam", [(0, 100), (1, 200), (2, 300)])
        assert idx.count_frames(1, "/cam") == 3

    def test_clear_topic_scoped(self, idx):
        idx.insert_frame_offsets(1, "/cam_a", [(0, 100)])
        idx.insert_frame_offsets(1, "/cam_b", [(0, 200)])
        idx.clear_frame_offsets(1, "/cam_a")
        assert idx.count_frames(1, "/cam_a") == 0
        assert idx.count_frames(1, "/cam_b") == 1

    def test_clear_all_bag(self, idx):
        idx.insert_frame_offsets(1, "/cam_a", [(0, 100)])
        idx.insert_frame_offsets(1, "/cam_b", [(0, 200)])
        idx.clear_frame_offsets(1)
        assert idx.count_frames(1, "/cam_a") == 0
        assert idx.count_frames(1, "/cam_b") == 0

    def test_empty_insert_is_noop(self, idx):
        idx.insert_frame_offsets(1, "/cam", [])
        assert not idx.has_frame_offsets(1, "/cam")


class TestAnnotations:
    def test_crud_round_trip(self, idx):
        aid = idx.add_annotation(1, 123456789, "robot fell")
        anns = idx.list_annotations(1)
        assert len(anns) == 1
        assert anns[0]["text"] == "robot fell"
        assert anns[0]["timestamp_ns"] == 123456789
        assert anns[0]["id"] == aid

        assert idx.update_annotation(aid, "robot tipped over")
        assert idx.list_annotations(1)[0]["text"] == "robot tipped over"

        assert idx.delete_annotation(aid)
        assert idx.list_annotations(1) == []

    def test_update_nonexistent_returns_false(self, idx):
        assert not idx.update_annotation(9999, "ghost")

    def test_delete_nonexistent_returns_false(self, idx):
        assert not idx.delete_annotation(9999)

    def test_topic_filter_includes_global(self, idx):
        idx.add_annotation(1, 1000, "global note", topic=None)
        idx.add_annotation(1, 2000, "imu note", topic="/imu/data")
        idx.add_annotation(1, 3000, "joints note", topic="/joint_states")

        imu = idx.list_annotations(1, topic="/imu/data")
        assert len(imu) == 2  # global + imu
        texts = {a["text"] for a in imu}
        assert texts == {"global note", "imu note"}

    def test_multiple_bags_isolated(self, idx):
        idx.add_annotation(1, 1000, "bag 1")
        idx.add_annotation(2, 2000, "bag 2")
        assert len(idx.list_annotations(1)) == 1
        assert len(idx.list_annotations(2)) == 1
        assert idx.list_annotations(1)[0]["text"] == "bag 1"

    def test_ordered_by_timestamp(self, idx):
        idx.add_annotation(1, 3000, "third")
        idx.add_annotation(1, 1000, "first")
        idx.add_annotation(1, 2000, "second")
        texts = [a["text"] for a in idx.list_annotations(1)]
        assert texts == ["first", "second", "third"]
