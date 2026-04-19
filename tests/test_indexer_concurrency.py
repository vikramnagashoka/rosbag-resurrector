"""Tests for thread-safe access to BagIndex.

The bridge, dashboard, and CLI scanner all share a single BagIndex
instance in real deployments. These tests exercise concurrent reads
and writes to verify the internal lock serializes access correctly.
"""

from __future__ import annotations

import tempfile
import threading
from pathlib import Path

import pytest

from resurrector.ingest.indexer import BagIndex
from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.ingest.scanner import scan as scan_dir
from resurrector.ingest.parser import parse_bag


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def populated_index(tmp_dir):
    """Build a small index with a handful of bags for concurrent reading."""
    for i in range(3):
        generate_bag(tmp_dir / f"bag_{i}.mcap", BagConfig(duration_sec=0.5))

    index_path = tmp_dir / "idx.db"
    index = BagIndex(index_path)
    for scanned in scan_dir(tmp_dir):
        meta = parse_bag(scanned.path).get_metadata()
        index.upsert_bag(scanned, meta)
    yield index
    index.close()


class TestConcurrentReads:
    def test_parallel_list_bags(self, populated_index):
        """Many threads calling list_bags at once must all succeed."""
        errors: list[Exception] = []

        def worker():
            try:
                bags = populated_index.list_bags()
                assert len(bags) == 3
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Concurrent reads failed: {errors[:3]}"

    def test_parallel_count(self, populated_index):
        errors: list[Exception] = []

        def worker():
            try:
                for _ in range(10):
                    populated_index.count()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors


class TestConcurrentWrites:
    def test_parallel_tagging(self, populated_index):
        """Many threads adding tags concurrently must not corrupt the index."""
        bags = populated_index.list_bags()
        bag_id = bags[0]["id"]
        errors: list[Exception] = []

        def worker(tag_key: str):
            try:
                populated_index.add_tag(bag_id, tag_key, "value")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=worker, args=(f"key_{i}",))
            for i in range(15)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # Verify all tags landed
        bag = populated_index.get_bag(bag_id)
        tag_keys = {tag["key"] for tag in bag["tags"]}
        assert len(tag_keys) == 15


class TestMixedReadsAndWrites:
    def test_reads_during_writes(self, populated_index):
        """Concurrent reads and writes must both succeed without deadlock."""
        bags = populated_index.list_bags()
        bag_id = bags[0]["id"]
        errors: list[Exception] = []

        def reader():
            try:
                for _ in range(30):
                    populated_index.list_bags()
            except Exception as e:
                errors.append(e)

        def writer():
            try:
                for i in range(30):
                    populated_index.update_health_score(bag_id, i)
            except Exception as e:
                errors.append(e)

        threads = (
            [threading.Thread(target=reader) for _ in range(4)]
            + [threading.Thread(target=writer) for _ in range(4)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Mixed r/w failed: {errors[:3]}"
