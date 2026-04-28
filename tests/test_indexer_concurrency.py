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


class TestSearchEmbeddings:
    """SQL-level coverage for `search_embeddings` that doesn't need CLIP.

    The full end-to-end test in test_vision.py uses a real CLIP model and
    is gated behind importorskip("sentence_transformers") — CI doesn't
    install the [vision] extra to avoid the 2 GB model download, so that
    test is always skipped. These tests insert synthetic 512-d embeddings
    directly so the SQL plumbing (param order, type binding) is exercised
    on every CI run.
    """

    def _make_indexed(self, populated_index, n_frames=4):
        """Insert n synthetic embeddings against the first bag."""
        bag_id = populated_index.list_bags()[0]["id"]
        rows = [
            (i + 1, bag_id, "/camera/rgb", 1_000_000_000 * (i + 1), i, [float(i) / 100] * 512)
            for i in range(n_frames)
        ]
        with populated_index._lock:
            populated_index.conn.executemany(
                "INSERT INTO frame_embeddings (id, bag_id, topic, timestamp_ns, frame_index, embedding)"
                " VALUES (?, ?, ?, ?, ?, ?::DOUBLE[512])",
                rows,
            )
        return bag_id

    def test_search_runs_without_binder_error(self, populated_index):
        """Regression: param-order bug raised
        `Cannot compare values of type DOUBLE and type DOUBLE[]`
        because WHERE's embedding placeholder was getting min_similarity.
        """
        self._make_indexed(populated_index)
        results = populated_index.search_embeddings(
            query_embedding=[0.01] * 512,
            top_k=10,
            min_similarity=0.0,
        )
        # We don't assert on result content (synthetic embeddings, not real CLIP) —
        # just that the SQL planned, bound, and executed without raising.
        assert isinstance(results, list)

    def test_search_with_bag_id_filter(self, populated_index):
        """The optional bag_id condition adds a placeholder mid-query —
        params must still match textual order."""
        bag_id = self._make_indexed(populated_index)
        results = populated_index.search_embeddings(
            query_embedding=[0.01] * 512,
            top_k=10,
            bag_id=bag_id,
            min_similarity=0.0,
        )
        assert isinstance(results, list)
        # All returned rows should be from the requested bag
        for r in results:
            assert r["bag_id"] == bag_id

    def test_search_returns_similarity_column(self, populated_index):
        """The SELECT also computes similarity — that placeholder
        must get the embedding too, not the threshold."""
        self._make_indexed(populated_index)
        results = populated_index.search_embeddings(
            query_embedding=[0.05] * 512,
            top_k=2,
            min_similarity=0.0,
        )
        if results:
            assert "similarity" in results[0]
            assert isinstance(results[0]["similarity"], float)
            # Cosine similarity is bounded; a real DOUBLE not a list
            assert -1.0 <= results[0]["similarity"] <= 1.0
