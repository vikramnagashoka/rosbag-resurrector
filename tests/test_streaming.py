"""Tests for the streaming export paths.

These verify that:
1. TopicView.iter_chunks respects chunk_size bounds.
2. TopicView.to_lazy_polars returns a LazyFrame.
3. All export formats stream (memory stays bounded) and produce correct output.
4. ExportError is raised when columns fail to serialize.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from resurrector.core.bag_frame import BagFrame
from resurrector.core.export import (
    Exporter,
    ExportError,
    ExportColumnFailure,
    _stream_numpy,
    _stream_hdf5,
)
from tests.fixtures.generate_test_bags import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_bag(tmp_dir):
    return generate_bag(tmp_dir / "sample.mcap", BagConfig(duration_sec=2.0))


class TestIterChunks:
    def test_yields_dataframes(self, sample_bag):
        bf = BagFrame(sample_bag)
        chunks = list(bf["/imu/data"].iter_chunks(chunk_size=100))
        assert len(chunks) > 0
        for c in chunks:
            assert isinstance(c, pl.DataFrame)

    def test_chunk_size_bound(self, sample_bag):
        bf = BagFrame(sample_bag)
        chunks = list(bf["/imu/data"].iter_chunks(chunk_size=50))
        # All chunks except possibly the last must be exactly chunk_size
        for c in chunks[:-1]:
            assert c.height == 50
        assert chunks[-1].height <= 50

    def test_total_rows_match(self, sample_bag):
        bf = BagFrame(sample_bag)
        view = bf["/imu/data"]
        total_from_chunks = sum(c.height for c in view.iter_chunks(chunk_size=30))
        total_from_eager = view.to_polars().height
        assert total_from_chunks == total_from_eager

    def test_empty_topic(self, tmp_dir):
        # Generate a bag with very short duration; confirm empty chunks don't break
        bag = generate_bag(tmp_dir / "tiny.mcap", BagConfig(duration_sec=0.01))
        bf = BagFrame(bag)
        for topic in bf.topic_names:
            chunks = list(bf[topic].iter_chunks(chunk_size=1000))
            # Any non-empty chunk has the expected column
            for c in chunks:
                assert "timestamp_ns" in c.columns


class TestLazyPolars:
    def test_returns_lazy_frame(self, sample_bag):
        bf = BagFrame(sample_bag)
        lf = bf["/imu/data"].to_lazy_polars()
        assert isinstance(lf, pl.LazyFrame)

    def test_collect_matches_eager(self, sample_bag):
        bf = BagFrame(sample_bag)
        view = bf["/imu/data"]
        eager = view.to_polars()
        # Use a fresh view because to_polars caches
        view2 = bf["/imu/data"]
        lazy = view2.to_lazy_polars().collect()
        assert eager.height == lazy.height
        assert set(eager.columns) == set(lazy.columns)


class TestStreamingExports:
    """Each export format should stream without loading all rows at once."""

    def test_parquet_streams(self, tmp_dir, sample_bag):
        bf = BagFrame(sample_bag)
        exporter = Exporter()
        exporter.export(
            bag_frame=bf, topics=["/imu/data"], format="parquet",
            output_dir=str(tmp_dir / "out"),
        )
        df = pl.read_parquet(tmp_dir / "out" / "imu_data.parquet")
        assert df.height == bf["/imu/data"].message_count

    def test_csv_streams(self, tmp_dir, sample_bag):
        bf = BagFrame(sample_bag)
        exporter = Exporter()
        exporter.export(
            bag_frame=bf, topics=["/imu/data"], format="csv",
            output_dir=str(tmp_dir / "out"),
        )
        csv_path = tmp_dir / "out" / "imu_data.csv"
        assert csv_path.exists()
        # Verify header + data rows
        df = pl.read_csv(csv_path)
        assert df.height == bf["/imu/data"].message_count
        assert "timestamp_ns" in df.columns

    def test_hdf5_streams_and_appends(self, tmp_dir, sample_bag):
        import h5py
        bf = BagFrame(sample_bag)
        exporter = Exporter()
        exporter.export(
            bag_frame=bf, topics=["/imu/data"], format="hdf5",
            output_dir=str(tmp_dir / "out"),
        )
        with h5py.File(tmp_dir / "out" / "imu_data.h5", "r") as f:
            assert "imu_data" in f
            ds = f["imu_data"]["timestamp_ns"]
            assert ds.shape[0] == bf["/imu/data"].message_count

    def test_numpy_streams(self, tmp_dir, sample_bag):
        bf = BagFrame(sample_bag)
        exporter = Exporter()
        exporter.export(
            bag_frame=bf, topics=["/imu/data"], format="numpy",
            output_dir=str(tmp_dir / "out"),
        )
        data = np.load(tmp_dir / "out" / "imu_data.npz")
        assert "timestamp_ns" in data
        assert len(data["timestamp_ns"]) == bf["/imu/data"].message_count


class TestExportError:
    def test_export_error_constructor(self):
        failures = [
            ExportColumnFailure(column="bad_col", error_type="TypeError", message="boom"),
        ]
        err = ExportError(failures, Path("/tmp/out.h5"))
        assert err.failures == failures
        assert "bad_col" in str(err)

    def test_hdf5_raises_on_failing_column(self, tmp_dir):
        """Feed a chunk with an unserializable column and assert ExportError."""
        # Polars allows Object dtype via list-of-list that h5py can't handle
        bad_chunk = pl.DataFrame({
            "timestamp_ns": [1, 2, 3],
            "bad_col": pl.Series([[1, 2], [3], [4, 5, 6]], dtype=pl.List(pl.Int64)),
            "good_col": [1.0, 2.0, 3.0],
        })
        with pytest.raises(ExportError) as exc_info:
            _stream_hdf5(iter([bad_chunk]), tmp_dir, "test")
        assert any(f.column == "bad_col" for f in exc_info.value.failures)


class TestMemoryBounds:
    """Smoke test: streaming an export of a realistic bag should not
    create intermediate giant objects."""

    def test_parquet_export_does_not_cache_full_topic(self, tmp_dir, sample_bag):
        bf = BagFrame(sample_bag)
        view = bf["/imu/data"]
        # Streaming export must not populate the TopicView's cache
        exporter = Exporter()
        exporter.export(
            bag_frame=bf, topics=["/imu/data"], format="parquet",
            output_dir=str(tmp_dir / "out"),
        )
        # The cache is only populated by to_polars(); streaming shouldn't touch it
        assert view._cached_df is None
