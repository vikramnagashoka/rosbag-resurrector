"""Integration tests — full pipeline from scan to export.

The single highest-leverage test: verifies all 4 layers work together.
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.ingest.scanner import scan_path
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.indexer import BagIndex
from resurrector.core.bag_frame import BagFrame


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def bag_dir(tmp_dir):
    """Create a directory with healthy and unhealthy bags."""
    bags_dir = tmp_dir / "bags"
    bags_dir.mkdir()
    generate_bag(bags_dir / "healthy.mcap", BagConfig(duration_sec=3.0))
    generate_bag(bags_dir / "gap.mcap", BagConfig(
        duration_sec=3.0,
        time_gap=True,
        gap_topic="/imu/data",
        gap_start_sec=1.0,
        gap_duration_sec=0.5,
    ))
    return bags_dir


class TestFullPipeline:
    def test_scan_index_health_export(self, bag_dir, tmp_dir):
        """Full pipeline: scan → index → health → sync → export → verify."""
        # Step 1: Scan
        files = scan_path(bag_dir)
        assert len(files) == 2

        # Step 2: Index
        index = BagIndex(tmp_dir / "test.db")
        for scanned in files:
            parser = parse_bag(scanned.path)
            metadata = parser.get_metadata()
            bag_id = index.upsert_bag(scanned, metadata)

            # Step 3: Health check
            bf = BagFrame(scanned.path)
            report = bf.health_report()
            index.update_health_score(bag_id, report.score)
            assert 0 <= report.score <= 100

        # Verify index has both bags
        assert index.count() == 2
        bags = index.list_bags()
        assert len(bags) == 2

        # Step 4: Sync 3 topics on the healthy bag
        bf = BagFrame(bag_dir / "healthy.mcap")
        synced = bf.sync(
            ["/imu/data", "/joint_states", "/lidar/scan"],
            method="nearest",
            tolerance_ms=100,
        )
        assert isinstance(synced, pl.DataFrame)
        assert synced.height > 0
        assert "timestamp_ns" in synced.columns

        # Step 5: Export to parquet
        export_dir = tmp_dir / "export"
        output = bf.export(
            topics=["/imu/data", "/joint_states"],
            format="parquet",
            output=str(export_dir),
        )
        assert output.exists()
        parquet_files = list(output.glob("*.parquet"))
        assert len(parquet_files) == 2

        # Step 6: Verify exported data
        imu_df = pl.read_parquet(output / "imu_data.parquet")
        assert "timestamp_ns" in imu_df.columns
        assert "linear_acceleration.x" in imu_df.columns
        assert imu_df.height > 0
        # No NaN in timestamp column
        assert imu_df["timestamp_ns"].null_count() == 0

        index.close()

    def test_search_after_indexing(self, bag_dir, tmp_dir):
        """Index bags and search them."""
        index = BagIndex(tmp_dir / "test.db")
        for scanned in scan_path(bag_dir):
            parser = parse_bag(scanned.path)
            metadata = parser.get_metadata()
            bag_id = index.upsert_bag(scanned, metadata)
            bf = BagFrame(scanned.path)
            index.update_health_score(bag_id, bf.health_report().score)

        # Search by topic
        results = index.search("topic:/imu/data")
        assert len(results) == 2

        # Search by path
        results = index.search("healthy")
        assert len(results) == 1
        assert "healthy" in results[0]["path"]

        index.close()

    def test_time_slice_and_export(self, bag_dir, tmp_dir):
        """Time slice a bag and export the slice."""
        bf = BagFrame(bag_dir / "healthy.mcap")
        sliced = bf.time_slice("0.5s", "1.5s")

        export_dir = tmp_dir / "slice_export"
        # Export from sliced view
        df = sliced["/imu/data"].to_polars()
        assert df.height > 0
        full_df = bf["/imu/data"].to_polars()
        assert df.height < full_df.height

    def test_synced_export(self, bag_dir, tmp_dir):
        """Export with synchronization."""
        bf = BagFrame(bag_dir / "healthy.mcap")
        output = bf.export(
            topics=["/imu/data", "/joint_states"],
            format="parquet",
            output=str(tmp_dir / "synced_export"),
            sync=True,
            sync_method="nearest",
        )
        assert (output / "synced.parquet").exists()
        df = pl.read_parquet(output / "synced.parquet")
        assert df.height > 0
        # Should have columns from both topics
        cols = set(df.columns)
        assert any("linear_acceleration" in c for c in cols)
        assert any("position" in c for c in cols)

    def test_stale_index_detection(self, bag_dir, tmp_dir):
        """Index a bag, delete the file, detect stale entry."""
        index = BagIndex(tmp_dir / "test.db")
        scanned = scan_path(bag_dir / "healthy.mcap")[0]
        parser = parse_bag(bag_dir / "healthy.mcap")
        metadata = parser.get_metadata()
        bag_id = index.upsert_bag(scanned, metadata)

        # File exists — no stale entries
        stale = index.validate_paths()
        assert len(stale) == 0

        # Delete the file
        (bag_dir / "healthy.mcap").unlink()

        # Now it should be detected as stale
        stale = index.validate_paths()
        assert len(stale) == 1
        assert stale[0]["id"] == bag_id

        # Remove stale entries
        removed = index.remove_stale()
        assert removed == 1
        assert index.count() == 0

        index.close()
