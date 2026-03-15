"""Tests for the export engine."""

import tempfile
from pathlib import Path

import numpy as np
import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.core.bag_frame import BagFrame


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def healthy_bag(tmp_dir):
    return generate_bag(tmp_dir / "healthy.mcap", BagConfig(duration_sec=2.0))


class TestExport:
    def test_export_parquet(self, tmp_dir, healthy_bag):
        bf = BagFrame(healthy_bag)
        output = bf.export(
            topics=["/imu/data"],
            format="parquet",
            output=str(tmp_dir / "export_parquet"),
        )
        assert (output / "imu_data.parquet").exists()

    def test_export_csv(self, tmp_dir, healthy_bag):
        bf = BagFrame(healthy_bag)
        output = bf.export(
            topics=["/imu/data"],
            format="csv",
            output=str(tmp_dir / "export_csv"),
        )
        assert (output / "imu_data.csv").exists()

    def test_export_numpy(self, tmp_dir, healthy_bag):
        bf = BagFrame(healthy_bag)
        output = bf.export(
            topics=["/imu/data"],
            format="numpy",
            output=str(tmp_dir / "export_numpy"),
        )
        assert (output / "imu_data.npz").exists()
        data = np.load(output / "imu_data.npz")
        assert "timestamp_ns" in data
        assert "linear_acceleration.x" in data

    def test_export_hdf5(self, tmp_dir, healthy_bag):
        bf = BagFrame(healthy_bag)
        output = bf.export(
            topics=["/imu/data"],
            format="hdf5",
            output=str(tmp_dir / "export_hdf5"),
        )
        assert (output / "imu_data.h5").exists()

    def test_export_synced(self, tmp_dir, healthy_bag):
        bf = BagFrame(healthy_bag)
        output = bf.export(
            topics=["/imu/data", "/joint_states"],
            format="parquet",
            output=str(tmp_dir / "export_synced"),
            sync=True,
        )
        assert (output / "synced.parquet").exists()

    def test_export_multiple_topics(self, tmp_dir, healthy_bag):
        bf = BagFrame(healthy_bag)
        output = bf.export(
            topics=["/imu/data", "/joint_states"],
            format="csv",
            output=str(tmp_dir / "export_multi"),
        )
        assert (output / "imu_data.csv").exists()
        assert (output / "joint_states.csv").exists()

    def test_export_with_downsample(self, tmp_dir, healthy_bag):
        bf = BagFrame(healthy_bag)
        output = bf.export(
            topics=["/imu/data"],
            format="parquet",
            output=str(tmp_dir / "export_ds"),
            downsample_hz=10,
        )
        assert (output / "imu_data.parquet").exists()
        # Downsampled should have fewer rows
        import polars as pl
        df = pl.read_parquet(output / "imu_data.parquet")
        full_df = bf["/imu/data"].to_polars()
        assert df.height < full_df.height

    def test_export_invalid_format(self, tmp_dir, healthy_bag):
        bf = BagFrame(healthy_bag)
        with pytest.raises(ValueError):
            bf.export(topics=["/imu/data"], format="invalid")
