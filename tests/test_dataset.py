"""Tests for the Dataset layer."""

import tempfile
from pathlib import Path

import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.core.dataset import (
    DatasetManager, BagRef, SyncConfig, DatasetMetadata,
)


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def healthy_bag(tmp_dir):
    return generate_bag(tmp_dir / "healthy.mcap", BagConfig(duration_sec=2.0))


@pytest.fixture
def mgr(tmp_dir):
    m = DatasetManager(tmp_dir / "test.db")
    yield m
    m.close()


class TestDatasetManager:
    def test_create_dataset(self, mgr):
        did = mgr.create("test-dataset", description="A test dataset")
        assert did > 0

    def test_create_duplicate_fails(self, mgr):
        mgr.create("test-dataset")
        with pytest.raises(Exception):
            mgr.create("test-dataset")

    def test_list_datasets(self, mgr):
        mgr.create("ds1")
        mgr.create("ds2")
        datasets = mgr.list_datasets()
        assert len(datasets) == 2
        names = {ds["name"] for ds in datasets}
        assert names == {"ds1", "ds2"}

    def test_get_dataset(self, mgr):
        mgr.create("myds", description="hello")
        ds = mgr.get_dataset("myds")
        assert ds is not None
        assert ds["name"] == "myds"
        assert ds["description"] == "hello"

    def test_get_nonexistent(self, mgr):
        assert mgr.get_dataset("nope") is None

    def test_delete_dataset(self, mgr):
        mgr.create("to-delete")
        assert mgr.delete_dataset("to-delete") is True
        assert mgr.get_dataset("to-delete") is None

    def test_delete_nonexistent(self, mgr):
        assert mgr.delete_dataset("nope") is False

    def test_create_version(self, mgr, healthy_bag):
        mgr.create("myds")
        vid = mgr.create_version(
            dataset_name="myds",
            version="1.0",
            bag_refs=[BagRef(path=str(healthy_bag))],
            topics=["/imu/data"],
            export_format="parquet",
        )
        assert vid > 0

        ds = mgr.get_dataset("myds")
        assert len(ds["versions"]) == 1
        assert ds["versions"][0]["version"] == "1.0"

    def test_create_version_nonexistent_dataset(self, mgr, healthy_bag):
        with pytest.raises(KeyError):
            mgr.create_version(
                dataset_name="nonexistent",
                version="1.0",
                bag_refs=[BagRef(path=str(healthy_bag))],
            )

    def test_create_version_with_sync(self, mgr, healthy_bag):
        mgr.create("synced-ds")
        vid = mgr.create_version(
            dataset_name="synced-ds",
            version="1.0",
            bag_refs=[BagRef(path=str(healthy_bag))],
            topics=["/imu/data", "/joint_states"],
            sync_config=SyncConfig(method="nearest", tolerance_ms=25.0),
            export_format="parquet",
        )
        assert vid > 0

    def test_create_version_with_metadata(self, mgr, healthy_bag):
        mgr.create("meta-ds")
        vid = mgr.create_version(
            dataset_name="meta-ds",
            version="1.0",
            bag_refs=[BagRef(path=str(healthy_bag))],
            metadata=DatasetMetadata(
                description="Training data for pick and place",
                license="MIT",
                robot_type="6-DOF arm",
                task="pick_and_place",
                tags=["manipulation", "training"],
            ),
        )
        assert vid > 0

    def test_export_version(self, mgr, healthy_bag, tmp_dir):
        mgr.create("export-ds")
        mgr.create_version(
            dataset_name="export-ds",
            version="1.0",
            bag_refs=[BagRef(path=str(healthy_bag))],
            topics=["/imu/data"],
            export_format="parquet",
        )
        output = mgr.export_version("export-ds", "1.0", str(tmp_dir / "output"))
        assert output.exists()
        assert (output / "manifest.json").exists()
        assert (output / "dataset_config.json").exists()
        assert (output / "README.md").exists()
        # Should have a parquet file
        parquet_files = list(output.glob("*.parquet"))
        assert len(parquet_files) > 0

    def test_export_nonexistent_dataset(self, mgr):
        with pytest.raises(KeyError):
            mgr.export_version("nope", "1.0")

    def test_export_nonexistent_version(self, mgr):
        mgr.create("exists")
        with pytest.raises(KeyError):
            mgr.export_version("exists", "99.0")

    def test_multiple_versions(self, mgr, healthy_bag):
        mgr.create("multi")
        mgr.create_version("multi", "1.0", [BagRef(path=str(healthy_bag))])
        mgr.create_version("multi", "2.0", [BagRef(path=str(healthy_bag))])
        ds = mgr.get_dataset("multi")
        assert len(ds["versions"]) == 2
