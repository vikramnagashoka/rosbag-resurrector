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


class TestMultiBagExport:
    """Would catch: every bag of a version exporting into one directory
    under the same file names, so only the last bag's data survived (a
    two-bag version kept 400 of 600 IMU rows)."""

    @pytest.fixture
    def two_bags(self, tmp_dir):
        a = generate_bag(tmp_dir / "run.mcap", BagConfig(duration_sec=1.0))
        (tmp_dir / "other").mkdir()
        b = generate_bag(tmp_dir / "other" / "run.mcap", BagConfig(duration_sec=2.0))
        return a, b

    def test_each_bag_gets_its_own_folder(self, mgr, two_bags, tmp_dir):
        import polars as pl
        from resurrector.core.bag_frame import BagFrame

        a, b = two_bags
        mgr.create("mb")
        mgr.create_version("mb", "1.0", [BagRef(path=str(a)), BagRef(path=str(b))],
                           topics=["/imu/data"])
        out = mgr.export_version("mb", "1.0", str(tmp_dir / "ds"))
        # Same file name in both bags: the index keeps them apart.
        for folder, bag in (("000_run", a), ("001_run", b)):
            rows = pl.read_parquet(out / folder / "imu_data.parquet").height
            assert rows == BagFrame(bag)["/imu/data"].message_count
        assert not (out / "imu_data.parquet").exists()
        readme = (out / "README.md").read_text()
        assert f"- `000_run/`: `{a}`" in readme and f"- `001_run/`: `{b}`" in readme
        import json
        manifest = json.loads((out / "manifest.json").read_text())
        assert "000_run/imu_data.parquet" in manifest and "001_run/imu_data.parquet" in manifest

    def test_single_bag_keeps_the_flat_layout(self, mgr, healthy_bag, tmp_dir):
        mgr.create("one")
        mgr.create_version("one", "1.0", [BagRef(path=str(healthy_bag))], topics=["/imu/data"])
        out = mgr.export_version("one", "1.0", str(tmp_dir / "ds"))
        assert (out / "imu_data.parquet").exists()

    def test_a_bag_with_no_messages_is_skipped(self, mgr, two_bags, tmp_dir, caplog):
        a, b = two_bags
        mgr.create("mb")
        # The first bag's own topic filter is /tf, which has no messages in
        # the synthetic bags; the second bag exports /imu/data.
        mgr.create_version("mb", "1.0", [BagRef(path=str(a), topics=["/tf"]), BagRef(path=str(b))],
                           topics=["/imu/data"])
        out = mgr.export_version("mb", "1.0", str(tmp_dir / "ds"))
        assert not (out / "000_run").exists()
        assert (out / "001_run" / "imu_data.parquet").exists()
        assert "Skipping that bag" in caplog.text
        # The README doesn't point at a folder that wasn't written.
        readme = (out / "README.md").read_text()
        assert f"- `{a}` (no messages to export, so no `000_run/` folder)" in readme

    def test_synced_bag_with_an_empty_slice_is_skipped(self, mgr, two_bags, tmp_dir):
        """Would catch: a synced version writing a header-only
        synced.parquet for a bag whose slice holds no messages."""
        a, b = two_bags
        mgr.create("mb")
        mgr.create_version("mb", "1.0",
                           [BagRef(path=str(a), start_time="5s", end_time="6s"), BagRef(path=str(b))],
                           topics=["/imu/data", "/joint_states"], sync_config=SyncConfig(method="nearest"))
        out = mgr.export_version("mb", "1.0", str(tmp_dir / "ds"))
        assert not (out / "000_run").exists()
        assert (out / "001_run" / "synced.parquet").exists()

    def test_unknown_topic_in_a_later_bag_writes_nothing(self, mgr, two_bags, tmp_dir):
        """Would catch: the first bag's files written before the second
        bag's missing topic failed the export."""
        from resurrector.core.exceptions import TopicNotFoundError

        a, b = two_bags
        mgr.create("mb")
        mgr.create_version("mb", "1.0", [BagRef(path=str(a)), BagRef(path=str(b), topics=["/nope"])],
                           topics=["/imu/data"])
        with pytest.raises(TopicNotFoundError, match="'/nope'"):
            mgr.export_version("mb", "1.0", str(tmp_dir / "ds"))
        assert not (tmp_dir / "ds").exists()

    def test_flat_files_from_an_older_export_stay_out_of_the_manifest(self, mgr, two_bags, tmp_dir, caplog):
        """0.8.6 exported every bag of a version flat, so a re-export finds
        that imu_data.parquet (the last bag's rows only) beside the new
        per-bag folders. Would catch: it landing in manifest.json and in
        the README's loading snippet."""
        import json

        a, b = two_bags
        mgr.create("mb")
        mgr.create_version("mb", "1.0", [BagRef(path=str(a)), BagRef(path=str(b))], topics=["/imu/data"])
        version_dir = tmp_dir / "ds" / "mb" / "1.0"
        version_dir.mkdir(parents=True)
        (version_dir / "imu_data.parquet").write_text("left from 0.8.6")
        out = mgr.export_version("mb", "1.0", str(tmp_dir / "ds"))
        manifest = json.loads((out / "manifest.json").read_text())
        assert sorted(manifest) == ["000_run/imu_data.parquet", "001_run/imu_data.parquet"]
        assert "outside its per-bag folders" in caplog.text and "imu_data.parquet" in caplog.text
        assert str(out / "000_run" / "imu_data.parquet") in (out / "README.md").read_text()


class TestFailedExportCleanup:
    """Would catch: a failed export leaving an empty <output>/<dataset>
    folder behind (the version folder was removed, its parent wasn't)."""

    def test_failure_removes_only_folders_it_created(self, mgr, healthy_bag, tmp_dir):
        from resurrector.core.exceptions import EmptyExportError

        mgr.create("ds")
        mgr.create_version("ds", "1.0", [BagRef(path=str(healthy_bag))], topics=["/tf"])
        root = tmp_dir / "exports"
        with pytest.raises(EmptyExportError):
            mgr.export_version("ds", "1.0", str(root / "nested"))
        assert not root.exists()

    def test_existing_version_folder_is_kept(self, mgr, healthy_bag, tmp_dir):
        from resurrector.core.exceptions import EmptyExportError

        mgr.create("ds")
        mgr.create_version("ds", "1.0", [BagRef(path=str(healthy_bag))], topics=["/tf"])
        version_dir = tmp_dir / "exports" / "ds" / "1.0"
        version_dir.mkdir(parents=True)
        (version_dir / "earlier.parquet").write_text("from an earlier export")
        with pytest.raises(EmptyExportError):
            mgr.export_version("ds", "1.0", str(tmp_dir / "exports"))
        assert (version_dir / "earlier.parquet").read_text() == "from an earlier export"

    @pytest.fixture
    def failing_lerobot(self, monkeypatch):
        """A LeRobot writer whose create() makes the root (as LeRobot's
        does) and whose first add_frame() fails."""
        from resurrector.core import export as export_module
        from resurrector.core import lerobot_export

        class _Dataset:
            def add_frame(self, frame):
                raise RuntimeError("encoder died")

            def finalize(self):
                pass

        class _LeRobotDataset:
            @staticmethod
            def create(root, **_kw):
                (root / "meta").mkdir(parents=True)
                return _Dataset()

        monkeypatch.setattr(lerobot_export, "import_lerobot_dataset", lambda: _LeRobotDataset)
        monkeypatch.setattr(export_module, "require_export_dependencies", lambda f: None)

    def test_failed_lerobot_export_leaves_no_empty_dataset_folder(self, mgr, healthy_bag, tmp_dir, failing_lerobot):
        """Would catch: <output>/<dataset> left behind after LeRobot
        removed the version folder (the cleanup stopped at the missing
        leaf instead of walking up)."""
        mgr.create("ds")
        mgr.create_version("ds", "1.0", [BagRef(path=str(healthy_bag))], topics=["/imu/data"],
                           export_format="lerobot")
        with pytest.raises(RuntimeError, match="encoder died"):
            mgr.export_version("ds", "1.0", str(tmp_dir / "exports"))
        assert not (tmp_dir / "exports").exists()

    def test_failed_lerobot_export_keeps_an_existing_version_folder(self, mgr, healthy_bag, tmp_dir, failing_lerobot):
        mgr.create("ds")
        mgr.create_version("ds", "1.0", [BagRef(path=str(healthy_bag))], topics=["/imu/data"],
                           export_format="lerobot")
        version_dir = tmp_dir / "exports" / "ds" / "1.0"
        version_dir.mkdir(parents=True)
        with pytest.raises(RuntimeError):
            mgr.export_version("ds", "1.0", str(tmp_dir / "exports"))
        assert version_dir.is_dir() and not any(version_dir.iterdir())

    def test_failed_bag_lerobot_export_removes_created_parents(self, healthy_bag, tmp_dir, failing_lerobot):
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.export import Exporter

        with pytest.raises(RuntimeError):
            Exporter().export(bag_frame=BagFrame(healthy_bag), topics=["/imu/data"],
                              format="lerobot", output_dir=str(tmp_dir / "n1" / "n2"))
        assert not (tmp_dir / "n1").exists()

    def test_unknown_topic_names_the_bag(self, mgr, healthy_bag, tmp_dir):
        from resurrector.core.exceptions import TopicNotFoundError

        mgr.create("ds")
        mgr.create_version("ds", "1.0", [BagRef(path=str(healthy_bag))], topics=["/nope"])
        with pytest.raises(TopicNotFoundError, match=f"not in {healthy_bag.name}"):
            mgr.export_version("ds", "1.0", str(tmp_dir / "exports"))
        assert not (tmp_dir / "exports").exists()
