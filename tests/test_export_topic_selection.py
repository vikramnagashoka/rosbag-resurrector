"""Exports name the topics they can't export instead of reporting success.

Before: ``resurrector export bag -t /nope`` logged "Topic '/nope' not
found, skipping", printed "Exported to ..." and exited 0 with nothing
written; a topic with no messages got a 0-byte CSV, empty HDF5 / Zarr /
.npz files and no Parquet file, also with exit 0.

Now: an unknown topic raises :class:`TopicNotFoundError` before anything
is written; a topic with no messages gets no file in any format (with a
warning); an export where every topic is empty raises
:class:`EmptyExportError` and leaves no empty directory behind.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from resurrector.cli.main import app
from resurrector.core.bag_frame import BagFrame
from resurrector.core.exceptions import EmptyExportError, TopicNotFoundError
from resurrector.core.export import Exporter
from resurrector.demo.sample_bag import BagConfig, generate_bag

FORMATS = ["parquet", "csv", "hdf5", "numpy"]


@pytest.fixture(scope="module")
def demo_bag(tmp_path_factory):
    """The synthetic bag: ``/tf`` is registered with no messages."""
    return generate_bag(tmp_path_factory.mktemp("bags") / "demo.mcap", BagConfig(duration_sec=1.0))


def _cli(*args):
    return CliRunner().invoke(app, list(args), env={"COLUMNS": "2000"})


class TestUnknownTopic:
    @pytest.mark.parametrize("fmt", FORMATS + ["lerobot"])
    def test_raises_before_writing(self, demo_bag, tmp_path, monkeypatch, fmt):
        # The topic check doesn't need LeRobot installed to run.
        monkeypatch.setattr("resurrector.core.export.require_export_dependencies", lambda f: None)
        out = tmp_path / "out"
        with pytest.raises(TopicNotFoundError) as exc:
            Exporter().export(bag_frame=BagFrame(demo_bag), topics=["/imu/data", "/nope"],
                              format=fmt, output_dir=str(out))
        assert exc.value.missing == ["/nope"]
        assert "Topic '/nope' is not in demo.mcap" in str(exc.value)
        assert "/imu/data" in str(exc.value)  # lists what the bag has
        assert not out.exists()

    def test_is_a_key_error_and_a_value_error(self, demo_bag, tmp_path):
        """Callers that caught KeyError (sync used to raise it) or
        ValueError (the CLI, the dashboard's 400) keep working."""
        bf = BagFrame(demo_bag)
        for cls in (KeyError, ValueError):
            with pytest.raises(cls):
                bf.export(topics=["/nope"], output=str(tmp_path / cls.__name__))

    def test_sync_and_split_paths_check_too(self, demo_bag, tmp_path):
        bf = BagFrame(demo_bag)
        with pytest.raises(TopicNotFoundError):
            bf.export(topics=["/imu/data", "/nope"], sync=True, output=str(tmp_path / "s"))
        for strategy in ("time", "random"):
            out = tmp_path / strategy
            with pytest.raises(TopicNotFoundError):
                bf.export(topics=["/nope"], split={"a": 0.5, "b": 0.5},
                          split_strategy=strategy, output=str(out))
            assert not out.exists()

    def test_cli_exits_1_naming_the_topic(self, demo_bag, tmp_path):
        """Would catch: "Exported to ..." and exit 0 with an empty output."""
        out = tmp_path / "out"
        result = _cli("export", str(demo_bag), "-t", "/nope", "-o", str(out))
        assert result.exit_code == 1, result.output
        assert isinstance(result.exception, SystemExit)
        assert "Export failed: Topic '/nope' is not in demo.mcap" in result.stderr
        assert "Exported to" not in result.output
        assert not out.exists()


class TestEmptyTopic:
    @pytest.mark.parametrize("fmt", FORMATS)
    def test_no_file_beside_topics_with_data(self, demo_bag, tmp_path, fmt, caplog):
        out = tmp_path / "out"
        Exporter().export(bag_frame=BagFrame(demo_bag), topics=["/tf", "/imu/data"],
                          format=fmt, output_dir=str(out))
        assert [p.stem for p in out.iterdir()] == ["imu_data"]
        assert "Topic '/tf' has no messages" in caplog.text

    @pytest.mark.parametrize("fmt", FORMATS)
    def test_only_empty_topics_raise_and_leave_nothing(self, demo_bag, tmp_path, fmt):
        out = tmp_path / "a" / "b"
        with pytest.raises(EmptyExportError) as exc:
            Exporter().export(bag_frame=BagFrame(demo_bag), topics=["/tf"],
                              format=fmt, output_dir=str(out))
        assert exc.value.topics == ["/tf"]
        # Both directories the export created are gone; tmp_path stays.
        assert list(tmp_path.iterdir()) == []

    def test_existing_output_dir_is_kept(self, demo_bag, tmp_path):
        (tmp_path / "keep.txt").write_text("mine")
        with pytest.raises(EmptyExportError):
            Exporter().export(bag_frame=BagFrame(demo_bag), topics=["/tf"],
                              format="csv", output_dir=str(tmp_path))
        assert [p.name for p in tmp_path.iterdir()] == ["keep.txt"]

    def test_empty_time_window_raises(self, demo_bag, tmp_path):
        """A slice past the camera topic's last frame has no rows."""
        sliced = BagFrame(demo_bag).time_slice(5.0, 6.0)
        with pytest.raises(EmptyExportError):
            Exporter().export(bag_frame=sliced, topics=["/imu/data"], output_dir=str(tmp_path / "w"))

    def test_cli_exits_1(self, demo_bag, tmp_path):
        out = tmp_path / "out"
        result = _cli("export", str(demo_bag), "-t", "/tf", "-f", "csv", "-o", str(out))
        assert result.exit_code == 1, result.output
        assert "No messages to export from demo.mcap: '/tf' has none" in result.stderr
        assert "Exported to" not in result.output
        assert not out.exists()

    def test_cli_mixed_topics_exit_0(self, demo_bag, tmp_path):
        out = tmp_path / "out"
        result = _cli("export", str(demo_bag), "-t", "/tf", "-t", "/imu/data", "-f", "csv", "-o", str(out))
        assert result.exit_code == 0, result.output
        assert [p.name for p in out.iterdir()] == ["imu_data.csv"]


def test_preset_filter_matching_nothing_is_an_error(tmp_path):
    """Would catch: camera-only on a bag without cameras exporting nothing
    and reporting success."""
    from pathlib import Path

    from tests.fixtures.changing_columns import write_joint_state_bag

    bag = write_joint_state_bag(Path(tmp_path / "nocam.mcap"), 10, range(0), 10)
    with pytest.raises(ValueError, match="'camera-only' preset exports image topics"):
        BagFrame(bag).export(preset="camera-only", output=str(tmp_path / "out"))


class TestDashboard:
    @pytest.fixture
    def client(self, demo_bag, tmp_path, monkeypatch):
        from resurrector.dashboard.api import app as api_app
        from resurrector.ingest.indexer import BagIndex
        from resurrector.ingest.parser import parse_bag
        from resurrector.ingest.scanner import scan_path

        db = tmp_path / "index.db"
        index = BagIndex(db)
        bag_id = index.upsert_bag(scan_path(demo_bag)[0], parse_bag(demo_bag).get_metadata())
        index.close()
        monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db))
        monkeypatch.setenv("RESURRECTOR_ALLOWED_ROOTS", str(tmp_path))
        return TestClient(api_app), bag_id

    @pytest.mark.parametrize(("topics", "text"), [
        ("/nope", "Topic '/nope' is not in demo.mcap"),
        ("/tf", "No messages to export from demo.mcap"),
    ], ids=["unknown", "empty"])
    def test_bag_export_answers_400(self, client, tmp_path, topics, text):
        """Would catch: a bare 500 (KeyError) or a 200 for an export that
        wrote nothing."""
        c, bag_id = client
        r = c.post(f"/api/bags/{bag_id}/export",
                   params={"topics": topics, "format": "csv", "output_dir": str(tmp_path / "o")})
        assert r.status_code == 400, r.text
        assert text in r.json()["detail"]

    @pytest.mark.parametrize(("topic", "text"), [
        ("/nope", "Topic '/nope' is not in demo.mcap"),
        ("/tf", "No messages to export from demo.mcap"),
    ], ids=["unknown", "empty"])
    def test_dataset_version_export_answers_400(self, client, demo_bag, tmp_path, topic, text):
        """Would catch: the route's ValueError branch answering 404, as if
        the dataset or version were missing, or the old 500."""
        from resurrector.core.dataset import BagRef, DatasetManager

        mgr = DatasetManager(tmp_path / "index.db")
        mgr.create("ds")
        mgr.create_version("ds", "1.0", [BagRef(path=str(demo_bag), topics=[topic])])
        mgr.close()
        c, _ = client
        r = c.post("/api/datasets/ds/versions/1.0/export", json={"output_dir": str(tmp_path / "o")})
        assert r.status_code == 400, r.text
        assert text in r.json()["detail"]


class TestReviewFollowUps:
    def test_sync_with_only_empty_topics_raises(self, tmp_path):
        """Would catch: --sync writing a 1-byte synced.csv and exiting 0
        when every topic is empty (the non-sync path already failed)."""
        from mcap.writer import Writer

        from resurrector.demo.sample_bag import SCHEMAS

        bag = tmp_path / "empty2.mcap"
        with open(bag, "wb") as f:
            w = Writer(f)
            w.start(profile="ros2", library="test")
            for topic, mt in (("/imu/data", "sensor_msgs/msg/Imu"),
                              ("/joint_states", "sensor_msgs/msg/JointState")):
                sid = w.register_schema(name=mt, encoding="ros2msg", data=SCHEMAS[mt]["data"].encode())
                w.register_channel(topic=topic, message_encoding="cdr", schema_id=sid)
            w.finish()
        out = tmp_path / "out"
        result = _cli("export", str(bag), "-t", "/imu/data", "-t", "/joint_states",
                      "--sync", "nearest", "-f", "csv", "-o", str(out))
        assert result.exit_code == 1, result.output
        assert "No messages to export" in result.stderr
        assert not out.exists()

    def test_dotdot_in_output_never_removes_an_existing_folder(self, demo_bag, tmp_path, monkeypatch):
        """Would catch: ``-o x/../E/new`` on a failed export removing the
        existing (empty) folder E, because the cleanup walked x/../E as if
        the export had created it."""
        (tmp_path / "E").mkdir()
        monkeypatch.chdir(tmp_path)
        with pytest.raises(EmptyExportError):
            Exporter().export(bag_frame=BagFrame(demo_bag), topics=["/tf"], output_dir="x/../E/new")
        assert (tmp_path / "E").is_dir()
        assert not (tmp_path / "E" / "new").exists()

    def test_unknown_format_is_named_first(self, demo_bag, tmp_path):
        with pytest.raises(ValueError, match="Unknown export format: bogus"):
            Exporter().export(bag_frame=BagFrame(demo_bag), topics=["/tf"], format="bogus",
                              output_dir=str(tmp_path / "o"))

    def test_concatenated_bags_export(self, tmp_path):
        """Would catch: AttributeError 'ConcatenatedBagFrame' object has no
        attribute 'topic_names' from the new topic check."""
        from resurrector.core.concatenated import concatenate_bags

        a = generate_bag(tmp_path / "a.mcap", BagConfig(duration_sec=0.5))
        b = generate_bag(tmp_path / "b.mcap", BagConfig(duration_sec=0.5))
        cat = concatenate_bags([a, b])
        out = tmp_path / "out"
        Exporter().export(bag_frame=cat, topics=["/imu/data"], output_dir=str(out))
        assert [p.name for p in out.iterdir()] == ["imu_data.parquet"]
        with pytest.raises(TopicNotFoundError, match="not in the 2 concatenated bags"):
            Exporter().export(bag_frame=cat, topics=["/nope"], output_dir=str(out))

    def test_random_split_warns_about_an_empty_topic(self, demo_bag, tmp_path, caplog):
        BagFrame(demo_bag).export(topics=["/imu/data", "/tf"], output=str(tmp_path / "r"),
                                  split={"a": 0.5, "b": 0.5}, split_strategy="random")
        assert "'tf' has no messages" in caplog.text
