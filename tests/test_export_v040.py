"""Tests for the v0.4.0 export-path changes.

Two coupled behaviours land in v0.4.0:

1. NumPy ``.npz`` export refuses topics > NUMPY_HARD_CAP rows up
   front, raising :class:`LargeTopicError`. The format can't append,
   so writing a million-row topic was a 1+ GB memory spike. Users on
   bigger topics should use Parquet.

2. RLDS TFRecord export doesn't materialize the chunk iterator to
   derive ``is_last`` per row; it looks one chunk ahead instead, so
   memory is bounded by chunk size.
"""

from __future__ import annotations

import sys
import tempfile
import importlib.machinery
import types
from pathlib import Path

import polars as pl
import pytest

from resurrector.core import export as export_module
from resurrector.core.bag_frame import BagFrame
from resurrector.core.exceptions import LargeTopicError
from resurrector.core.export import Exporter, _stream_rlds
from tests.fixtures.generate_test_bags import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def small_bag(tmp_dir):
    """~400 IMU msg, ~200 joint_states msg, etc. — fits comfortably under any cap."""
    return generate_bag(tmp_dir / "small.mcap", BagConfig(duration_sec=2.0))


class TestNumpyHardCap:
    def test_under_cap_succeeds(self, tmp_dir, small_bag):
        bf = BagFrame(small_bag)
        Exporter().export(
            bag_frame=bf, topics=["/imu/data"], format="numpy",
            output_dir=str(tmp_dir / "out"),
        )
        # No exception means the export went through.
        assert (tmp_dir / "out" / "imu_data.npz").exists()

    def test_over_cap_raises(self, monkeypatch, tmp_dir, small_bag):
        """Lower the cap so the small bag's IMU topic (~400 msgs) blows it."""
        monkeypatch.setattr(export_module, "NUMPY_HARD_CAP", 100)
        bf = BagFrame(small_bag)
        with pytest.raises(LargeTopicError) as exc:
            Exporter().export(
                bag_frame=bf, topics=["/imu/data"], format="numpy",
                output_dir=str(tmp_dir / "out"),
            )
        assert exc.value.topic_name == "/imu/data"
        assert exc.value.threshold == 100
        assert exc.value.message_count > 100

    def test_over_cap_other_formats_still_work(self, monkeypatch, tmp_dir, small_bag):
        """The cap is NumPy-specific. Parquet must still succeed past it."""
        monkeypatch.setattr(export_module, "NUMPY_HARD_CAP", 100)
        bf = BagFrame(small_bag)
        Exporter().export(
            bag_frame=bf, topics=["/imu/data"], format="parquet",
            output_dir=str(tmp_dir / "out"),
        )
        assert (tmp_dir / "out" / "imu_data.parquet").exists()


def _fake_tensorflow(written: list) -> types.ModuleType:
    """The slice of the tensorflow API ``_stream_rlds`` touches.

    tensorflow isn't installable on every Python we test (and isn't in
    any extra), so the writer's own logic is tested against this stub.
    ``SerializeToString`` returns the Example itself so tests can read
    the step flags back from ``written``.
    """
    class _ValueList:
        def __init__(self, value):
            self.value = list(value)

    class Feature:
        def __init__(self, int64_list=None, float_list=None, bytes_list=None):
            self.int64_list = int64_list
            self.float_list = float_list
            self.bytes_list = bytes_list

    class Features:
        def __init__(self, feature):
            self.feature = feature

    class Example:
        def __init__(self, features):
            self.features = features

        def SerializeToString(self):
            return self

    class TFRecordWriter:
        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def write(self, record):
            written.append(record)

    tf = types.ModuleType("tensorflow")
    # A real spec, so Exporter's find_spec pre-flight sees the stub as an
    # installed tensorflow (a None __spec__ makes find_spec raise).
    tf.__spec__ = importlib.machinery.ModuleSpec("tensorflow", loader=None)
    tf.train = types.SimpleNamespace(
        Feature=Feature, Features=Features, Example=Example,
        Int64List=_ValueList, FloatList=_ValueList, BytesList=_ValueList,
    )
    tf.io = types.SimpleNamespace(TFRecordWriter=TFRecordWriter)
    return tf


def _step_flag(example, flag: str) -> int:
    return example.features.feature[f"step/{flag}"].int64_list.value[0]


class TestRldsStreaming:
    """RLDS export derives ``is_last`` with a one-chunk lookahead, so the
    chunk iterator is never materialized and the flag lands on the row
    that was actually written last (not on ``message_count - 1``).
    """

    @pytest.fixture
    def written(self, monkeypatch):
        records: list = []
        monkeypatch.setitem(sys.modules, "tensorflow", _fake_tensorflow(records))
        return records

    def test_is_last_on_final_row(self, tmp_dir, written):
        c1 = pl.DataFrame({"timestamp_ns": [0, 1], "v": [1.0, 2.0]})
        c2 = pl.DataFrame({"timestamp_ns": [2, 3], "v": [3.0, 4.0]})

        result = _stream_rlds(iter([c1, c2]), tmp_dir, "test")

        assert result.rows_written == 4
        assert [_step_flag(ex, "is_last") for ex in written] == [0, 0, 0, 1]
        assert [_step_flag(ex, "is_first") for ex in written] == [1, 0, 0, 0]

    def test_trailing_empty_chunk_does_not_hide_is_last(self, tmp_dir, written):
        c1 = pl.DataFrame({"timestamp_ns": [0, 1], "v": [1.0, 2.0]})
        empty = c1.clear()

        _stream_rlds(iter([c1, empty]), tmp_dir, "test_empty_tail")

        assert [_step_flag(ex, "is_last") for ex in written] == [0, 1]

    def test_reads_one_chunk_ahead(self, tmp_dir, written):
        """Would catch: ``list(chunks)`` to count rows up front (what the
        synced export path did through 0.8.4, where no row count was
        passed). Each chunk may only be pulled once the chunk two back
        has been written."""
        written_when_pulled = []

        def lazy_iter():
            for i in range(5):
                written_when_pulled.append(len(written))
                yield pl.DataFrame({"timestamp_ns": [2 * i, 2 * i + 1], "v": [0.0, 0.0]})

        result = _stream_rlds(lazy_iter(), tmp_dir, "test_lazy")

        assert result.rows_written == 10
        assert written_when_pulled == [0, 0, 2, 4, 6]
        assert [_step_flag(ex, "is_last") for ex in written] == [0] * 9 + [1]

    def test_downsampled_export_marks_last_step(self, tmp_dir, small_bag, written):
        """Would catch: ``is_last`` keyed to ``view.message_count - 1``.
        Downsampling writes fewer rows than the topic has, so through
        0.8.4 no step was ever marked last."""
        bf = BagFrame(small_bag)
        Exporter().export(
            bag_frame=bf, topics=["/imu/data"], format="rlds",
            output_dir=str(tmp_dir / "out"), downsample_hz=10.0,
        )

        assert 0 < len(written) < bf["/imu/data"].message_count
        assert [_step_flag(ex, "is_last") for ex in written] == [0] * (len(written) - 1) + [1]

    def test_synced_export_marks_last_step(self, tmp_dir, small_bag, written):
        bf = BagFrame(small_bag)
        topics = ["/imu/data", "/joint_states"]
        Exporter().export(
            bag_frame=bf, topics=topics, format="rlds",
            output_dir=str(tmp_dir / "out"), sync=True,
        )

        assert len(written) == bf.sync(topics).height
        assert [_step_flag(ex, "is_last") for ex in written] == [0] * (len(written) - 1) + [1]

    @pytest.mark.parametrize("sync,downsample_hz", [
        (False, None), (True, None), (True, 10.0),
    ], ids=["unsynced", "synced", "synced-downsampled"])
    def test_time_sliced_export_marks_last_step(
        self, tmp_dir, small_bag, written, monkeypatch, sync, downsample_hz,
    ):
        """Would catch: ``is_last`` keyed to ``view.message_count - 1``.
        A time-sliced view still reports the whole topic's message count,
        so through 0.8.4 an unsliced count was never reached. Small
        chunks make the synced stream span several, so the flag has to
        come from the lookahead, not the first chunk."""
        monkeypatch.setattr(export_module, "CHUNK_SIZE", 32)
        sliced = BagFrame(small_bag).time_slice(0.5, 1.5)
        topics = ["/imu/data", "/joint_states"] if sync else ["/imu/data"]
        Exporter().export(
            bag_frame=sliced, topics=topics, format="rlds",
            output_dir=str(tmp_dir / "out"), sync=sync, downsample_hz=downsample_hz,
        )

        assert 0 < len(written) < sliced["/imu/data"].message_count
        assert [_step_flag(ex, "is_last") for ex in written] == [0] * (len(written) - 1) + [1]
        assert [_step_flag(ex, "is_first") for ex in written] == [1] + [0] * (len(written) - 1)
