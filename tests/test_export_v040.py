"""Tests for the v0.4.0 export-path changes.

Two coupled behaviours land in v0.4.0:

1. NumPy ``.npz`` export refuses topics > NUMPY_HARD_CAP rows up
   front, raising :class:`LargeTopicError`. The format can't append,
   so writing a million-row topic was a 1+ GB memory spike. Users on
   bigger topics should use Parquet.

2. RLDS TFRecord export no longer materializes the chunk iterator to
   derive ``is_last`` per row. Instead it takes ``total_rows`` from
   the index and uses a running counter — which means memory is
   bounded by chunk size.
"""

from __future__ import annotations

import tempfile
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


class TestRldsStreaming:
    """RLDS export should NOT materialize the chunk iterator when total_rows
    is supplied. We can't easily measure RSS in a unit test, but we CAN
    verify the writer respects total_rows-based is_last and the chunk
    iterator gets consumed lazily.
    """

    def test_total_rows_respected(self, tmp_dir):
        """Pass total_rows=4 with two chunks of 2 rows each; second row of
        the second chunk should be marked is_last."""
        try:
            import tensorflow as tf
        except ImportError:
            pytest.skip("tensorflow not installed (RLDS needs it)")

        c1 = pl.DataFrame({"timestamp_ns": [0, 1], "v": [1.0, 2.0]})
        c2 = pl.DataFrame({"timestamp_ns": [2, 3], "v": [3.0, 4.0]})

        result = _stream_rlds(
            iter([c1, c2]),
            tmp_dir,
            "test",
            total_rows=4,
        )
        assert result.rows_written == 4

        # Read back the TFRecord and check is_last on each row.
        ds = tf.data.TFRecordDataset(str(tmp_dir / "test.tfrecord"))
        is_last_values = []
        for raw in ds:
            ex = tf.train.Example()
            ex.ParseFromString(raw.numpy())
            is_last_values.append(
                ex.features.feature["step/is_last"].int64_list.value[0]
            )
        assert is_last_values == [0, 0, 0, 1]

    def test_chunk_iterator_consumed_lazily(self, tmp_dir):
        """When total_rows is supplied, _stream_rlds should pull from the
        iterator one chunk at a time — never materialize via list()."""
        try:
            import tensorflow as tf  # noqa: F401
        except ImportError:
            pytest.skip("tensorflow not installed")

        # Build a custom iterator that records how many chunks have been
        # pulled. If _stream_rlds called list() on it, all 3 would be
        # pulled before the writer ran.
        chunks_pulled = []

        def lazy_iter():
            for i in range(3):
                df = pl.DataFrame({"timestamp_ns": [i * 2, i * 2 + 1], "v": [0.0, 0.0]})
                chunks_pulled.append(i)
                yield df

        result = _stream_rlds(
            lazy_iter(),
            tmp_dir,
            "test_lazy",
            total_rows=6,
        )
        assert result.rows_written == 6
        # Iteration should have consumed exactly 3 chunks (no extra).
        assert chunks_pulled == [0, 1, 2]

    def test_fallback_when_total_rows_missing(self, tmp_dir):
        """If a caller doesn't pass total_rows, the writer falls back
        to materializing once — preserves backward compat."""
        try:
            import tensorflow as tf  # noqa: F401
        except ImportError:
            pytest.skip("tensorflow not installed")

        c1 = pl.DataFrame({"timestamp_ns": [0, 1], "v": [1.0, 2.0]})
        result = _stream_rlds(iter([c1]), tmp_dir, "test_fallback")
        assert result.rows_written == 2
