"""Tests for the RLDS ML-pipeline export format.

RLDS auto-skips when tensorflow isn't installed. LeRobot export is covered
in test_lerobot_export.py (round-trips through the real LeRobot library).
"""

from __future__ import annotations

import json
import math
import sys
import tempfile
from pathlib import Path

import polars as pl
import pytest

from resurrector.core.bag_frame import BagFrame
from resurrector.core.export import Exporter
from tests.fixtures.generate_test_bags import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_bag(tmp_dir):
    return generate_bag(tmp_dir / "sample.mcap", BagConfig(duration_sec=2.0))


tf_available = False
try:
    import tensorflow  # noqa: F401
    tf_available = True
except ImportError:
    pass


@pytest.mark.skipif(not tf_available, reason="tensorflow not installed")
class TestRLDSExport:
    def test_writes_tfrecord(self, tmp_dir, sample_bag):
        bf = BagFrame(sample_bag)
        out = tmp_dir / "rlds_out"
        bf.export(
            topics=["/imu/data", "/joint_states"],
            format="rlds",
            output=str(out),
            sync=True,
        )
        assert (out / "synced.tfrecord").exists()

    def test_tfrecord_has_step_features(self, tmp_dir, sample_bag):
        import tensorflow as tf
        bf = BagFrame(sample_bag)
        out = tmp_dir / "rlds_out"
        bf.export(
            topics=["/imu/data", "/joint_states"],
            format="rlds",
            output=str(out),
            sync=True,
        )
        ds = tf.data.TFRecordDataset(str(out / "synced.tfrecord"))
        first = next(iter(ds))
        example = tf.train.Example()
        example.ParseFromString(first.numpy())
        keys = set(example.features.feature.keys())
        # Required RLDS step features
        assert "step/reward" in keys
        assert "step/discount" in keys
        assert "step/is_first" in keys
        assert "step/is_last" in keys
        assert "step/is_terminal" in keys
        # First step should be is_first=True, is_last=False
        assert example.features.feature["step/is_first"].int64_list.value[0] == 1
        assert example.features.feature["step/is_last"].int64_list.value[0] == 0


def test_rlds_raises_helpful_error_when_tf_missing(tmp_dir, sample_bag, monkeypatch):
    """If tensorflow isn't available, the user gets a clear install hint.

    Hides tensorflow via sys.modules so this runs (rather than skips) in
    CI's all-exports job too, where a skip would trip the RLDS guard step.
    """
    import sys
    monkeypatch.setitem(sys.modules, "tensorflow", None)
    bf = BagFrame(sample_bag)
    with pytest.raises(ImportError, match="tensorflow"):
        bf.export(
            topics=["/imu/data", "/joint_states"],
            format="rlds",
            output=str(tmp_dir / "rlds_out"),
            sync=True,
        )


def test_unknown_format_lists_lerobot_and_rlds(tmp_dir, sample_bag):
    """The error message should mention the new formats so users know they exist."""
    bf = BagFrame(sample_bag)
    with pytest.raises(ValueError, match="lerobot.*rlds|rlds.*lerobot"):
        bf.export(topics=["/imu/data"], format="bogus")


# ---------------------------------------------------------------------------
# One feature type per column, and nulls that are never the text "None".
#
# Through the column-alignment fix, the writer chose each value's feature
# from its Python type and stringified anything else, so a null became
# bytes_list "None". With iter_chunks keeping a field first seen after
# row 100 (JointState velocity), the rows before it are null: step 0 got
# bytes_list "None" and step 130 float_list 131.0, one key with two
# types, which a TFDS/RLDS reader with a float spec fails on.
#
# Each test runs against the fake tensorflow from test_export_v040.py
# (always) and the real one (in CI's all-exports job).
# ---------------------------------------------------------------------------


@pytest.fixture(params=["fake-tf", "real-tf"])
def rlds_steps(request, monkeypatch):
    """``write(chunks, out) -> (steps, error)``: ``steps`` is one dict per
    written step, feature key -> (list kind, values); ``error`` is the
    ExportError raised, if any."""
    from resurrector.core.export import ExportError, _stream_rlds

    real = request.param == "real-tf"
    if real and not tf_available:
        pytest.skip("tensorflow not installed")
    written: list = []
    if not real:
        from tests.test_export_v040 import _fake_tensorflow
        monkeypatch.setitem(sys.modules, "tensorflow", _fake_tensorflow(written))

    def kind_and_values(feature):
        if real:
            kind = feature.WhichOneof("kind")
            return kind, list(getattr(feature, kind).value)
        for kind in ("float_list", "int64_list", "bytes_list"):
            if getattr(feature, kind) is not None:
                return kind, list(getattr(feature, kind).value)

    def write(chunks, out):
        error = None
        try:
            _stream_rlds(iter(chunks), out, "t")
        except ExportError as e:
            error = e
        if real:
            import tensorflow as tf
            examples = [
                tf.train.Example.FromString(r.numpy())
                for r in tf.data.TFRecordDataset(str(out / "t.tfrecord"))
            ]
        else:
            examples = written
        steps = [
            {k: kind_and_values(f) for k, f in ex.features.feature.items()}
            for ex in examples
        ]
        return steps, error

    return write


def _column(steps, col):
    key = "step/timestamp_ns" if col == "timestamp_ns" else f"step/observation/{col}"
    return [step.get(key) for step in steps]


def _same(a, b) -> bool:
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    return a == b


def _assert_column(steps, col, kind, values):
    """Every step has ``col`` as ``kind`` with ``values[i]``; None in
    ``values`` means the step has no such feature."""
    got = _column(steps, col)
    assert len(got) == len(values)
    for i, (feature, want) in enumerate(zip(got, values)):
        if want is None:
            assert feature is None, (col, i, feature)
            continue
        assert feature is not None, (col, i)
        assert feature[0] == kind, (col, i, feature)
        assert len(feature[1]) == 1 and _same(feature[1][0], want), (col, i, feature, want)


def _assert_no_none_text(steps):
    for step in steps:
        for key, (kind, values) in step.items():
            assert b"None" not in values, key


def test_rlds_feature_type_comes_from_the_dtype_and_nulls_follow_it(rlds_steps, tmp_path):
    """A field that appears partway through a chunk, fields a later chunk
    lacks, and nulls of every policy: float -> NaN, text and binary ->
    b"", integer and Boolean -> no feature for that step."""
    nan = float("nan")
    chunks = [
        pl.DataFrame({
            "timestamp_ns": [0, 1, 2, 3],
            "f": [None, None, 1.5, 2.5],
            "s": [None, "a", None, "b"],
            "i": [None, 7, None, 8],
            "b": [True, None, False, None],
            "bin": [b"\x00", None, b"\x01", b"\x02"],
        }),
        # Lacks every column above; "late" first appears partway through.
        pl.DataFrame({"timestamp_ns": [4, 5], "late": [None, 9.0]}),
    ]
    steps, error = rlds_steps(chunks, tmp_path)
    assert error is None
    assert len(steps) == 6
    _assert_column(steps, "timestamp_ns", "int64_list", [0, 1, 2, 3, 4, 5])
    _assert_column(steps, "f", "float_list", [nan, nan, 1.5, 2.5, nan, nan])
    _assert_column(steps, "s", "bytes_list", [b"", b"a", b"", b"b", b"", b""])
    _assert_column(steps, "i", "int64_list", [None, 7, None, 8, None, None])
    _assert_column(steps, "b", "int64_list", [1, None, 0, None, None, None])
    _assert_column(steps, "bin", "bytes_list", [b"\x00", b"", b"\x01", b"\x02", b"", b""])
    # Steps written before "late" existed can't carry it.
    _assert_column(steps, "late", "float_list", [None, None, None, None, nan, 9.0])
    _assert_no_none_text(steps)


def test_rlds_all_null_column_waits_for_its_type(rlds_steps, tmp_path):
    """A column that is null in a whole chunk (dtype Null) says nothing
    about its type, so it isn't written until a chunk does."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [0, 1], "v": [None, None]}),
        pl.DataFrame({"timestamp_ns": [2], "v": [3.0]}),
    ]
    steps, error = rlds_steps(chunks, tmp_path)
    assert error is None
    _assert_column(steps, "v", "float_list", [None, None, 3.0])
    _assert_no_none_text(steps)


def test_rlds_type_change_is_left_out_and_reported(rlds_steps, tmp_path):
    """Would catch: Int64 values written into a String feature as "7", or
    2.5 truncated into an int64 feature. Such a column is left out from
    the chunk that breaks its type on (a later chunk that would fit
    again too), and the export fails with ExportError after the file is
    written. A whole float in an int64 feature, or an int in a float
    one, converts."""
    from resurrector.core.export import FAILURE_TYPE_CHANGE

    chunks = [
        pl.DataFrame({"timestamp_ns": [0], "s": ["a"], "i": [1], "f": [1.0], "w": [1]}),
        pl.DataFrame({"timestamp_ns": [1], "s": [7], "i": [2.5], "f": [3], "w": [2.0]}),
        pl.DataFrame({"timestamp_ns": [2], "s": ["c"], "i": [4], "f": [4.5], "w": [3]}),
    ]
    steps, error = rlds_steps(chunks, tmp_path)
    assert len(steps) == 3
    _assert_column(steps, "s", "bytes_list", [b"a", None, None])
    _assert_column(steps, "i", "int64_list", [1, None, None])
    _assert_column(steps, "f", "float_list", [1.0, 3.0, 4.5])
    _assert_column(steps, "w", "int64_list", [1, 2, 3])
    assert error is not None
    assert [(f.column, f.kind) for f in error.failures] == [
        ("s", FAILURE_TYPE_CHANGE), ("i", FAILURE_TYPE_CHANGE),
    ]
    assert "steps from row 1 on leave it out" in error.failures[0].message
    text = " ".join(str(error).split())
    assert "These columns are in that file but have no values where" in text
    assert "export to" not in text


def test_rlds_first_chunk_overflow_is_not_in_the_file(rlds_steps, tmp_path):
    """Would catch: UInt64 values past int64's range in the chunk that
    types a column reported as "set by UInt64 values in an earlier chunk"
    (there is none) and "in that file", when no step carries it. Parquet
    stores such values, so the message suggests it."""
    from resurrector.core.export import FAILURE_UNSTORABLE

    chunks = [
        pl.DataFrame({"timestamp_ns": [0, 1], "u": pl.Series([1, 2**63 + 5], dtype=pl.UInt64)}),
        pl.DataFrame({"timestamp_ns": [2], "u": pl.Series([3], dtype=pl.UInt64)}),
    ]
    steps, error = rlds_steps(chunks, tmp_path)
    assert len(steps) == 3
    assert _column(steps, "u") == [None, None, None]
    assert [(f.column, f.kind) for f in error.failures] == [("u", FAILURE_UNSTORABLE)]
    assert "earlier chunk" not in error.failures[0].message
    text = " ".join(str(error).split())
    assert "These columns are not in that file" in text
    assert "export to Parquet" in text


def test_rlds_later_chunk_overflow_names_the_range(rlds_steps, tmp_path):
    """Would catch: UInt64 values past int64's range in a later chunk
    reported as "don't fit (set by UInt64 values in an earlier chunk)",
    which misstates the cause (the dtype didn't change) and gives no fix."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [0], "u": pl.Series([1], dtype=pl.UInt64)}),
        pl.DataFrame({"timestamp_ns": [1], "u": pl.Series([2**63 + 5], dtype=pl.UInt64)}),
    ]
    steps, error = rlds_steps(chunks, tmp_path)
    assert _column(steps, "u")[1] is None
    [failure] = error.failures
    assert "past int64's range from row 1 on" in failure.message
    assert "Parquet stores them" in failure.message
    assert "earlier chunk" not in failure.message


@pytest.mark.skipif(not tf_available, reason="tensorflow not installed")
def test_rlds_joint_state_parses_with_a_typed_spec(tmp_dir, monkeypatch):
    """The verifier's late.mcap case end to end: JointState velocity first
    appears at row 120 of a 150-row chunk and is gone from the last
    chunk. Every step parses against a float32 spec for velocity (NaN
    where the message had none); through the fix, step 0 was bytes_list
    "None" and the parse failed."""
    import numpy as np
    import tensorflow as tf

    from resurrector.core import export as export_module
    from tests.fixtures.changing_columns import write_joint_state_bag

    bag = write_joint_state_bag(tmp_dir / "late.mcap", 400, range(120, 250), 200)
    monkeypatch.setattr(export_module, "CHUNK_SIZE", 150)
    out = tmp_dir / "rlds"
    Exporter().export(
        bag_frame=BagFrame(bag), topics=["/joint_states"], format="rlds",
        output_dir=str(out),
    )
    spec = {
        "step/timestamp_ns": tf.io.FixedLenFeature([], tf.int64),
        "step/observation/position.0": tf.io.FixedLenFeature([], tf.float32),
        "step/observation/velocity.0": tf.io.FixedLenFeature([], tf.float32),
        "step/observation/effort.0": tf.io.FixedLenFeature([], tf.float32),
        "step/observation/header.frame_id": tf.io.FixedLenFeature([], tf.string),
    }
    velocity, effort = [], []
    for record in tf.data.TFRecordDataset(str(out / "joint_states.tfrecord")):
        step = tf.io.parse_single_example(record, spec)
        velocity.append(float(step["step/observation/velocity.0"]))
        effort.append(float(step["step/observation/effort.0"]))
    assert len(velocity) == 400
    i = np.arange(400)
    want_velocity = np.where((i >= 120) & (i < 250), 1000.0 + i, np.nan)
    want_effort = np.where(i < 200, 5.0 + i, np.nan)
    np.testing.assert_array_equal(np.array(velocity), want_velocity)
    np.testing.assert_array_equal(np.array(effort), want_effort)
