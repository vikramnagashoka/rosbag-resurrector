"""Exports keep every row aligned with its timestamp when a topic's
columns change between chunks.

A JointState driver that publishes ``velocity: []`` for a while and then
fills it (or stops filling ``effort``) gives chunks with different column
sets. Before this, HDF5, Zarr and ``.npz`` appended each column only for
the chunks that had it, so ``velocity.0[0]`` held message 50 000's value
next to message 0's timestamp, and exited 0. CSV wrote a later chunk's
columns under the first chunk's header, and Parquet died on a raw schema
mismatch. ``iter_chunks`` also inferred each chunk's schema from its first
100 rows, dropping a field first seen after that.

The rules now:

- HDF5, Zarr, NumPy: rows where a column is absent get its missing value
  (NaN, ``""``), written in ``CHUNK_SIZE`` pieces; a column whose dtype
  has none fails and is removed from the file. Every column written has
  one row per ``timestamp_ns``.
- CSV, Parquet: the first chunk fixes the columns. A column a later chunk
  lacks is empty / null there; a column first seen later is reported
  (ExportError) and the rest of the file is still right. A Parquet column
  whose later values can't be cast losslessly, or flip between text and
  non-text, is reported and null from there on.
- ``materialize_ipc_cache`` (one Arrow IPC file) combines chunks the way
  ``to_polars()`` does, rewriting the cached rows wider when a later
  chunk adds a column or widens a dtype.
"""

from __future__ import annotations

import struct
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest
from polars.testing import assert_frame_equal
from typer.testing import CliRunner

from resurrector.cli.main import app
from resurrector.core import export as export_module
from resurrector.core.bag_frame import BagFrame
from resurrector.core.export import (
    ExportError,
    _stream_csv,
    _stream_hdf5,
    _stream_numpy,
    _stream_parquet,
    _stream_zarr,
    _write_numpy_columns,
)
from resurrector.demo.sample_bag import SCHEMAS, _encode_joint_state
from resurrector.ingest.parser import register_decoder, unregister_decoder

ARRAY_FORMATS = ["hdf5", "zarr", "numpy"]
TABLE_FORMATS = ["csv", "parquet"]
ALL_FORMATS = TABLE_FORMATS + ARRAY_FORMATS

_WRITERS = {
    "csv": _stream_csv, "parquet": _stream_parquet, "hdf5": _stream_hdf5,
    "numpy": _stream_numpy, "zarr": _stream_zarr,
}
_EXT = {"csv": "csv", "parquet": "parquet", "hdf5": "h5", "numpy": "npz", "zarr": "zarr"}


def _need(fmt: str) -> None:
    if fmt == "zarr":
        pytest.importorskip("zarr")


def _read(fmt: str, path: Path, group: str) -> dict[str, list]:
    """Every column of an export as a list, missing values as None."""
    if fmt == "csv":
        df = pl.read_csv(path, infer_schema_length=None)
        return {c: df[c].to_list() for c in df.columns}
    if fmt == "parquet":
        df = pl.read_parquet(path)
        return {c: df[c].to_list() for c in df.columns}
    if fmt == "hdf5":
        import h5py
        out = {}
        with h5py.File(path, "r") as f:
            for col, ds in f[group].items():
                text = h5py.check_string_dtype(ds.dtype) is not None
                out[col] = (ds.asstr()[:] if text else ds[:]).tolist()
    elif fmt == "numpy":
        with np.load(path) as data:
            out = {col: data[col].tolist() for col in data.files}
    else:
        import zarr
        g = zarr.open_group(str(path), mode="r")
        out = {col: g[col][:].tolist() for col in g.array_keys()}
    return {
        col: [None if isinstance(v, float) and np.isnan(v) else v for v in values]
        for col, values in out.items()
    }


def _write(fmt: str, chunks: list[pl.DataFrame], out: Path):
    """Write ``chunks`` with ``fmt``'s writer; return (columns, failures)."""
    _need(fmt)
    failures = []
    try:
        _WRITERS[fmt](iter(chunks), out, "t")
    except ExportError as e:
        failures = e.failures
    return _read(fmt, out / f"t.{_EXT[fmt]}", "t"), failures


def _missing(fmt: str, kind: str):
    """What a missing value reads back as: None for CSV / Parquet and
    array floats, "" for array strings."""
    return "" if fmt in ARRAY_FORMATS and kind == "str" else None


# ---------------------------------------------------------------------------
# A real JointState bag through the CLI, every format.
# ---------------------------------------------------------------------------

BASE_NS = 1_700_000_000_000_000_000
JS_ROWS = 400
JS_CHUNK = 150


def _joint_bag(path: Path, velocity_from: int, effort_until: int) -> Path:
    """/joint_states: ``velocity`` is [] before message ``velocity_from``,
    ``effort`` is [] from message ``effort_until`` on."""
    from mcap.writer import Writer

    info = SCHEMAS["sensor_msgs/msg/JointState"]
    with open(path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="resurrector-test")
        sid = writer.register_schema(
            name="sensor_msgs/msg/JointState", encoding=info["encoding"],
            data=info["data"].encode(),
        )
        cid = writer.register_channel(
            topic="/joint_states", message_encoding="cdr", schema_id=sid,
        )
        for i in range(JS_ROWS):
            t = BASE_NS + i * 1_000_000
            data = _encode_joint_state(
                t // 10**9, t % 10**9, ["a", "b"],
                [i * 0.5, -float(i)],
                [1000.0 + i, 2000.0 + i] if i >= velocity_from else [],
                [5.0 + i, 6.0] if i < effort_until else [],
            )
            writer.add_message(cid, log_time=t, publish_time=t, sequence=i, data=data)
        writer.finish()
    return path


# Chunks are rows [0, 150), [150, 300), [300, 400). effort stops at 200,
# mid-chunk, so the last chunk has no effort columns at all.
JS_BAGS = {
    # velocity starts at row 120 of the first chunk: past the 100 rows
    # polars infers a schema from by default.
    "velocity-late-in-first-chunk": (120, 200),
    # velocity is absent from the whole first chunk.
    "velocity-from-second-chunk": (150, 200),
}


@pytest.fixture(scope="module", params=list(JS_BAGS))
def joint_bag(request, tmp_path_factory) -> tuple[Path, int, int]:
    velocity_from, effort_until = JS_BAGS[request.param]
    path = tmp_path_factory.mktemp("js") / f"{request.param}.mcap"
    return _joint_bag(path, velocity_from, effort_until), velocity_from, effort_until


@pytest.mark.parametrize("fmt", ALL_FORMATS)
def test_cli_export_keeps_joint_state_rows_aligned(joint_bag, tmp_path, monkeypatch, fmt):
    """Would catch: velocity.0[0] holding message 150's value next to
    message 0's timestamp (HDF5 / Zarr / npz, exit 0), CSV writing the
    velocity values under the effort header, or Parquet's raw schema
    mismatch."""
    _need(fmt)
    bag, velocity_from, effort_until = joint_bag
    monkeypatch.setattr(export_module, "CHUNK_SIZE", JS_CHUNK)
    out = tmp_path / "out"
    result = CliRunner().invoke(app, [
        "export", str(bag), "-t", "/joint_states", "-f", fmt, "-o", str(out),
    ])
    cols = _read(fmt, out / f"joint_states.{_EXT[fmt]}", "joint_states")

    velocity_reported = fmt in TABLE_FORMATS and velocity_from >= JS_CHUNK
    if velocity_reported:
        # The CSV header / Parquet schema came from the first chunk, which
        # had no velocity: reported, left out, everything else correct.
        assert result.exit_code == 1, result.output
        flat = " ".join(result.output.split())
        assert "velocity.0" in result.output and "velocity.1" in result.output
        assert f"first appears at row {JS_CHUNK}" in flat
        # Parquet can't add a late column either; HDF5 and Zarr can.
        assert "export to Parquet" not in flat
        assert "export to HDF5 or Zarr" in flat
        assert "velocity.0" not in cols and "velocity.1" not in cols
    else:
        assert result.exit_code == 0, result.output

    i = [(t - BASE_NS) // 1_000_000 for t in cols["timestamp_ns"]]
    assert i == list(range(JS_ROWS))
    assert all(len(values) == JS_ROWS for values in cols.values()), {
        c: len(v) for c, v in cols.items()
    }
    assert cols["position.0"] == [n * 0.5 for n in i]
    assert cols["position.1"] == [-float(n) for n in i]
    assert cols["effort.0"] == [5.0 + n if n < effort_until else None for n in i]
    assert cols["effort.1"] == [6.0 if n < effort_until else None for n in i]
    if not velocity_reported:
        assert cols["velocity.0"] == [1000.0 + n if n >= velocity_from else None for n in i]
        assert cols["velocity.1"] == [2000.0 + n if n >= velocity_from else None for n in i]


def test_iter_chunks_keeps_a_field_first_seen_after_row_100(tmp_path):
    """Would catch: polars' default 100-row schema inference dropping
    velocity from the first chunk, so rows 120-149 lose their values."""
    bag = _joint_bag(tmp_path / "js.mcap", velocity_from=120, effort_until=JS_ROWS)
    first = next(BagFrame(bag)["/joint_states"].iter_chunks(chunk_size=JS_CHUNK))
    assert first.height == JS_CHUNK
    assert first["velocity.0"].to_list() == [None] * 120 + [1000.0 + i for i in range(120, 150)]


def test_iter_chunks_keeps_a_field_first_seen_after_row_1000(tmp_path):
    """Would catch: a capped ``infer_schema_length`` (1000, say, to save
    time): exports read 50 000-row chunks, so a field first seen past the
    cap is dropped again."""
    from tests.fixtures.changing_columns import write_joint_state_bag

    bag = write_joint_state_bag(tmp_path / "js.mcap", 2100, range(1500, 2100), 2100)
    [chunk] = list(BagFrame(bag)["/joint_states"].iter_chunks(chunk_size=5000))
    assert chunk.height == 2100
    assert chunk["velocity.0"].to_list() == [None] * 1500 + [1000.0 + i for i in range(1500, 2100)]


# ---------------------------------------------------------------------------
# iter_chunks schema inference on a custom decoder whose fields change
# type and appear late within one chunk.
# ---------------------------------------------------------------------------

MIXED_TYPE = "resurrector_test/msg/Mixed"


def _decode_mixed(data: bytes) -> dict:
    (i,) = struct.unpack_from("<I", data, 4)
    late = i >= 120
    row = {
        "i": i,
        "n": i + 0.5 if late else i,       # int, then float
        "s": "x" if late else None,        # null, then str
        "b": 3 if late else True,          # bool, then int
    }
    if late:
        row["late"] = float(i)             # key first seen at row 120
    return row


@pytest.fixture
def mixed_bag(tmp_path) -> Path:
    from mcap.writer import Writer

    register_decoder(MIXED_TYPE, _decode_mixed)
    path = tmp_path / "mixed.mcap"
    with open(path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="resurrector-test")
        sid = writer.register_schema(name=MIXED_TYPE, encoding="ros2msg", data=b"uint32 i\n")
        cid = writer.register_channel(topic="/mixed", message_encoding="cdr", schema_id=sid)
        for i in range(200):
            t = BASE_NS + i * 1_000_000
            writer.add_message(cid, log_time=t, publish_time=t,
                               data=b"\x00\x01\x00\x00" + struct.pack("<I", i))
        writer.finish()
    yield path
    unregister_decoder(MIXED_TYPE)


def test_iter_chunks_infers_the_schema_from_every_row(mixed_bag):
    """Would catch, with polars' default 100-row inference: ``late``
    dropped, ``n`` truncated to 130 instead of 130.5, ``b`` read as
    Boolean with 3 turned into False, and ``s`` raising ComputeError."""
    [chunk] = list(BagFrame(mixed_bag)["/mixed"].iter_chunks(chunk_size=500))
    assert chunk.height == 200
    assert chunk["n"].dtype == pl.Float64
    assert chunk["n"][130] == 130.5 and chunk["n"][7] == 7.0
    assert chunk["s"].dtype == pl.String
    assert chunk["s"][130] == "x" and chunk["s"][7] is None
    assert chunk["b"].to_list()[118:122] == [1, 1, 3, 3]
    assert chunk["late"].to_list()[118:122] == [None, None, 120.0, 121.0]


def test_ipc_cache_matches_to_polars_when_columns_change(joint_bag):
    """Would catch: ``materialize_ipc_cache`` (the streaming alternative
    LargeTopicError points to) dying with ArrowInvalid "Tried to write
    record batch with different schema" when velocity appears or effort
    disappears between chunks."""
    view = BagFrame(joint_bag[0])["/joint_states"]
    with view.materialize_ipc_cache(chunk_size=JS_CHUNK) as cache:
        got = cache.scan().collect()
    assert_frame_equal(got, view.to_polars(), check_column_order=False)


def test_ipc_cache_widens_a_dtype_like_to_polars(mixed_bag):
    """Chunks of 100 rows: the second adds ``late``, turns ``n`` from int
    to float, ``s`` from Null to String and ``b`` from Boolean to Int64,
    so the cached first chunk is rewritten wider."""
    view = BagFrame(mixed_bag)["/mixed"]
    with view.materialize_ipc_cache(chunk_size=100) as cache:
        got = cache.scan().collect()
    want = view.to_polars()
    assert_frame_equal(got, want, check_column_order=False)
    assert got["n"].dtype == pl.Float64 and got["n"][7] == 7.0


def test_ipc_cache_removes_its_files_when_it_fails(mixed_bag, tmp_path, monkeypatch):
    import tempfile

    from resurrector.core import bag_frame as bag_frame_module

    def broken(src, dst, schema):
        dst.write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(bag_frame_module, "_rewrite_ipc", broken)
    with pytest.raises(OSError, match="disk full"):
        BagFrame(mixed_bag)["/mixed"].materialize_ipc_cache(chunk_size=100)
    assert list(tmp_path.glob("resurrector_*")) == []


# ---------------------------------------------------------------------------
# Writer level, every format: a known column missing from a chunk, a
# column that appears late, reordered columns.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", ALL_FORMATS)
def test_known_column_missing_from_a_chunk_is_missing_there(fmt, tmp_path):
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2], "x": [1.0, 2.0], "s": ["a", "b"]}),
        pl.DataFrame({"timestamp_ns": [3]}),
        pl.DataFrame({"timestamp_ns": [4, 5], "x": [4.0, 5.0], "s": ["d", None]}),
    ]
    cols, failures = _write(fmt, chunks, tmp_path)
    assert failures == []
    gap, null_s = _missing(fmt, "float"), _missing(fmt, "str")
    assert cols["timestamp_ns"] == [1, 2, 3, 4, 5]
    assert cols["x"] == [1.0, 2.0, gap, 4.0, 5.0]
    assert cols["s"] == ["a", "b", null_s, "d", null_s]


@pytest.mark.parametrize("fmt", ALL_FORMATS)
def test_reordered_columns_land_under_their_own_header(fmt, tmp_path):
    """Would catch: CSV writing a later chunk's columns in that chunk's
    order under the first chunk's header."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [1], "x": [1.0], "y": [10.0]}),
        pl.DataFrame({"y": [20.0], "timestamp_ns": [2], "x": [2.0]}),
    ]
    cols, failures = _write(fmt, chunks, tmp_path)
    assert failures == []
    assert cols["x"] == [1.0, 2.0] and cols["y"] == [10.0, 20.0]


@pytest.mark.parametrize("fmt", ARRAY_FORMATS)
def test_column_first_seen_later_is_back_filled(fmt, tmp_path):
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2]}),
        pl.DataFrame({"timestamp_ns": [3], "x": [3.0], "s": ["c"]}),
        pl.DataFrame({"timestamp_ns": [4], "x": [4.0]}),
    ]
    cols, failures = _write(fmt, chunks, tmp_path)
    assert failures == []
    assert cols["x"] == [None, None, 3.0, 4.0]
    assert cols["s"] == ["", "", "c", ""]


@pytest.mark.parametrize("fmt", ARRAY_FORMATS)
def test_untyped_column_absent_from_a_chunk_stays_aligned(fmt, tmp_path):
    """Would catch: a column still held (null in every row so far, so its
    dtype is unknown) getting no held rows for a chunk that lacks it. It
    then ends a row short and is failed and removed ("3 rows written
    where the export has 4")."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2], "x": [None, None]}),
        pl.DataFrame({"timestamp_ns": [3]}),
        pl.DataFrame({"timestamp_ns": [4], "x": [1.0]}),
    ]
    cols, failures = _write(fmt, chunks, tmp_path)
    assert failures == []
    assert cols["timestamp_ns"] == [1, 2, 3, 4]
    assert cols["x"] == [None, None, None, 1.0]


def _dtype(fmt: str, path: Path, col: str) -> np.dtype:
    if fmt == "hdf5":
        import h5py
        with h5py.File(path, "r") as f:
            return f["t"][col].dtype
    if fmt == "numpy":
        with np.load(path) as data:
            return data[col].dtype
    import zarr
    return zarr.open_group(str(path), mode="r")[col].dtype


@pytest.mark.parametrize("fmt", ARRAY_FORMATS)
@pytest.mark.parametrize("case", ["absent-later", "absent-first", "null-first"])
def test_float32_fill_stays_float32(fmt, tmp_path, case):
    """Would catch: Float32's missing value written as float64 NaN. HDF5
    and Zarr then create a back-filled column as float64, and ``.npz``
    promotes the joined column to float64."""
    def f32(values):
        return pl.Series("x", values, dtype=pl.Float32)

    chunks = {
        "absent-later": [
            pl.DataFrame({"timestamp_ns": [1, 2], "x": f32([1.0, 2.0])}),
            pl.DataFrame({"timestamp_ns": [3]}),
        ],
        "absent-first": [
            pl.DataFrame({"timestamp_ns": [1, 2]}),
            pl.DataFrame({"timestamp_ns": [3], "x": f32([3.0])}),
        ],
        "null-first": [
            pl.DataFrame({"timestamp_ns": [1, 2], "x": [None, None]}),
            pl.DataFrame({"timestamp_ns": [3], "x": f32([3.0])}),
        ],
    }[case]
    cols, failures = _write(fmt, chunks, tmp_path)
    assert failures == []
    assert _dtype(fmt, tmp_path / f"t.{_EXT[fmt]}", "x") == np.float32
    want = [1.0, 2.0, None] if case == "absent-later" else [None, None, 3.0]
    assert cols["x"] == want


@pytest.mark.parametrize("fmt", TABLE_FORMATS)
def test_column_first_seen_later_is_reported(fmt, tmp_path):
    """The header / schema can't grow, so the column is reported and the
    rest of the file is still right."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2], "y": [1.0, 2.0]}),
        pl.DataFrame({"timestamp_ns": [3], "y": [3.0], "x": [3.0]}),
        pl.DataFrame({"timestamp_ns": [4], "x": [4.0], "y": [4.0]}),
    ]
    cols, failures = _write(fmt, chunks, tmp_path)
    assert [(f.column, f.error_type) for f in failures] == [("x", "ValueError")]
    assert "first appears at row 2" in failures[0].message
    assert "not in the file" in failures[0].message
    assert cols == {"timestamp_ns": [1, 2, 3, 4], "y": [1.0, 2.0, 3.0, 4.0]}


@pytest.mark.parametrize("fmt", TABLE_FORMATS)
def test_new_column_in_a_zero_row_chunk_is_not_reported(fmt, tmp_path):
    """It carries no values, so nothing is lost by leaving it out."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [1]}),
        pl.DataFrame(schema={"timestamp_ns": pl.Int64, "x": pl.Float64}),
        pl.DataFrame({"timestamp_ns": [2]}),
    ]
    cols, failures = _write(fmt, chunks, tmp_path)
    assert failures == []
    assert cols == {"timestamp_ns": [1, 2]}


@pytest.mark.parametrize("fmt", ALL_FORMATS)
def test_random_column_dropouts_stay_aligned(fmt, tmp_path):
    """Chunks with random subsets of the columns, in random order: every
    value lands on its own row in every format."""
    rng = np.random.default_rng(7)
    dtypes = {"timestamp_ns": pl.Int64, "x": pl.Float64, "s": pl.String}
    chunks, start = [], 0
    for _ in range(12):
        n = int(rng.integers(0, 6))
        ts = list(range(start, start + n))
        start += n
        data = {"timestamp_ns": ts}
        if rng.random() < 0.6:
            data["x"] = [float(t) for t in ts]
        if rng.random() < 0.6:
            data["s"] = [f"v{t}" for t in ts]
        order = list(data)
        rng.shuffle(order)
        chunks.append(pl.DataFrame({c: data[c] for c in order},
                                   schema={c: dtypes[c] for c in order}))
    expected = pl.concat(chunks, how="diagonal_relaxed")
    first = set(chunks[0].columns)
    cols, failures = _write(fmt, chunks, tmp_path)

    rows = expected["timestamp_ns"].to_list()
    assert cols["timestamp_ns"] == rows
    for col, kind in [("x", "float"), ("s", "str")]:
        if fmt in TABLE_FORMATS and col not in first:
            has_rows = any(col in c.columns and c.height for c in chunks)
            assert (col in {f.column for f in failures}) == has_rows
            assert col not in cols
            continue
        want = [v if v is not None else _missing(fmt, kind) for v in expected[col].to_list()]
        assert cols[col] == want, col
    assert all(len(v) == len(rows) for v in cols.values())


# ---------------------------------------------------------------------------
# Array writers: the fill is bounded, and a dtype with no missing value
# fails cleanly instead of shifting rows.
# ---------------------------------------------------------------------------


def test_fill_rows_are_appended_in_chunk_sized_pieces(monkeypatch):
    """Back-fill for a late column and the fill for a chunk that lacks a
    known column are both written in pieces of at most CHUNK_SIZE rows."""
    monkeypatch.setattr(export_module, "CHUNK_SIZE", 4)
    chunks = [pl.DataFrame({"timestamp_ns": list(range(i * 5, i * 5 + 5))}) for i in range(2)]
    chunks.append(pl.DataFrame({"timestamp_ns": [10, 11], "x": [1.0, 2.0]}))
    chunks.append(pl.DataFrame({"timestamp_ns": list(range(12, 21))}))
    appended: list[tuple[str, int]] = []
    rows, failures = _write_numpy_columns(
        iter(chunks), lambda col, arr, text: appended.append((col, len(arr))),
    )
    assert (rows, failures) == (21, [])
    assert [n for col, n in appended if col == "x"] == [4, 4, 2, 2, 4, 4, 1]


@pytest.mark.parametrize("fmt", ARRAY_FORMATS)
@pytest.mark.parametrize("case", ["absent-later", "first-seen-later", "null-later"])
def test_column_without_a_missing_value_fails_cleanly(fmt, tmp_path, case):
    """A Datetime column has no missing value here (NaT is not written),
    so rows it lacks can't be filled: it fails and is left out, and the
    other columns are complete. HDF5 rejects datetimes in any case."""
    d = pl.Series("d", [1, 2], dtype=pl.Datetime("us"))
    chunks = {
        "absent-later": [
            pl.DataFrame({"timestamp_ns": [1, 2], "d": d, "x": [1.0, 2.0]}),
            pl.DataFrame({"timestamp_ns": [3], "x": [3.0]}),
        ],
        "first-seen-later": [
            pl.DataFrame({"timestamp_ns": [1, 2], "x": [1.0, 2.0]}),
            pl.DataFrame({"timestamp_ns": [3, 4], "d": d, "x": [3.0, 4.0]}),
        ],
        "null-later": [
            pl.DataFrame({"timestamp_ns": [1, 2], "d": d, "x": [1.0, 2.0]}),
            pl.DataFrame({"timestamp_ns": [3], "d": [None], "x": [3.0]}),
        ],
    }[case]
    cols, failures = _write(fmt, chunks, tmp_path)
    assert [f.column for f in failures] == ["d"]
    assert "d" not in cols
    rows = sum(c.height for c in chunks)
    assert cols["timestamp_ns"] == list(range(1, rows + 1))
    assert cols["x"] == [float(r) for r in range(1, rows + 1)]


@pytest.mark.parametrize("fmt", ["zarr", "numpy"])
def test_zero_row_chunk_without_a_column_needs_no_fill(fmt, tmp_path):
    """A 0-row chunk that lacks a column has no rows to fill, so even a
    column with no missing value (Datetime) is fine."""
    def d(values):
        return pl.Series("d", values, dtype=pl.Datetime("us"))

    chunks = [
        pl.DataFrame({"timestamp_ns": [1], "d": d([1])}),
        pl.DataFrame(schema={"timestamp_ns": pl.Int64}),
        pl.DataFrame({"timestamp_ns": [2], "d": d([2])}),
    ]
    cols, failures = _write(fmt, chunks, tmp_path)
    assert failures == []
    assert cols["timestamp_ns"] == [1, 2]
    assert len(cols["d"]) == 2


@pytest.mark.parametrize("fmt", ARRAY_FORMATS)
def test_timestamp_missing_from_a_chunk_fails_it_not_the_rest(fmt, tmp_path):
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2], "x": [1.0, 2.0]}),
        pl.DataFrame({"x": [3.0]}),
    ]
    cols, failures = _write(fmt, chunks, tmp_path)
    [failure] = failures
    assert failure.column == "timestamp_ns"
    assert "absent from rows 2 to 2" in failure.message
    assert "timestamp_ns" not in cols
    assert cols["x"] == [1.0, 2.0, 3.0]


def test_every_column_must_end_with_rows_written_rows(monkeypatch):
    """The final check: a column whose appends fall short of the row
    count is failed, not left short. Simulated by a converter that drops
    one array."""
    real = export_module._NumpyColumns.pad

    def short_pad(self, col, rows, start):
        arrays, failure = real(self, col, rows, start)
        return iter(()), failure

    monkeypatch.setattr(export_module._NumpyColumns, "pad", short_pad)
    chunks = [
        pl.DataFrame({"timestamp_ns": [1], "x": [1.0]}),
        pl.DataFrame({"timestamp_ns": [2]}),
    ]
    rows, failures = _write_numpy_columns(iter(chunks), lambda *a: None)
    assert rows == 2
    assert [(f.column, f.message) for f in failures] == [
        ("x", "1 rows written where the export has 2"),
    ]


# ---------------------------------------------------------------------------
# 0-row chunks and Zarr chunk shape.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", ARRAY_FORMATS)
def test_zero_row_chunks_still_create_their_columns(fmt, tmp_path):
    schema = {"timestamp_ns": pl.Int64, "s": pl.String, "x": pl.Float64, "n": pl.Null}
    cols, failures = _write(fmt, [pl.DataFrame(schema=schema)] * 2, tmp_path)
    assert failures == []
    assert cols == {"timestamp_ns": [], "s": [], "x": [], "n": []}


def test_zero_row_arrays_are_appended_only_to_create_a_column():
    """``append`` sees a 0-row array only at the end, once per column that
    never got a row; a column that did is created by its first real rows."""
    schema = {"timestamp_ns": pl.Int64, "s": pl.String}
    chunks = [
        pl.DataFrame(schema=schema),
        pl.DataFrame({"timestamp_ns": [1], "s": ["a"]}),
        pl.DataFrame(schema={**schema, "e": pl.Float64}),
    ]
    appended: list[tuple[str, int]] = []
    rows, failures = _write_numpy_columns(
        iter(chunks), lambda col, arr, text: appended.append((col, len(arr))),
    )
    assert (rows, failures) == (1, [])
    assert appended == [("timestamp_ns", 1), ("s", 1), ("e", 1)]

    appended.clear()
    rows, failures = _write_numpy_columns(
        iter([pl.DataFrame(schema=schema)]),
        lambda col, arr, text: appended.append((col, len(arr))),
    )
    assert (rows, failures, appended) == (0, [], [("timestamp_ns", 0), ("s", 0)])


@pytest.mark.parametrize("case", ["typed-by-a-tiny-tail", "empty-first-chunk", "small-first-chunk"])
def test_zarr_chunk_shape_does_not_follow_the_typing_chunk(tmp_path, monkeypatch, case):
    """Would catch: a 3-row chunk typing a column held through 100 null
    rows giving it 3-row Zarr chunks (34 files instead of 3), or an empty
    first chunk giving 1-row chunks."""
    zarr = pytest.importorskip("zarr")
    monkeypatch.setattr(export_module, "CHUNK_SIZE", 50)
    if case == "typed-by-a-tiny-tail":
        chunks = [pl.DataFrame({"timestamp_ns": list(range(i * 50, i * 50 + 50)),
                                "x": [None] * 50, "s": [None] * 50}) for i in range(2)]
        chunks.append(pl.DataFrame({"timestamp_ns": [100, 101, 102],
                                    "x": [1.0, 2.0, 3.0], "s": ["a", "b", "c"]}))
    elif case == "empty-first-chunk":
        chunks = [pl.DataFrame(schema={"timestamp_ns": pl.Int64, "x": pl.Float64, "s": pl.String})]
        chunks.append(pl.DataFrame({"timestamp_ns": list(range(103)),
                                    "x": [1.0] * 103, "s": ["a"] * 103}))
    else:
        chunks = [pl.DataFrame({"timestamp_ns": [0], "x": [1.0], "s": ["a"]})]
        chunks.append(pl.DataFrame({"timestamp_ns": list(range(1, 103)),
                                    "x": [1.0] * 102, "s": ["a"] * 102}))
    _stream_zarr(iter(chunks), tmp_path, "t")
    group = zarr.open_group(str(tmp_path / "t.zarr"), mode="r")
    for col in ["timestamp_ns", "x", "s"]:
        assert group[col].shape == (103,), col
        assert group[col].chunks == (50,), col
        # At most 3: zarr may skip a chunk that is all fill value ("").
        chunk_files = [p for p in (tmp_path / "t.zarr" / col).rglob("*")
                       if p.is_file() and not p.name.startswith((".z", "zarr.json"))]
        assert len(chunk_files) <= 3, (col, len(chunk_files))
    if case == "typed-by-a-tiny-tail":
        assert np.isnan(group["x"][:100]).all()
        assert group["x"][100:].tolist() == [1.0, 2.0, 3.0]
        assert group["s"][:].tolist() == [""] * 100 + ["a", "b", "c"]
    else:
        assert group["x"][:].tolist() == [1.0] * 103
        assert group["s"][:].tolist() == ["a"] * 103


# ---------------------------------------------------------------------------
# Parquet: a later chunk's type is cast only when no value changes.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("first,later,want", [
    (pl.Series("v", [1], dtype=pl.Int64), pl.Series("v", [3.0]), [1, 3]),
    (pl.Series("v", [1.5]), pl.Series("v", [3], dtype=pl.Int64), [1.5, 3.0]),
    (pl.Series("v", [None], dtype=pl.Float64), pl.Series("v", [None], dtype=pl.Null), [None, None]),
    (pl.Series("v", ["a"]), pl.Series("v", ["b"], dtype=pl.Categorical), ["a", "b"]),
], ids=["int-then-whole-float", "float-then-int", "float-then-null", "string-then-categorical"])
def test_parquet_casts_a_later_type_when_lossless(tmp_path, first, later, want):
    chunks = [pl.DataFrame({"timestamp_ns": [1], "v": first}),
              pl.DataFrame({"timestamp_ns": [2], "v": later})]
    cols, failures = _write("parquet", chunks, tmp_path)
    assert failures == []
    assert cols["v"] == want


@pytest.mark.parametrize("first,later", [
    (pl.Series("v", [1], dtype=pl.Int64), pl.Series("v", [2.5])),
    (pl.Series("v", [1.0], dtype=pl.Float32), pl.Series("v", [16777217.0])),
    (pl.Series("v", [True]), pl.Series("v", [3], dtype=pl.Int64)),
    (pl.Series("v", [None], dtype=pl.Null), pl.Series("v", [2.0])),
    (pl.Series("v", ["a"]), pl.Series("v", [[1, 2]])),
    (pl.Series("v", ["a"]), pl.Series("v", [7], dtype=pl.Int64)),
    (pl.Series("v", [1.5]), pl.Series("v", ["2.5"])),
    (pl.Series("v", [True]), pl.Series("v", ["true"])),
], ids=["int-then-fraction", "float32-then-wider-float64", "bool-then-int",
        "null-then-float", "string-then-list", "string-then-int",
        "float-then-numeric-text", "bool-then-text"])
def test_parquet_reports_a_lossy_later_type(tmp_path, first, later):
    """Would catch: pyarrow's ``safe`` cast letting 16777217.0 become
    16777216.0 or 3 become True, or a raw ArrowInvalid / schema-mismatch
    error. Text and non-text never convert into each other, as in the
    HDF5 / Zarr / npz writers: Int64 7 in a String column was stored as
    "7", and "2.5" in a Float64 one as 2.5. The column is null from the
    failing chunk on (the next chunk, which would cast fine, too); every
    other column is complete."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [1], "v": first, "y": [1.0]}),
        pl.DataFrame({"timestamp_ns": [2], "v": later, "y": [2.0]}),
        pl.DataFrame({"timestamp_ns": [3], "v": first, "y": [3.0]}),
    ]
    cols, failures = _write("parquet", chunks, tmp_path)
    assert [(f.column, f.error_type) for f in failures] == [("v", "TypeError")]
    assert "from row 1 on" in failures[0].message
    assert cols["timestamp_ns"] == [1, 2, 3] and cols["y"] == [1.0, 2.0, 3.0]
    assert cols["v"][1:] == [None, None]
    assert pq.read_metadata(tmp_path / "t.parquet").num_rows == 3


# ---------------------------------------------------------------------------
# .npz text: one fixed-width copy, and a warning when it gets big.
# ---------------------------------------------------------------------------


def test_npz_text_is_built_once_and_warns_when_large(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(export_module, "_NPZ_TEXT_WARN_BYTES", 1000)
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2], "s": ["a", None], "short": ["x", "y"]}),
        pl.DataFrame({"timestamp_ns": [3], "s": ["é" * 300], "short": ["z"]}),
    ]
    with caplog.at_level("WARNING", logger="resurrector.core.export"):
        _stream_numpy(iter(chunks), tmp_path, "t")
    with np.load(tmp_path / "t.npz") as data:
        assert data["s"].dtype == np.dtype("<U300")
        assert data["s"].tolist() == ["a", "", "é" * 300]
        assert data["short"].dtype == np.dtype("<U1")
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "'s'" in warnings[0] and "Parquet" in warnings[0] and "Zarr" in warnings[0]
