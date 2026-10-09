"""String columns in the HDF5, Zarr and NumPy exports.

Every stamped ROS topic has a ``header.frame_id`` String column, and
camera topics add ``format`` / ``encoding``. Zarr export used to refuse
every String column, so ``-f zarr`` and the ``multimodal`` preset failed
on nearly every real topic. HDF5 wrote a null string as the text
"None", and ``.npz`` held strings in object arrays that ``np.load``
won't read without ``allow_pickle=True``.

One policy now, for all three: strings are written as text, and a
missing string as ``""`` (none of the formats has a string null).

The Zarr tests skip when zarr isn't installed; CI's ``extras-test
(all-exports)`` job fails if any test here skips.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from typer.testing import CliRunner

from resurrector.cli.main import app
from resurrector.core import export as export_module
from resurrector.core import sync as sync_module
from resurrector.core.bag_frame import BagFrame
from resurrector.core.export import (
    ExportError,
    Exporter,
    _stream_hdf5,
    _stream_numpy,
    _stream_zarr,
)
from resurrector.demo.sample_bag import BagConfig, generate_bag

STAMPED_TOPICS = [
    "/camera/compressed", "/camera/rgb", "/imu/data", "/joint_states", "/lidar/scan",
]
# Topic -> its String columns in the demo bag. Checked against the bag, so
# a parser change that drops one fails here instead of weakening the tests.
TOPIC_STRINGS = {
    "/camera/compressed": {"header.frame_id", "format"},
    "/camera/rgb": {"header.frame_id", "encoding"},
    "/imu/data": {"header.frame_id"},
    "/joint_states": {"header.frame_id"},
    "/lidar/scan": {"header.frame_id"},
}
SYNC_TOPICS = ["/imu/data", "/joint_states", "/camera/rgb"]


@pytest.fixture(scope="module")
def demo_bag(tmp_path_factory) -> Path:
    """The demo bag, small. ``/tf`` is registered but has no messages."""
    path = tmp_path_factory.mktemp("strings") / "demo.mcap"
    return generate_bag(path, BagConfig(
        duration_sec=2.0, imu_hz=100.0, joint_hz=50.0, camera_hz=5.0,
        lidar_hz=5.0, compressed_hz=5.0,
    ))


@pytest.fixture(scope="module")
def gap_bag(tmp_path_factory) -> Path:
    """``/joint_states`` stops for a second, so a sync anchored on
    ``/imu/data`` has unmatched rows: null strings in the synced table."""
    path = tmp_path_factory.mktemp("strings_gap") / "gap.mcap"
    return generate_bag(path, BagConfig(
        duration_sec=3.0, imu_hz=50.0, joint_hz=50.0, camera_hz=5.0,
        lidar_hz=2.0, include_compressed=False, include_tf=False,
        time_gap=True, gap_topic="/joint_states",
        gap_start_sec=1.0, gap_duration_sec=1.0,
    ))


@pytest.fixture(params=["hdf5", "zarr", "numpy"])
def fmt(request) -> str:
    if request.param == "zarr":
        pytest.importorskip("zarr")
    return request.param


@pytest.fixture
def zarr_installed():
    return pytest.importorskip("zarr")


def _read_zarr(path: Path) -> dict[str, list]:
    import zarr
    group = zarr.open(str(path), mode="r")
    return {name: group[name][:].tolist() for name in group.array_keys()}


def _read_hdf5(path: Path, name: str) -> dict[str, list]:
    import h5py
    out = {}
    with h5py.File(path, "r") as f:
        for col, ds in f[name].items():
            text = h5py.check_string_dtype(ds.dtype) is not None
            out[col] = (ds.asstr()[:] if text else ds[:]).tolist()
    return out


def _read_npz(path: Path) -> dict[str, list]:
    # np.load's default allow_pickle=False: a string column stored as an
    # object array raises here.
    with np.load(path) as data:
        return {col: data[col].tolist() for col in data.files}


def _read(fmt: str, out: Path, name: str) -> dict[str, list]:
    if fmt == "zarr":
        return _read_zarr(out / f"{name}.zarr")
    if fmt == "hdf5":
        return _read_hdf5(out / f"{name}.h5", name)
    return _read_npz(out / f"{name}.npz")


def _dtype(fmt: str, out: Path, name: str, col: str) -> np.dtype:
    if fmt == "zarr":
        import zarr
        return zarr.open(str(out / f"{name}.zarr"), mode="r")[col].dtype
    if fmt == "hdf5":
        import h5py
        with h5py.File(out / f"{name}.h5", "r") as f:
            return f[name][col].dtype
    with np.load(out / f"{name}.npz") as data:
        return data[col].dtype


_WRITERS = {"hdf5": _stream_hdf5, "zarr": _stream_zarr, "numpy": _stream_numpy}


def _write(fmt: str, chunks: list[pl.DataFrame], out: Path) -> dict[str, list]:
    _WRITERS[fmt](iter(chunks), out, "t")
    return _read(fmt, out, "t")


def _same(got: list, want: list) -> bool:
    """Element-wise equality with NaN == NaN."""
    return len(got) == len(want) and all(
        g == w or (isinstance(g, float) and isinstance(w, float) and np.isnan(g) and np.isnan(w))
        for g, w in zip(got, want)
    )


# ---------------------------------------------------------------------------
# Whole-bag exports: every stamped topic, the multimodal preset, --sync.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("topic", STAMPED_TOPICS)
def test_zarr_exports_every_stamped_topic_with_its_strings(
    demo_bag, tmp_path, zarr_installed, topic,
):
    """Would catch: the 0.8.5 Zarr writer refusing String columns, which
    raised ExportError naming header.frame_id on every one of these."""
    bf = BagFrame(demo_bag)
    Exporter().export(bag_frame=bf, topics=[topic], format="zarr", output_dir=str(tmp_path))

    source = bf[topic].to_polars()
    strings = {c for c, dt in source.schema.items() if dt == pl.String}
    assert strings == TOPIC_STRINGS[topic]
    got = _read_zarr(tmp_path / f"{topic.lstrip('/').replace('/', '_')}.zarr")
    assert set(got) == set(source.columns)
    for col in source.columns:
        if col in strings:
            assert got[col] == source[col].to_list(), col
        else:
            np.testing.assert_array_equal(
                got[col], source[col].cast(pl.Float64).to_numpy()
                if col != "timestamp_ns" else source[col].to_numpy(),
                err_msg=col,
            )


def test_cli_zarr_export_of_a_stamped_topic_succeeds(demo_bag, tmp_path, zarr_installed):
    """The reported repro: ``resurrector export bag.mcap -t /imu/data -f zarr``
    exited 1 naming header.frame_id."""
    out = tmp_path / "cli"
    result = CliRunner().invoke(app, [
        "export", str(demo_bag), "-t", "/imu/data", "-f", "zarr", "-o", str(out),
    ])
    assert result.exit_code == 0, result.output
    frame_ids = set(_read_zarr(out / "imu_data.zarr")["header.frame_id"])
    assert frame_ids == {"imu_link"}


def test_empty_topic_exports_without_crashing(demo_bag, tmp_path, fmt):
    """``/tf`` is in the demo bag with no messages."""
    bf = BagFrame(demo_bag)
    assert bf["/tf"].message_count == 0
    Exporter().export(bag_frame=bf, topics=["/tf"], format=fmt, output_dir=str(tmp_path))
    assert _read(fmt, tmp_path, "tf") == {}


def test_multimodal_preset_writes_strings(demo_bag, tmp_path, zarr_installed):
    """The multimodal preset is Zarr + sync over every topic. Would
    catch: the 0.8.5 failure on its 7 string columns."""
    bf = BagFrame(demo_bag)
    bf.export(preset="multimodal", output=str(tmp_path))

    expected = bf.sync(bf.topic_names, method="nearest")
    strings = [c for c, dt in expected.schema.items() if dt == pl.String]
    assert len(strings) == 7
    got = _read_zarr(tmp_path / "synced.zarr")
    assert set(got) == set(expected.columns)
    for col in strings:
        assert got[col] == expected[col].fill_null("").to_list(), col


@pytest.mark.parametrize("engine", ["eager", "streaming"])
def test_synced_export_strings_with_unmatched_rows(
    gap_bag, tmp_path, monkeypatch, fmt, engine,
):
    """``--sync nearest``: unmatched rows leave the other topics' strings
    null, written as ``""``. Small chunks, so nulls start and stop
    mid-export on both sync engines.

    Would catch: HDF5 writing those rows as "None", ``.npz`` needing
    pickle to load them, or Zarr refusing the columns.
    """
    monkeypatch.setattr(export_module, "CHUNK_SIZE", 16)
    if engine == "streaming":
        monkeypatch.setattr(sync_module, "LARGE_TOPIC_THRESHOLD", 5)
    bf = BagFrame(gap_bag)
    expected = bf.sync(SYNC_TOPICS, method="nearest", engine="eager")
    strings = [c for c, dt in expected.schema.items() if dt == pl.String]
    assert "joint_states__header.frame_id" in strings
    assert expected["joint_states__header.frame_id"].null_count() > 0
    assert expected["joint_states__header.frame_id"].drop_nulls().len() > 0

    Exporter().export(
        bag_frame=bf, topics=SYNC_TOPICS, format=fmt, output_dir=str(tmp_path),
        sync=True, sync_method="nearest",
    )
    got = _read(fmt, tmp_path, "synced")
    assert set(got) == set(expected.columns)
    assert got["timestamp_ns"] == expected["timestamp_ns"].to_list()
    for col in strings:
        assert got[col] == expected[col].fill_null("").to_list(), col
        assert "None" not in got[col], col


# ---------------------------------------------------------------------------
# Writer level: null policy, dtype changes between chunks, columns that
# can't be stored.
# ---------------------------------------------------------------------------


def test_null_strings_are_written_as_empty(fmt, tmp_path):
    """Would catch: HDF5's ``arr.astype(str)`` turning None into "None",
    and ``.npz`` object arrays that np.load refuses without pickle."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2, 3], "s": ["a", None, "é日本"]}),
        pl.DataFrame({"timestamp_ns": [4, 5], "s": [None, ""]}),
    ]
    got = _write(fmt, chunks, tmp_path)
    assert got["s"] == ["a", "", "é日本", "", ""]
    assert got["timestamp_ns"] == [1, 2, 3, 4, 5]


def test_later_chunk_with_longer_strings_round_trips(fmt, tmp_path):
    """The first chunk's strings are 1 character; later ones are wider,
    multi-byte UTF-8, 300 characters long, or outside the BMP. Would
    catch: an array whose string width is fixed by the first chunk
    (a ``<U1`` Zarr array stored ``['a', '', 'b', 'x']``)."""
    later = ["base_link_é日本", "x" * 300, "emoji \U0001F916"]
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2], "s": ["a", None]}),
        pl.DataFrame({"timestamp_ns": [3, 4, 5], "s": later}),
    ]
    got = _write(fmt, chunks, tmp_path)
    assert got["s"] == ["a", ""] + later


def test_zarr_strings_are_stored_as_vlen_utf8(tmp_path, zarr_installed):
    """The on-disk format, not just what zarr reads back: variable-length
    UTF-8. Would catch: a pickle object codec on zarr 2 (reads back the
    same through zarr, but only Python can read it), or a fixed-width
    dtype on zarr 3."""
    import json

    _write("zarr", [pl.DataFrame({"timestamp_ns": [1], "s": ["é日本"]})], tmp_path)
    meta_dir = tmp_path / "t.zarr" / "s"
    if (meta_dir / "zarr.json").exists():
        meta = json.loads((meta_dir / "zarr.json").read_text())
        assert meta["zarr_format"] == 3
        assert meta["data_type"] == "string"
        assert {"name": "vlen-utf8", "configuration": {}} in meta["codecs"]
    else:
        meta = json.loads((meta_dir / ".zarray").read_text())
        assert meta["zarr_format"] == 2
        assert meta["dtype"] == "|O"
        assert meta["filters"] == [{"id": "vlen-utf8"}]


def test_npz_strings_load_without_pickle(tmp_path):
    _stream_numpy(iter([pl.DataFrame({"timestamp_ns": [1, 2], "s": ["ab", None]})]),
                  tmp_path, "t")
    with np.load(tmp_path / "t.npz") as data:
        assert data["s"].dtype.kind == "U"
        assert data["s"].tolist() == ["ab", ""]


def test_categorical_and_enum_columns_are_text(fmt, tmp_path):
    chunks = [pl.DataFrame({
        "timestamp_ns": [1, 2, 3],
        "cat": pl.Series(["x", None, "y"], dtype=pl.Categorical),
        "enum": pl.Series(["lo", "hi", None], dtype=pl.Enum(["lo", "hi"])),
    })]
    got = _write(fmt, chunks, tmp_path)
    assert got["cat"] == ["x", "", "y"]
    assert got["enum"] == ["lo", "hi", ""]


@pytest.mark.parametrize("order", ["null-first", "null-last"])
def test_all_null_chunk_before_or_after_strings(fmt, tmp_path, order):
    """A chunk where the column is all null has polars dtype Null, not
    String. Would catch: the column's array typed from that chunk (as
    float32 NaN) so the later strings fail, or a Null chunk after the
    strings failing the column."""
    nulls = pl.DataFrame({"timestamp_ns": [1, 2], "s": [None, None]})
    text = pl.DataFrame({"timestamp_ns": [3, 4], "s": ["a", "b"]})
    assert nulls.schema["s"] == pl.Null
    if order == "null-first":
        got = _write(fmt, [nulls, text], tmp_path)
        assert got["s"] == ["", "", "a", "b"]
    else:
        got = _write(fmt, [text, nulls], tmp_path)
        assert got["s"] == ["a", "b", "", ""]


def test_all_null_chunks_before_a_float_column_keep_float64(fmt, tmp_path, monkeypatch):
    """Rows held back for an all-null start are written in bounded pieces
    once the dtype is known. Would catch: the column typed float32 from
    the Null chunk (0.8.5), rounding 16777217.0 to 16777216.0."""
    monkeypatch.setattr(export_module, "CHUNK_SIZE", 3)
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2, 3, 4], "x": [None] * 4}),
        pl.DataFrame({"timestamp_ns": [5, 6, 7], "x": [None] * 3}),
        pl.DataFrame({"timestamp_ns": [8, 9], "x": [0.1, 16777217.0]}),
    ]
    got = _write(fmt, chunks, tmp_path)
    assert _same(got["x"], [np.nan] * 7 + [0.1, 16777217.0])
    assert _dtype(fmt, tmp_path, "t", "x") == np.float64


def test_held_null_rows_are_appended_in_chunk_sized_pieces(monkeypatch):
    """A long all-null start is written back in pieces of at most
    CHUNK_SIZE rows, so memory stays bounded by the chunk size."""
    monkeypatch.setattr(export_module, "CHUNK_SIZE", 4)
    chunks = [pl.DataFrame({"s": [None] * 4}) for _ in range(5)]
    chunks.append(pl.DataFrame({"s": ["a", "b"]}))
    appended: list[tuple[str, int, bool]] = []
    rows, failures = export_module._write_numpy_columns(
        iter(chunks), lambda col, arr, text: appended.append((col, len(arr), text)),
    )
    assert (rows, failures) == (22, [])
    assert appended == [("s", 4, True)] * 5 + [("s", 2, True)]


def test_column_null_in_every_chunk_is_float64_nan(fmt, tmp_path):
    """Its dtype is never known, so it takes the numeric missing value.
    Would catch: float32 (polars' Null conversion) in one format and not
    another."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [1, 2], "s": [None, None]}),
        pl.DataFrame({"timestamp_ns": [3], "s": [None]}),
    ]
    got = _write(fmt, chunks, tmp_path)
    assert _same(got["s"], [np.nan] * 3)
    assert _dtype(fmt, tmp_path, "t", "s") == np.float64
    assert got["timestamp_ns"] == [1, 2, 3]


def test_empty_chunk_between_string_chunks(fmt, tmp_path):
    """Would catch: HDF5 assigning a 0-row chunk to ``ds[-0:]`` (the whole
    dataset) and Zarr sizing chunks as 0 from an empty first chunk."""
    schema = {"timestamp_ns": pl.Int64, "s": pl.String}
    chunks = [
        pl.DataFrame(schema=schema),
        pl.DataFrame({"timestamp_ns": [1], "s": ["a"]}),
        pl.DataFrame(schema=schema),
        pl.DataFrame({"timestamp_ns": [2], "s": ["b"]}),
    ]
    got = _write(fmt, chunks, tmp_path)
    assert got["s"] == ["a", "b"]


@pytest.mark.parametrize("chunks", [
    [
        pl.DataFrame({"timestamp_ns": [1], "s": ["a"], "y": [1.0]}),
        pl.DataFrame({"timestamp_ns": [2], "s": [2.5], "y": [2.0]}),
    ],
    [
        pl.DataFrame({"timestamp_ns": [1], "s": [None], "y": [1.0]}),
        pl.DataFrame({"timestamp_ns": [2], "s": ["a"], "y": [2.0]}),
        pl.DataFrame({"timestamp_ns": [3], "s": [7], "y": [3.0]}),
    ],
], ids=["string-then-float", "null-string-then-int"])
def test_string_column_that_changes_dtype_fails_cleanly(fmt, tmp_path, chunks):
    """A later chunk can't quietly turn a string column into "2.5". The
    failed column is removed from the file rather than left truncated."""
    with pytest.raises(ExportError) as exc:
        _write(fmt, chunks, tmp_path)
    [failure] = exc.value.failures
    assert failure.column == "s"
    assert failure.error_type == "TypeError"
    assert "was written as String" in failure.message
    got = _read(fmt, tmp_path, "t")
    assert "s" not in got
    assert got["y"] == [float(i + 1) for i in range(len(chunks))]


_US = pl.Datetime("us")


@pytest.mark.parametrize("fmt_name", ["zarr", "numpy"])
@pytest.mark.parametrize("chunks", [
    [
        pl.DataFrame({"timestamp_ns": [1], "d": pl.Series([1], dtype=pl.Duration("us")),
                      "y": [1.0]}),
        pl.DataFrame({"timestamp_ns": [2], "d": pl.Series([2], dtype=_US), "y": [2.0]}),
    ],
    [
        pl.DataFrame({"timestamp_ns": [1], "d": pl.Series([1], dtype=_US), "y": [1.0]}),
        pl.DataFrame({"timestamp_ns": [2], "d": [None], "y": [2.0]}),
    ],
], ids=["duration-then-datetime", "datetime-then-null"])
def test_datetime_column_that_changes_dtype_fails_cleanly(fmt_name, tmp_path, chunks):
    """Datetime and Duration have no missing value and no shared dtype in
    these formats. Would catch: Zarr storing the datetimes as durations,
    or appending NaT for the Null chunk, and ``.npz`` crashing with a raw
    DTypePromotionError. (HDF5 refuses datetimes outright.)"""
    if fmt_name == "zarr":
        pytest.importorskip("zarr")
    with pytest.raises(ExportError) as exc:
        _write(fmt_name, chunks, tmp_path)
    [failure] = exc.value.failures
    assert failure.column == "d"
    assert failure.error_type == "TypeError"
    assert "in an earlier one" in failure.message
    got = _read(fmt_name, tmp_path, "t")
    assert "d" not in got
    assert got["y"] == [1.0, 2.0]


@pytest.mark.parametrize("bad", [
    pl.Series([[1, 2], [3]], dtype=pl.List(pl.Int64)),
    pl.Series([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]),
    pl.Series([b"\x00\x01", b"\x02"]),
], ids=["list", "struct", "binary"])
def test_non_string_object_columns_fail_clearly(fmt, tmp_path, bad):
    """Would catch: such a column written as Python reprs (HDF5's
    ``astype(str)``), pickled into the ``.npz``, or crashing Zarr. The
    other columns are still written."""
    chunks = [pl.DataFrame({"timestamp_ns": [1, 2], "bad": bad, "s": ["a", None]})]
    with pytest.raises(ExportError) as exc:
        _WRITERS[fmt](iter(chunks), tmp_path, "t")
    [failure] = exc.value.failures
    assert failure.column == "bad"
    assert failure.kind == "unstorable"
    # The Parquet advice is the summary's, once (test_export_error_advice.py).
    assert "Parquet" not in failure.message
    assert str(exc.value).count("export to Parquet") == 1
    got = _read(fmt, tmp_path, "t")
    assert "bad" not in got
    assert got["s"] == ["a", ""]
    assert got["timestamp_ns"] == [1, 2]
