"""HDF5, Zarr and NumPy exports: column dtypes come from the polars schema.

These writers append each chunk to an array whose dtype is fixed when
the column is first written. Taking that dtype from the first chunk's
NumPy conversion made it depend on whether the first chunk happened to
hold a null (Int64 with a null converts to float64, without one to
int64; Boolean with a null to an object array). The dtype is now chosen
from the polars dtype, assuming any column can hold nulls:

- floats keep their width; null -> NaN
- integers and Booleans -> float64 (1.0 / 0.0 for Booleans); null -> NaN
- ``timestamp_ns`` stays int64 (every row has one, and float64 would
  round nanosecond timestamps)
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from resurrector.core.export import (
    ExportError,
    _stream_hdf5,
    _stream_numpy,
    _stream_zarr,
)

NO_NULLS = pl.DataFrame({
    "timestamp_ns": [1, 2],
    "i": [10, 20],
    "b": [True, False],
    "f32": pl.Series([1.5, 2.5], dtype=pl.Float32),
    "f64": [0.25, 0.5],
})
WITH_NULLS = pl.DataFrame({
    "timestamp_ns": [3, 4],
    "i": [30, None],
    "b": [None, True],
    "f32": pl.Series([None, 4.5], dtype=pl.Float32),
    "f64": [None, 0.75],
})

EXPECTED_DTYPES = {
    "timestamp_ns": np.int64,
    "i": np.float64,
    "b": np.float64,
    "f32": np.float32,
    "f64": np.float64,
}
EXPECTED_LATE = {
    "timestamp_ns": [1, 2, 3, 4],
    "i": [10.0, 20.0, 30.0, np.nan],
    "b": [1.0, 0.0, np.nan, 1.0],
    "f32": [1.5, 2.5, np.nan, 4.5],
    "f64": [0.25, 0.5, np.nan, 0.75],
}


def _write(fmt: str, chunks: list[pl.DataFrame], out: Path) -> dict[str, np.ndarray]:
    if fmt == "hdf5":
        import h5py
        _stream_hdf5(iter(chunks), out, "t")
        with h5py.File(out / "t.h5", "r") as f:
            return {k: f["t"][k][:] for k in f["t"]}
    if fmt == "zarr":
        import zarr
        _stream_zarr(iter(chunks), out, "t")
        group = zarr.open_group(str(out / "t.zarr"), mode="r")
        return {k: group[k][:] for k in group.array_keys()}
    _stream_numpy(iter(chunks), out, "t")
    with np.load(out / "t.npz") as data:
        return {k: data[k] for k in data.files}


@pytest.fixture(params=["hdf5", "zarr", "numpy"])
def fmt(request):
    if request.param == "zarr":
        pytest.importorskip("zarr")
    return request.param


@pytest.mark.parametrize("nulls_first", [False, True], ids=["nulls-later", "nulls-first"])
def test_dtype_does_not_depend_on_which_chunk_has_nulls(fmt, nulls_first, tmp_path):
    """Would catch: the dataset sized from a null-free first chunk as
    int64 / bool, after which a NaN from a later chunk is stored as 0
    (int) or fails the column (Boolean)."""
    chunks = [WITH_NULLS, NO_NULLS] if nulls_first else [NO_NULLS, WITH_NULLS]
    got = _write(fmt, chunks, tmp_path)

    assert {k: v.dtype for k, v in got.items()} == {
        k: np.dtype(v) for k, v in EXPECTED_DTYPES.items()
    }
    order = [2, 3, 0, 1] if nulls_first else [0, 1, 2, 3]
    for col, values in EXPECTED_LATE.items():
        np.testing.assert_array_equal(got[col], np.asarray(values)[order], err_msg=col)


def test_null_free_boolean_is_written_as_float(fmt, tmp_path):
    """One representation for Booleans whether or not a null ever shows
    up: 1.0 / 0.0, NaN for a missing value."""
    got = _write(fmt, [NO_NULLS.select("timestamp_ns", "b")], tmp_path)
    assert got["b"].dtype == np.float64
    np.testing.assert_array_equal(got["b"], [1.0, 0.0])


def test_column_that_changes_kind_fails_instead_of_corrupting(fmt, tmp_path):
    """A later chunk the column's dtype can't hold (text in a numeric
    column, even text that parses as a number) is reported as a failed
    column, not stored as garbage or quietly parsed."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [1], "x": [1.0], "y": [2.0]}),
        pl.DataFrame({"timestamp_ns": [2], "x": ["2.5"], "y": [3.0]}),
    ]
    with pytest.raises(ExportError) as exc:
        _write(fmt, chunks, tmp_path)
    assert [f.column for f in exc.value.failures] == ["x"]


@pytest.mark.parametrize("big", [2**60 + 1, 2**53 + 1, -(2**53) - 1])
def test_large_integers_warn_about_float_rounding(fmt, big, tmp_path, caplog):
    """float64 holds integers exactly only up to 2**53; say so rather
    than round silently. Would catch: checking the float64 array, where
    2**53 + 1 has already rounded to 2**53 and looks in range."""
    chunks = [pl.DataFrame({"timestamp_ns": [1, 2], "id": [1, big]})]
    with caplog.at_level(logging.WARNING, logger="resurrector.core.export"):
        got = _write(fmt, chunks, tmp_path)
    assert got["id"].dtype == np.float64
    assert "'id'" in caplog.text and "2**53" in caplog.text


def test_integers_within_float64_range_do_not_warn(fmt, tmp_path, caplog):
    chunks = [pl.DataFrame({"timestamp_ns": [1, 2], "id": [-(2**53), 2**53]})]
    with caplog.at_level(logging.WARNING, logger="resurrector.core.export"):
        got = _write(fmt, chunks, tmp_path)
    assert got["id"].tolist() == [-(2.0**53), 2.0**53]
    assert "2**53" not in caplog.text


def test_timestamp_ns_stays_exact(fmt, tmp_path):
    ts = [1_700_000_000_123_456_789, 1_700_000_000_123_456_790]
    got = _write(fmt, [pl.DataFrame({"timestamp_ns": ts, "x": [1.0, 2.0]})], tmp_path)
    assert got["timestamp_ns"].dtype == np.int64
    assert got["timestamp_ns"].tolist() == ts
