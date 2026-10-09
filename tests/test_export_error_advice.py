"""What ExportError tells the user to do depends on the format and on
why each column failed.

Through the column-alignment fix, ExportError suggested Parquet for any
HDF5, Zarr or ``.npz`` failure and nothing else, and said every failed
column was "not in that file". The CSV and Parquet writers can now fail
too: a column first seen after the first chunk (which HDF5 and Zarr
keep, Parquet doesn't), and a Parquet column whose later values don't fit
its type, which stays in the file, null from that chunk on. The array
writers' per-column reason also said "export to Parquet", so that
advice appeared twice.

The rules (see ExportError's docstring):

- ``unstorable`` (no numeric or string array for it): not in the file;
  Parquet stores it.
- ``late_column`` (CSV, Parquet): not in the file; HDF5 and Zarr add it.
- ``untyped_first_chunk`` (Parquet): in the file as all null; HDF5 and
  Zarr keep its later values.
- ``type_change``: removed by HDF5/Zarr/npz; kept by Parquet (null from
  the failing chunk on) and RLDS (left out of those steps). No advice:
  no other format takes the values either.
- ``no_missing_value``: not in the file; no advice.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from resurrector.core.export import (
    FAILURE_LATE_COLUMN,
    FAILURE_NO_MISSING_VALUE,
    FAILURE_OTHER,
    FAILURE_TYPE_CHANGE,
    FAILURE_UNSTORABLE,
    FAILURE_UNTYPED,
    ExportColumnFailure,
    ExportError,
    _stream_csv,
    _stream_hdf5,
    _stream_numpy,
    _stream_parquet,
    _stream_zarr,
)
from resurrector.core.exceptions import ResurrectorError

PARQUET_ADVICE = "export to Parquet"
HDF5_ADVICE = "export to HDF5 or Zarr"
NOT_IN_FILE = "These columns are not in that file; every other column is complete."
KEPT = "These columns are in that file but have no values where the reasons above say"

ALL_KINDS = [
    FAILURE_UNSTORABLE, FAILURE_LATE_COLUMN, FAILURE_UNTYPED,
    FAILURE_TYPE_CHANGE, FAILURE_NO_MISSING_VALUE, FAILURE_OTHER,
]
SUFFIXES = {
    ".parquet": "parquet", ".csv": "csv", ".h5": "hdf5", ".zarr": "zarr",
    ".npz": "numpy", ".tfrecord": "rlds", ".bin": None,
}


def _flat(text: str) -> str:
    return " ".join(text.split())


# ---------------------------------------------------------------------------
# The message, kind by kind and format by format.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("suffix", list(SUFFIXES))
@pytest.mark.parametrize("kind", ALL_KINDS)
def test_advice_follows_kind_and_format(suffix, kind):
    """Would catch: "export to Parquet" for a Parquet file or a late
    column (Parquet can't add one either), HDF5 advice for an HDF5 file,
    or advice for a type change no format would take."""
    out = Path("/data/out") / f"t{suffix}"
    err = ExportError([ExportColumnFailure("c", "TypeError", "why", kind)], out)
    text = str(err)
    fmt = SUFFIXES[suffix]
    assert err.format == fmt
    wants_parquet = kind == FAILURE_UNSTORABLE and fmt != "parquet"
    wants_hdf5 = kind in (FAILURE_LATE_COLUMN, FAILURE_UNTYPED) and fmt not in ("hdf5", "zarr")
    assert err.suggests_parquet is wants_parquet
    assert (PARQUET_ADVICE in text) is wants_parquet
    assert (HDF5_ADVICE in text) is wants_hdf5
    assert text.count("Parquet") == (1 if wants_parquet else 0)
    kept = (fmt == "parquet" and kind in (FAILURE_UNTYPED, FAILURE_TYPE_CHANGE)) or (
        fmt == "rlds" and kind == FAILURE_TYPE_CHANGE
    )
    assert (KEPT in text) is kept
    assert (NOT_IN_FILE in text) is (not kept)
    assert "  - c: TypeError: why\n" in text
    assert "The export stopped there, so any later topics, splits or bags were not exported." in text


def test_mixed_failures_say_which_columns_are_in_the_file():
    """A late column and a type change in one Parquet file: the late one
    is not in the file, the other is, with nulls."""
    err = ExportError([
        ExportColumnFailure("x", "ValueError", "late", FAILURE_LATE_COLUMN),
        ExportColumnFailure("v", "TypeError", "lossy", FAILURE_TYPE_CHANGE),
        ExportColumnFailure("w", "TypeError", "lossy", FAILURE_TYPE_CHANGE),
    ], Path("/out/t.parquet"))
    flat = _flat(str(err))
    assert (
        "x is not in that file, and v and w are in it but without values "
        "where the reasons above say; every other column is complete."
    ) in flat
    assert PARQUET_ADVICE not in flat
    assert flat.endswith(
        "To keep a column that first appears after the first chunk, export "
        "to HDF5 or Zarr, which fill the rows before it with missing values."
    )


def test_untyped_column_advice_mentions_first_values():
    err = ExportError(
        [ExportColumnFailure("v", "TypeError", "all null", FAILURE_UNTYPED)],
        Path("/out/t.parquet"),
    )
    assert "a column that first appears, or first has values, after the first chunk" in str(err)


def test_kind_defaults_to_other_and_attributes_keep_their_shape():
    """Positional construction (column, error_type, message) still works,
    and ExportError is a ResurrectorError."""
    failure = ExportColumnFailure("c", "TypeError", "boom")
    assert failure.kind == FAILURE_OTHER
    err = ExportError([failure], Path("/out/t.h5"))
    assert isinstance(err, ResurrectorError)
    assert err.failures == [failure] and err.output == Path("/out/t.h5")
    assert not err.suggests_parquet


# ---------------------------------------------------------------------------
# The real writers produce the right kinds, so the right advice.
# ---------------------------------------------------------------------------

_WRITERS = {
    "csv": (_stream_csv, "csv"), "parquet": (_stream_parquet, "parquet"),
    "hdf5": (_stream_hdf5, "h5"), "zarr": (_stream_zarr, "zarr"),
    "numpy": (_stream_numpy, "npz"),
}


def _chunks(case: str) -> list[pl.DataFrame]:
    d = pl.Series("v", [1, 2], dtype=pl.Datetime("us"))
    return {
        "late": [
            pl.DataFrame({"timestamp_ns": [1, 2], "y": [1.0, 2.0]}),
            pl.DataFrame({"timestamp_ns": [3], "y": [3.0], "v": [3.0]}),
        ],
        "text-then-int": [
            pl.DataFrame({"timestamp_ns": [1], "y": [1.0], "v": ["a"]}),
            pl.DataFrame({"timestamp_ns": [2], "y": [2.0], "v": [7]}),
        ],
        "int-then-fraction": [
            pl.DataFrame({"timestamp_ns": [1], "y": [1.0], "v": [1]}),
            pl.DataFrame({"timestamp_ns": [2], "y": [2.0], "v": [2.5]}),
        ],
        "null-then-float": [
            pl.DataFrame({"timestamp_ns": [1], "y": [1.0], "v": [None]}),
            pl.DataFrame({"timestamp_ns": [2], "y": [2.0], "v": [2.0]}),
        ],
        "list": [
            pl.DataFrame({"timestamp_ns": [1, 2], "y": [1.0, 2.0], "v": [[1.0], [2.0, 3.0]]}),
        ],
        "datetime-absent-later": [
            pl.DataFrame({"timestamp_ns": [1, 2], "y": [1.0, 2.0], "v": d}),
            pl.DataFrame({"timestamp_ns": [3], "y": [3.0]}),
        ],
    }[case]


# (format, case) -> (kind of v's failure, in the file?, advice)
REAL_CASES = {
    ("csv", "late"): (FAILURE_LATE_COLUMN, False, "hdf5"),
    ("parquet", "late"): (FAILURE_LATE_COLUMN, False, "hdf5"),
    ("parquet", "text-then-int"): (FAILURE_TYPE_CHANGE, True, None),
    ("parquet", "int-then-fraction"): (FAILURE_TYPE_CHANGE, True, None),
    ("parquet", "null-then-float"): (FAILURE_UNTYPED, True, "hdf5"),
    ("hdf5", "list"): (FAILURE_UNSTORABLE, False, "parquet"),
    ("zarr", "list"): (FAILURE_UNSTORABLE, False, "parquet"),
    ("numpy", "list"): (FAILURE_UNSTORABLE, False, "parquet"),
    ("hdf5", "text-then-int"): (FAILURE_TYPE_CHANGE, False, None),
    ("zarr", "text-then-int"): (FAILURE_TYPE_CHANGE, False, None),
    ("numpy", "text-then-int"): (FAILURE_TYPE_CHANGE, False, None),
    # h5py has no datetime type, so HDF5 can't store the column at all.
    ("hdf5", "datetime-absent-later"): (FAILURE_UNSTORABLE, False, "parquet"),
    ("zarr", "datetime-absent-later"): (FAILURE_NO_MISSING_VALUE, False, None),
    ("numpy", "datetime-absent-later"): (FAILURE_NO_MISSING_VALUE, False, None),
}


@pytest.mark.parametrize(("fmt", "case"), list(REAL_CASES), ids=[f"{f}-{c}" for f, c in REAL_CASES])
def test_real_writer_failure_gets_its_advice(fmt, case, tmp_path):
    """Would catch: a CSV or Parquet failure saying "export to Parquet",
    a Parquet type-change column called "not in that file" (it is, null
    from that row on), or the array writers' Parquet advice appearing
    twice (once in the column's reason, once in the summary)."""
    if fmt == "zarr":
        pytest.importorskip("zarr")
    writer, ext = _WRITERS[fmt]
    with pytest.raises(ExportError) as exc:
        writer(iter(_chunks(case)), tmp_path, "t")
    err = exc.value
    kind, kept, advice = REAL_CASES[(fmt, case)]
    assert err.output == tmp_path / f"t.{ext}"
    assert [(f.column, f.kind) for f in err.failures] == [("v", kind)]
    text = _flat(str(err))
    assert (KEPT in text) is kept
    assert (NOT_IN_FILE in text) is (not kept)
    assert (PARQUET_ADVICE in text) is (advice == "parquet")
    assert (HDF5_ADVICE in text) is (advice == "hdf5")
    if fmt in ("csv", "parquet"):
        assert PARQUET_ADVICE not in text
    else:
        # The advice is the summary's alone, said once.
        assert "Parquet" not in err.failures[0].message
        assert text.count("Parquet") == (1 if advice == "parquet" else 0)


def test_cli_parquet_late_column_points_to_hdf5_not_parquet(tmp_path, monkeypatch):
    """The verifier's case end to end: a JointState topic whose velocity
    starts in the second chunk, exported to Parquet and CSV through the
    CLI. The advice is HDF5 or Zarr, never Parquet."""
    from resurrector.cli.main import app
    from resurrector.core import export as export_module
    from tests.fixtures.changing_columns import write_joint_state_bag

    bag = write_joint_state_bag(tmp_path / "js.mcap", 400, range(150, 400), 200)
    monkeypatch.setattr(export_module, "CHUNK_SIZE", 150)
    for fmt in ("parquet", "csv"):
        result = CliRunner().invoke(app, [
            "export", str(bag), "-t", "/joint_states", "-f", fmt,
            "-o", str(tmp_path / fmt),
        ], env={"COLUMNS": "400"})
        assert result.exit_code == 1, result.output
        flat = _flat(result.output)
        assert "velocity.0" in flat and "velocity.1" in flat
        assert NOT_IN_FILE in flat
        assert PARQUET_ADVICE not in flat
        assert HDF5_ADVICE in flat
