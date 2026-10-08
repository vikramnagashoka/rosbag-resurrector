"""ExportError reaches the user as a readable error, not a traceback or a 500.

When a writer can't store some columns (strings in Zarr, nested lists in
HDF5) it raises :class:`ExportError`. Before this was fixed nothing caught
it: ``resurrector export`` printed a Rich traceback ending in "Failed to
serialize 1 column(s) to ...: header.frame_id" with no reason, and the
dashboard's export routes returned a bare 500 the export dialog showed as
"Export failed: Internal Server Error".

Every test fakes the failing writer (``_stream_hdf5``) instead of relying
on a format that can't store some column today, so these tests keep
testing the error surfacing after any one writer learns a new dtype.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

import resurrector.core.export as export_mod
from resurrector.cli.main import app as cli_app
from resurrector.core.dataset import BagRef, DatasetManager
from resurrector.core.export import ExportColumnFailure, ExportError
from resurrector.demo.sample_bag import BagConfig, generate_bag
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path

# What _stream_hdf5 reports for a variable-length list column.
LIST_REASON = (
    "HDF5 does not support dtype object containing sequences "
    "(e.g. variable-length lists)"
)
FAILURES = [
    ExportColumnFailure(column="ranges", error_type="TypeError", message=LIST_REASON),
    ExportColumnFailure(column="intensities", error_type="TypeError", message=LIST_REASON),
]


def _flat(text: str) -> str:
    """Collapse whitespace so terminal wrapping can't split a phrase."""
    return " ".join(text.split())


def _assert_explains(text: str, output: Path) -> None:
    """``text`` names each failed column with its reason, says the file is
    partial and that later output was skipped, and gives the fix."""
    flat = _flat(text)
    assert f"2 column(s) could not be written to {output}" in flat
    for f in FAILURES:
        assert f"{f.column}: {f.error_type}: {f.message}" in flat
    assert "That file is partial" in flat
    assert "every other column is complete" in flat
    assert "any later topics, splits or bags were not exported" in flat
    assert "export to Parquet" in flat


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d).resolve()


@pytest.fixture
def bag(tmp_dir):
    return generate_bag(tmp_dir / "run.mcap", BagConfig(duration_sec=1.0))


@pytest.fixture
def written(monkeypatch):
    """Make HDF5 export fail two columns, like the real writer does on a
    topic with list columns: it writes the file, then raises ExportError.
    Returns the paths of the files it wrote."""
    paths: list[Path] = []

    def failing_hdf5(chunks, output_path, name):
        for _ in chunks:  # consume the stream like a real writer
            pass
        filepath = output_path / f"{name}.h5"
        filepath.touch()
        paths.append(filepath)
        raise ExportError(list(FAILURES), filepath)

    monkeypatch.setattr(export_mod, "_stream_hdf5", failing_hdf5)
    return paths


class TestMessage:
    def test_names_reasons_partial_file_and_fix(self):
        """Would catch: the message listing only column names, so the user
        never learns why a column failed, that the file on disk is partial,
        or what to do about it."""
        out = Path("/data/exports/run_7/lidar_scan.h5")
        err = ExportError(list(FAILURES), out)
        _assert_explains(str(err), out)
        # Word for word, since the dashboard's tests render a copy of it
        # (EXPORT_COLUMN_FAILURES_MESSAGE in dashboard/app/src/
        # exportPresetFixtures.ts).
        assert str(err) == (
            f"2 column(s) could not be written to {out}:\n"
            f"  - ranges: TypeError: {LIST_REASON}\n"
            f"  - intensities: TypeError: {LIST_REASON}\n"
            "That file is partial: the columns above are missing or incomplete; "
            "every other column is complete. The export stopped there, so any "
            "later topics, splits or bags were not exported. To keep these "
            "columns, export to Parquet, which stores every column type."
        )
        # Public attributes keep their shape.
        assert err.failures == FAILURES
        assert err.output == out


class TestCli:
    @pytest.mark.parametrize(("extra_args", "failed_file"), [
        ([], "imu_data.h5"),
        (["--preset", "training-tabular", "-t", "/joint_states"], "synced.h5"),
        (["--split", "train=0.5", "--split", "val=0.5"], "train/imu_data.h5"),
    ], ids=["plain", "preset", "split"])
    def test_export_prints_reasons_and_exits_1(
        self, bag, tmp_dir, written, extra_args, failed_file,
    ):
        """Would catch: ``resurrector export`` ending in a Rich traceback
        (ExportError uncaught) on the plain, preset (synced) and split
        paths, all of which reach the writer through Exporter.export."""
        out = tmp_dir / "out"
        result = CliRunner().invoke(
            cli_app,
            ["export", str(bag), "-t", "/imu/data", "-f", "hdf5", "-o", str(out), *extra_args],
            env={"COLUMNS": "400"},
        )
        assert result.exit_code == 1, result.output
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert "Traceback" not in result.output
        assert written == [out / failed_file]
        assert _flat(result.output).startswith("Export failed: ")
        _assert_explains(result.output, written[0])

    def test_dataset_export_prints_reasons_and_exits_1(self, bag, tmp_dir, written):
        """Would catch: ``resurrector dataset export`` tracing back on the
        same ExportError (it runs the same exporter per bag)."""
        db = tmp_dir / "index.db"
        mgr = DatasetManager(db)
        mgr.create("pick-place")
        mgr.create_version(
            "pick-place", "1.0",
            bag_refs=[BagRef(path=str(bag), topics=["/imu/data"])],
            export_format="hdf5",
        )
        mgr.close()

        result = CliRunner().invoke(
            cli_app,
            ["dataset", "export", "pick-place", "1.0", "-o", str(tmp_dir / "ds"), "--db", str(db)],
            env={"COLUMNS": "400"},
        )
        assert result.exit_code == 1, result.output
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert "Traceback" not in result.output
        _assert_explains(result.output, written[0])


@pytest.fixture
def api(bag, tmp_dir, monkeypatch):
    """``(client, bag_id)`` for a dashboard whose index holds ``bag``."""
    db = tmp_dir / "index.db"
    index = BagIndex(db)
    bag_id = index.upsert_bag(scan_path(bag)[0], parse_bag(bag).get_metadata())
    index.close()
    monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db))
    monkeypatch.setenv("RESURRECTOR_ALLOWED_ROOTS", str(tmp_dir))
    from resurrector.dashboard.api import app
    return TestClient(app), bag_id


def _assert_structured_422(r, written: list[Path]) -> None:
    """The 422 body the dashboard's ApiError reads: ``detail.message`` is
    the exception's own text (a string, so never "[object Object]")."""
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail["kind"] == "export_column_failures"
    assert isinstance(detail["message"], str)
    assert len(written) == 1
    assert detail["message"] == str(ExportError(list(FAILURES), written[0]))
    _assert_explains(detail["message"], written[0])
    assert detail["output"] == str(written[0])
    assert detail["failures"] == [
        {"column": f.column, "error_type": f.error_type, "message": f.message}
        for f in FAILURES
    ]


class TestApi:
    def test_bag_export_returns_422_with_reasons(self, api, tmp_dir, written):
        """Would catch: POST /api/bags/{id}/export answering a failed column
        with a bare 500, which the export dialog shows as "Export failed:
        Internal Server Error"."""
        client, bag_id = api
        r = client.post(
            f"/api/bags/{bag_id}/export",
            params={"topics": "/imu/data", "format": "hdf5", "output_dir": str(tmp_dir / "out")},
        )
        _assert_structured_422(r, written)
        assert written[0].name == "imu_data.h5"

    def test_trim_returns_422_with_reasons(self, api, tmp_dir, written):
        """Would catch: the trim popover's export hitting the same bare 500."""
        client, bag_id = api
        r = client.post(
            f"/api/bags/{bag_id}/trim",
            json={
                "start_sec": 0.0, "end_sec": 0.5, "topics": ["/imu/data"],
                "format": "hdf5", "output_path": str(tmp_dir / "trim"),
            },
        )
        _assert_structured_422(r, written)

    def test_dataset_export_returns_422_with_reasons(self, api, bag, tmp_dir, written):
        """Would catch: the dataset export route's catch-all turning the
        error into a 500 whose text drops every column's reason."""
        mgr = DatasetManager(tmp_dir / "index.db")
        mgr.create("pick-place")
        mgr.create_version(
            "pick-place", "1.0",
            bag_refs=[BagRef(path=str(bag), topics=["/imu/data"])],
            export_format="hdf5",
        )
        mgr.close()
        client, _ = api
        r = client.post(
            "/api/datasets/pick-place/versions/1.0/export",
            json={"output_dir": str(tmp_dir / "ds")},
        )
        _assert_structured_422(r, written)

    def test_successful_export_response_unchanged(self, api, tmp_dir):
        """Control: with the real writer the same request still succeeds
        with export_bag's usual body."""
        out = tmp_dir / "ok"
        client, bag_id = api
        r = client.post(
            f"/api/bags/{bag_id}/export",
            params={"topics": "/imu/data", "format": "hdf5", "output_dir": str(out)},
        )
        assert r.status_code == 200, r.text
        assert r.json() == {"status": "completed", "output_path": str(out)}
        assert (out / "imu_data.h5").is_file()
