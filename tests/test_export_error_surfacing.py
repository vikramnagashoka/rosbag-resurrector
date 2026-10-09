"""ExportError reaches the user as a readable error, not a traceback or a 500.

When the chosen format can't store some columns, the writer raises
:class:`ExportError`. In v0.8.5 nothing caught it: ``resurrector export``
printed a Rich traceback ending in "Failed to serialize 1 column(s) to
...: header.frame_id" with no reason, and the dashboard's export routes
returned a bare 500 the export dialog showed as "Export failed: Internal
Server Error".

Every test fakes the failing writer instead of relying on a format that
can't store some column today, so these tests keep testing the error
surfacing whatever the writers learn to store. The per-column reasons are
illustrative.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

import resurrector
import resurrector.core.export as export_mod
from resurrector.cli.main import app as cli_app
from resurrector.core.dataset import (
    BagRef,
    DatasetManager,
    parquet_version_hint,
    version_format_hint,
)
from resurrector.core.export import (
    FAILURE_LATE_COLUMN,
    FAILURE_UNSTORABLE,
    ExportColumnFailure,
    ExportError,
)
from resurrector.demo.sample_bag import BagConfig, generate_bag
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path

# What the real CSV writer reports for a column first seen in a later chunk.
LATE_COLUMN_REASON = (
    "first appears at row 50000, after the CSV header took its columns "
    "from the first chunk, so it is not in the file"
)
CSV_FAILURES = [
    ExportColumnFailure(column=c, error_type="ValueError", message=LATE_COLUMN_REASON,
                        kind=FAILURE_LATE_COLUMN)
    for c in ("position.6", "velocity.6")
]
HDF5_ZARR_ADVICE = (
    "To keep a column that first appears after the first chunk, export to "
    "HDF5 or Zarr, which fill the rows before it with missing values."
)
H5_FAILURES = [
    ExportColumnFailure(
        column="points", error_type="TypeError",
        message="List(Float64) columns can't be written as a numeric or string array",
        kind=FAILURE_UNSTORABLE,
    ),
]
FAILURES = {"csv": CSV_FAILURES, "hdf5": H5_FAILURES}
EXT = {"csv": "csv", "hdf5": "h5"}

PARQUET_ADVICE = (
    "To keep a column this format can't store, export to Parquet, which "
    "stores every column type."
)
DATASET_HINT = "add a version with the same bags and settings"


def _flat(text: str) -> str:
    """Collapse whitespace so terminal wrapping can't split a phrase."""
    return " ".join(text.split())


def _assert_explains(text: str, output: Path, failures: list[ExportColumnFailure]) -> None:
    """``text`` names each failed column with its reason, says those
    columns are not in the file and that later output was skipped."""
    flat = _flat(text)
    assert f"{len(failures)} column(s) could not be written to {output}" in flat
    for f in failures:
        assert f"{f.column}: {f.error_type}: {f.message}" in flat
    assert "These columns are not in that file; every other column is complete." in flat
    assert "any later topics, splits or bags were not exported" in flat
    assert "missing or incomplete" not in flat


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d).resolve()


@pytest.fixture
def bag(tmp_dir):
    return generate_bag(tmp_dir / "run.mcap", BagConfig(duration_sec=1.0))


@pytest.fixture
def failing_writer(monkeypatch):
    """``failing_writer(fmt)`` makes that format's writer fail its
    FAILURES columns the way a real writer does: it writes the file, then
    raises ExportError. Returns the list the paths it wrote go into."""
    paths: list[Path] = []

    def install(fmt: str) -> list[Path]:
        def failing(chunks, output_path, name):
            for _ in chunks:  # consume the stream like a real writer
                pass
            filepath = output_path / f"{name}.{EXT[fmt]}"
            filepath.touch()
            paths.append(filepath)
            raise ExportError(list(FAILURES[fmt]), filepath)

        monkeypatch.setattr(export_mod, f"_stream_{fmt}", failing)
        return paths

    return install


def _make_version(db: Path, bag: Path, fmt: str) -> None:
    mgr = DatasetManager(db)
    mgr.create("pick-place")
    mgr.create_version(
        "pick-place", "1.0",
        bag_refs=[BagRef(path=str(bag), topics=["/imu/data"])],
        export_format=fmt,
    )
    mgr.close()


class TestMessage:
    def test_csv_message_word_for_word(self):
        """Would catch: the message listing only column names, so the user
        never learns why a column failed or that those columns aren't in
        the file; or suggesting Parquet for a CSV failure Parquet doesn't
        fix."""
        out = Path(
            "/data/exports/pick_and_place_session_2026_10_08_with_the_new_"
            "gripper_calibration_and_long_arm/joint_states.csv"
        )
        err = ExportError(list(CSV_FAILURES), out)
        _assert_explains(str(err), out, CSV_FAILURES)
        assert not err.suggests_parquet
        # Word for word, since the dashboard's tests render a copy of it
        # (EXPORT_COLUMN_FAILURES_MESSAGE in dashboard/app/src/
        # exportPresetFixtures.ts).
        assert str(err) == (
            f"2 column(s) could not be written to {out}:\n"
            f"  - position.6: ValueError: {LATE_COLUMN_REASON}\n"
            f"  - velocity.6: ValueError: {LATE_COLUMN_REASON}\n"
            "These columns are not in that file; every other column is complete. "
            "The export stopped there, so any later topics, splits or bags were "
            f"not exported. {HDF5_ZARR_ADVICE}"
        )
        assert err.suggested_formats == ["hdf5", "zarr"]
        # Public attributes keep their shape.
        assert err.failures == CSV_FAILURES
        assert err.output == out

    @pytest.mark.parametrize(("name", "suggests"), [
        ("imu_data.h5", True), ("imu_data.zarr", True), ("imu_data.npz", True),
        ("imu_data.parquet", False),
    ])
    def test_parquet_suggested_for_an_unstorable_column_unless_parquet(self, name, suggests):
        """Would catch: telling a user whose Parquet export failed to
        export to Parquet. (Which kinds get which advice is in
        test_export_error_advice.py.)"""
        out = Path("/data/out") / name
        err = ExportError(list(H5_FAILURES), out)
        assert err.suggests_parquet is suggests
        assert str(err).endswith(PARQUET_ADVICE) is suggests
        _assert_explains(str(err), out, H5_FAILURES)

    def test_dataset_hint_names_the_suggested_formats_and_quotes_the_name(self):
        """Would catch: a hint that only knows Parquet (a late CSV/Parquet
        column needs HDF5 or Zarr), or one that pastes a dataset name with
        a space into the command unquoted, so running it fails with
        "Got unexpected extra argument(s)"."""
        assert version_format_hint("pick place", ["hdf5", "zarr"]) == (
            "A dataset version's format is set when the version is added. To get "
            "HDF5 or Zarr, add a version with the same bags and settings and "
            "-f hdf5 or -f zarr, then export that version: resurrector dataset "
            "add-version 'pick place' <new-version> -b <bag> ... -f hdf5"
        )

    def test_dataset_hint_for_a_name_starting_with_a_dash(self):
        """Would catch: ``add-version -runs ...``, which the CLI reads as
        option -r ("No such option: -r")."""
        hint = version_format_hint("-runs", ["hdf5", "zarr"])
        assert hint.endswith(
            "resurrector dataset add-version -b <bag> ... -f hdf5 -- -runs <new-version>"
        )

    def test_dataset_parquet_hint_word_for_word(self):
        """Would catch: the hint telling a dataset user to "export to
        Parquet", which neither `dataset export` nor the Datasets page can
        do, since the version pins the format. The CLI and the dashboard
        both print this one helper's text (see TestCli and TestApi)."""
        assert parquet_version_hint("pick-place") == (
            "A dataset version's format is set when the version is added. To get "
            "Parquet, add a version with the same bags and settings and -f parquet, "
            "then export that version: resurrector dataset add-version pick-place "
            "<new-version> -b <bag> ... -f parquet"
        )


class TestCli:
    @pytest.mark.parametrize(("extra_args", "failed_file"), [
        ([], "imu_data.csv"),
        (["--preset", "training-tabular", "-t", "/joint_states"], "synced.csv"),
        (["--split", "train=0.5", "--split", "val=0.5"], "train/imu_data.csv"),
    ], ids=["plain", "preset", "split"])
    def test_export_prints_reasons_and_exits_1(
        self, bag, tmp_dir, failing_writer, extra_args, failed_file,
    ):
        """Would catch: ``resurrector export`` ending in a traceback
        (ExportError uncaught) on the plain, preset (synced) and split
        paths, all of which reach the writer through Exporter.export."""
        written = failing_writer("csv")
        out = tmp_dir / "out"
        result = CliRunner().invoke(
            cli_app,
            ["export", str(bag), "-t", "/imu/data", "-f", "csv", "-o", str(out), *extra_args],
            env={"COLUMNS": "400"},
        )
        assert result.exit_code == 1, result.output
        # An uncaught ExportError would be here instead of SystemExit.
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert written == [out / failed_file]
        assert result.stdout == ""
        assert _flat(result.stderr).startswith("Export failed: ")
        _assert_explains(result.stderr, written[0], CSV_FAILURES)

    @pytest.mark.parametrize(("extra_args", "writer_error", "exit_code", "expected"), [
        (["--preset", "no-such-preset"], None, 1,
         "Export failed: Unknown preset 'no-such-preset'. Available: "),
        (["--split", "train"], None, 2,
         "Invalid --split entry 'train'; expected NAME=RATIO (e.g. train=0.8)"),
        (["--split", "train=most"], None, 2,
         "--split 'train=most': ratio must be numeric"),
        (["--split", "train=1.0", "--split-strategy", "stratified"], None, 1,
         "stratified split is not yet supported"),
        ([], FileExistsError("/data/out/imu_data.csv already exists"), 1,
         "Export failed: /data/out/imu_data.csv already exists"),
        ([], ImportError("CSV export needs csvkit: pip install csvkit"), 1,
         "CSV export needs csvkit: pip install csvkit"),
    ], ids=["unknown-preset", "split-no-ratio", "split-bad-ratio", "stratified",
            "file-exists", "missing-dependency"])
    def test_other_export_errors_go_to_stderr(
        self, bag, tmp_dir, monkeypatch, extra_args, writer_error, exit_code, expected,
    ):
        """Would catch: one of ``resurrector export``'s other error paths
        (bad preset or --split, a split strategy that doesn't exist yet, an
        existing output, a missing optional dependency) still printing to
        stdout, where ``2>`` misses it and a pipeline reads it as data."""
        if writer_error is not None:
            def failing(chunks, output_path, name):
                raise writer_error
            monkeypatch.setattr(export_mod, "_stream_csv", failing)
        result = CliRunner().invoke(
            cli_app,
            ["export", str(bag), "-t", "/imu/data", "-f", "csv",
             "-o", str(tmp_dir / "out"), *extra_args],
            env={"COLUMNS": "400"},
        )
        assert result.exit_code == exit_code, result.output
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert result.stdout == ""
        assert expected in _flat(result.stderr)

    def test_dataset_export_prints_reasons_and_exits_1(self, bag, tmp_dir, failing_writer):
        """Would catch: ``resurrector dataset export`` tracing back on the
        same ExportError (it runs the same exporter per bag), or printing
        it to stdout."""
        written = failing_writer("csv")
        db = tmp_dir / "index.db"
        _make_version(db, bag, "csv")
        result = CliRunner().invoke(
            cli_app,
            ["dataset", "export", "pick-place", "1.0", "-o", str(tmp_dir / "ds"), "--db", str(db)],
            env={"COLUMNS": "400"},
        )
        assert result.exit_code == 1, result.output
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert result.stdout == ""
        _assert_explains(result.stderr, written[0], CSV_FAILURES)
        # Parquet can't add a late column either; HDF5 or Zarr can, and only
        # a new version switches the format.
        assert "-f parquet" not in _flat(result.output)
        assert version_format_hint("pick-place", ["hdf5", "zarr"]) in _flat(result.stderr)

    @pytest.mark.parametrize("error", ["shape", "format", "exists"])
    def test_dataset_export_frame_and_target_errors_in_one_line(self, bag, tmp_dir, monkeypatch, error):
        """Would catch: ``resurrector dataset export`` ending in a Rich
        traceback for a LeRobot frame guard (a ValueError) or an existing
        LeRobot target, which ``resurrector export`` prints in one line."""
        from resurrector.core.exceptions import LeRobotFrameFormatError, LeRobotFrameShapeError

        err = {
            "shape": LeRobotFrameShapeError("/camera/rgb", (1, 1, 3), True, min_width=25, min_height=4),
            "format": LeRobotFrameFormatError("/depth", "uint16", (48, 64)),
            "exists": FileExistsError("LeRobot export target out/1.0 already exists and is not empty."),
        }[error]

        def refuse(self, *args, **kwargs):
            raise err

        db = tmp_dir / "index.db"
        _make_version(db, bag, "csv")
        monkeypatch.setattr(DatasetManager, "export_version", refuse)
        result = CliRunner().invoke(
            cli_app,
            ["dataset", "export", "pick-place", "1.0", "-o", str(tmp_dir / "ds"), "--db", str(db)],
            env={"COLUMNS": "400"},
        )
        assert result.exit_code == 1, result.output
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert result.stdout == ""
        assert _flat(result.stderr) == f"Export failed: {_flat(str(err))}"

    def test_dataset_export_says_how_to_get_parquet(self, bag, tmp_dir, failing_writer):
        """Would catch: advice to "export to Parquet" that this command
        can't follow, since it has no --format: the version pins it."""
        written = failing_writer("hdf5")
        db = tmp_dir / "index.db"
        _make_version(db, bag, "hdf5")
        result = CliRunner().invoke(
            cli_app,
            ["dataset", "export", "pick-place", "1.0", "-o", str(tmp_dir / "ds"), "--db", str(db)],
            env={"COLUMNS": "400"},
        )
        assert result.exit_code == 1, result.output
        assert result.stdout == ""
        flat = _flat(result.stderr)
        _assert_explains(result.stderr, written[0], H5_FAILURES)
        assert PARQUET_ADVICE in flat
        assert parquet_version_hint("pick-place") in flat

    def test_dataset_export_unknown_dataset(self, tmp_dir):
        """Would catch: an unknown dataset ending in a KeyError traceback,
        or its message printed with KeyError's extra quotes."""
        result = CliRunner().invoke(
            cli_app,
            ["dataset", "export", "nosuch", "1.0", "-o", str(tmp_dir / "ds"),
             "--db", str(tmp_dir / "index.db")],
            env={"COLUMNS": "400"},
        )
        assert result.exit_code == 1, result.output
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert result.stdout == ""
        assert _flat(result.stderr) == "Export failed: Dataset 'nosuch' not found"


# The checkout this test process imports `resurrector` from. Subprocesses
# get it first on PYTHONPATH so they run the code under test, not whatever
# checkout the venv was installed from (often another worktree).
CHECKOUT = Path(resurrector.__file__).resolve().parents[1]


def _cli_env() -> dict[str, str]:
    """A non-terminal environment like a script's: no COLUMNS, so Rich
    falls back to its 80-column width, and no colour codes."""
    env = {k: v for k, v in os.environ.items()
           if k not in ("COLUMNS", "LINES", "FORCE_COLOR", "TTY_COMPATIBLE")}
    env["NO_COLOR"] = "1"
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (str(CHECKOUT), os.environ.get("PYTHONPATH")) if p
    )
    return env


def _run_cli(args: list[str], cwd: Path, script: str | None = None) -> subprocess.CompletedProcess:
    """``python -m resurrector.cli.main ARGS`` in a fresh process, from
    ``cwd`` (not the checkout, so the current directory can't pick which
    code runs). ``script`` runs first, to fake a writer; then the module
    runs as ``-m`` would run it."""
    if script is None:
        cmd = [sys.executable, "-m", "resurrector.cli.main"]
    else:
        run_main = 'import runpy; runpy.run_module("resurrector.cli.main", run_name="__main__", alter_sys=True)'
        cmd = [sys.executable, "-c", textwrap.dedent(script) + "\n" + run_main]
    return subprocess.run(
        [*cmd, *args], capture_output=True, text=True, env=_cli_env(), cwd=cwd, timeout=300,
    )


class TestCliProcess:
    """The real CLI in its own process: real stdout/stderr, a real exit
    code, and a real traceback if anything escapes. CliRunner can't show
    any of these (it mixes the streams and keeps exceptions)."""

    def test_subprocess_runs_this_checkout(self, tmp_dir):
        """Would catch: these tests running the CLI of whatever checkout
        the venv's ``resurrector`` points at (a venv installed from the
        main repo, tests run from a worktree), so a regression in the code
        under test passes, or a fix there fails."""
        proc = subprocess.run(
            [sys.executable, "-c", "import resurrector; print(resurrector.__file__)"],
            capture_output=True, text=True, env=_cli_env(), cwd=tmp_dir, timeout=60,
        )
        assert proc.returncode == 0, proc.stderr
        assert Path(proc.stdout.strip()).resolve() == Path(resurrector.__file__).resolve()

    def test_export_error_goes_to_stderr_one_column_per_line(self, bag, tmp_dir):
        """Would catch: the error going to stdout (scripts redirecting
        2> get nothing), Rich hard-wrapping it at 80 columns so a column's
        line breaks in two, or a traceback."""
        out = tmp_dir / "out"
        # The real app with the CSV writer failing; nothing else faked.
        proc = _run_cli(
            ["export", str(bag), "-t", "/imu/data", "-f", "csv", "-o", str(out)],
            cwd=tmp_dir,
            script=f"""
                import resurrector.core.export as export_mod
                from resurrector.core.export import ExportColumnFailure, ExportError

                def failing(chunks, output_path, name):
                    for _ in chunks:
                        pass
                    path = output_path / f"{{name}}.csv"
                    path.touch()
                    raise ExportError(
                        [ExportColumnFailure(c, "ValueError", {LATE_COLUMN_REASON!r},
                                             kind={FAILURE_LATE_COLUMN!r})
                         for c in ("position.6", "velocity.6")],
                        path,
                    )

                export_mod._stream_csv = failing
            """,
        )
        assert proc.returncode == 1, proc.stdout + proc.stderr
        assert "Traceback" not in proc.stdout + proc.stderr
        assert proc.stdout == ""
        expected = ExportError(list(CSV_FAILURES), out / "imu_data.csv")
        assert f"Export failed: {expected}" in proc.stderr
        lines = proc.stderr.splitlines()
        for f in CSV_FAILURES:
            line = f"  - {f.column}: {f.error_type}: {f.message}"
            assert len(line) > 80  # long enough that a hard wrap would split it
            assert line in lines

    def test_dataset_export_error_goes_to_stderr_with_hint(self, bag, tmp_dir):
        """Would catch: ``resurrector dataset export`` printing the failed
        columns or the add-version hint to stdout, hard-wrapping either at
        80 columns, or tracing back. CliRunner mixes the streams, so only a
        real process shows which one each line went to."""
        db = tmp_dir / "index.db"
        _make_version(db, bag, "hdf5")
        failure = H5_FAILURES[0]
        proc = _run_cli(
            ["dataset", "export", "pick-place", "1.0", "-o", str(tmp_dir / "ds"), "--db", str(db)],
            cwd=tmp_dir,
            script=f"""
                import resurrector.core.export as export_mod
                from resurrector.core.export import ExportColumnFailure, ExportError

                def failing(chunks, output_path, name):
                    for _ in chunks:
                        pass
                    path = output_path / f"{{name}}.h5"
                    path.touch()
                    raise ExportError(
                        [ExportColumnFailure({failure.column!r}, {failure.error_type!r},
                                             {failure.message!r}, kind={failure.kind!r})],
                        path,
                    )

                export_mod._stream_hdf5 = failing
            """,
        )
        assert proc.returncode == 1, proc.stdout + proc.stderr
        assert "Traceback" not in proc.stdout + proc.stderr
        assert proc.stdout == ""
        [written] = (tmp_dir / "ds").rglob("*.h5")
        expected = ExportError(list(H5_FAILURES), written)
        assert expected.suggests_parquet
        assert f"Export failed: {expected}" in proc.stderr
        lines = proc.stderr.splitlines()
        line = f"  - {failure.column}: {failure.error_type}: {failure.message}"
        hint = parquet_version_hint("pick-place")
        for whole in (line, hint):
            assert len(whole) > 80  # long enough that a hard wrap would split it
            assert whole in lines
        assert lines[-1] == hint

    def test_dataset_export_unknown_dataset_real_cli(self, tmp_dir):
        """Would catch: the CLI tracing back on an unknown dataset, or
        printing the error to stdout."""
        proc = _run_cli(
            ["dataset", "export", "nosuch", "1.0",
             "-o", str(tmp_dir / "ds"), "--db", str(tmp_dir / "index.db")],
            cwd=tmp_dir,
        )
        assert proc.returncode == 1, proc.stdout + proc.stderr
        assert "Traceback" not in proc.stdout + proc.stderr
        assert proc.stdout == ""
        assert proc.stderr.strip() == "Export failed: Dataset 'nosuch' not found"


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


def _assert_structured_422(
    r, written: list[Path], failures: list[ExportColumnFailure],
) -> str:
    """The 422 body the dashboard's ApiError reads: ``detail.message`` is
    a string (so never "[object Object]") that starts with the
    exception's own text. Returns the message."""
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail["kind"] == "export_column_failures"
    assert isinstance(detail["message"], str)
    assert len(written) == 1
    assert detail["message"].startswith(str(ExportError(list(failures), written[0])))
    _assert_explains(detail["message"], written[0], failures)
    assert detail["output"] == str(written[0])
    assert detail["failures"] == [
        {"column": f.column, "error_type": f.error_type, "message": f.message}
        for f in failures
    ]
    return detail["message"]


class TestApi:
    def test_bag_export_returns_422_with_reasons(self, api, tmp_dir, failing_writer):
        """Would catch: POST /api/bags/{id}/export answering a failed column
        with a bare 500, which the export dialog shows as "Export failed:
        Internal Server Error"."""
        written = failing_writer("csv")
        client, bag_id = api
        r = client.post(
            f"/api/bags/{bag_id}/export",
            params={"topics": "/imu/data", "format": "csv", "output_dir": str(tmp_dir / "out")},
        )
        message = _assert_structured_422(r, written, CSV_FAILURES)
        assert message == str(ExportError(list(CSV_FAILURES), written[0]))
        assert written[0].name == "imu_data.csv"

    def test_trim_returns_422_with_reasons(self, api, tmp_dir, failing_writer):
        """Would catch: the trim popover's export hitting the same bare 500."""
        written = failing_writer("csv")
        client, bag_id = api
        r = client.post(
            f"/api/bags/{bag_id}/trim",
            json={
                "start_sec": 0.0, "end_sec": 0.5, "topics": ["/imu/data"],
                "format": "csv", "output_path": str(tmp_dir / "trim"),
            },
        )
        message = _assert_structured_422(r, written, CSV_FAILURES)
        assert message == str(ExportError(list(CSV_FAILURES), written[0]))

    def test_dataset_export_returns_422_with_reasons(self, api, bag, tmp_dir, failing_writer):
        """Would catch: the dataset export route's catch-all turning the
        error into a 500 whose text drops every column's reason."""
        written = failing_writer("csv")
        _make_version(tmp_dir / "index.db", bag, "csv")
        client, _ = api
        r = client.post(
            "/api/datasets/pick-place/versions/1.0/export",
            json={"output_dir": str(tmp_dir / "ds")},
        )
        message = _assert_structured_422(r, written, CSV_FAILURES)
        # A late CSV column is kept by HDF5 or Zarr, which only a new
        # version can switch to.
        assert message == (
            f"{ExportError(list(CSV_FAILURES), written[0])}\n"
            f"{version_format_hint('pick-place', ['hdf5', 'zarr'])}"
        )

    def test_dataset_export_says_how_to_get_parquet(self, api, bag, tmp_dir, failing_writer):
        """Would catch: the Datasets page telling the user to "export to
        Parquet" with no way to pick a format there."""
        written = failing_writer("hdf5")
        _make_version(tmp_dir / "index.db", bag, "hdf5")
        client, _ = api
        r = client.post(
            "/api/datasets/pick-place/versions/1.0/export",
            json={"output_dir": str(tmp_dir / "ds")},
        )
        message = _assert_structured_422(r, written, H5_FAILURES)
        # The same hint `resurrector dataset export` prints.
        assert message == (
            f"{ExportError(list(H5_FAILURES), written[0])}\n"
            f"{parquet_version_hint('pick-place')}"
        )

    def test_dataset_export_frame_error_returns_400(self, api, bag, tmp_dir, monkeypatch):
        """Would catch: a LeRobot frame error (a ValueError) on a dataset
        version answered as 404 Not Found, as if the version were missing;
        the single-bag export route answers it with 400."""
        from resurrector.core.exceptions import LeRobotFrameShapeError

        _make_version(tmp_dir / "index.db", bag, "csv")
        err = LeRobotFrameShapeError("/camera/rgb", (1, 1, 3), True, min_width=25, min_height=4)

        def refuse(self, *args, **kwargs):
            raise err

        monkeypatch.setattr(DatasetManager, "export_version", refuse)
        client, _ = api
        r = client.post(
            "/api/datasets/pick-place/versions/1.0/export",
            json={"output_dir": str(tmp_dir / "ds")},
        )
        assert r.status_code == 400, r.text
        assert r.json() == {"detail": str(err)}

    @pytest.mark.parametrize(("error", "status"), [
        ("format", 400), ("exists", 409), ("missing-extra", 503),
    ])
    def test_dataset_export_maps_errors_like_bag_export(self, api, bag, tmp_dir, monkeypatch, error, status):
        """Would catch: a LeRobot format error answered 404, an existing
        target or a missing extra falling into the route's catch-all as a
        500 ("Partial output may exist", when nothing was written), with no
        install banner for the missing extra."""
        from resurrector.core.exceptions import LeRobotFrameFormatError
        from resurrector.core.lerobot_export import INSTALL_HINT

        err = {
            "format": LeRobotFrameFormatError("/depth", "uint16", (48, 64)),
            "exists": FileExistsError("LeRobot export target already exists and is not empty."),
            "missing-extra": ImportError(INSTALL_HINT),
        }[error]

        def refuse(self, *args, **kwargs):
            raise err

        _make_version(tmp_dir / "index.db", bag, "csv")
        monkeypatch.setattr(DatasetManager, "export_version", refuse)
        client, _ = api
        r = client.post(
            "/api/datasets/pick-place/versions/1.0/export",
            json={"output_dir": str(tmp_dir / "ds")},
        )
        assert r.status_code == status, r.text
        detail = r.json()["detail"]
        if error == "missing-extra":
            assert detail["kind"] == "capability_unavailable"
            assert detail["capability"] == "lerobot"
            assert detail["message"] == INSTALL_HINT
        else:
            assert detail == str(err)

    def test_trim_missing_extra_returns_503(self, api, tmp_dir, monkeypatch):
        """Would catch: trimming to Zarr without the extra answered with a
        bare 500 while bag export answers the same case with the 503 the
        UI turns into an install banner."""
        import resurrector.core.trim as trim_mod

        def no_zarr(*args, **kwargs):
            raise ImportError("Zarr export requires the zarr package. Install with: "
                              "pip install 'rosbag-resurrector[all-exports]'")

        monkeypatch.setattr(trim_mod, "trim_to_format", no_zarr)
        client, bag_id = api
        r = client.post(
            f"/api/bags/{bag_id}/trim",
            json={"start_sec": 0.0, "end_sec": 0.5, "topics": ["/imu/data"],
                  "format": "zarr", "output_path": str(tmp_dir / "trim")},
        )
        assert r.status_code == 503, r.text
        assert r.json()["detail"]["kind"] == "capability_unavailable"
        assert r.json()["detail"]["capability"] == "all_exports"

    @pytest.mark.parametrize(("name", "version", "detail"), [
        ("nosuch", "1.0", "Dataset 'nosuch' not found"),
        ("pick-place", "9.9", "Version '9.9' not found for dataset 'pick-place'"),
    ], ids=["dataset", "version"])
    def test_dataset_export_unknown_returns_404(self, api, bag, tmp_dir, name, version, detail):
        """Would catch: an unknown dataset or version falling into the
        route's catch-all as a 500 ("Export failed: \\"Dataset 'nosuch'
        not found\\". Partial output may exist ...")."""
        _make_version(tmp_dir / "index.db", bag, "csv")
        client, _ = api
        r = client.post(
            f"/api/datasets/{name}/versions/{version}/export",
            json={"output_dir": str(tmp_dir / "ds")},
        )
        assert r.status_code == 404, r.text
        assert r.json() == {"detail": detail}
        assert not (tmp_dir / "ds").exists()

    def test_successful_export_response_unchanged(self, api, tmp_dir):
        """Control: with the real writer the same request still succeeds
        with export_bag's usual body."""
        out = tmp_dir / "ok"
        client, bag_id = api
        r = client.post(
            f"/api/bags/{bag_id}/export",
            params={"topics": "/imu/data", "format": "csv", "output_dir": str(out)},
        )
        assert r.status_code == 200, r.text
        assert r.json() == {"status": "completed", "output_path": str(out)}
        assert (out / "imu_data.csv").is_file()
