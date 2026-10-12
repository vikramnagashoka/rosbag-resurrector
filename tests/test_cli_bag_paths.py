"""Commands that open a bag answer a bad path with one line, not a traceback.

Before: ``resurrector export missing.mcap`` (and ``info``, ``health``, ...)
ended in a Rich traceback box over ``FileNotFoundError: Bag file not
found``; a file that isn't an MCAP ended in ``InvalidMagic`` or
``struct.error`` tracebacks. Now each prints one line to stderr and exits 1.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from resurrector.cli.main import app
from resurrector.demo.sample_bag import BagConfig, generate_bag

# Each command with the arguments it needs besides the bag path.
COMMANDS = {
    "info": [],
    "health": [],
    "export": ["-o", "{out}"],
    "quicklook": [],
    "export-frames": ["-t", "/camera/rgb", "-o", "{out}"],
    "report": ["-o", "{out}.md"],
}


@pytest.fixture(scope="module")
def good_bag(tmp_path_factory):
    return generate_bag(tmp_path_factory.mktemp("good") / "good.mcap", BagConfig(duration_sec=0.5))


def _bad_paths(tmp_path, good_bag):
    notes = tmp_path / "notes.mcap"
    notes.write_text("my notes")
    cut = tmp_path / "cut.mcap"
    cut.write_bytes(good_bag.read_bytes()[:2000])
    return {
        "missing": (tmp_path / "missing.mcap", "Bag file not found"),
        "not-mcap": (notes, "Can't read"),
        "cut-off": (cut, "Can't read"),
        "wrong-format": (tmp_path, "Unsupported file format"),
    }


def _run(*args):
    return CliRunner().invoke(app, [str(a) for a in args], env={"COLUMNS": "2000"})


@pytest.mark.parametrize("command", list(COMMANDS))
@pytest.mark.parametrize("case", ["missing", "not-mcap", "cut-off", "wrong-format"])
def test_bad_bag_path_is_one_line(command, case, tmp_path, good_bag):
    if command == "health" and case == "wrong-format":
        pytest.skip("health scans a directory; covered below")
    path, text = _bad_paths(tmp_path, good_bag)[case]
    extra = [a.format(out=tmp_path / "out") for a in COMMANDS[command]]
    result = _run(command, path, *extra)
    assert result.exit_code == 1, result.output
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert text in result.stderr, result.stderr
    assert "Traceback" not in result.output
    assert len(result.stderr.strip().splitlines()) == 1, result.stderr


def test_diff_names_the_bad_bag(tmp_path, good_bag):
    result = _run("diff", good_bag, tmp_path / "missing.mcap")
    assert result.exit_code == 1
    assert "Bag file not found" in result.stderr and "missing.mcap" in result.stderr


def test_health_on_a_directory_reports_the_good_bags(tmp_path, good_bag):
    """One unreadable bag in a directory doesn't stop the others' reports;
    the command still exits 1."""
    (tmp_path / "good.mcap").write_bytes(good_bag.read_bytes())
    (tmp_path / "broken.mcap").write_text("not a bag")
    result = _run("health", tmp_path, "-f", "json")
    assert result.exit_code == 1, result.output
    assert "good.mcap" in result.stdout
    assert "Can't read" in result.stderr and "broken.mcap" in result.stderr
