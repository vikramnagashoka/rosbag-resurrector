"""The scripts in examples/ must run on a plain install.

README.md tells users to run them in sequence as a smoke test of a fresh
install. A script that only works on the maintainer's machine (a
hard-coded dev venv path) or only inside a repo checkout (importing the
``tests`` package) breaks that promise for everyone else, and so does a
script that crashes instead of skipping when an optional extra is
missing.
"""

from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = REPO_ROOT / "examples"

# Dev venvs that only exist on the maintainer's machine: the old
# /tmp/v0NN-build ones and the current ~/.venvs/resurrector.
_DEV_VENV = re.compile(r"/tmp/v0\d|\.venvs/")
# A string literal starting with a per-user home directory.
_HOME_LITERAL = re.compile(r"""['"](/Users/|/home/)\w""")


def _example_sources() -> list[Path]:
    return sorted(EXAMPLES.glob("*.py"))


def test_examples_are_found():
    assert len(list(EXAMPLES.glob("[0-9][0-9]_*.py"))) >= 25


def _grep(paths: list[Path], pattern: re.Pattern) -> list[str]:
    hits = []
    for path in paths:
        if path.resolve() == Path(__file__).resolve():
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if pattern.search(line):
                hits.append(f"{path.relative_to(REPO_ROOT)}:{lineno}: {line.strip()}")
    return hits


def test_no_hardcoded_dev_paths_in_examples_or_tests():
    # tests/ is included because tests/test_qc.py once pointed its CLI
    # tests at /tmp/v060-build, so they were skipped everywhere else.
    tests = sorted((REPO_ROOT / "tests").rglob("*.py"))
    offenders = (
        _grep(_example_sources() + tests, _DEV_VENV)
        + _grep(_example_sources(), _HOME_LITERAL)
    )
    assert not offenders, (
        "Hard-coded developer paths (run the CLI with "
        "[sys.executable, '-m', 'resurrector.cli.main', ...] instead):\n"
        + "\n".join(offenders)
    )


def test_examples_do_not_import_the_tests_package():
    # A pip-installed user has examples/ but no tests/ on sys.path.
    offenders = []
    for path in _example_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            elif isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            else:
                continue
            for name in names:
                if name == "tests" or name.startswith("tests."):
                    offenders.append(f"{path.name}:{node.lineno}: {name}")
    assert not offenders, offenders


def _run_example_04(tmp_path, blocked: tuple[str, ...]) -> subprocess.CompletedProcess:
    from resurrector.demo.sample_bag import BagConfig, generate_bag

    home = tmp_path / "home"
    (home / ".resurrector").mkdir(parents=True)
    generate_bag(
        home / ".resurrector" / "explore_sample.mcap",
        BagConfig(duration_sec=0.5),
    )
    script = EXAMPLES / "04_image_video_export.py"
    # sys.modules[name] = None makes `import name` raise ImportError, the
    # same thing an install without that package sees.
    boot = (
        "import runpy, sys\n"
        + "".join(f"sys.modules[{name!r}] = None\n" for name in blocked)
        + f"sys.path.insert(0, {str(EXAMPLES)!r})\n"
        f"runpy.run_path({str(script)!r}, run_name='__main__')\n"
    )
    env = {**os.environ, "HOME": str(home)}
    return subprocess.run(
        [sys.executable, "-c", boot],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
    )


def _skip_lines(stdout: str) -> list[str]:
    return [line.strip() for line in stdout.splitlines() if "[SKIP]" in line]


def test_example_04_on_a_base_install(tmp_path):
    """A base install has Pillow but not OpenCV: only the MP4 step skips.

    Would catch: the example (or a regression in the base dependencies)
    skipping JPEG decoding and PNG export, which need only Pillow.
    """
    proc = _run_example_04(tmp_path, blocked=("cv2",))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "decoded shape=(48, 64, 3)" in proc.stdout
    assert "Wrote 5 PNG file(s)" in proc.stdout
    skips = _skip_lines(proc.stdout)
    assert len(skips) == 1 and "MP4" in skips[0], proc.stdout
    assert "[vision-lite]" in skips[0]


def test_example_04_skips_image_sections_without_pillow(tmp_path):
    """04 must exit 0 on a partial install that lacks Pillow (pip --no-deps).

    Before the fix the script died with ImportError on the first
    CompressedImage frame. The Pillow hints name the package itself:
    Pillow ships with the base install, not with [vision-lite].
    """
    proc = _run_example_04(tmp_path, blocked=("PIL", "cv2"))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "frame 0:" in proc.stdout  # raw sensor_msgs/Image still decodes
    jpeg, png, mp4 = _skip_lines(proc.stdout)
    for line in (jpeg, png):
        assert "pip install Pillow" in line and "vision-lite" not in line, line
    assert "[vision-lite]" in mp4


def test_example_26_runs_the_qc_cli(tmp_path):
    """26 shells out to `resurrector qc`; it must find the CLI on any install.

    It used to call /tmp/v060-build/bin/resurrector, so everywhere but the
    maintainer's old machine it died with FileNotFoundError.
    """
    env = {**os.environ, "HOME": str(tmp_path / "home")}
    proc = subprocess.run(
        [sys.executable, str(EXAMPLES / "26_bag_qc_fleet.py")],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    cli_section = proc.stdout.split("Same thing via the CLI", 1)[1]
    assert "5 bag(s)" in cli_section, cli_section
