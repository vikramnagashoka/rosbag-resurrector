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

import pytest

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


def test_examples_make_bags_through_the_common_helpers():
    """Would catch: an example calling ``generate_bag`` itself (example
    25 did), which ends in a traceback on an install without Pillow
    instead of the one-line hint, and reuses a stale sample bag. Bags
    come from ``_common.ensure_bag`` / ``generate_bag_or_exit``."""
    offenders = []
    for path in _example_sources():
        if path.name == "_common.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module == "resurrector.demo.sample_bag"
                and any(a.name == "generate_bag" for a in node.names)
            ):
                offenders.append(f"{path.name}:{node.lineno}")
    assert not offenders, offenders


def _good_sample(path: Path) -> None:
    from resurrector.demo.sample_bag import BagConfig, generate_bag

    generate_bag(path, BagConfig(duration_sec=0.5))


def _placeholder_sample(path: Path) -> None:
    """A sample like 0.8.5 installs without Pillow wrote: 1x1 gray JPEG frames."""
    import io
    from unittest import mock

    from PIL import Image

    from resurrector.demo import sample_bag

    def one_pixel_jpeg(*_args) -> bytes:
        buf = io.BytesIO()
        Image.new("L", (1, 1), 128).save(buf, format="JPEG")
        return buf.getvalue()

    with mock.patch.object(sample_bag, "_make_test_jpeg", one_pixel_jpeg):
        sample_bag.generate_bag(path, sample_bag.BagConfig(duration_sec=0.5))


def _run_example_04(tmp_path, blocked: tuple[str, ...], sample=_good_sample) -> subprocess.CompletedProcess:
    """Run 04 with HOME in ``tmp_path``. ``sample`` (or None for no sample)
    writes ~/.resurrector/explore_sample.mcap first, in this process,
    where nothing is blocked."""
    home = tmp_path / "home"
    (home / ".resurrector").mkdir(parents=True)
    if sample is not None:
        sample(home / ".resurrector" / "explore_sample.mcap")
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
    """04 must exit 0 on a partial install that lacks Pillow (pip --no-deps)
    and whose sample bag already exists (the bag is generated here, with
    Pillow, before the child process blocks it).

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


def test_example_without_pillow_or_sample_exits_cleanly(tmp_path):
    """No Pillow and no sample bag yet: the generator can't run, so the
    example prints its one-line hint and exits 1.

    Would catch: the generator's ImportError escaping _common as a
    traceback, before any of 04's own [SKIP] handling could run.
    """
    proc = _run_example_04(tmp_path, blocked=("PIL", "cv2"), sample=None)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "Traceback" not in proc.stderr, proc.stderr
    assert "[ERROR]" in proc.stderr and "pip install Pillow" in proc.stderr, proc.stderr
    assert not (tmp_path / "home" / ".resurrector" / "explore_sample.mcap").exists()


def test_placeholder_sample_is_regenerated(tmp_path):
    """A sample with 1x1 placeholder frames (written by 0.8.5 without
    Pillow) is regenerated, not reused.

    Would catch: _common reusing explore_sample.mcap however it was made,
    so upgraded users kept decoding 1x1 frames in every example.
    """
    proc = _run_example_04(tmp_path, blocked=("cv2",), sample=_placeholder_sample)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "Regenerating" in proc.stdout and "1x1 placeholders" in proc.stdout, proc.stdout
    assert "decoded shape=(48, 64, 3)" in proc.stdout
    assert "decoded shape=(1, 1)" not in proc.stdout


def test_scene_bag_is_never_left_half_written(tmp_path, monkeypatch):
    """23 reuses an existing scene bag, so generate_scene_bag must not
    leave a truncated one behind (an MCAP cut off mid-write has no
    summary and every later read of it fails).

    Would catch: writing straight to the output path, which left the
    header and the TF messages on disk when encoding a point cloud failed.
    """
    from resurrector.demo import scene_bag

    out = tmp_path / "v05_scene_demo.mcap"
    previous = scene_bag.generate_scene_bag(out).read_bytes()

    def boom(*_args, **_kwargs):
        raise RuntimeError("encoder died")

    monkeypatch.setattr(scene_bag, "encode_pointcloud2", boom)
    with pytest.raises(RuntimeError, match="encoder died"):
        scene_bag.generate_scene_bag(tmp_path / "fresh.mcap")
    with pytest.raises(RuntimeError, match="encoder died"):
        scene_bag.generate_scene_bag(out)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["v05_scene_demo.mcap"]
    assert out.read_bytes() == previous


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
