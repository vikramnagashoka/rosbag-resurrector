"""Guards on what PyPI and installers see: pyproject.toml, README.md and
the native packages built from packaging/.

These read the source files, not installed metadata, so a regression
shows up in the default suite before a release is built:

- Dependency specs must resolve without warnings. Typer dropped its
  ``all`` extra once rich + shellingham became default dependencies, and
  uv warns about the missing extra on every install.
- The license must be a PEP 639 SPDX expression. setuptools deprecated
  the TOML-table form and ``License ::`` classifiers (builds stop
  working after 2027-02-18).
- README.md is the PyPI project page, and PyPI has no repo checkout
  behind it, so a path-relative link like ``[x](ARCHITECTURE.md)`` 404s
  there. Only absolute URLs and in-page ``#anchors`` are allowed.
- The .deb and .dmg built from packaging/ must carry the same license
  and version as the wheel.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from packaging.requirements import Requirement

if sys.version_info >= (3, 11):
    import tomllib
else:  # pytest depends on tomli below 3.11, so it is always importable here.
    import tomli as tomllib

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def pyproject() -> dict:
    with open(REPO_ROOT / "pyproject.toml", "rb") as f:
        return tomllib.load(f)


def _requirement(specs: list[str], name: str) -> Requirement:
    matches = [r for r in map(Requirement, specs) if r.name == name]
    assert len(matches) == 1, f"expected one {name!r} requirement, got {matches}"
    return matches[0]


def test_typer_requirement_needs_no_extra(pyproject):
    typer = _requirement(pyproject["project"]["dependencies"], "typer")
    assert typer.extras == set(), (
        f"{typer} names an extra; typer removed 'all' in 0.12.0, when rich + "
        "shellingham became default dependencies, so uv warns on install"
    )
    # 0.11.1 is the last release with the extra; rich/shellingham lived only there.
    assert not typer.specifier.contains("0.11.1")
    # 0.12.1 - 0.15.3 declare click>=8.0 with no cap but break on click 8.2
    # (`--help` raises in make_metavar); 0.16.0 is the first compatible release.
    assert not typer.specifier.contains("0.15.3")
    assert typer.specifier.contains("0.16.0")


def test_license_is_spdx_expression(pyproject):
    project = pyproject["project"]
    assert project["license"] == "MIT", "use a PEP 639 SPDX string, not a table"
    for pattern in project["license-files"]:
        assert list(REPO_ROOT.glob(pattern)), f"license-files {pattern!r} matches nothing"
    license_classifiers = [c for c in project["classifiers"] if c.startswith("License ::")]
    assert license_classifiers == [], (
        "License classifiers are deprecated alongside an SPDX license expression"
    )


def test_build_backend_supports_pep639(pyproject):
    setuptools = _requirement(pyproject["build-system"]["requires"], "setuptools")
    # setuptools < 77 rejects a string `project.license` outright.
    assert not setuptools.specifier.contains("76.1.0")
    # 77.0.0-77.0.2 were the first PEP 639 releases; 77.0.3 fixed their
    # license-file glob errors and the opaque ImportError on packaging<24.2.
    assert not setuptools.specifier.contains("77.0.2")
    assert setuptools.specifier.contains("77.0.3")


_FENCED_BLOCK = re.compile(r"^(```|~~~).*?^\1", re.MULTILINE | re.DOTALL)
_CODE_SPAN = re.compile(r"`[^`\n]*`")
_LINK_TARGETS = (
    re.compile(r"\]\(\s*<?([^)\s>]+)"),                    # [text](target), ![alt](target)
    re.compile(r"^ {0,3}\[[^\]]+\]:\s*<?([^\s>]+)", re.M),  # [ref]: target
    re.compile(r"""\b(?:href|src)\s*=\s*["']([^"']+)""", re.I),  # raw HTML
)
_ALLOWED = re.compile(r"^(https?://|mailto:|#)")


def _readme_link_targets() -> list[str]:
    text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    text = _CODE_SPAN.sub("", _FENCED_BLOCK.sub("", text))
    return [m for pattern in _LINK_TARGETS for m in pattern.findall(text)]


def test_readme_link_scanner_sees_links():
    # Sanity floor so a broken regex can't make the next test vacuous. The
    # README has a dozen https links (badges, releases, docs); a regex that
    # stopped matching inline links would see none of them.
    https_links = [t for t in _readme_link_targets() if t.startswith("https://")]
    assert len(https_links) >= 5, https_links


def test_readme_has_no_path_relative_links():
    relative = [t for t in _readme_link_targets() if not _ALLOWED.match(t)]
    assert relative == [], (
        "README.md is rendered on PyPI, where path-relative links 404. Use "
        "https://github.com/vikramnagashoka/rosbag-resurrector/blob/main/<path> "
        f"(or /tree/main/<dir> for directories). Offending targets: {relative}"
    )


def test_cli_help_renders_with_rich_markdown_mode():
    """`--help` still goes through Rich with rich_markup_mode="markdown".

    rich is a direct dependency, so dropping typer's extra must not
    change this. Markdown mode is what keeps pip-extra brackets like
    ``[all-exports]`` from being parsed as Rich markup and stripped.
    """
    from typer.testing import CliRunner

    from resurrector.cli.main import app

    result = CliRunner().invoke(app, ["export", "--help"])
    assert result.exit_code == 0, result.output
    # Typer forces ANSI styling under GITHUB_ACTIONS; compare plain text.
    plain = re.sub(r"\x1b\[[0-9;]*m", "", result.output)
    assert "╭─ Options" in plain  # Rich panel; plain Click prints "Options:"
    assert "[all-exports]" in plain


def test_readme_example_count_matches_examples_dir():
    text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    claims = [
        int(n)
        for pattern in (
            r"\b(\d+) standalone exploration scripts\b",
            r"\ball (\d+) exit 0\b",
        )
        for n in re.findall(pattern, text)
    ]
    assert claims, (
        "README no longer states how many example scripts there are; "
        "update or delete this test"
    )
    actual = len(list((REPO_ROOT / "examples").glob("[0-9][0-9]_*.py")))
    assert claims == [actual] * len(claims), (
        f"README claims {claims} example scripts; examples/ has {actual} "
        "numbered scripts"
    )


def test_packaging_has_no_proprietary_license():
    hits = [
        f"{path.relative_to(REPO_ROOT)}:{lineno}"
        for path in sorted((REPO_ROOT / "packaging").rglob("*"))
        if path.is_file()
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8", errors="replace").splitlines(), 1
        )
        if "proprietary" in line.lower()
    ]
    assert hits == [], f"the project is MIT-licensed; found 'Proprietary' at {hits}"


def test_deb_license_matches_pyproject(pyproject):
    script = (REPO_ROOT / "packaging" / "ubuntu" / "build_deb.sh").read_text()
    licenses = re.findall(r"--license\s+[\"']?([^\"'\s]+)", script)
    assert licenses == [pyproject["project"]["license"]]


def _run_in_scratch_checkout(tmp_path: Path, script: str, *args: str) -> str:
    """Run a packaging script from a copy of the repo that has no build output.

    Each script derives the repo root from its own location, so a copy
    under tmp_path with pyproject.toml beside it runs exactly as in a real
    checkout, but stops at the missing PyInstaller binary instead of
    building anything.
    """
    for rel in (script, "pyproject.toml"):
        dest = tmp_path / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(REPO_ROOT / rel, dest)
    env = {k: v for k, v in os.environ.items() if k not in {"VERSION", "MAKEFLAGS", "MAKELEVEL"}}
    if script.endswith("Makefile"):
        cmd = ["make", "-n", "-C", str((tmp_path / script).parent), *args]
    else:
        cmd = ["bash", str(tmp_path / script), *args]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=60)
    return result.stdout + result.stderr


needs_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
needs_make = pytest.mark.skipif(shutil.which("make") is None, reason="needs make")


@needs_bash
@pytest.mark.parametrize(
    ("script", "label"),
    [
        ("packaging/ubuntu/build_deb.sh", "rosbag-resurrector_{v}"),
        ("packaging/macos/create_dmg.sh", "RosBag-Resurrector-v{v}-macos"),
    ],
)
def test_package_scripts_default_to_pyproject_version(pyproject, tmp_path, script, label):
    out = _run_in_scratch_checkout(tmp_path, script)
    assert label.format(v=pyproject["project"]["version"]) in out, out
    # An explicit version (what CI passes from the tag) still wins.
    out = _run_in_scratch_checkout(tmp_path, script, "9.9.9")
    assert label.format(v="9.9.9") in out, out


@needs_make
@pytest.mark.parametrize(
    ("target", "script"),
    [("ubuntu", "build_deb.sh"), ("macos", "create_dmg.sh")],
)
def test_makefile_defaults_to_pyproject_version(pyproject, tmp_path, target, script):
    if " " in str(tmp_path):
        pytest.skip("packaging/Makefile splits its own path on spaces")
    out = _run_in_scratch_checkout(tmp_path, "packaging/Makefile", target)
    assert f"{script} {pyproject['project']['version']}\n" in out, out
