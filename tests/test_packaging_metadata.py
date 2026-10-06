"""Guards on what PyPI and installers see: pyproject.toml and README.md.

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
"""

from __future__ import annotations

import re
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
        f"{typer} names an extra; typer >= 0.12.1 ships rich + shellingham "
        "as plain dependencies and has no 'all' extra, so uv warns on install"
    )
    # Before 0.12 rich/shellingham lived only in the removed extra.
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
    targets = _readme_link_targets()
    # Sanity floor so a broken regex can't make the next test vacuous.
    assert any(t.startswith("https://") for t in targets)
    assert any(t.startswith("#") for t in targets)


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
