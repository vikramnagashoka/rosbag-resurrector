"""Tests for `resurrector doctor` environment check."""

from __future__ import annotations

import io
import os

from rich.console import Console

from resurrector.cli.doctor import (
    CheckResult,
    _check_allowed_roots,
    _check_index_path,
    _check_module,
    _check_python,
    render,
    run_all_checks,
)


class TestIndividualChecks:
    def test_python_version_pass(self):
        result = _check_python()
        assert result.status == "pass"

    def test_module_pass_for_bundled(self):
        # mcap is always installed as a core dep
        result = _check_module("mcap", "MCAP", "fix hint")
        assert result.status == "pass"

    def test_module_warn_for_missing(self):
        result = _check_module(
            "this_package_does_not_exist_xyz", "Ghost", "install ghost",
        )
        assert result.status == "warn"
        assert "install ghost" in result.fix_hint

    def test_index_path_runs(self):
        r = _check_index_path()
        assert r.status in {"pass", "warn", "fail"}

    def test_allowed_roots_reads_env(self, monkeypatch):
        monkeypatch.setenv("RESURRECTOR_ALLOWED_ROOTS", os.sep + "tmp")
        r = _check_allowed_roots()
        assert r.status == "pass"
        assert "1 root" in r.detail or "tmp" in r.detail


class TestRunAllChecks:
    def test_returns_checks_with_required_fields(self):
        results = run_all_checks()
        assert len(results) > 5
        for r in results:
            assert isinstance(r, CheckResult)
            assert r.status in {"pass", "warn", "fail"}
            assert r.name
            assert r.detail

    def test_includes_core_checks(self):
        results = run_all_checks()
        names = {r.name for r in results}
        assert "Python" in names
        assert "MCAP parser" in names
        assert "DuckDB index" in names


class TestRender:
    def test_pip_extras_brackets_preserved(self, monkeypatch):
        # Square brackets in Rich are markup syntax. Without escape() the
        # render strips "[vision]" / "[all-exports]" etc. and the user sees
        # `pip install rosbag-resurrector` — same command they already ran.
        results = [
            CheckResult(
                "Zarr export", "warn", "zarr not installed",
                "pip install rosbag-resurrector[all-exports]",
                tier="optional",
            ),
        ]
        buf = io.StringIO()
        # Patch the module-level Console to write into our buffer
        import resurrector.cli.doctor as doc
        monkeypatch.setattr(
            doc, "Console", lambda: Console(file=buf, width=200, force_terminal=False)
        )
        render(results)
        out = buf.getvalue()
        assert "[all-exports]" in out, (
            f"pip extras bracket eaten by Rich markup parser; output was:\n{out}"
        )
