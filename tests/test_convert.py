"""Tests for the legacy-format auto-convert helper."""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from resurrector.ingest.convert import (
    ConversionError,
    convert_to_mcap,
    needs_conversion,
)


class TestNeedsConversion:
    def test_mcap_does_not_need_conversion(self):
        assert not needs_conversion(Path("foo.mcap"))

    def test_bag_needs_conversion(self):
        assert needs_conversion(Path("foo.bag"))

    def test_db3_needs_conversion(self):
        assert needs_conversion(Path("foo.db3"))

    def test_unknown_does_not_need_conversion(self):
        assert not needs_conversion(Path("foo.xyz"))


class TestConvertToMcap:
    def test_mcap_input_returns_same_path(self, tmp_path):
        mcap = tmp_path / "already.mcap"
        mcap.write_bytes(b"x" * 10)
        assert convert_to_mcap(mcap) == mcap

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            convert_to_mcap(tmp_path / "ghost.bag")

    def test_missing_converter_raises_clear_error(self, tmp_path):
        bag = tmp_path / "legacy.bag"
        bag.write_bytes(b"x")
        with patch("resurrector.ingest.convert.shutil.which", return_value=None):
            with pytest.raises(ConversionError) as exc:
                convert_to_mcap(bag)
        assert "mcap" in str(exc.value)
        assert "PATH" in str(exc.value)

    def test_converter_failure_surfaced(self, tmp_path):
        bag = tmp_path / "legacy.bag"
        bag.write_bytes(b"x")

        class FakeResult:
            returncode = 1
            stderr = "simulated failure"
            stdout = ""

        with patch("resurrector.ingest.convert.shutil.which", return_value="/bin/mcap"):
            with patch("resurrector.ingest.convert.subprocess.run", return_value=FakeResult()):
                with pytest.raises(ConversionError) as exc:
                    convert_to_mcap(bag)
        assert "failed" in str(exc.value).lower()
        assert "simulated failure" in str(exc.value)
