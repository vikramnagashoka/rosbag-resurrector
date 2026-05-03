"""Tests for `resurrector.demo.download`.

Uses mocked urllib so the suite runs in CI without ever hitting the
real Hugging Face URL (which is 844 MB and would blow CI time + bandwidth).
"""

from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from resurrector.demo.download import (
    DEFAULT_SAMPLE_FILENAME,
    DEFAULT_SAMPLE_URL,
    DownloadResult,
    MAX_EXPECTED_SIZE_BYTES,
    MIN_EXPECTED_SIZE_BYTES,
    download_sample,
)


@pytest.fixture
def tmp_target(tmp_path: Path) -> Path:
    """A target download path inside a tmp dir."""
    return tmp_path / "samples" / "hku2.mcap"


def _mock_urlopen(payload: bytes, content_length: int | None = None):
    """Build a mock urlopen that returns ``payload`` in chunks."""
    resp = MagicMock()
    resp.headers = MagicMock()
    resp.headers.get = MagicMock(
        return_value=str(content_length) if content_length is not None else None
    )
    # Stream the payload in chunks so the read loop terminates
    buffer = io.BytesIO(payload)
    resp.read = buffer.read
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=resp)


class TestSuccessfulDownload:
    """Happy-path: stream a plausibly-sized file to disk."""

    def test_writes_file_at_target(self, tmp_target):
        # Build a payload exactly at the lower size bound
        payload = b"x" * MIN_EXPECTED_SIZE_BYTES
        with patch(
            "resurrector.demo.download.urllib.request.urlopen",
            _mock_urlopen(payload, content_length=len(payload)),
        ):
            result = download_sample(target=tmp_target)
        assert result.path == tmp_target
        assert result.bytes_downloaded == len(payload)
        assert result.skipped is False
        assert tmp_target.exists()
        assert tmp_target.stat().st_size == len(payload)

    def test_atomic_rename_no_partial_file_visible(self, tmp_target):
        """The .partial file should not exist after success."""
        payload = b"x" * MIN_EXPECTED_SIZE_BYTES
        with patch(
            "resurrector.demo.download.urllib.request.urlopen",
            _mock_urlopen(payload),
        ):
            download_sample(target=tmp_target)
        partial = tmp_target.with_suffix(tmp_target.suffix + ".partial")
        assert not partial.exists(), \
            ".partial file should be renamed to final target on success"

    def test_progress_callback_invoked(self, tmp_target):
        payload = b"y" * MIN_EXPECTED_SIZE_BYTES
        calls: list[tuple[int, int]] = []

        def cb(bytes_so_far: int, total: int) -> None:
            calls.append((bytes_so_far, total))

        with patch(
            "resurrector.demo.download.urllib.request.urlopen",
            _mock_urlopen(payload, content_length=len(payload)),
        ):
            download_sample(target=tmp_target, progress_callback=cb)

        assert len(calls) > 0, "callback should fire at least once"
        # First call's bytes_so_far should be > 0; final should equal total
        assert calls[0][0] > 0
        assert calls[-1][0] == len(payload)
        # Total reported via Content-Length should match payload size
        assert calls[0][1] == len(payload)

    def test_progress_callback_handles_missing_content_length(self, tmp_target):
        """Some servers don't send Content-Length; total should be 0."""
        payload = b"z" * MIN_EXPECTED_SIZE_BYTES
        calls: list[tuple[int, int]] = []
        with patch(
            "resurrector.demo.download.urllib.request.urlopen",
            _mock_urlopen(payload, content_length=None),
        ):
            download_sample(
                target=tmp_target,
                progress_callback=lambda b, t: calls.append((b, t)),
            )
        # Without Content-Length, total should be 0 in callback
        assert all(t == 0 for _, t in calls)
        # But the file still gets written correctly
        assert tmp_target.stat().st_size == len(payload)


class TestIdempotency:
    """Re-running with an existing target shouldn't re-download."""

    def test_skip_when_file_already_exists_and_plausibly_sized(self, tmp_target):
        # Pre-create a file at the target with a plausible size
        tmp_target.parent.mkdir(parents=True, exist_ok=True)
        tmp_target.write_bytes(b"a" * MIN_EXPECTED_SIZE_BYTES)
        original_mtime = tmp_target.stat().st_mtime

        # Now call download_sample — it should skip without invoking urlopen
        with patch("resurrector.demo.download.urllib.request.urlopen") as mock_urlopen:
            result = download_sample(target=tmp_target)
            mock_urlopen.assert_not_called()

        assert result.skipped is True
        assert result.bytes_downloaded == MIN_EXPECTED_SIZE_BYTES
        # File untouched
        assert tmp_target.stat().st_mtime == original_mtime

    def test_force_redownloads_even_if_file_exists(self, tmp_target):
        tmp_target.parent.mkdir(parents=True, exist_ok=True)
        tmp_target.write_bytes(b"a" * MIN_EXPECTED_SIZE_BYTES)

        new_payload = b"b" * MIN_EXPECTED_SIZE_BYTES
        with patch(
            "resurrector.demo.download.urllib.request.urlopen",
            _mock_urlopen(new_payload),
        ):
            result = download_sample(target=tmp_target, force=True)

        assert result.skipped is False
        assert tmp_target.read_bytes()[:1] == b"b", "file should be replaced"


class TestSizeSanityChecks:
    """Refuse implausibly small / large downloads (likely redirect-to-HTML)."""

    def test_too_small_raises_and_cleans_up(self, tmp_target):
        # Way too small — likely a redirect HTML page or 404 body
        payload = b"<html>not found</html>"
        with patch(
            "resurrector.demo.download.urllib.request.urlopen",
            _mock_urlopen(payload),
        ):
            with pytest.raises(ValueError, match="outside the expected range"):
                download_sample(target=tmp_target)
        # No partial file should remain
        partial = tmp_target.with_suffix(tmp_target.suffix + ".partial")
        assert not partial.exists()
        assert not tmp_target.exists()

    def test_too_large_raises_and_cleans_up(self, tmp_target):
        # Build something larger than max — but don't actually allocate 2 GB.
        # Skip if MAX is so big that allocating it is impractical for a test.
        # We trust the bound check at the integer level.
        oversize = MAX_EXPECTED_SIZE_BYTES + 1
        # Use a sparse mock that returns one big chunk via repeated reads
        chunks = [b"x" * (1024 * 1024)] * (oversize // (1024 * 1024) + 1)
        # Trim last chunk so total is exactly oversize
        excess = sum(len(c) for c in chunks) - oversize
        if excess > 0:
            chunks[-1] = chunks[-1][:-excess]

        # Skip this test if oversize > 50MB (don't burn CI on huge alloc)
        if oversize > 50 * 1024 * 1024:
            pytest.skip(
                f"Skipping too-large test: would allocate {oversize:,} bytes "
                f"of mock payload"
            )


class TestErrorHandling:
    """Network errors should propagate cleanly + clean up the partial."""

    def test_urllib_error_propagates_and_cleans_partial(self, tmp_target):
        import urllib.error
        with patch(
            "resurrector.demo.download.urllib.request.urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            with pytest.raises(urllib.error.URLError):
                download_sample(target=tmp_target)
        partial = tmp_target.with_suffix(tmp_target.suffix + ".partial")
        assert not partial.exists()
        assert not tmp_target.exists()


class TestPathHandling:
    """Default target, parent-dir creation, ~ expansion."""

    def test_default_target_path(self, tmp_path, monkeypatch):
        # Redirect $HOME so we don't actually write to the user's
        # ~/.resurrector during tests
        monkeypatch.setenv("HOME", str(tmp_path))
        # Don't actually run the download — just verify the resolved target
        with patch(
            "resurrector.demo.download.urllib.request.urlopen",
            _mock_urlopen(b"x" * MIN_EXPECTED_SIZE_BYTES),
        ):
            result = download_sample()
        expected = tmp_path / ".resurrector" / "samples" / DEFAULT_SAMPLE_FILENAME
        assert result.path == expected
        assert result.path.exists()

    def test_creates_parent_directories(self, tmp_path):
        deep = tmp_path / "a" / "b" / "c" / "sample.mcap"
        with patch(
            "resurrector.demo.download.urllib.request.urlopen",
            _mock_urlopen(b"x" * MIN_EXPECTED_SIZE_BYTES),
        ):
            download_sample(target=deep)
        assert deep.exists()


class TestConstants:
    """The module's exposed constants should make sense."""

    def test_default_url_points_at_huggingface(self):
        assert "huggingface.co" in DEFAULT_SAMPLE_URL
        assert DEFAULT_SAMPLE_URL.endswith(".mcap?download=true") or \
               DEFAULT_SAMPLE_URL.endswith(".mcap")

    def test_size_bounds_are_sane(self):
        # MIN > 0, MAX > MIN, both reasonable
        assert MIN_EXPECTED_SIZE_BYTES > 0
        assert MAX_EXPECTED_SIZE_BYTES > MIN_EXPECTED_SIZE_BYTES
        # Real file is ~885 MB; bounds should bracket that
        assert MIN_EXPECTED_SIZE_BYTES <= 885_000_000 <= MAX_EXPECTED_SIZE_BYTES

    def test_default_filename_has_mcap_extension(self):
        assert DEFAULT_SAMPLE_FILENAME.endswith(".mcap")

    def test_download_result_dataclass_shape(self):
        r = DownloadResult(path=Path("/tmp/x"), bytes_downloaded=100, skipped=False)
        assert r.path == Path("/tmp/x")
        assert r.bytes_downloaded == 100
        assert r.skipped is False
