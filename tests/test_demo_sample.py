"""The synthetic sample generator and `resurrector demo` never leave a broken bag.

A bag cut off mid-write has no MCAP summary, so every later reader fails on
it, including `resurrector demo`, which reuses an existing sample. Two ways
that happened: a partial install without Pillow raised mid-write (after the
header was already on disk), and installs without Pillow before Pillow became
a base dependency wrote 1x1 grayscale placeholder frames that LeRobot export
and the dashboard's frame views break on.
"""

from __future__ import annotations

import io
import sys

import pytest
from typer.testing import CliRunner

from resurrector.cli.main import app
from resurrector.core.bag_frame import BagFrame
from resurrector.demo import sample_bag
from resurrector.demo.sample_bag import BagConfig, generate_bag, stale_sample_reason


def _placeholder_jpeg(*_args) -> bytes:
    """The 1x1 grayscale JPEG installs without Pillow used to write."""
    from PIL import Image

    buf = io.BytesIO()
    Image.new("L", (1, 1), 128).save(buf, format="JPEG")
    return buf.getvalue()


@pytest.fixture
def placeholder_bag(tmp_path, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(sample_bag, "_make_test_jpeg", _placeholder_jpeg)
        return generate_bag(tmp_path / "old_sample.mcap", BagConfig(duration_sec=0.5))


def _first_compressed_shape(path) -> tuple[int, ...]:
    _, frame = next(iter(BagFrame(path)["/camera/compressed"].iter_images()))
    return frame.shape


class TestGenerateBagOutput:
    def test_missing_pillow_raises_one_line_hint_and_writes_nothing(self, tmp_path, monkeypatch):
        """Would catch: a partial install (pip --no-deps) without Pillow
        leaving a 55-byte MCAP behind a bare ModuleNotFoundError."""
        monkeypatch.setitem(sys.modules, "PIL", None)
        with pytest.raises(ImportError, match=r"pip install Pillow") as exc:
            generate_bag(tmp_path / "d.mcap", BagConfig(duration_sec=0.5))
        assert "\n" not in str(exc.value)
        assert isinstance(exc.value.__cause__, ImportError)
        assert list(tmp_path.iterdir()) == []

    def test_missing_pillow_is_detected_before_writing_starts(self, tmp_path, monkeypatch):
        """The Pillow check runs before the output is opened, not after
        seconds of IMU/lidar encoding."""
        opened = []
        real = sample_bag._atomic_output

        def spy(path):
            opened.append(path)
            return real(path)

        monkeypatch.setattr(sample_bag, "_atomic_output", spy)
        monkeypatch.setitem(sys.modules, "PIL", None)
        with pytest.raises(ImportError):
            generate_bag(tmp_path / "d.mcap", BagConfig(duration_sec=0.5))
        assert opened == []

    def test_no_pillow_needed_without_camera_frames(self, tmp_path, monkeypatch):
        monkeypatch.setitem(sys.modules, "PIL", None)
        out = generate_bag(tmp_path / "d.mcap",
                           BagConfig(duration_sec=0.5, include_compressed=False))
        assert "/camera/compressed" not in BagFrame(out).topic_names

    def test_failure_mid_write_leaves_no_partial_file(self, tmp_path, monkeypatch):
        """Would catch: writing straight to the output path, so a failure
        after the MCAP header (or Ctrl-C) left a truncated bag that broke
        every later `resurrector demo` with "Could not read summary"."""
        def boom(*_args):
            raise RuntimeError("encoder died")

        monkeypatch.setattr(sample_bag, "_make_test_jpeg", boom)
        with pytest.raises(RuntimeError, match="encoder died"):
            generate_bag(tmp_path / "d.mcap", BagConfig(duration_sec=0.5))
        assert list(tmp_path.iterdir()) == []

    def test_failed_regeneration_keeps_the_previous_bag(self, tmp_path, monkeypatch):
        out = generate_bag(tmp_path / "d.mcap", BagConfig(duration_sec=0.5))
        before = out.read_bytes()

        def interrupted(*_args):
            raise KeyboardInterrupt

        monkeypatch.setattr(sample_bag, "_make_test_jpeg", interrupted)
        with pytest.raises(KeyboardInterrupt):
            generate_bag(out, BagConfig(duration_sec=0.5))
        assert out.read_bytes() == before
        assert [p.name for p in tmp_path.iterdir()] == ["d.mcap"]


class TestStaleSampleReason:
    def test_good_sample_is_kept(self, tmp_path):
        assert stale_sample_reason(generate_bag(tmp_path / "d.mcap",
                                                BagConfig(duration_sec=0.5))) is None

    def test_placeholder_frames_are_flagged(self, placeholder_bag):
        assert _first_compressed_shape(placeholder_bag) == (1, 1)
        assert "1x1 placeholders" in stale_sample_reason(placeholder_bag)

    @pytest.mark.parametrize("keep", [55, 0.5])
    def test_truncated_sample_is_flagged(self, tmp_path, keep):
        good = generate_bag(tmp_path / "good.mcap", BagConfig(duration_sec=0.5)).read_bytes()
        cut = tmp_path / "cut.mcap"
        cut.write_bytes(good[: keep if isinstance(keep, int) else int(len(good) * keep)])
        assert "incomplete" in stale_sample_reason(cut)

    def test_empty_file_is_flagged(self, tmp_path):
        (tmp_path / "e.mcap").write_bytes(b"")
        assert stale_sample_reason(tmp_path / "e.mcap") == "it is empty"

    def test_bag_without_compressed_topic_is_kept(self, tmp_path):
        out = generate_bag(tmp_path / "d.mcap",
                           BagConfig(duration_sec=0.5, include_compressed=False))
        assert stale_sample_reason(out) is None

    def test_files_from_other_writers_are_never_flagged(self, tmp_path):
        """A user's own bag (or any non-MCAP file) at the -o path must not
        be overwritten, even when it is broken."""
        from resurrector.demo.scene_bag import generate_scene_bag

        other = generate_scene_bag(tmp_path / "scene.mcap").read_bytes()
        cut = tmp_path / "their_bag.mcap"
        cut.write_bytes(other[: len(other) // 2])
        assert stale_sample_reason(cut) is None
        (tmp_path / "notes.mcap").write_text("not an mcap")
        assert stale_sample_reason(tmp_path / "notes.mcap") is None


class TestDemoCommand:
    def _demo(self, *args, env=None):
        return CliRunner().invoke(app, ["demo", *args], env={"COLUMNS": "200", **(env or {})})

    def test_placeholder_sample_is_regenerated_with_a_notice(self, placeholder_bag):
        """Would catch: upgraded users keeping the 1x1 sample forever,
        because `resurrector demo` reuses any existing file."""
        result = self._demo("-o", str(placeholder_bag))
        assert result.exit_code == 0, result.output
        assert "Regenerating" in result.output and "1x1 placeholders" in result.output
        assert "already exists" not in result.output
        assert _first_compressed_shape(placeholder_bag) == (48, 64, 3)

    def test_truncated_sample_is_regenerated(self, tmp_path):
        out = tmp_path / "d.mcap"
        good = generate_bag(out, BagConfig(duration_sec=0.5)).read_bytes()
        out.write_bytes(good[:55])
        result = self._demo("-o", str(out))
        assert result.exit_code == 0, result.output
        assert "incomplete" in result.output
        assert _first_compressed_shape(out) == (48, 64, 3)

    def test_good_sample_is_reused(self, tmp_path):
        out = generate_bag(tmp_path / "d.mcap", BagConfig(duration_sec=0.5))
        before = out.read_bytes()
        result = self._demo("-o", str(out))
        assert result.exit_code == 0, result.output
        assert "already exists" in result.output and "Regenerating" not in result.output
        assert out.read_bytes() == before

    def test_missing_pillow_prints_hint_not_traceback(self, tmp_path, monkeypatch):
        monkeypatch.setitem(sys.modules, "PIL", None)
        result = self._demo("-o", str(tmp_path / "d.mcap"))
        assert result.exit_code == 1
        assert "pip install Pillow" in result.output
        assert not isinstance(result.exception, ImportError)
        assert list(tmp_path.iterdir()) == []
