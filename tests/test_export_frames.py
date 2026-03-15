"""Tests for image/video frame export."""

import tempfile
from pathlib import Path

import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig

# Frame export requires Pillow
PIL = pytest.importorskip("PIL")


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def bag_with_images(tmp_dir):
    return generate_bag(
        tmp_dir / "images.mcap",
        BagConfig(duration_sec=2.0, camera_hz=10.0, include_compressed=True, compressed_hz=5.0),
    )


class TestExportFrames:
    def test_export_frame_sequence_png(self, tmp_dir, bag_with_images):
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.export import Exporter

        bf = BagFrame(bag_with_images)
        view = bf["/camera/rgb"]
        exporter = Exporter()
        result = exporter.export_frames(view, tmp_dir / "frames_png", format="png")
        assert result.exists()
        png_files = list(result.glob("*.png"))
        assert len(png_files) > 0

    def test_export_frame_sequence_jpeg(self, tmp_dir, bag_with_images):
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.export import Exporter

        bf = BagFrame(bag_with_images)
        view = bf["/camera/compressed"]
        exporter = Exporter()
        result = exporter.export_frames(view, tmp_dir / "frames_jpg", format="jpeg")
        assert result.exists()
        jpg_files = list(result.glob("*.jpg"))
        assert len(jpg_files) > 0

    def test_export_every_n(self, tmp_dir, bag_with_images):
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.export import Exporter

        bf = BagFrame(bag_with_images)
        view = bf["/camera/rgb"]
        exporter = Exporter()
        # Export all frames
        all_result = exporter.export_frames(view, tmp_dir / "all", format="png")
        all_count = len(list(all_result.glob("*.png")))
        # Export every 3rd frame
        sub_result = exporter.export_frames(view, tmp_dir / "sub", format="png", every_n=3)
        sub_count = len(list(sub_result.glob("*.png")))
        assert sub_count < all_count
        assert sub_count > 0

    def test_export_max_frames(self, tmp_dir, bag_with_images):
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.export import Exporter

        bf = BagFrame(bag_with_images)
        view = bf["/camera/rgb"]
        exporter = Exporter()
        result = exporter.export_frames(view, tmp_dir / "max5", format="png", max_frames=5)
        count = len(list(result.glob("*.png")))
        assert count == 5


class TestExportVideo:
    def test_export_video_mp4(self, tmp_dir, bag_with_images):
        cv2 = pytest.importorskip("cv2")
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.export import Exporter

        bf = BagFrame(bag_with_images)
        view = bf["/camera/rgb"]
        exporter = Exporter()
        video_path = tmp_dir / "output.mp4"
        result = exporter.export_video(view, video_path, fps=10.0)
        assert result.exists()
        assert result.stat().st_size > 0
