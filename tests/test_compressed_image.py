"""Tests for CompressedImage parsing and image iteration."""

import tempfile
from pathlib import Path

import numpy as np
import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.core.bag_frame import BagFrame, TopicView
from resurrector.ingest.parser import MCAPParser

# CompressedImage tests require Pillow
PIL = pytest.importorskip("PIL")


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def bag_with_compressed(tmp_dir):
    return generate_bag(
        tmp_dir / "compressed.mcap",
        BagConfig(duration_sec=2.0, include_compressed=True, compressed_hz=10.0),
    )


class TestCompressedImageParsing:
    def test_parse_metadata(self, bag_with_compressed):
        parser = MCAPParser(bag_with_compressed)
        meta = parser.get_metadata()
        topic_names = [t.name for t in meta.topics]
        assert "/camera/compressed" in topic_names

    def test_parse_compressed_message(self, bag_with_compressed):
        parser = MCAPParser(bag_with_compressed)
        messages = list(parser.read_messages(topics=["/camera/compressed"]))
        assert len(messages) > 0
        msg = messages[0]
        assert "format" in msg.data
        assert msg.data["format"] == "jpeg"
        assert "data_length" in msg.data
        assert msg.data["data_length"] > 0

    def test_get_compressed_image_array(self, bag_with_compressed):
        from resurrector.ingest.parser import get_compressed_image_array
        parser = MCAPParser(bag_with_compressed)
        messages = list(parser.read_messages(topics=["/camera/compressed"]))
        arr = get_compressed_image_array(messages[0])
        assert arr is not None
        assert isinstance(arr, np.ndarray)
        assert arr.shape[0] == 48  # height
        assert arr.shape[1] == 64  # width
        assert len(arr.shape) == 3  # RGB

    def test_is_image_topic(self, bag_with_compressed):
        bf = BagFrame(bag_with_compressed)
        assert bf["/camera/compressed"].is_image_topic is True
        assert bf["/camera/rgb"].is_image_topic is True
        assert bf["/imu/data"].is_image_topic is False

    def test_iter_images_compressed(self, bag_with_compressed):
        bf = BagFrame(bag_with_compressed)
        view = bf["/camera/compressed"]
        frames = list(view.iter_images())
        assert len(frames) > 0
        ts, arr = frames[0]
        assert isinstance(ts, int)
        assert isinstance(arr, np.ndarray)
        assert arr.shape[0] == 48
        assert arr.shape[1] == 64

    def test_iter_images_raw(self, bag_with_compressed):
        bf = BagFrame(bag_with_compressed)
        view = bf["/camera/rgb"]
        frames = list(view.iter_images())
        assert len(frames) > 0
        ts, arr = frames[0]
        assert isinstance(arr, np.ndarray)

    def test_iter_images_non_image_raises(self, bag_with_compressed):
        bf = BagFrame(bag_with_compressed)
        with pytest.raises(TypeError, match="not an image topic"):
            list(bf["/imu/data"].iter_images())
