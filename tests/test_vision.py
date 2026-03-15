"""Tests for the vision module — frame sampling, CLIP embeddings, and search.

These tests require the vision extras:
    pip install rosbag-resurrector[vision]

Tests are skipped if sentence-transformers is not installed.
"""

import tempfile
from pathlib import Path

import numpy as np
import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig

# Pillow is needed for all vision tests
PIL = pytest.importorskip("PIL")


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def bag_with_images(tmp_dir):
    return generate_bag(
        tmp_dir / "vision.mcap",
        BagConfig(duration_sec=2.0, camera_hz=10.0, include_compressed=True),
    )


class TestFrameSampler:
    def test_sample_rate(self, bag_with_images):
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.vision import FrameSampler

        bf = BagFrame(bag_with_images)
        view = bf["/camera/rgb"]
        sampler = FrameSampler(target_hz=5.0, enable_change_detection=False)
        frames = list(sampler.sample(view))
        # At 5Hz for 2s, expect ~10 frames (±2 for edge effects)
        assert 5 <= len(frames) <= 15

    def test_sample_yields_correct_format(self, bag_with_images):
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.vision import FrameSampler

        bf = BagFrame(bag_with_images)
        sampler = FrameSampler(target_hz=2.0, enable_change_detection=False)
        frames = list(sampler.sample(bf["/camera/rgb"]))
        assert len(frames) > 0
        ts, idx, arr = frames[0]
        assert isinstance(ts, int)
        assert isinstance(idx, int)
        assert isinstance(arr, np.ndarray)

    def test_change_detection_skips_identical(self, bag_with_images):
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.vision import FrameSampler

        bf = BagFrame(bag_with_images)
        view = bf["/camera/rgb"]

        # Without change detection
        sampler_no_cd = FrameSampler(target_hz=5.0, enable_change_detection=False)
        frames_no_cd = list(sampler_no_cd.sample(view))

        # With change detection (might skip some similar frames)
        sampler_cd = FrameSampler(target_hz=5.0, enable_change_detection=True, skip_threshold=0.001)
        frames_cd = list(sampler_cd.sample(view))

        # Change detection should yield <= frames without it
        assert len(frames_cd) <= len(frames_no_cd)


class TestCLIPEmbedder:
    """Tests requiring sentence-transformers (skip if not installed)."""

    @pytest.fixture(autouse=True)
    def skip_if_no_clip(self):
        pytest.importorskip("sentence_transformers")

    def test_embed_image(self):
        from resurrector.core.vision import CLIPEmbedder
        embedder = CLIPEmbedder(backend="local")
        # Create a simple test image
        img = np.random.randint(0, 255, (224, 224, 3), dtype=np.uint8)
        emb = embedder.embed_image(img)
        assert emb.shape == (512,)
        assert emb.dtype == np.float32

    def test_embed_text(self):
        from resurrector.core.vision import CLIPEmbedder
        embedder = CLIPEmbedder(backend="local")
        emb = embedder.embed_text("a robot picking up a ball")
        assert emb.shape == (512,)
        assert emb.dtype == np.float32

    def test_embed_batch(self):
        from resurrector.core.vision import CLIPEmbedder
        embedder = CLIPEmbedder(backend="local")
        images = [np.random.randint(0, 255, (64, 64, 3), dtype=np.uint8) for _ in range(4)]
        embs = embedder.embed_images_batch(images, batch_size=2)
        assert embs.shape == (4, 512)

    def test_similarity_sanity(self):
        """Text about an image should be more similar to matching images."""
        from resurrector.core.vision import CLIPEmbedder
        embedder = CLIPEmbedder(backend="local")

        # Red image
        red = np.full((64, 64, 3), [255, 0, 0], dtype=np.uint8)
        # Blue image
        blue = np.full((64, 64, 3), [0, 0, 255], dtype=np.uint8)

        emb_red = embedder.embed_image(red)
        emb_blue = embedder.embed_image(blue)
        emb_text = embedder.embed_text("a red image")

        # Cosine similarity
        sim_red = np.dot(emb_red, emb_text) / (np.linalg.norm(emb_red) * np.linalg.norm(emb_text))
        sim_blue = np.dot(emb_blue, emb_text) / (np.linalg.norm(emb_blue) * np.linalg.norm(emb_text))

        # "a red image" should be more similar to the red image
        assert sim_red > sim_blue


class TestFrameSearchEngine:
    """End-to-end tests requiring sentence-transformers."""

    @pytest.fixture(autouse=True)
    def skip_if_no_clip(self):
        pytest.importorskip("sentence_transformers")

    def test_index_and_search(self, bag_with_images, tmp_dir):
        from resurrector.ingest.scanner import scan_path
        from resurrector.ingest.parser import parse_bag
        from resurrector.ingest.indexer import BagIndex
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.vision import FrameSearchEngine

        # Index the bag first
        index = BagIndex(tmp_dir / "test.db")
        scanned = scan_path(bag_with_images)[0]
        parser = parse_bag(bag_with_images)
        meta = parser.get_metadata()
        bag_id = index.upsert_bag(scanned, meta)

        # Index frames
        engine = FrameSearchEngine(index)
        n = engine.index_bag(bag_id, bag_with_images, sample_hz=2.0)
        assert n > 0

        # Verify embeddings stored
        assert index.has_frame_embeddings(bag_id)
        assert index.count_frame_embeddings(bag_id) == n

        # Search
        results = engine.search("colorful image", top_k=5)
        assert len(results) > 0
        assert results[0].similarity > 0

        index.close()

    def test_incremental_indexing(self, bag_with_images, tmp_dir):
        from resurrector.ingest.scanner import scan_path
        from resurrector.ingest.parser import parse_bag
        from resurrector.ingest.indexer import BagIndex
        from resurrector.core.vision import FrameSearchEngine

        index = BagIndex(tmp_dir / "test.db")
        scanned = scan_path(bag_with_images)[0]
        parser = parse_bag(bag_with_images)
        meta = parser.get_metadata()
        bag_id = index.upsert_bag(scanned, meta)

        engine = FrameSearchEngine(index)

        # First index
        n1 = engine.index_bag(bag_id, bag_with_images, sample_hz=2.0)
        assert n1 > 0

        # Second index without force — should skip
        n2 = engine.index_bag(bag_id, bag_with_images, sample_hz=2.0)
        assert n2 == 0

        # With force — should re-index
        n3 = engine.index_bag(bag_id, bag_with_images, sample_hz=2.0, force=True)
        assert n3 > 0

        index.close()

    def test_search_temporal_clips(self, bag_with_images, tmp_dir):
        from resurrector.ingest.scanner import scan_path
        from resurrector.ingest.parser import parse_bag
        from resurrector.ingest.indexer import BagIndex
        from resurrector.core.vision import FrameSearchEngine

        index = BagIndex(tmp_dir / "test.db")
        scanned = scan_path(bag_with_images)[0]
        parser = parse_bag(bag_with_images)
        meta = parser.get_metadata()
        bag_id = index.upsert_bag(scanned, meta)

        engine = FrameSearchEngine(index)
        engine.index_bag(bag_id, bag_with_images, sample_hz=5.0)

        clips = engine.search_temporal("colored frame", clip_duration_sec=2.0, top_k=5)
        assert len(clips) > 0
        assert clips[0].frame_count > 0
        assert clips[0].duration_sec >= 0

        index.close()
