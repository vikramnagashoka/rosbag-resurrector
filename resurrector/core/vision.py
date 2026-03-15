"""Vision module — frame sampling, CLIP embeddings, and semantic search.

Provides:
- FrameSampler: extract frames at target rate with change detection
- CLIPEmbedder: generate CLIP embeddings (local or OpenAI API)
- FrameSearchEngine: index bags and search by natural language

Requires: pip install rosbag-resurrector[vision] (local CLIP)
     or:  pip install rosbag-resurrector[vision-openai] (API-based)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

import numpy as np

if TYPE_CHECKING:
    from resurrector.core.bag_frame import TopicView
    from resurrector.ingest.indexer import BagIndex

logger = logging.getLogger("resurrector.core.vision")


# --- Result dataclasses ---

@dataclass
class FrameSearchResult:
    """A single frame matching a search query."""
    bag_id: int
    bag_path: str
    topic: str
    timestamp_ns: int
    similarity: float
    frame_index: int

    @property
    def timestamp_sec(self) -> float:
        return self.timestamp_ns / 1e9


@dataclass
class ClipSearchResult:
    """A temporal clip (group of consecutive matching frames)."""
    bag_id: int
    bag_path: str
    topic: str
    start_timestamp_ns: int
    end_timestamp_ns: int
    avg_similarity: float
    peak_similarity: float
    frame_count: int

    @property
    def start_sec(self) -> float:
        return self.start_timestamp_ns / 1e9

    @property
    def end_sec(self) -> float:
        return self.end_timestamp_ns / 1e9

    @property
    def duration_sec(self) -> float:
        return (self.end_timestamp_ns - self.start_timestamp_ns) / 1e9


# --- Frame Sampler ---

class FrameSampler:
    """Extract frames from image topics at a target rate with optional change detection."""

    def __init__(
        self,
        target_hz: float = 5.0,
        skip_threshold: float = 0.02,
        enable_change_detection: bool = True,
    ):
        self.target_hz = target_hz
        self.skip_threshold = skip_threshold
        self.enable_change_detection = enable_change_detection

    def sample(
        self, topic_view: "TopicView",
    ) -> Iterator[tuple[int, int, np.ndarray]]:
        """Yield (timestamp_ns, frame_index, frame_array) at the target sample rate.

        If change detection is enabled, skips frames nearly identical to
        the previous sampled frame (MSE below threshold).
        """
        interval_ns = int(1e9 / self.target_hz)
        next_target_ns = 0
        prev_small: np.ndarray | None = None
        frame_idx = 0

        for ts, arr in topic_view.iter_images():
            if next_target_ns == 0:
                next_target_ns = ts

            if ts < next_target_ns:
                frame_idx += 1
                continue

            # Change detection: skip near-identical frames
            if self.enable_change_detection and prev_small is not None:
                small = self._downscale(arr)
                mse = np.mean((small.astype(float) - prev_small.astype(float)) ** 2)
                mse /= 255.0 ** 2  # Normalize to [0, 1]
                if mse < self.skip_threshold:
                    next_target_ns += interval_ns
                    frame_idx += 1
                    continue
                prev_small = small
            else:
                prev_small = self._downscale(arr)

            yield ts, frame_idx, arr
            next_target_ns = ts + interval_ns
            frame_idx += 1

    @staticmethod
    def _downscale(arr: np.ndarray, size: int = 64) -> np.ndarray:
        """Downscale an image for fast comparison. Uses simple strided slicing."""
        h, w = arr.shape[:2]
        step_h = max(1, h // size)
        step_w = max(1, w // size)
        return arr[::step_h, ::step_w]


# --- CLIP Embedder ---

class CLIPEmbedder:
    """Generate CLIP embeddings for images and text.

    Supports two backends:
    - "local": sentence-transformers (requires pip install rosbag-resurrector[vision])
    - "openai": OpenAI CLIP API (requires pip install rosbag-resurrector[vision-openai])
    - "auto": try local first, fall back to openai
    """

    MODEL_NAME = "clip-ViT-B-32"
    EMBEDDING_DIM = 512

    def __init__(
        self,
        backend: str = "auto",
        model_name: str | None = None,
        device: str | None = None,
    ):
        self.backend = backend
        self.model_name = model_name or self.MODEL_NAME
        self.device = device
        self._model = None
        self._openai_client = None
        self._resolved_backend: str | None = None

    def _ensure_loaded(self):
        """Lazy-load the embedding model on first use."""
        if self._resolved_backend is not None:
            return

        if self.backend in ("auto", "local"):
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(self.model_name, device=self.device)
                self._resolved_backend = "local"
                logger.info("Using local CLIP model: %s", self.model_name)
                return
            except ImportError:
                if self.backend == "local":
                    raise ImportError(
                        "Local CLIP requires sentence-transformers. "
                        "Install with: pip install rosbag-resurrector[vision]"
                    )

        if self.backend in ("auto", "openai"):
            try:
                import openai
                self._openai_client = openai.OpenAI()
                self._resolved_backend = "openai"
                logger.info("Using OpenAI CLIP API")
                return
            except ImportError:
                if self.backend == "openai":
                    raise ImportError(
                        "OpenAI backend requires the openai package. "
                        "Install with: pip install rosbag-resurrector[vision-openai]"
                    )

        raise ImportError(
            "Semantic search requires either:\n"
            "  pip install rosbag-resurrector[vision]         # Local CLIP (recommended)\n"
            "  pip install rosbag-resurrector[vision-openai]  # OpenAI API (lighter install)"
        )

    def embed_image(self, image: np.ndarray) -> np.ndarray:
        """Embed a single image. Returns float32 array of shape (EMBEDDING_DIM,)."""
        self._ensure_loaded()
        if self._resolved_backend == "local":
            from PIL import Image as PILImage
            pil_img = PILImage.fromarray(image)
            emb = self._model.encode(pil_img, convert_to_numpy=True)
            return emb.astype(np.float32).flatten()
        else:
            return self._openai_embed_image(image)

    def embed_images_batch(
        self, images: list[np.ndarray], batch_size: int = 32,
    ) -> np.ndarray:
        """Embed a batch of images. Returns float32 array of shape (N, EMBEDDING_DIM)."""
        self._ensure_loaded()
        if self._resolved_backend == "local":
            from PIL import Image as PILImage
            pil_images = [PILImage.fromarray(img) for img in images]
            results = []
            for i in range(0, len(pil_images), batch_size):
                batch = pil_images[i:i + batch_size]
                embs = self._model.encode(batch, convert_to_numpy=True, batch_size=batch_size)
                results.append(embs)
            return np.vstack(results).astype(np.float32)
        else:
            # OpenAI doesn't support batch image embedding — process one by one
            results = [self._openai_embed_image(img) for img in images]
            return np.vstack(results).astype(np.float32)

    def embed_text(self, text: str) -> np.ndarray:
        """Embed a text query. Returns float32 array of shape (EMBEDDING_DIM,)."""
        self._ensure_loaded()
        if self._resolved_backend == "local":
            emb = self._model.encode(text, convert_to_numpy=True)
            return emb.astype(np.float32).flatten()
        else:
            return self._openai_embed_text(text)

    def _openai_embed_image(self, image: np.ndarray) -> np.ndarray:
        """Embed an image via OpenAI API (encode as base64 JPEG)."""
        import base64
        import io
        from PIL import Image as PILImage

        pil_img = PILImage.fromarray(image)
        buf = io.BytesIO()
        pil_img.save(buf, format="JPEG", quality=85)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

        response = self._openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=f"data:image/jpeg;base64,{b64}",
        )
        emb = np.array(response.data[0].embedding, dtype=np.float32)
        # Pad/truncate to EMBEDDING_DIM
        if len(emb) > self.EMBEDDING_DIM:
            emb = emb[:self.EMBEDDING_DIM]
        elif len(emb) < self.EMBEDDING_DIM:
            emb = np.pad(emb, (0, self.EMBEDDING_DIM - len(emb)))
        return emb

    def _openai_embed_text(self, text: str) -> np.ndarray:
        """Embed text via OpenAI API."""
        response = self._openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=text,
        )
        emb = np.array(response.data[0].embedding, dtype=np.float32)
        if len(emb) > self.EMBEDDING_DIM:
            emb = emb[:self.EMBEDDING_DIM]
        elif len(emb) < self.EMBEDDING_DIM:
            emb = np.pad(emb, (0, self.EMBEDDING_DIM - len(emb)))
        return emb


# --- Frame Search Engine ---

class FrameSearchEngine:
    """Orchestrates frame indexing and semantic search."""

    def __init__(self, index: "BagIndex", embedder: CLIPEmbedder | None = None):
        self._index = index
        self._embedder = embedder

    def _get_embedder(self) -> CLIPEmbedder:
        if self._embedder is None:
            self._embedder = CLIPEmbedder()
        return self._embedder

    def index_bag(
        self,
        bag_id: int,
        bag_path: str | Path,
        topic: str | None = None,
        sample_hz: float = 5.0,
        batch_size: int = 32,
        force: bool = False,
    ) -> int:
        """Index frames from a bag. Returns number of frames indexed.

        Args:
            bag_id: DuckDB bag ID.
            bag_path: Path to the bag file.
            topic: Image topic to index. None = auto-detect first image topic.
            sample_hz: Frame sampling rate.
            batch_size: Embedding batch size.
            force: Re-index even if embeddings already exist.
        """
        from resurrector.core.bag_frame import BagFrame

        bf = BagFrame(bag_path)

        # Auto-detect image topics
        if topic is None:
            image_topics = [t for t in bf.topics if t.message_type in (
                "sensor_msgs/msg/Image", "sensor_msgs/msg/CompressedImage",
            )]
            if not image_topics:
                logger.info("No image topics found in %s", bag_path)
                return 0
            topic = image_topics[0].name

        # Skip if already indexed
        if not force and self._index.has_frame_embeddings(bag_id, topic):
            logger.info("Skipping %s — already indexed", bag_path)
            return 0

        if force:
            self._index.delete_frame_embeddings(bag_id, topic)

        view = bf[topic]
        sampler = FrameSampler(target_hz=sample_hz)
        embedder = self._get_embedder()

        # Batch frames for efficient embedding
        batch_frames: list[np.ndarray] = []
        batch_ts: list[int] = []
        batch_idx: list[int] = []
        total_indexed = 0

        for ts, frame_idx, arr in sampler.sample(view):
            batch_frames.append(arr)
            batch_ts.append(ts)
            batch_idx.append(frame_idx)

            if len(batch_frames) >= batch_size:
                embeddings = embedder.embed_images_batch(batch_frames, batch_size)
                self._index.upsert_frame_embeddings(
                    bag_id, topic, batch_ts, batch_idx,
                    embeddings.tolist(),
                )
                total_indexed += len(batch_frames)
                batch_frames.clear()
                batch_ts.clear()
                batch_idx.clear()

        # Remaining batch
        if batch_frames:
            embeddings = embedder.embed_images_batch(batch_frames, batch_size)
            self._index.upsert_frame_embeddings(
                bag_id, topic, batch_ts, batch_idx,
                embeddings.tolist(),
            )
            total_indexed += len(batch_frames)

        logger.info("Indexed %d frames from %s topic %s", total_indexed, bag_path, topic)
        return total_indexed

    def search(
        self,
        query: str,
        top_k: int = 20,
        bag_id: int | None = None,
        min_similarity: float = 0.15,
    ) -> list[FrameSearchResult]:
        """Search for frames matching a text description."""
        embedder = self._get_embedder()
        query_emb = embedder.embed_text(query)

        rows = self._index.search_embeddings(
            query_embedding=query_emb.tolist(),
            top_k=top_k,
            bag_id=bag_id,
            min_similarity=min_similarity,
        )

        return [
            FrameSearchResult(
                bag_id=r["bag_id"],
                bag_path=r["bag_path"],
                topic=r["topic"],
                timestamp_ns=r["timestamp_ns"],
                similarity=r["similarity"],
                frame_index=r["frame_index"],
            )
            for r in rows
        ]

    def search_temporal(
        self,
        query: str,
        clip_duration_sec: float = 5.0,
        top_k: int = 10,
        bag_id: int | None = None,
        min_similarity: float = 0.15,
    ) -> list[ClipSearchResult]:
        """Search for temporal clips — groups of consecutive matching frames.

        Groups frames within `clip_duration_sec` of each other into clips.
        Returns clips sorted by average similarity.
        """
        # Get more individual results to group
        frame_results = self.search(
            query, top_k=top_k * 10, bag_id=bag_id, min_similarity=min_similarity,
        )

        if not frame_results:
            return []

        # Group consecutive frames into clips
        window_ns = int(clip_duration_sec * 1e9)
        clips: list[ClipSearchResult] = []
        used = set()

        # Sort by bag/topic/time for grouping
        sorted_results = sorted(frame_results, key=lambda r: (r.bag_id, r.topic, r.timestamp_ns))

        for i, result in enumerate(sorted_results):
            if i in used:
                continue

            # Start a new clip
            clip_frames = [result]
            used.add(i)

            for j in range(i + 1, len(sorted_results)):
                if j in used:
                    continue
                other = sorted_results[j]
                if (other.bag_id == result.bag_id
                    and other.topic == result.topic
                    and other.timestamp_ns - clip_frames[-1].timestamp_ns <= window_ns):
                    clip_frames.append(other)
                    used.add(j)

            sims = [f.similarity for f in clip_frames]
            clips.append(ClipSearchResult(
                bag_id=result.bag_id,
                bag_path=result.bag_path,
                topic=result.topic,
                start_timestamp_ns=clip_frames[0].timestamp_ns,
                end_timestamp_ns=clip_frames[-1].timestamp_ns,
                avg_similarity=float(np.mean(sims)),
                peak_similarity=float(np.max(sims)),
                frame_count=len(clip_frames),
            ))

        # Sort by avg similarity descending, return top_k
        clips.sort(key=lambda c: c.avg_similarity, reverse=True)
        return clips[:top_k]


def save_search_results(
    results: list[FrameSearchResult] | list[ClipSearchResult],
    query: str,
    save_dir: Path,
    extract_clips: bool = False,
) -> Path:
    """Save search results to disk with extracted frames/clips and metadata.

    Args:
        results: Search results to save.
        query: The search query text.
        save_dir: Directory to save results.
        extract_clips: If True and results are ClipSearchResult, extract MP4 clips.

    Returns:
        Path to the save directory.
    """
    from resurrector.core.bag_frame import BagFrame
    from resurrector.core.export import Exporter

    save_dir.mkdir(parents=True, exist_ok=True)
    exporter = Exporter()
    metadata_entries = []

    for rank, result in enumerate(results, 1):
        if isinstance(result, ClipSearchResult):
            bag_name = Path(result.bag_path).stem
            entry = {
                "rank": rank,
                "similarity": round(result.avg_similarity, 4),
                "peak_similarity": round(result.peak_similarity, 4),
                "bag_path": result.bag_path,
                "topic": result.topic,
                "start_timestamp_sec": round(result.start_sec, 2),
                "end_timestamp_sec": round(result.end_sec, 2),
                "duration_sec": round(result.duration_sec, 2),
                "frame_count": result.frame_count,
            }

            if extract_clips:
                try:
                    bf = BagFrame(result.bag_path)
                    # Pad clip by 1 second on each side
                    start_sec = max(0, result.start_sec - bf.metadata.start_time_ns / 1e9 - 1.0)
                    end_sec = result.end_sec - bf.metadata.start_time_ns / 1e9 + 1.0
                    sliced = bf.time_slice(start_sec, end_sec)
                    view = sliced[result.topic]
                    clip_name = f"{rank:02d}_{bag_name}_{result.start_sec:.1f}s-{result.end_sec:.1f}s.mp4"
                    exporter.export_video(view, save_dir / clip_name)
                    entry["saved_file"] = clip_name
                except Exception as e:
                    logger.warning("Failed to extract clip for result %d: %s", rank, e)
                    entry["saved_file"] = None
        else:
            # FrameSearchResult — extract single frame as JPEG
            bag_name = Path(result.bag_path).stem
            entry = {
                "rank": rank,
                "similarity": round(result.similarity, 4),
                "bag_path": result.bag_path,
                "topic": result.topic,
                "timestamp_sec": round(result.timestamp_sec, 2),
                "frame_index": result.frame_index,
            }

            try:
                from PIL import Image as PILImage
                bf = BagFrame(result.bag_path)
                view = bf[result.topic]
                # Seek to the frame by iterating
                for ts, arr in view.iter_images():
                    if ts >= result.timestamp_ns:
                        img_name = f"{rank:02d}_{bag_name}_{result.timestamp_sec:.1f}s.jpg"
                        PILImage.fromarray(arr).save(save_dir / img_name, quality=90)
                        entry["saved_file"] = img_name
                        break
                else:
                    entry["saved_file"] = None
            except Exception as e:
                logger.warning("Failed to extract frame for result %d: %s", rank, e)
                entry["saved_file"] = None

        metadata_entries.append(entry)

    # Write results.json
    results_json = {
        "query": query,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "result_count": len(metadata_entries),
        "results": metadata_entries,
    }
    json_path = save_dir / "results.json"
    json_path.write_text(json.dumps(results_json, indent=2))

    logger.info("Saved %d results to %s", len(metadata_entries), save_dir)
    return save_dir
