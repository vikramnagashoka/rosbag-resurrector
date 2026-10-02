"""LeRobot dataset export via LeRobot's own writer.

The pre-v0.8.4 exporter hand-rolled a LeRobot v2.0 directory layout. Current
LeRobot (codebase v3.0) refuses to load it, and the hand-rolled version also
dropped camera pixels, declared string columns as float32 and wrote a
non-integer fps. Rather than chase the spec, this module drives
``LeRobotDataset.create() -> add_frame() -> save_episode() -> finalize()``,
so the on-disk format is whatever the installed LeRobot considers valid.

Our job is the mapping from a bag to LeRobot frames:

- **Uniform time grid.** LeRobot derives ``timestamp = frame_index / fps``,
  so every frame must sit on an exact ``1/fps`` grid. Topics are resampled
  onto that grid with a backward as-of join: each frame gets the latest
  sample at or before its grid time. That is causal (no future sample
  leaks into a frame, which matters for policy training).
- **Grid bounds** are ``[max(first_ts), min(last_ts)]`` across the selected
  topics, so no topic is ever extrapolated before it starts or held past
  the point where it stopped publishing.
- ``observation.state`` = numeric fields of non-image topics (minus header
  stamps), ``action`` = numeric fields of ``action_topics`` if given, and
  ``observation.images.<topic>`` = one video stream per image topic.

Memory: the resampler streams ``iter_chunks()`` and holds one chunk plus the
grid-shaped output; images stream one frame at a time (LeRobot spills frames
to disk before encoding). The output itself is grid-sized by definition.
"""

from __future__ import annotations

import logging
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator, Sequence

import numpy as np
import polars as pl

if TYPE_CHECKING:
    from resurrector.core.bag_frame import BagFrame, TopicView

logger = logging.getLogger(__name__)

INSTALL_HINT = (
    "LeRobot export needs the [lerobot] extra (Python 3.12+): "
    "pip install 'rosbag-resurrector[lerobot]'"
)

DEFAULT_FPS = 30

# Fields that are bookkeeping, not signal. Header stamps duplicate the
# timeline; image geometry fields are constant per camera.
_SKIP_SUFFIXES = (
    "header.stamp_sec", "header.stamp_nsec", "data_length",
)
_NUMERIC = (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Boolean)


def lerobot_available() -> bool:
    """True when LeRobot's dataset writer is importable."""
    try:
        from lerobot.datasets.lerobot_dataset import LeRobotDataset  # noqa: F401
        return True
    except Exception:
        return False


@dataclass
class LeRobotExportResult:
    path: Path
    episodes: int
    frames: int
    fps: int
    state_names: list[str] = field(default_factory=list)
    action_names: list[str] = field(default_factory=list)
    camera_keys: list[str] = field(default_factory=list)


# ----------------------------------------------------------------- helpers

def build_grid(start_ns: int, end_ns: int, fps: int) -> np.ndarray:
    """Uniform int64 timestamps from ``start_ns`` to ``end_ns`` at ``fps``."""
    if end_ns < start_ns:
        return np.empty(0, dtype=np.int64)
    step = 1e9 / fps
    n = int((end_ns - start_ns) // step) + 1
    return start_ns + np.round(np.arange(n) * step).astype(np.int64)


def numeric_columns(schema: pl.Schema) -> list[str]:
    """Signal columns of a topic chunk: numeric, not timestamps/bookkeeping."""
    return [
        name for name, dtype in schema.items()
        if name != "timestamp_ns"
        and dtype in _NUMERIC
        and not name.endswith(_SKIP_SUFFIXES)
    ]


def asof_on_grid(
    chunks: Iterable[pl.DataFrame],
    grid: np.ndarray,
    value_cols: Sequence[str],
) -> pl.DataFrame:
    """Streaming backward as-of resample of time-ordered chunks onto ``grid``.

    Equivalent to ``grid.join_asof(full_topic, strategy="backward")`` but
    holds only the current chunk plus a one-row carry from the previous
    chunk, so memory is bounded by chunk size rather than topic size.
    A grid point is resolved once a chunk ends at or after it.
    """
    cols = ["timestamp_ns", *value_cols]
    parts: list[pl.DataFrame] = []
    carry: pl.DataFrame | None = None
    gi = 0

    def _resolve(upto: int, src: pl.DataFrame) -> None:
        nonlocal gi
        g = pl.DataFrame({"timestamp_ns": grid[gi:upto]})
        parts.append(g.join_asof(src, on="timestamp_ns", strategy="backward"))
        gi = upto

    for chunk in chunks:
        if chunk.height == 0:
            continue
        chunk = (
            chunk.select(cols)
            .with_columns(pl.col("timestamp_ns").cast(pl.Int64))
            .sort("timestamp_ns")
        )
        hi = int(np.searchsorted(grid, chunk["timestamp_ns"][-1], side="right"))
        if hi > gi:
            src = chunk if carry is None else pl.concat([carry, chunk], how="vertical_relaxed")
            _resolve(hi, src)
        carry = chunk.tail(1)

    if gi < len(grid):
        if carry is None:
            carry = pl.DataFrame(schema={c: pl.Float64 for c in cols}).with_columns(
                pl.col("timestamp_ns").cast(pl.Int64)
            )
        _resolve(len(grid), carry)

    if not parts:
        return pl.DataFrame({"timestamp_ns": grid})
    return pl.concat(parts, how="vertical_relaxed")


def to_rgb(arr: np.ndarray, encoding: str | None) -> np.ndarray:
    """Normalize a decoded camera frame to HxWx3 uint8 RGB."""
    if arr.ndim == 2:
        arr = np.stack([arr] * 3, axis=-1)
    elif arr.shape[-1] == 4:
        arr = arr[..., :3]
        if encoding and encoding.startswith("bgra"):
            arr = arr[..., ::-1]
    elif encoding and encoding.startswith("bgr"):
        arr = arr[..., ::-1]
    return np.ascontiguousarray(arr, dtype=np.uint8)


def _decode_frame(view: "TopicView", msg) -> np.ndarray | None:
    from resurrector.ingest.parser import get_compressed_image_array, get_image_array

    if view.message_type == "sensor_msgs/msg/CompressedImage":
        arr = get_compressed_image_array(msg)
        return None if arr is None else to_rgb(arr, None)
    arr = get_image_array(msg)
    return None if arr is None else to_rgb(arr, msg.data.get("encoding"))


def frames_on_grid(view: "TopicView", grid: np.ndarray) -> Iterator[np.ndarray]:
    """Yield one RGB frame per grid point (latest frame at or before it)."""
    msgs = iter(view.iter_messages())
    current: np.ndarray | None = None
    pending = next(msgs, None)
    for t in grid:
        while pending is not None and pending.timestamp_ns <= t:
            frame = _decode_frame(view, pending)
            if frame is not None:
                current = frame
            pending = next(msgs, None)
        if current is None:
            raise ValueError(f"No decodable frame on {view.name} at or before {t}")
        yield current


def _time_bounds(view: "TopicView") -> tuple[int, int] | None:
    first = last = None
    for chunk in view.iter_chunks():
        if chunk.height == 0:
            continue
        ts = chunk["timestamp_ns"]
        lo, hi = int(ts.min()), int(ts.max())
        first = lo if first is None else min(first, lo)
        last = hi if last is None else max(last, hi)
    return None if first is None else (first, last)


def _image_time_bounds(view: "TopicView") -> tuple[int, int] | None:
    first = last = None
    for msg in view.iter_messages():
        first = msg.timestamp_ns if first is None else first
        last = msg.timestamp_ns
    return None if first is None else (first, last)


def _camera_key(topic: str) -> str:
    return "observation.images." + re.sub(r"[^A-Za-z0-9_]+", "_", topic.strip("/"))


def _label(topic: str, col: str) -> str:
    return f"{topic.strip('/')}/{col}"


# --------------------------------------------------------------- episodes

@dataclass
class _Episode:
    grid: np.ndarray
    state: np.ndarray            # (frames, n_state) float32
    action: np.ndarray | None    # (frames, n_action) float32
    state_names: list[str]
    action_names: list[str]
    image_views: list["TopicView"]


def _resample_topics(
    bf: "BagFrame", topics: Sequence[str], grid: np.ndarray,
) -> tuple[np.ndarray, list[str]]:
    blocks: list[np.ndarray] = []
    names: list[str] = []
    for topic in topics:
        view = bf[topic]
        chunks = iter(view.iter_chunks())
        first = next(chunks, None)
        if first is None:
            continue
        cols = numeric_columns(first.schema)
        if not cols:
            logger.warning("Topic %s has no numeric fields; skipped for LeRobot", topic)
            continue

        def _all(first=first, rest=chunks):
            yield first
            yield from rest

        df = asof_on_grid(_all(), grid, cols).drop("timestamp_ns")
        df = df.select([c for c in cols if df[c].null_count() < df.height])
        df = df.fill_null(strategy="forward").fill_null(strategy="backward")
        blocks.append(df.cast(pl.Float32).to_numpy())
        names.extend(_label(topic, c) for c in df.columns)
    if not blocks:
        return np.zeros((len(grid), 0), dtype=np.float32), []
    return np.concatenate(blocks, axis=1).astype(np.float32), names


def _plan_episode(
    bf: "BagFrame", topics: Sequence[str], action_topics: Sequence[str], fps: int,
) -> _Episode:
    image_views, state_topics = [], []
    for t in topics:
        try:
            view = bf[t]
        except KeyError:
            logger.warning("Topic '%s' not found, skipping", t)
            continue
        if view.is_image_topic:
            image_views.append(view)
        elif t not in action_topics:
            state_topics.append(t)
    act_topics = [t for t in action_topics if t in bf.topic_names]

    bounds = []
    for t in [*state_topics, *act_topics]:
        b = _time_bounds(bf[t])
        if b is not None:
            bounds.append(b)
    for v in image_views:
        b = _image_time_bounds(v)
        if b is not None:
            bounds.append(b)
    if not bounds:
        raise ValueError("None of the selected topics contain messages")

    grid = build_grid(max(b[0] for b in bounds), min(b[1] for b in bounds), fps)
    if len(grid) == 0:
        raise ValueError(
            "Selected topics do not overlap in time; nothing to export"
        )

    state, state_names = _resample_topics(bf, state_topics, grid)
    action, action_names = (None, [])
    if act_topics:
        action, action_names = _resample_topics(bf, act_topics, grid)
    if state.shape[1] == 0 and not image_views:
        raise ValueError("No numeric or image data in the selected topics")
    return _Episode(grid, state, action, state_names, action_names, image_views)


def _features(ep: _Episode, cam_shapes: dict[str, tuple[int, int, int]], use_videos: bool) -> dict:
    feats: dict = {}
    if ep.state_names:
        feats["observation.state"] = {
            "dtype": "float32", "shape": (len(ep.state_names),), "names": ep.state_names,
        }
    if ep.action_names:
        feats["action"] = {
            "dtype": "float32", "shape": (len(ep.action_names),), "names": ep.action_names,
        }
    for key, shape in cam_shapes.items():
        feats[key] = {
            "dtype": "video" if use_videos else "image",
            "shape": shape,
            "names": ["height", "width", "channels"],
        }
    return feats


def _prepare_root(root: Path) -> None:
    """LeRobot's create() requires a non-existent root. Allow an empty dir."""
    if root.exists():
        if root.is_dir() and not any(root.iterdir()):
            root.rmdir()
        else:
            raise FileExistsError(
                f"LeRobot export target {root} already exists and is not empty. "
                "Choose a new output directory."
            )


# ------------------------------------------------------------------ public

def export_lerobot(
    bags: Sequence["BagFrame"],
    topics: Sequence[str] | None,
    output_dir: str | Path,
    *,
    fps: int = DEFAULT_FPS,
    task: str = "rosbag episode",
    action_topics: Sequence[str] = (),
    repo_id: str | None = None,
    robot_type: str | None = None,
    use_videos: bool = True,
) -> LeRobotExportResult:
    """Write one LeRobot episode per bag into a new dataset at ``output_dir``.

    Args:
        bags: One or more :class:`BagFrame`. Each becomes one episode; all
            must produce the same feature set (same topics/fields/cameras).
        topics: Topics to include; ``None`` = every topic in the first bag.
        output_dir: Dataset root. Must not exist (an empty dir is accepted).
        fps: Integer frame rate of the uniform grid.
        task: Natural-language task label attached to every frame.
        action_topics: Topics whose numeric fields form the ``action``
            vector instead of ``observation.state``.
        repo_id: LeRobot repo id stored in metadata (default ``local/<dir>``).
        use_videos: Encode cameras as MP4 (LeRobot default) vs PNG images.

    Raises:
        ImportError: LeRobot isn't installed (see :data:`INSTALL_HINT`).
        FileExistsError: ``output_dir`` exists and is non-empty.
        ValueError: No overlapping data, or episodes disagree on features.
    """
    try:
        from lerobot.datasets.lerobot_dataset import LeRobotDataset
    except Exception as e:  # ImportError, or lerobot's own require_package
        raise ImportError(INSTALL_HINT) from e

    if not bags:
        raise ValueError("export_lerobot needs at least one bag")
    fps = int(round(fps))
    if fps <= 0:
        raise ValueError(f"fps must be a positive integer, got {fps}")

    root = Path(output_dir)
    _prepare_root(root)
    repo_id = repo_id or f"local/{re.sub(r'[^A-Za-z0-9_.-]+', '_', root.name) or 'dataset'}"

    dataset = None
    expected_features: dict | None = None
    total_frames = 0
    first_ep: _Episode | None = None
    cam_keys: list[str] = []
    try:
        for bf in bags:
            sel = list(topics) if topics else list(bf.topic_names)
            ep = _plan_episode(bf, sel, action_topics, fps)
            streams = {_camera_key(v.name): frames_on_grid(v, ep.grid) for v in ep.image_views}
            firsts = {k: next(it) for k, it in streams.items()}
            cam_shapes = {k: tuple(int(x) for x in f.shape) for k, f in firsts.items()}
            feats = _features(ep, cam_shapes, use_videos)

            if dataset is None:
                expected_features = feats
                first_ep = ep
                cam_keys = list(cam_shapes)
                dataset = LeRobotDataset.create(
                    repo_id=repo_id, fps=fps, features=feats, root=root,
                    robot_type=robot_type, use_videos=use_videos,
                )
            elif feats != expected_features:
                raise ValueError(
                    f"Bag {bf} produces a different feature set than the first "
                    "bag; every episode in a LeRobot dataset must share features"
                )

            for i in range(len(ep.grid)):
                frame: dict = {"task": task}
                if ep.state_names:
                    frame["observation.state"] = ep.state[i]
                if ep.action is not None and ep.action_names:
                    frame["action"] = ep.action[i]
                for key, it in streams.items():
                    frame[key] = firsts.pop(key) if key in firsts else next(it)
                dataset.add_frame(frame)
            dataset.save_episode()
            total_frames += len(ep.grid)
    except BaseException:
        if dataset is not None:
            try:
                dataset.finalize()
            except Exception:
                pass
        shutil.rmtree(root, ignore_errors=True)
        raise
    dataset.finalize()

    logger.info("Wrote LeRobot dataset: %d episode(s), %d frames @ %d fps -> %s",
                len(bags), total_frames, fps, root)
    return LeRobotExportResult(
        path=root,
        episodes=len(bags),
        frames=total_frames,
        fps=fps,
        state_names=first_ep.state_names if first_ep else [],
        action_names=first_ep.action_names if first_ep else [],
        camera_keys=cam_keys,
    )
