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
  leaks into a frame, which matters for policy training), with one logged
  exception: a field missing from a topic's first frames (a driver that
  starts publishing JointState velocity late) takes its first later value
  there, because a NaN would make LeRobot's statistics for it NaN.
- **Grid bounds** are ``[max(first_ts), min(last_ts)]`` across the selected
  topics, so no topic is ever extrapolated before it starts or held past
  the point where it stopped publishing. A single field that stops while
  its topic keeps publishing is held at its last value, also logged.
- ``observation.state`` = numeric fields of non-image topics (minus header
  stamps), ``action`` = numeric fields of ``action_topics`` if given, and
  ``observation.images.<topic>`` = one video stream per image topic.

Memory: the resampler streams ``iter_chunks()`` and holds one chunk plus the
grid-shaped output; images stream one frame at a time (LeRobot spills each
frame to a PNG, then encodes the episode's PNGs to MP4 or embeds them in the
data parquet and deletes them; the emptied spill directories are removed
after ``finalize()``). The output itself is grid-sized by definition.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator, Sequence

import numpy as np
import polars as pl

from resurrector.core.exceptions import (  # noqa: F401  (re-exported)
    LeRobotFrameFormatError,
    LeRobotFrameShapeError,
)

if TYPE_CHECKING:
    from resurrector.core.bag_frame import BagFrame, TopicView

logger = logging.getLogger(__name__)

INSTALL_HINT = (
    "LeRobot export needs the [lerobot] extra (Python 3.12+): "
    "pip install 'rosbag-resurrector[lerobot]'"
)
# LeRobot's own floor; pyproject's [lerobot] marker installs nothing below
# it (tests/test_rlds_capability.py checks the two agree).
LEROBOT_MIN_PYTHON = (3, 12)

DEFAULT_FPS = 30

# Fields that are bookkeeping, not signal. Header stamps duplicate the
# timeline; image geometry fields are constant per camera.
_SKIP_SUFFIXES = (
    "header.stamp_sec", "header.stamp_nsec", "data_length",
)
_NUMERIC = (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Boolean)


def import_lerobot_dataset():
    """Import and return LeRobot's ``LeRobotDataset`` writer class.

    Pulls in torch, so it costs seconds the first time. Raises
    ``ImportError(INSTALL_HINT)`` chained to the real failure, which may
    be a missing package or lerobot's own ``require_package`` check.
    """
    try:
        from lerobot.datasets.lerobot_dataset import LeRobotDataset
    except Exception as e:
        raise ImportError(INSTALL_HINT) from e
    return LeRobotDataset


def lerobot_available() -> bool:
    """True when LeRobot's dataset writer is importable (imports it)."""
    try:
        import_lerobot_dataset()
    except ImportError:
        return False
    return True


# Smallest frames LeRobot's default AV1 encoder (libsvtav1) handles,
# measured with LeRobot 0.6.1 / PyAV 15.1 (SVT-AV1 3.0.0): it refuses
# (avcodec_open2 error) a height or width under 4, and for widths 4-24 it
# never returns from save_episode(), whatever the height or frame count.
MIN_VIDEO_WIDTH = 25
MIN_VIDEO_HEIGHT = 4


def check_frame_shape(
    topic: str,
    shape: tuple[int, ...],
    use_videos: bool,
    bag: str | os.PathLike | None = None,
) -> None:
    """Raise :class:`LeRobotFrameShapeError` if LeRobot would mishandle ``shape``.

    ``shape`` is a decoded ``(H, W, 3)`` frame as handed to ``add_frame``;
    ``bag`` (the source bag's path) only goes into the error message.
    """
    h, w = shape[:2]
    bad = h in (1, 3) or (
        use_videos and (h < MIN_VIDEO_HEIGHT or w < MIN_VIDEO_WIDTH)
    )
    if bad:
        raise LeRobotFrameShapeError(
            topic, shape, use_videos,
            min_width=MIN_VIDEO_WIDTH, min_height=MIN_VIDEO_HEIGHT, bag=bag,
        )


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
    value_cols: Sequence[str] | None,
) -> pl.DataFrame:
    """Streaming backward as-of resample of time-ordered chunks onto ``grid``.

    Equivalent to ``grid.join_asof(full_topic, strategy="backward")``,
    where ``full_topic`` combines the chunks the way
    :meth:`TopicView.to_polars` does (a column a chunk lacks is null in
    its rows), but holds only the current chunk plus a one-row carry from
    the previous chunk, so memory is bounded by chunk size rather than
    topic size. A grid point is resolved once a chunk ends at or after it.

    ``value_cols`` names the columns to resample; one a chunk lacks is
    null at the grid points whose latest sample is in that chunk. With
    ``None``, every :func:`numeric_columns` column of any chunk is
    resampled, in the order first seen; one that first appears in a later
    chunk is null at the grid points resolved before it. Either way the
    result is the same for any chunking of the topic.
    """
    cols = None if value_cols is None else ["timestamp_ns", *value_cols]
    seen: list[str] = ["timestamp_ns"]
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
        if value_cols is None:
            seen.extend(c for c in numeric_columns(chunk.schema) if c not in seen)
        wanted = seen if cols is None else cols
        chunk = (
            chunk.select([
                pl.col(c) if c in chunk.columns else pl.lit(None).alias(c)
                for c in wanted
            ])
            .with_columns(pl.col("timestamp_ns").cast(pl.Int64))
            .sort("timestamp_ns")
        )
        hi = int(np.searchsorted(grid, chunk["timestamp_ns"][-1], side="right"))
        if hi > gi:
            src = chunk if carry is None else pl.concat([carry, chunk], how="diagonal_relaxed")
            _resolve(hi, src)
        carry = chunk.tail(1)

    if gi < len(grid):
        if carry is None:
            carry = pl.DataFrame(schema={c: pl.Float64 for c in cols or seen}).with_columns(
                pl.col("timestamp_ns").cast(pl.Int64)
            )
        _resolve(len(grid), carry)

    if not parts:
        return pl.DataFrame({"timestamp_ns": grid})
    return pl.concat(parts, how="diagonal_relaxed")


def to_rgb(arr: np.ndarray, encoding: str | None, topic: str | None = None) -> np.ndarray:
    """Normalize a decoded camera frame to HxWx3 uint8 RGB, losslessly.

    Gray is replicated to three channels, gray+alpha (a PNG "LA" frame)
    and RGBA/BGRA drop alpha, BGR is reversed, and 1-bit frames become
    0/255. Any other dtype (16-bit, 32-bit, float) or layout raises
    :class:`LeRobotFrameFormatError` naming ``topic``: an 8-bit cast would
    wrap the values and scaling would flatten them (see the class).
    """
    if arr.dtype == np.bool_:
        arr = arr.astype(np.uint8) * 255
    if arr.dtype != np.uint8:
        raise LeRobotFrameFormatError(topic, arr.dtype, arr.shape)
    if arr.ndim == 3 and arr.shape[-1] in (1, 2):
        arr = arr[..., 0]
    if arr.ndim == 2:
        arr = np.stack([arr] * 3, axis=-1)
    elif arr.ndim == 3 and arr.shape[-1] == 4:
        arr = arr[..., :3]
        if encoding and encoding.startswith("bgra"):
            arr = arr[..., ::-1]
    elif arr.ndim == 3 and arr.shape[-1] == 3:
        if encoding and encoding.startswith("bgr"):
            arr = arr[..., ::-1]
    else:
        raise LeRobotFrameFormatError(topic, arr.dtype, arr.shape)
    return np.ascontiguousarray(arr)


def _decode_frame(view: "TopicView", msg) -> np.ndarray | None:
    from resurrector.ingest.parser import get_compressed_image_array, get_image_array

    if view.message_type == "sensor_msgs/msg/CompressedImage":
        arr = get_compressed_image_array(msg)
        return None if arr is None else to_rgb(arr, None, view.name)
    arr = get_image_array(msg)
    return None if arr is None else to_rgb(arr, msg.data.get("encoding"), view.name)


def frames_on_grid(view: "TopicView", grid: np.ndarray) -> Iterator[np.ndarray]:
    """Yield one RGB frame per grid point (latest frame at or before it)."""
    msgs = iter(view.iter_messages())
    current: np.ndarray | None = None
    undecoded = None
    pending = next(msgs, None)
    for t in grid:
        while pending is not None and pending.timestamp_ns <= t:
            frame = _decode_frame(view, pending)
            if frame is not None:
                current = frame
            else:
                undecoded = pending
            pending = next(msgs, None)
        if current is None:
            # The encoding is what a user needs to see: raw 16UC1 / mono16
            # / rgb16 frames, for one, don't decode at all.
            enc = undecoded and (undecoded.data.get("encoding") or undecoded.data.get("format"))
            detail = f" (frame encoding {enc!r})" if enc else ""
            raise ValueError(f"No decodable frame on {view.name} at or before {t}{detail}")
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


def _start_method_is_fork() -> bool:
    """Whether new processes would be forked, without fixing the start method.

    ``multiprocessing.get_start_method()`` (allow_none=False) permanently
    sets the default context as a side effect, so a caller's later
    ``set_start_method()`` would raise "context has already been set".
    The first entry of ``get_all_start_methods()`` is the documented
    platform default. (LeRobot's own ``save_episode()`` currently fixes the
    start method anyway; this avoids adding a side effect of our own.)
    """
    method = multiprocessing.get_start_method(allow_none=True)
    return (method or multiprocessing.get_all_start_methods()[0]) == "fork"


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
    """Every numeric field of each topic on ``grid``, as float32 columns.

    Fields come from every chunk, not just the first, so one a driver
    starts publishing late (JointState velocity) is kept, and one it
    stops publishing doesn't fail the export; where chunks start and end
    doesn't change the result. A frame whose latest sample lacks a field
    holds the field's previous value or, before its first value, its
    first value, with a warning naming the frames filled that way: NaN
    would make LeRobot's statistics for the field NaN. Fields with no
    value at any frame are dropped.
    """
    blocks: list[np.ndarray] = []
    names: list[str] = []
    for topic in topics:
        chunks = iter(bf[topic].iter_chunks())
        first = next(chunks, None)
        if first is None:
            continue

        def _all(first=first, rest=chunks):
            yield first
            yield from rest

        df = asof_on_grid(_all(), grid, None).drop("timestamp_ns")
        cols = [c for c in df.columns if df[c].null_count() < df.height]
        if not cols:
            logger.warning("Topic %s has no numeric fields; skipped for LeRobot", topic)
            continue
        df = df.select(cols)
        late = {c: int(df[c].is_not_null().arg_true()[0]) for c in cols}
        late = {c: n for c, n in late.items() if n}
        if late:
            logger.warning(
                "Topic %s: %s; those frames hold the field's first value, "
                "which comes later in time",
                topic, ", ".join(
                    f"{c} has no value in the first {n} of {df.height} frames"
                    for c, n in late.items()
                ),
            )
        stopped = {c: int(df[c].is_not_null().arg_true()[-1]) + 1 for c in cols}
        stopped = {c: n for c, n in stopped.items() if n < df.height}
        if stopped:
            logger.warning(
                "Topic %s: %s; those frames hold the field's last value",
                topic, ", ".join(
                    f"{c} has no value after frame {n - 1} of {df.height}"
                    for c, n in stopped.items()
                ),
            )
        df = df.fill_null(strategy="forward").fill_null(strategy="backward")
        blocks.append(df.cast(pl.Float32).to_numpy())
        names.extend(_label(topic, c) for c in df.columns)
    if not blocks:
        return np.zeros((len(grid), 0), dtype=np.float32), []
    return np.concatenate(blocks, axis=1).astype(np.float32), names


def _match_field_order(ep: _Episode, names: tuple[list[str], list[str]]) -> _Episode:
    """``ep`` with its state and action columns in the order of ``names``
    (the first episode's), when it has the same fields in another order.

    Fields are ordered as first seen, so a bag whose driver started
    publishing a field later than in the first bag lists the same fields
    differently. Different field sets are left alone, so the feature check
    still refuses them.
    """
    def _reorder(values, have, want):
        if values is None or have == want or sorted(have) != sorted(want):
            return values, have
        return values[:, [have.index(n) for n in want]], list(want)

    state, state_names = _reorder(ep.state, ep.state_names, names[0])
    action, action_names = _reorder(ep.action, ep.action_names, names[1])
    return _Episode(ep.grid, state, action, state_names, action_names, ep.image_views)


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


def _remove_empty_image_dirs(root: Path) -> None:
    """Remove the empty ``images/`` spill directories LeRobot leaves behind.

    LeRobot (0.6.x) writes each camera frame as a PNG under
    ``images/<camera>/episode-NNNNNN/``. In ``save_episode()`` it then either
    encodes those PNGs to MP4 (video mode) or embeds their bytes into
    ``data/*.parquet`` (image mode, ``use_videos=False``), and deletes each
    ``episode-NNNNNN`` directory. Either way ``images/<camera>/`` and
    ``images/`` stay behind, empty. Only empty directories are removed
    (``os.rmdir`` refuses anything else), so if a LeRobot version ever keeps
    frames on disk under ``images/``, they stay.
    """
    images = root / "images"
    if not images.is_dir():
        return
    for dirpath, _dirs, _files in os.walk(images, topdown=False):
        try:
            os.rmdir(dirpath)
        except OSError:
            pass  # not empty


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
        LeRobotFrameShapeError: A camera's frames are a size LeRobot
            mishandles (see the class); for the first bag, raised before
            ``output_dir`` exists.
        LeRobotFrameFormatError: A camera's pixels aren't 8-bit (16-bit,
            float, ...) and can't become RGB losslessly; usually raised on
            the topic's first frame, before ``output_dir`` exists.
        Either error on a later frame or bag removes the partial dataset.
        ValueError: No overlapping data, or episodes disagree on features.
    """
    LeRobotDataset = import_lerobot_dataset()

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
    # Keep only the first episode's names: holding the _Episode itself would
    # keep its whole frame grid alive through every later bag.
    first_names: tuple[list[str], list[str]] | None = None
    cam_keys: list[str] = []
    try:
        for bf in bags:
            sel = list(topics) if topics else list(bf.topic_names)
            ep = _plan_episode(bf, sel, action_topics, fps)
            if first_names is not None:
                ep = _match_field_order(ep, first_names)
            streams = {_camera_key(v.name): frames_on_grid(v, ep.grid) for v in ep.image_views}
            firsts = {k: next(it) for k, it in streams.items()}
            cam_shapes = {k: tuple(int(x) for x in f.shape) for k, f in firsts.items()}
            for v in ep.image_views:
                check_frame_shape(v.name, cam_shapes[_camera_key(v.name)], use_videos,
                                  bag=getattr(bf, "path", None))
            feats = _features(ep, cam_shapes, use_videos)

            if dataset is None:
                expected_features = feats
                first_names = (ep.state_names, ep.action_names)
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
            # LeRobot encodes multiple cameras in a process pool. Under the
            # spawn/forkserver start methods (macOS, Windows, Linux on 3.14+)
            # each worker re-imports the caller's __main__, so a plain script
            # without an `if __name__ == "__main__":` guard re-runs its own
            # export and dies with BrokenProcessPool. Only fork is safe.
            dataset.save_episode(parallel_encoding=_start_method_is_fork())
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
    _remove_empty_image_dirs(root)

    logger.info("Wrote LeRobot dataset: %d episode(s), %d frames @ %d fps -> %s",
                len(bags), total_frames, fps, root)
    return LeRobotExportResult(
        path=root,
        episodes=len(bags),
        frames=total_frames,
        fps=fps,
        state_names=first_names[0] if first_names else [],
        action_names=first_names[1] if first_names else [],
        camera_keys=cam_keys,
    )
