"""Export bag data to ML-friendly formats.

Supports: Parquet, HDF5, CSV, NumPy, Zarr, LeRobot, RLDS.

Per the v0.4.0 performance contract, the streaming-friendly formats
(Parquet, HDF5, CSV, Zarr, LeRobot, RLDS) write chunk-by-chunk so
peak memory is bounded by ``CHUNK_SIZE``. NumPy ``.npz`` is the
exception: the format can't be incrementally appended, so writing
requires materializing every column. We hard-cap NumPy export at
``NUMPY_HARD_CAP`` rows and raise :class:`LargeTopicError` past that
— users on bigger topics should use Parquet (which streams).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator

import numpy as np

from resurrector.core.exceptions import LargeTopicError

if TYPE_CHECKING:
    from resurrector.core.bag_frame import BagFrame

logger = logging.getLogger("resurrector.core.export")

CHUNK_SIZE = 50_000

# NumPy .npz can't append; we have to hold every column in memory until
# savez_compressed flushes. Past ~1 M rows the materialized arrays plus
# compression buffers easily exceed 1 GB. Refuse early with a clear
# error pointing to Parquet, which streams.
NUMPY_HARD_CAP = 1_000_000


@dataclass
class ExportColumnFailure:
    """One column that failed to serialize during export."""
    column: str
    error_type: str
    message: str


class ExportError(Exception):
    """Raised when one or more columns fail to serialize during export.

    The output file may be partial. Inspect ``failures`` to see which
    columns were dropped and why.
    """

    def __init__(self, failures: list[ExportColumnFailure], output: Path):
        self.failures = failures
        self.output = output
        cols = ", ".join(f.column for f in failures)
        super().__init__(
            f"Failed to serialize {len(failures)} column(s) to {output}: {cols}"
        )


@dataclass
class ExportResult:
    path: Path
    rows_written: int
    failures: list[ExportColumnFailure] = field(default_factory=list)


class Exporter:
    """Lower-level export engine. Most users should call :meth:`BagFrame.export` instead.

    Use this directly if you want fine-grained control: per-topic
    streaming via :meth:`export_frames` / :meth:`export_video`, or
    custom orchestration where ``BagFrame.export`` doesn't quite fit.

    All export paths stream chunk-by-chunk. Peak memory is roughly the
    size of one chunk (``CHUNK_SIZE`` rows), regardless of total topic
    size. ``numpy`` is the documented exception — it materializes
    per-topic and refuses topics over ``NUMPY_HARD_CAP`` (1 M rows).

    Example::

        from resurrector import BagFrame
        from resurrector.core.export import Exporter

        bf = BagFrame("experiment.mcap")
        Exporter().export(
            bag_frame=bf,
            topics=["/imu/data", "/joint_states"],
            format="parquet",
            output_dir="./out",
        )
    """

    def export(
        self,
        bag_frame: "BagFrame",
        topics: list[str],
        format: str = "parquet",
        output_dir: str = "./export",
        sync: bool = False,
        sync_method: str = "nearest",
        downsample_hz: float | None = None,
    ) -> Path:
        """Stream-export selected topics to the given format.

        Args:
            bag_frame: The :class:`BagFrame` to read from.
            topics: List of topic names. Missing topics are logged and
                skipped — the export does NOT fail on a single missing
                topic.
            format: ``parquet`` (default), ``hdf5``, ``csv``, ``numpy``,
                ``zarr`` (needs ``[all-exports]``), ``lerobot``, or
                ``rlds`` (needs ``[all-exports]``).
            output_dir: Directory to write into. Created if missing.
            sync: When True (and 2+ topics), time-align via
                :meth:`BagFrame.sync` before exporting; the result is
                written as a single ``synced.<ext>`` file.
            sync_method: ``nearest`` / ``interpolate`` / ``sample_and_hold``.
                Only used when ``sync`` is True.
            downsample_hz: Per-chunk resampling rate before writing. ``None``
                preserves the native rate.

        Returns:
            ``Path`` to ``output_dir``.

        Raises:
            LargeTopicError: If ``format == "numpy"`` and a topic has
                more than ``NUMPY_HARD_CAP`` rows.
            ValueError: For unknown format strings.
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        if sync and len(topics) > 1:
            import polars as pl
            df = bag_frame.sync(topics, method=sync_method)
            if downsample_hz:
                from resurrector.core.transforms import downsample_temporal
                df = downsample_temporal(df, downsample_hz)
            self._stream_dataframe_chunks(iter([df]), format, output_path, "synced")
            return output_path

        for topic in topics:
            try:
                view = bag_frame[topic]
            except KeyError:
                logger.warning("Topic '%s' not found, skipping", topic)
                continue

            # Pre-flight: NumPy export is hard-capped because .npz
            # can't append. Refuse early with a clear pointer to
            # Parquet rather than letting the user wait through a
            # multi-GB materialization.
            if format == "numpy" and view.message_count > NUMPY_HARD_CAP:
                raise LargeTopicError(
                    topic_name=view.name,
                    message_count=view.message_count,
                    threshold=NUMPY_HARD_CAP,
                )

            safe_name = topic.lstrip("/").replace("/", "_")
            chunks = _transform_chunks(
                view.iter_chunks(CHUNK_SIZE), downsample_hz
            )
            self._stream_dataframe_chunks(
                chunks, format, output_path, safe_name,
                expected_total_rows=view.message_count,
            )

        return output_path

    def _stream_dataframe_chunks(
        self,
        chunks: Iterable,
        format: str,
        output_path: Path,
        name: str,
        expected_total_rows: int | None = None,
    ) -> ExportResult:
        """Dispatch streaming chunks to the right format writer.

        ``expected_total_rows`` is forwarded to writers that need it
        (currently only ``_stream_rlds``, which uses it to derive
        ``is_last`` per step without materializing the chunks). Writers
        that don't need it ignore the parameter.
        """
        if format == "parquet":
            return _stream_parquet(chunks, output_path, name)
        elif format == "csv":
            return _stream_csv(chunks, output_path, name)
        elif format == "hdf5":
            return _stream_hdf5(chunks, output_path, name)
        elif format == "numpy":
            return _stream_numpy(chunks, output_path, name)
        elif format == "zarr":
            return _stream_zarr(chunks, output_path, name)
        elif format == "lerobot":
            return _stream_lerobot(chunks, output_path, name)
        elif format == "rlds":
            return _stream_rlds(
                chunks, output_path, name,
                total_rows=expected_total_rows,
            )
        else:
            raise ValueError(
                f"Unknown export format: {format}. "
                f"Supported: parquet, hdf5, csv, numpy, zarr, lerobot, rlds"
            )

    def export_frames(
        self,
        topic_view,
        output_dir: str | Path,
        format: str = "png",
        max_frames: int | None = None,
        every_n: int = 1,
    ) -> Path:
        """Write every frame of an image topic as a numbered image file.

        Args:
            topic_view: A :class:`~resurrector.core.bag_frame.TopicView`
                for an image topic.
            output_dir: Parent directory; a sub-directory named after
                the topic is created beneath it.
            format: ``png`` (lossless) or ``jpeg`` (smaller, lossy).
            max_frames: Stop after this many frames. ``None`` for no limit.
            every_n: Sample every Nth frame (1 = every frame, 5 = thin a
                30 Hz stream to 6 Hz).

        Returns:
            ``Path`` to the topic's frames directory.

        Raises:
            ImportError: If Pillow isn't installed (in the
                ``[vision-lite]`` extra).

        Example::

            Exporter().export_frames(bf["/camera/rgb"], "./frames", every_n=5)
        """
        try:
            from PIL import Image as PILImage
        except ImportError:
            raise ImportError(
                "Frame export requires Pillow. "
                "Install with: pip install rosbag-resurrector[vision-lite]"
            )

        output_path = Path(output_dir)
        safe_name = topic_view.name.lstrip("/").replace("/", "_")
        frames_dir = output_path / safe_name
        frames_dir.mkdir(parents=True, exist_ok=True)

        count = 0
        for i, (ts, arr) in enumerate(topic_view.iter_images()):
            if i % every_n != 0:
                continue
            img = PILImage.fromarray(arr)
            ext = "jpg" if format == "jpeg" else format
            img.save(frames_dir / f"frame_{count:06d}.{ext}")
            count += 1
            if max_frames and count >= max_frames:
                break

        logger.info("Exported %d frames to %s", count, frames_dir)
        return frames_dir

    def export_video(
        self,
        topic_view,
        output_path: str | Path,
        fps: float | None = None,
        codec: str = "mp4v",
    ) -> Path:
        """Encode an image topic as a single MP4 video file (via OpenCV).

        Useful for quick visual review of a long camera recording without
        materializing thousands of PNGs.

        Args:
            topic_view: A :class:`TopicView` for an image topic.
            output_path: MP4 file to write. Parent directory is created.
            fps: Output frame rate. Defaults to ``topic_view.frequency_hz``
                (the recorded rate) if available, else 30.0.
            codec: FourCC string for the encoder. Default ``"mp4v"``;
                use ``"avc1"`` for broader compatibility on some players.

        Returns:
            ``Path`` to the written MP4.

        Raises:
            ImportError: If OpenCV isn't installed (in ``[vision-lite]``).

        Example::

            Exporter().export_video(bf["/camera/rgb"], "preview.mp4", fps=10)
        """
        try:
            import cv2
        except ImportError:
            raise ImportError(
                "Video export requires OpenCV. "
                "Install with: pip install rosbag-resurrector[vision-lite]"
            )

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if fps is None:
            fps = topic_view.frequency_hz or 30.0

        writer = None
        count = 0
        try:
            for ts, arr in topic_view.iter_images():
                if writer is None:
                    h, w = arr.shape[:2]
                    fourcc = cv2.VideoWriter_fourcc(*codec)
                    writer = cv2.VideoWriter(str(output_file), fourcc, fps, (w, h))
                if len(arr.shape) == 3 and arr.shape[2] == 3:
                    arr = arr[:, :, ::-1]
                writer.write(arr)
                count += 1
        finally:
            if writer is not None:
                writer.release()

        logger.info("Exported %d frames as video to %s", count, output_file)
        return output_file


def _transform_chunks(chunks: Iterable, downsample_hz: float | None) -> Iterator:
    """Apply optional downsampling to each chunk as it streams through."""
    if downsample_hz is None:
        yield from chunks
        return
    from resurrector.core.transforms import downsample_temporal
    for chunk in chunks:
        yield downsample_temporal(chunk, downsample_hz)


# ---------------------------------------------------------------------------
# Streaming writers — one per format. Each consumes an iterable of
# pl.DataFrame chunks, writes them, and returns an ExportResult with any
# per-column failures collected.
# ---------------------------------------------------------------------------


def _safe_column_to_numpy(df, col: str) -> tuple[np.ndarray | None, ExportColumnFailure | None]:
    """Convert one column to numpy, returning the array or a failure record."""
    try:
        return df[col].to_numpy(), None
    except Exception as e:
        return None, ExportColumnFailure(
            column=col, error_type=type(e).__name__, message=str(e),
        )


def _stream_parquet(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    import pyarrow.parquet as pq

    filepath = output_path / f"{name}.parquet"
    writer = None
    rows_written = 0
    try:
        for chunk in chunks:
            table = chunk.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(str(filepath), table.schema)
            writer.write_table(table)
            rows_written += chunk.height
    finally:
        if writer is not None:
            writer.close()

    logger.info("Streamed %d rows to %s", rows_written, filepath)
    return ExportResult(path=filepath, rows_written=rows_written)


def _stream_csv(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    filepath = output_path / f"{name}.csv"
    rows_written = 0
    first = True
    with open(filepath, "wb") as f:
        for chunk in chunks:
            csv_bytes = chunk.write_csv(file=None, include_header=first).encode("utf-8")
            f.write(csv_bytes)
            rows_written += chunk.height
            first = False
    logger.info("Streamed %d rows to %s", rows_written, filepath)
    return ExportResult(path=filepath, rows_written=rows_written)


def _stream_hdf5(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    """Stream chunks to HDF5 using resizable datasets (append mode).

    Each column becomes a resizable dataset; each chunk extends it.
    Columns that fail to serialize are collected and reported.
    """
    import h5py

    filepath = output_path / f"{name}.h5"
    rows_written = 0
    failures: list[ExportColumnFailure] = []
    failed_cols: set[str] = set()

    with h5py.File(filepath, "w") as f:
        group = f.create_group(name)
        datasets: dict[str, h5py.Dataset] = {}

        for chunk in chunks:
            chunk_rows = chunk.height
            for col in chunk.columns:
                if col in failed_cols:
                    continue
                arr, failure = _safe_column_to_numpy(chunk, col)
                if failure is not None:
                    failures.append(failure)
                    failed_cols.add(col)
                    continue

                try:
                    if arr.dtype.kind in ("U", "S", "O"):
                        # String-ish column. Validate it's actually string-like;
                        # object dtype can hide nested lists which can't be
                        # represented in HDF5. Probe the first element.
                        if arr.dtype.kind == "O" and len(arr) > 0:
                            sample = arr[0]
                            if isinstance(sample, (list, tuple, np.ndarray)):
                                raise TypeError(
                                    f"HDF5 does not support dtype {arr.dtype} "
                                    f"containing sequences (e.g. variable-length lists)"
                                )
                        if col not in datasets:
                            dt = h5py.string_dtype()
                            datasets[col] = group.create_dataset(
                                col, shape=(0,), maxshape=(None,), dtype=dt,
                            )
                        arr = arr.astype(str)
                    else:
                        if col not in datasets:
                            datasets[col] = group.create_dataset(
                                col, shape=(0,), maxshape=(None,),
                                dtype=arr.dtype, compression="gzip",
                            )
                    ds = datasets[col]
                    new_size = ds.shape[0] + arr.shape[0]
                    ds.resize((new_size,))
                    ds[-arr.shape[0]:] = arr
                except Exception as e:
                    failures.append(ExportColumnFailure(
                        column=col,
                        error_type=type(e).__name__,
                        message=str(e),
                    ))
                    failed_cols.add(col)
            rows_written += chunk_rows

    logger.info("Streamed %d rows to %s", rows_written, filepath)
    if failures:
        raise ExportError(failures, filepath)
    return ExportResult(path=filepath, rows_written=rows_written, failures=failures)


def _stream_numpy(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    """Write chunks into an .npz archive.

    NumPy's .npz format can't be incrementally appended, so this writer
    accumulates column arrays in memory and ``savez_compressed`` flushes
    at the end. Memory scales with total topic size, NOT chunk size —
    the v0.4.0 ``Exporter.export`` pre-flight refuses topics larger
    than ``NUMPY_HARD_CAP`` (1 M rows) before reaching here, raising
    :class:`LargeTopicError` with a pointer to Parquet (which streams).
    The cap exists because past ~1 M rows the materialized arrays
    plus ``savez_compressed`` buffers easily exceed 1 GB.
    """
    filepath = output_path / f"{name}.npz"
    rows_written = 0
    failures: list[ExportColumnFailure] = []
    failed_cols: set[str] = set()
    col_chunks: dict[str, list[np.ndarray]] = {}

    for chunk in chunks:
        chunk_rows = chunk.height
        for col in chunk.columns:
            if col in failed_cols:
                continue
            arr, failure = _safe_column_to_numpy(chunk, col)
            if failure is not None:
                failures.append(failure)
                failed_cols.add(col)
                col_chunks.pop(col, None)
                continue
            col_chunks.setdefault(col, []).append(arr)
        rows_written += chunk_rows

    arrays = {
        col: np.concatenate(parts) if len(parts) > 1 else parts[0]
        for col, parts in col_chunks.items()
    }
    np.savez_compressed(filepath, **arrays)

    logger.info("Streamed %d rows to %s", rows_written, filepath)
    if failures:
        raise ExportError(failures, filepath)
    return ExportResult(path=filepath, rows_written=rows_written, failures=failures)


def _stream_lerobot(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    """Export to LeRobot dataset format.

    Layout (per LeRobot dataset spec):
        <output_path>/
            data/chunk-000/episode_000000.parquet
            meta/info.json
            meta/episodes.jsonl
            meta/tasks.jsonl

    Each export call produces one episode (episode_000000). For multi-bag
    datasets, use the DatasetManager which composes multiple bags into a
    consistent dataset structure.
    """
    import json

    data_dir = output_path / "data" / "chunk-000"
    meta_dir = output_path / "meta"
    data_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    # Stream the parquet file for this episode
    episode_parquet = data_dir / "episode_000000.parquet"
    rows_written = 0
    columns: list[str] = []
    fps_estimate = 0.0
    first_ts: int | None = None
    last_ts: int | None = None

    import pyarrow.parquet as pq

    writer = None
    try:
        for chunk in chunks:
            if not columns:
                columns = list(chunk.columns)
            # Add LeRobot's required step indices
            chunk = chunk.with_row_index(
                name="frame_index", offset=rows_written,
            )
            # Track timestamps for fps estimation
            if "timestamp_ns" in chunk.columns and chunk.height > 0:
                ts_min = chunk["timestamp_ns"].min()
                ts_max = chunk["timestamp_ns"].max()
                if first_ts is None:
                    first_ts = ts_min
                last_ts = ts_max if last_ts is None else max(last_ts, ts_max)

            table = chunk.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(str(episode_parquet), table.schema)
            writer.write_table(table)
            rows_written += chunk.height
    finally:
        if writer is not None:
            writer.close()

    if first_ts is not None and last_ts is not None and last_ts > first_ts:
        duration_sec = (last_ts - first_ts) / 1e9
        fps_estimate = round(rows_written / duration_sec, 2) if duration_sec > 0 else 0.0

    # Write meta/info.json
    info = {
        "codebase_version": "v2.0",
        "robot_type": "unknown",
        "total_episodes": 1,
        "total_frames": rows_written,
        "total_tasks": 1,
        "total_videos": 0,
        "total_chunks": 1,
        "chunks_size": 1000,
        "fps": fps_estimate,
        "splits": {"train": "0:1"},
        "data_path": "data/chunk-{episode_chunk:03d}/episode_{episode_index:06d}.parquet",
        "features": {
            col: {"dtype": "float32", "shape": [1], "names": None}
            for col in columns
            if col not in ("frame_index", "timestamp_ns")
        },
    }
    (meta_dir / "info.json").write_text(json.dumps(info, indent=2))

    # episodes.jsonl — one line per episode
    episodes_line = {
        "episode_index": 0,
        "tasks": [name],
        "length": rows_written,
    }
    (meta_dir / "episodes.jsonl").write_text(json.dumps(episodes_line) + "\n")

    # tasks.jsonl — one line per distinct task
    tasks_line = {"task_index": 0, "task": name}
    (meta_dir / "tasks.jsonl").write_text(json.dumps(tasks_line) + "\n")

    logger.info("Wrote LeRobot dataset (%d frames) to %s", rows_written, output_path)
    return ExportResult(path=output_path, rows_written=rows_written)


def _stream_rlds(
    chunks: Iterable,
    output_path: Path,
    name: str,
    total_rows: int | None = None,
) -> ExportResult:
    """Export to RLDS (TFRecord) format — streaming.

    Each chunk becomes a contiguous run of steps inside a single episode.
    Per-step features:
        observation: dict of all numeric columns (excluding timestamp_ns)
        action: empty dict (rosbag has no explicit action signal — users
                can post-process to extract actions from /cmd_vel etc.)
        reward: 0.0
        discount: 1.0
        is_first: True for first step
        is_last: True for last step
        is_terminal: True for last step

    Output: <output_path>/<name>.tfrecord

    Memory: bounded by chunk size. The v0.3.x version did
    ``list(chunks)`` upfront so it could derive ``is_last`` per row;
    v0.4.0 takes ``total_rows`` from the caller (the index already
    knows ``view.message_count``) and uses a running counter, which
    eliminates the materialization.

    If ``total_rows`` is None we fall back to a one-time materialization
    so the writer remains correct for callers who don't have a count
    handy. Inside ``Exporter.export`` we always pass it.
    """
    try:
        import tensorflow as tf
    except ImportError:
        raise ImportError(
            "RLDS export requires tensorflow. "
            "Install with: pip install rosbag-resurrector[all-exports]"
        )

    output_path.mkdir(parents=True, exist_ok=True)
    filepath = output_path / f"{name}.tfrecord"
    rows_written = 0
    columns: list[str] = []

    def _to_feature(value) -> tf.train.Feature:
        if isinstance(value, (int, bool)):
            return tf.train.Feature(int64_list=tf.train.Int64List(value=[int(value)]))
        if isinstance(value, float):
            return tf.train.Feature(float_list=tf.train.FloatList(value=[float(value)]))
        if isinstance(value, str):
            return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value.encode("utf-8")]))
        # Fallback: stringify
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=[str(value).encode("utf-8")]))

    # Streaming-friendly path: caller supplied total_rows from the
    # index. Otherwise fall back to materializing once (the v0.3.x
    # behavior) so this writer is still safe to call from outside the
    # Exporter.
    if total_rows is None:
        chunk_iter = list(chunks)
        total_rows = sum(c.height for c in chunk_iter)
    else:
        chunk_iter = chunks

    with tf.io.TFRecordWriter(str(filepath)) as writer:
        for chunk in chunk_iter:
            if not columns:
                columns = list(chunk.columns)
            chunk_dicts = chunk.to_dicts()
            for row_idx, row in enumerate(chunk_dicts):
                global_idx = rows_written + row_idx
                is_first = global_idx == 0
                is_last = global_idx == total_rows - 1

                feature_map: dict[str, tf.train.Feature] = {}
                for col, val in row.items():
                    if col == "timestamp_ns":
                        feature_map["step/timestamp_ns"] = _to_feature(val)
                    else:
                        feature_map[f"step/observation/{col}"] = _to_feature(val)

                feature_map["step/reward"] = _to_feature(0.0)
                feature_map["step/discount"] = _to_feature(1.0)
                feature_map["step/is_first"] = _to_feature(is_first)
                feature_map["step/is_last"] = _to_feature(is_last)
                feature_map["step/is_terminal"] = _to_feature(is_last)

                example = tf.train.Example(features=tf.train.Features(feature=feature_map))
                writer.write(example.SerializeToString())
            rows_written += chunk.height

    logger.info("Wrote RLDS TFRecord (%d steps) to %s", rows_written, filepath)
    return ExportResult(path=filepath, rows_written=rows_written)


def _stream_zarr(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    """Stream chunks to Zarr using appendable arrays."""
    try:
        import zarr
    except ImportError:
        raise ImportError(
            "Zarr export requires the zarr package. "
            "Install with: pip install rosbag-resurrector[all-exports]"
        )

    filepath = output_path / f"{name}.zarr"
    rows_written = 0
    failures: list[ExportColumnFailure] = []
    failed_cols: set[str] = set()

    store = zarr.DirectoryStore(str(filepath))
    root = zarr.group(store, overwrite=True)
    arrays: dict[str, zarr.Array] = {}

    for chunk in chunks:
        chunk_rows = chunk.height
        for col in chunk.columns:
            if col in failed_cols:
                continue
            arr, failure = _safe_column_to_numpy(chunk, col)
            if failure is not None:
                failures.append(failure)
                failed_cols.add(col)
                continue
            if arr.dtype.kind in ("U", "O"):
                # Zarr doesn't cleanly support variable-length strings
                failures.append(ExportColumnFailure(
                    column=col,
                    error_type="UnsupportedDtype",
                    message=f"zarr export does not support dtype {arr.dtype}",
                ))
                failed_cols.add(col)
                continue
            if col not in arrays:
                arrays[col] = root.create_dataset(
                    col, shape=(0,), chunks=(min(chunk_rows, CHUNK_SIZE),),
                    dtype=arr.dtype,
                )
            arrays[col].append(arr)
        rows_written += chunk_rows

    logger.info("Streamed %d rows to %s", rows_written, filepath)
    if failures:
        raise ExportError(failures, filepath)
    return ExportResult(path=filepath, rows_written=rows_written, failures=failures)
