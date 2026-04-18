"""Export bag data to ML-friendly formats.

Supports: Parquet, HDF5, CSV, NumPy, Zarr.

All formats stream chunk-by-chunk from the underlying MCAP so memory
usage is bounded regardless of topic size.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator

import numpy as np

if TYPE_CHECKING:
    from resurrector.core.bag_frame import BagFrame

logger = logging.getLogger("resurrector.core.export")

CHUNK_SIZE = 50_000


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
    """Export bag data to various ML-friendly formats.

    All export paths stream chunk-by-chunk. Peak memory is roughly the
    size of one chunk (CHUNK_SIZE rows), regardless of total topic size.
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

            safe_name = topic.lstrip("/").replace("/", "_")
            chunks = _transform_chunks(
                view.iter_chunks(CHUNK_SIZE), downsample_hz
            )
            self._stream_dataframe_chunks(chunks, format, output_path, safe_name)

        return output_path

    def _stream_dataframe_chunks(
        self,
        chunks: Iterable,
        format: str,
        output_path: Path,
        name: str,
    ) -> ExportResult:
        """Dispatch streaming chunks to the right format writer."""
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
        else:
            raise ValueError(
                f"Unknown export format: {format}. "
                f"Supported: parquet, hdf5, csv, numpy, zarr"
            )

    def export_frames(
        self,
        topic_view,
        output_dir: str | Path,
        format: str = "png",
        max_frames: int | None = None,
        every_n: int = 1,
    ) -> Path:
        """Export an image topic as numbered image files."""
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
        """Export an image topic as an MP4 video file."""
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
    """Stream chunks into an .npz archive.

    NumPy's .npz format can't be incrementally appended, so we accumulate
    column arrays in memory and savez at the end. Memory is still bounded
    by the total converted data (not the full raw messages), and failing
    columns are collected rather than silently dropped.
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
