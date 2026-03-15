"""Export bag data to ML-friendly formats.

Supports: Parquet, HDF5, CSV, NumPy, Zarr.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from resurrector.core.bag_frame import BagFrame

logger = logging.getLogger("resurrector.core.export")

# Maximum rows to hold in memory per chunk when streaming
CHUNK_SIZE = 50_000


class Exporter:
    """Export bag data to various ML-friendly formats."""

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
        """Export topics from a bag to the specified format.

        Args:
            bag_frame: The BagFrame to export from.
            topics: List of topic names to export.
            format: Output format (parquet, hdf5, csv, numpy, zarr).
            output_dir: Output directory path.
            sync: Whether to synchronize topics before export.
            sync_method: Synchronization method.
            downsample_hz: Target frequency for downsampling.

        Returns:
            Path to the output directory.
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        if sync and len(topics) > 1:
            import polars as pl
            df = bag_frame.sync(topics, method=sync_method)
            if downsample_hz:
                from resurrector.core.transforms import downsample_temporal
                df = downsample_temporal(df, downsample_hz)
            self._export_dataframe(df, format, output_path, "synced")
        else:
            for topic in topics:
                try:
                    view = bag_frame[topic]
                except KeyError:
                    logger.warning("Topic '%s' not found, skipping", topic)
                    continue

                # Stream large topics in chunks to avoid OOM
                msg_count = view.message_count
                if msg_count > CHUNK_SIZE and format == "parquet":
                    logger.info(
                        "Streaming export for '%s' (%d messages)", topic, msg_count
                    )
                    safe_name = topic.lstrip("/").replace("/", "_")
                    self._export_streaming_parquet(
                        view, output_path, safe_name, downsample_hz,
                    )
                else:
                    df = view.to_polars()
                    if downsample_hz:
                        from resurrector.core.transforms import downsample_temporal
                        df = downsample_temporal(df, downsample_hz)
                    safe_name = topic.lstrip("/").replace("/", "_")
                    self._export_dataframe(df, format, output_path, safe_name)

        return output_path

    def _export_streaming_parquet(
        self, view, output_path: Path, name: str, downsample_hz: float | None,
    ) -> None:
        """Stream large topics to Parquet without loading all into memory."""
        import polars as pl
        import pyarrow as pa
        import pyarrow.parquet as pq

        filepath = output_path / f"{name}.parquet"
        writer = None
        rows_written = 0

        try:
            chunk_rows: list[dict] = []
            for msg in view.iter_messages():
                row = {"timestamp_ns": msg.timestamp_ns}
                from resurrector.core.bag_frame import _flatten_dict
                _flatten_dict(msg.data, row)
                chunk_rows.append(row)

                if len(chunk_rows) >= CHUNK_SIZE:
                    df = pl.DataFrame(chunk_rows)
                    if downsample_hz:
                        from resurrector.core.transforms import downsample_temporal
                        df = downsample_temporal(df, downsample_hz)
                    table = df.to_arrow()
                    if writer is None:
                        writer = pq.ParquetWriter(str(filepath), table.schema)
                    writer.write_table(table)
                    rows_written += df.height
                    chunk_rows.clear()

            # Write remaining
            if chunk_rows:
                df = pl.DataFrame(chunk_rows)
                if downsample_hz:
                    from resurrector.core.transforms import downsample_temporal
                    df = downsample_temporal(df, downsample_hz)
                table = df.to_arrow()
                if writer is None:
                    writer = pq.ParquetWriter(str(filepath), table.schema)
                writer.write_table(table)
                rows_written += df.height

        finally:
            if writer is not None:
                writer.close()

        logger.info("Streamed %d rows to %s", rows_written, filepath)

    def _export_dataframe(
        self, df, format: str, output_path: Path, name: str,
    ) -> None:
        """Export a single DataFrame to the specified format."""
        import polars as pl

        if format == "parquet":
            self._to_parquet(df, output_path, name)
        elif format == "hdf5":
            self._to_hdf5(df, output_path, name)
        elif format == "csv":
            self._to_csv(df, output_path, name)
        elif format == "numpy":
            self._to_numpy(df, output_path, name)
        elif format == "zarr":
            self._to_zarr(df, output_path, name)
        else:
            raise ValueError(
                f"Unknown export format: {format}. "
                f"Supported: parquet, hdf5, csv, numpy, zarr"
            )

    def _to_parquet(self, df, output_path: Path, name: str) -> None:
        """Export to Apache Parquet."""
        df.write_parquet(output_path / f"{name}.parquet")

    def _to_hdf5(self, df, output_path: Path, name: str) -> None:
        """Export to HDF5."""
        import h5py
        filepath = output_path / f"{name}.h5"
        with h5py.File(filepath, "w") as f:
            group = f.create_group(name)
            for col in df.columns:
                try:
                    data = df[col].to_numpy()
                    if data.dtype.kind in ('U', 'O', 'S'):
                        # String data — store as variable-length strings
                        dt = h5py.string_dtype()
                        group.create_dataset(col, data=data.astype(str), dtype=dt)
                    else:
                        group.create_dataset(col, data=data, compression="gzip")
                except Exception:
                    # Skip columns that can't be serialized
                    pass

    def _to_csv(self, df, output_path: Path, name: str) -> None:
        """Export to CSV."""
        df.write_csv(output_path / f"{name}.csv")

    def _to_numpy(self, df, output_path: Path, name: str) -> None:
        """Export to NumPy .npz archive."""
        arrays = {}
        for col in df.columns:
            try:
                arrays[col] = df[col].to_numpy()
            except Exception:
                pass
        np.savez_compressed(output_path / f"{name}.npz", **arrays)

    def _to_zarr(self, df, output_path: Path, name: str) -> None:
        """Export to Zarr format (requires zarr package)."""
        try:
            import zarr
        except ImportError:
            raise ImportError(
                "Zarr export requires the zarr package. "
                "Install with: pip install rosbag-resurrector[all-exports]"
            )
        store = zarr.DirectoryStore(str(output_path / f"{name}.zarr"))
        root = zarr.group(store)
        for col in df.columns:
            try:
                data = df[col].to_numpy()
                if data.dtype.kind not in ('U', 'O'):
                    root.create_dataset(col, data=data, chunks=True)
            except Exception:
                pass

    def export_frames(
        self,
        topic_view,
        output_dir: str | Path,
        format: str = "png",
        max_frames: int | None = None,
        every_n: int = 1,
    ) -> Path:
        """Export an image topic as a sequence of numbered image files.

        Args:
            topic_view: A TopicView for an image topic.
            output_dir: Directory to save frames.
            format: Image format — "png" or "jpeg".
            max_frames: Maximum number of frames to export.
            every_n: Export every Nth frame (for subsampling).

        Returns:
            Path to the output directory.
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
        """Export an image topic as an MP4 video file.

        Args:
            topic_view: A TopicView for an image topic.
            output_path: Path for the output video file.
            fps: Frames per second. Defaults to the topic's native frequency.
            codec: FourCC codec string.

        Returns:
            Path to the output video file.
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

                # Convert RGB to BGR for OpenCV
                if len(arr.shape) == 3 and arr.shape[2] == 3:
                    arr = arr[:, :, ::-1]
                writer.write(arr)
                count += 1
        finally:
            if writer is not None:
                writer.release()

        logger.info("Exported %d frames as video to %s", count, output_file)
        return output_file
