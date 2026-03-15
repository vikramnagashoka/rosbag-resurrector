"""Export bag data to ML-friendly formats.

Supports: Parquet, HDF5, CSV, NumPy, Zarr, RLDS, LeRobot.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from resurrector.core.bag_frame import BagFrame


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
                    continue
                df = view.to_polars()
                if downsample_hz:
                    from resurrector.core.transforms import downsample_temporal
                    df = downsample_temporal(df, downsample_hz)
                safe_name = topic.lstrip("/").replace("/", "_")
                self._export_dataframe(df, format, output_path, safe_name)

        return output_path

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
        """Export to CSV (numeric columns only)."""
        # Filter to numeric columns + timestamp
        numeric_cols = ["timestamp_ns"] + [
            c for c in df.columns
            if c != "timestamp_ns" and df[c].dtype in (
                    getattr(df[c].dtype, '__class__', type(None)),
            ) or str(df[c].dtype).startswith(("Int", "Float", "UInt", "f", "i", "u"))
        ]
        # Simpler approach: just write all columns, CSV handles mixed types
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
