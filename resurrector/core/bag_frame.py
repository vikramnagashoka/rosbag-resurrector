"""BagFrame — the core data abstraction for rosbag analysis.

Provides a pandas-like API for working with robotics bag data:
lazy loading, topic selection, time slicing, conversion to
Polars/Pandas DataFrames, and integrated health checking.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import numpy as np
import polars as pl

from resurrector.ingest.parser import BagMetadata, MCAPParser, Message, TopicInfo, parse_bag
from resurrector.ingest.health_check import BagHealthReport, HealthChecker, HealthConfig


class TopicView:
    """Lazy view of a single topic in a bag file.

    Supports conversion to Polars/Pandas DataFrames and iteration
    over raw messages.
    """

    def __init__(
        self,
        bag_path: Path,
        topic_name: str,
        topic_info: TopicInfo,
        start_time_ns: int | None = None,
        end_time_ns: int | None = None,
    ):
        self._bag_path = bag_path
        self._topic_name = topic_name
        self._topic_info = topic_info
        self._start_time_ns = start_time_ns
        self._end_time_ns = end_time_ns
        self._cached_df: pl.DataFrame | None = None

    @property
    def name(self) -> str:
        return self._topic_name

    @property
    def message_type(self) -> str:
        return self._topic_info.message_type

    @property
    def message_count(self) -> int:
        return self._topic_info.message_count

    @property
    def frequency_hz(self) -> float | None:
        return self._topic_info.frequency_hz

    def iter_messages(self) -> Iterator[Message]:
        """Iterate over raw messages for this topic."""
        parser = parse_bag(self._bag_path)
        yield from parser.read_messages(
            topics=[self._topic_name],
            start_time_ns=self._start_time_ns,
            end_time_ns=self._end_time_ns,
        )

    def to_polars(self) -> pl.DataFrame:
        """Convert topic messages to a Polars DataFrame.

        Flattens nested message fields using dot notation:
        e.g., linear_acceleration.x, orientation.w
        """
        if self._cached_df is not None:
            return self._cached_df

        rows: list[dict[str, Any]] = []
        for msg in self.iter_messages():
            row = {"timestamp_ns": msg.timestamp_ns}
            _flatten_dict(msg.data, row)
            rows.append(row)

        if not rows:
            self._cached_df = pl.DataFrame({"timestamp_ns": []})
            return self._cached_df

        self._cached_df = pl.DataFrame(rows)
        return self._cached_df

    def to_pandas(self):
        """Convert topic messages to a Pandas DataFrame."""
        return self.to_polars().to_pandas()

    def to_numpy(self) -> dict[str, np.ndarray]:
        """Convert numeric columns to numpy arrays."""
        df = self.to_polars()
        result = {}
        for col in df.columns:
            try:
                result[col] = df[col].to_numpy()
            except Exception:
                pass
        return result

    def __len__(self) -> int:
        return self._topic_info.message_count

    def __repr__(self) -> str:
        freq = f"{self._topic_info.frequency_hz:.1f}Hz" if self._topic_info.frequency_hz else "unknown Hz"
        return (
            f"TopicView('{self._topic_name}', type={self._topic_info.message_type}, "
            f"count={self._topic_info.message_count}, freq={freq})"
        )


class BagFrame:
    """Main data abstraction for working with rosbag files.

    Provides a pandas-like API for robotics data:

        bf = BagFrame("experiment.mcap")
        bf.info()                          # Overview of the bag
        imu = bf["/imu/data"]              # Select a topic (lazy)
        df = imu.to_polars()               # Get as Polars DataFrame
        segment = bf.time_slice("10s", "30s")  # Time slice
        synced = bf.sync(["/imu/data", "/joint_states"])  # Synchronize
    """

    def __init__(self, path: str | Path):
        self._path = Path(path)
        if not self._path.exists():
            raise FileNotFoundError(f"Bag file not found: {self._path}")

        self._parser = parse_bag(self._path)
        self._metadata: BagMetadata | None = None
        self._health_report: BagHealthReport | None = None

    @property
    def path(self) -> Path:
        return self._path

    @property
    def metadata(self) -> BagMetadata:
        """Bag metadata (lazy loaded)."""
        if self._metadata is None:
            self._metadata = self._parser.get_metadata()
        return self._metadata

    @property
    def topics(self) -> list[TopicInfo]:
        """List of topics in the bag."""
        return self.metadata.topics

    @property
    def topic_names(self) -> list[str]:
        """List of topic names."""
        return [t.name for t in self.topics]

    @property
    def duration_sec(self) -> float:
        return self.metadata.duration_sec

    @property
    def message_count(self) -> int:
        return self.metadata.message_count

    def __getitem__(self, topic_name: str) -> TopicView:
        """Select a topic by name. Returns a lazy TopicView."""
        topic_info = self._find_topic(topic_name)
        return TopicView(self._path, topic_name, topic_info)

    def _find_topic(self, name: str) -> TopicInfo:
        """Find a topic by name, raising KeyError if not found."""
        for t in self.topics:
            if t.name == name:
                return t
        available = ", ".join(self.topic_names)
        raise KeyError(f"Topic '{name}' not found. Available: {available}")

    def info(self) -> str:
        """Print a summary of the bag contents (like df.info())."""
        meta = self.metadata
        health = self.health_report()

        lines = []
        lines.append(f"RosBag Resurrector — {self._path.name}")
        lines.append(f"Health Score: {health.score}/100 ({len(health.warnings)} warnings)")
        lines.append(
            f"Duration: {meta.duration_sec:.1f}s | "
            f"Size: {_format_size(self._path.stat().st_size)} | "
            f"Topics: {len(meta.topics)}"
        )
        lines.append("")

        # Topic table
        header = f"{'Topic':<30} {'Type':<25} {'Count':>8} {'Freq(Hz)':>10} {'Health':>8}"
        lines.append(header)
        lines.append("-" * len(header))
        for topic in meta.topics:
            freq = f"{topic.frequency_hz:.1f}" if topic.frequency_hz else "?"
            topic_health = health.topic_scores.get(topic.name)
            if topic_health:
                if topic_health.score >= 90:
                    health_str = "OK"
                elif topic_health.score >= 70:
                    health_str = f"WARN({topic_health.score})"
                else:
                    health_str = f"BAD({topic_health.score})"
            else:
                health_str = "?"
            lines.append(
                f"{topic.name:<30} {topic.message_type:<25} "
                f"{topic.message_count:>8,} {freq:>10} {health_str:>8}"
            )

        output = "\n".join(lines)
        print(output)
        return output

    def time_slice(self, start: str | float, end: str | float) -> "BagFrame":
        """Create a time-sliced view of the bag.

        Args:
            start: Start time as seconds (float) or string like "10s", "1.5min".
            end: End time as seconds (float) or string like "30s", "2min".

        Returns:
            A new BagFrame-like object filtered to the time range.
        """
        start_sec = _parse_time(start)
        end_sec = _parse_time(end)
        return TimeslicedBagFrame(self, start_sec, end_sec)

    def health_report(self) -> BagHealthReport:
        """Run health checks and return a detailed report."""
        if self._health_report is not None:
            return self._health_report

        checker = HealthChecker()
        topic_timestamps: dict[str, list[int]] = {}
        topic_sizes: dict[str, list[int]] = {}

        for msg in self._parser.read_messages():
            topic_timestamps.setdefault(msg.topic, []).append(msg.timestamp_ns)
            if msg.raw_data:
                topic_sizes.setdefault(msg.topic, []).append(len(msg.raw_data))

        self._health_report = checker.run_all_checks(
            topic_timestamps=topic_timestamps,
            topic_message_sizes=topic_sizes,
            bag_start_ns=self.metadata.start_time_ns,
            bag_end_ns=self.metadata.end_time_ns,
        )
        return self._health_report

    def sync(
        self,
        topics: list[str],
        method: str = "nearest",
        tolerance_ms: float = 50.0,
        anchor: str | None = None,
    ) -> pl.DataFrame:
        """Synchronize multiple topics by timestamp.

        Args:
            topics: List of topic names to synchronize.
            method: Sync method — "nearest", "interpolate", or "sample_and_hold".
            tolerance_ms: Maximum time difference for matching (ms).
            anchor: Topic to use as the time reference. Defaults to highest-frequency topic.

        Returns:
            A unified Polars DataFrame with columns prefixed by topic name.
        """
        from resurrector.core.sync import synchronize
        topic_views = {name: self[name] for name in topics}
        return synchronize(topic_views, method=method, tolerance_ms=tolerance_ms, anchor=anchor)

    def export(
        self,
        topics: list[str] | None = None,
        format: str = "parquet",
        output: str = "./export",
        sync: bool = False,
        sync_method: str = "nearest",
        downsample_hz: float | None = None,
    ) -> Path:
        """Export bag data to ML-friendly formats.

        Args:
            topics: Topics to export (default: all).
            format: Output format — "parquet", "hdf5", "csv", "numpy", "zarr".
            output: Output directory path.
            sync: Whether to synchronize topics before export.
            sync_method: Sync method if sync=True.
            downsample_hz: Target frequency for downsampling.

        Returns:
            Path to the output directory.
        """
        from resurrector.core.export import Exporter
        exporter = Exporter()
        topic_names = topics or self.topic_names
        return exporter.export(
            bag_frame=self,
            topics=topic_names,
            format=format,
            output_dir=output,
            sync=sync,
            sync_method=sync_method,
            downsample_hz=downsample_hz,
        )

    def __repr__(self) -> str:
        return (
            f"BagFrame('{self._path.name}', "
            f"duration={self.metadata.duration_sec:.1f}s, "
            f"topics={len(self.topics)}, "
            f"messages={self.metadata.message_count:,})"
        )


class TimeslicedBagFrame:
    """A time-filtered view of a BagFrame."""

    def __init__(self, parent: BagFrame, start_sec: float, end_sec: float):
        self._parent = parent
        self._start_sec = start_sec
        self._end_sec = end_sec
        self._start_ns = parent.metadata.start_time_ns + int(start_sec * 1e9)
        self._end_ns = parent.metadata.start_time_ns + int(end_sec * 1e9)

    @property
    def path(self) -> Path:
        return self._parent.path

    @property
    def topics(self) -> list[TopicInfo]:
        return self._parent.topics

    @property
    def topic_names(self) -> list[str]:
        return self._parent.topic_names

    @property
    def duration_sec(self) -> float:
        return self._end_sec - self._start_sec

    def __getitem__(self, topic_name: str) -> TopicView:
        topic_info = self._parent._find_topic(topic_name)
        return TopicView(
            self._parent._path, topic_name, topic_info,
            start_time_ns=self._start_ns,
            end_time_ns=self._end_ns,
        )

    def sync(self, topics: list[str], **kwargs) -> pl.DataFrame:
        from resurrector.core.sync import synchronize
        topic_views = {name: self[name] for name in topics}
        return synchronize(topic_views, **kwargs)

    def __repr__(self) -> str:
        return (
            f"TimeslicedBagFrame('{self._parent._path.name}', "
            f"t=[{self._start_sec:.1f}s, {self._end_sec:.1f}s])"
        )


def _parse_time(t: str | float) -> float:
    """Parse a time value to seconds."""
    if isinstance(t, (int, float)):
        return float(t)
    t = t.strip().lower()
    if t.endswith("s"):
        t = t[:-1]
        if t.endswith("m"):
            # "ms" — already stripped 's'
            return float(t[:-1]) / 1000.0
        return float(t)
    elif t.endswith("min"):
        return float(t[:-3]) * 60.0
    elif t.endswith("m"):
        return float(t[:-1]) * 60.0
    elif t.endswith("h"):
        return float(t[:-1]) * 3600.0
    return float(t)


def _format_size(size_bytes: int) -> str:
    """Format byte size to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def _flatten_dict(d: dict, out: dict, prefix: str = "") -> None:
    """Flatten a nested dict using dot notation, skipping internal keys."""
    for key, value in d.items():
        if key.startswith("_"):
            continue
        full_key = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
        if isinstance(value, dict):
            _flatten_dict(value, out, full_key)
        elif isinstance(value, list):
            # For small lists (e.g., joint positions), expand to indexed columns
            if len(value) <= 20 and all(isinstance(v, (int, float)) for v in value):
                for i, v in enumerate(value):
                    out[f"{full_key}.{i}"] = v
            # Skip large lists (e.g., lidar ranges) — they need special handling
        else:
            out[full_key] = value
