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

from resurrector.core.exceptions import LargeTopicError
from resurrector.ingest.parser import (
    BagMetadata, MCAPParser, Message, TopicInfo, parse_bag,
    get_image_array, get_compressed_image_array,
)
from resurrector.ingest.health_check import BagHealthReport, HealthChecker, HealthConfig

_IMAGE_TYPES = {
    "sensor_msgs/msg/Image",
    "sensor_msgs/msg/CompressedImage",
}

# Per the v0.4.0 performance contract: eager .to_polars() / .to_pandas()
# / .to_numpy() refuse topics above this threshold unless the caller
# passes force=True. Beyond ~1M messages a flattened Polars DataFrame
# typically exceeds 100 MB and the user almost always wanted streaming
# (iter_chunks or materialize_ipc_cache) instead. Tests can monkeypatch
# this constant down to verify the guard fires.
LARGE_TOPIC_THRESHOLD = 1_000_000


class IpcCache:
    """Handle to a streamed Arrow IPC cache of a single topic.

    Returned by ``TopicView.materialize_ipc_cache()``. The cache file
    lives on disk under the OS temp dir; ``scan()`` returns a
    ``pl.LazyFrame`` backed by it for filter/projection pushdown.

    Lifecycle is explicit. Use as a context manager, or call
    ``close()`` yourself, or both. ``__del__`` is a best-effort
    backstop only — do not rely on it (interpreter-shutdown ordering
    is not guaranteed, and on Windows an open mmap can prevent unlink).
    """

    def __init__(self, path: Path | None, _empty: bool = False):
        self._path = path
        self._empty = _empty
        self._closed = _empty  # an empty cache has nothing to close
        self._warned_unclosed = False

    @property
    def path(self) -> Path | None:
        """The on-disk Arrow IPC file, or None for an empty cache."""
        return self._path

    def scan(self) -> pl.LazyFrame:
        """Return a Polars LazyFrame over the cached topic data.

        Calling scan() after close() raises a clear error so notebook
        users get a real exception instead of silent corruption.
        """
        if self._closed and not self._empty:
            raise RuntimeError(
                "IpcCache.scan() called after close() — the cache file "
                "has been deleted. Re-create the cache via "
                "TopicView.materialize_ipc_cache()."
            )
        if self._empty or self._path is None:
            return pl.LazyFrame({"timestamp_ns": []})
        return pl.scan_ipc(str(self._path))

    def close(self) -> None:
        """Delete the temp file. Idempotent — safe to call multiple times."""
        if self._closed:
            return
        if self._path is not None:
            self._path.unlink(missing_ok=True)
        self._closed = True

    def __enter__(self) -> "IpcCache":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        # Best-effort backstop. Notebook users who forget to close get
        # a warning so the leak is visible rather than silent.
        if not self._closed and self._path is not None:
            try:
                import warnings
                if not self._warned_unclosed:
                    warnings.warn(
                        f"IpcCache for {self._path.name} was not closed; "
                        f"deleting via __del__. Use a context manager or "
                        f"call close() explicitly.",
                        ResourceWarning,
                        stacklevel=2,
                    )
                self._path.unlink(missing_ok=True)
            except Exception:
                # GC during interpreter shutdown can fail in many ways
                # (modules torn down, etc.); never propagate from __del__.
                pass


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

    @property
    def is_image_topic(self) -> bool:
        """True if this topic contains image data (raw or compressed)."""
        return self._topic_info.message_type in _IMAGE_TYPES

    def iter_messages(self) -> Iterator[Message]:
        """Iterate over raw messages for this topic."""
        parser = parse_bag(self._bag_path)
        yield from parser.read_messages(
            topics=[self._topic_name],
            start_time_ns=self._start_time_ns,
            end_time_ns=self._end_time_ns,
        )

    def iter_images(self) -> Iterator[tuple[int, np.ndarray]]:
        """Yield (timestamp_ns, numpy_array) for image topics.

        Works with both sensor_msgs/msg/Image and
        sensor_msgs/msg/CompressedImage topics.

        Raises:
            TypeError: If this topic is not an image type.
        """
        if not self.is_image_topic:
            raise TypeError(
                f"Topic '{self._topic_name}' is not an image topic "
                f"(type: {self._topic_info.message_type})"
            )

        is_compressed = self._topic_info.message_type == "sensor_msgs/msg/CompressedImage"
        for msg in self.iter_messages():
            if is_compressed:
                arr = get_compressed_image_array(msg)
            else:
                arr = get_image_array(msg)
            if arr is not None:
                yield msg.timestamp_ns, arr

    def iter_chunks(self, chunk_size: int = 50_000) -> Iterator[pl.DataFrame]:
        """Yield topic messages as Polars DataFrames in fixed-size chunks.

        This is the core streaming primitive. Memory usage is bounded by
        chunk_size regardless of total topic size.

        Yields:
            pl.DataFrame of up to chunk_size rows with flattened columns.
        """
        buffer: list[dict[str, Any]] = []
        for msg in self.iter_messages():
            row = {"timestamp_ns": msg.timestamp_ns}
            _flatten_dict(msg.data, row)
            buffer.append(row)
            if len(buffer) >= chunk_size:
                yield pl.DataFrame(buffer)
                buffer = []
        if buffer:
            yield pl.DataFrame(buffer)

    def materialize_ipc_cache(self, chunk_size: int = 50_000) -> "IpcCache":
        """Stream the topic to a temporary Arrow IPC file and return a handle.

        Returns an ``IpcCache`` whose ``.scan()`` produces a
        ``pl.LazyFrame`` with real filter/projection pushdown. Memory
        usage is bounded by ``chunk_size`` regardless of topic size.

        Lifecycle is **explicit** — the temp file is owned by the
        returned cache and is deleted only when ``.close()`` is called
        (or when the cache is used as a context manager). This
        replaces the v0.3.x ``to_lazy_polars()`` method, which leaked
        the temp file.

        Examples
        --------
        Context-manager usage (preferred — file is cleaned up
        deterministically when the block exits)::

            with bf["/imu/data"].materialize_ipc_cache() as cache:
                df = cache.scan().filter(pl.col("x") > 0).collect()

        Explicit usage::

            cache = bf["/imu/data"].materialize_ipc_cache()
            try:
                df = cache.scan().filter(pl.col("x") > 0).collect()
            finally:
                cache.close()
        """
        import tempfile
        import pyarrow.ipc as ipc

        tmp = tempfile.NamedTemporaryFile(
            prefix=f"resurrector_{self._topic_name.lstrip('/').replace('/', '_')}_",
            suffix=".arrow",
            delete=False,
        )
        tmp.close()
        tmp_path = Path(tmp.name)

        writer = None
        wrote_any = False
        try:
            for chunk in self.iter_chunks(chunk_size):
                if chunk.height == 0:
                    continue
                table = chunk.to_arrow()
                if writer is None:
                    writer = ipc.new_file(str(tmp_path), table.schema)
                writer.write_table(table)
                wrote_any = True
        finally:
            if writer is not None:
                writer.close()

        if not wrote_any:
            # Empty topic — delete the placeholder file immediately and
            # return a cache that scans to an empty LazyFrame.
            tmp_path.unlink(missing_ok=True)
            return IpcCache(path=None, _empty=True)

        return IpcCache(path=tmp_path)

    def to_polars(self, force: bool = False) -> pl.DataFrame:
        """Convert topic messages to a Polars DataFrame.

        Flattens nested message fields using dot notation:
        e.g., linear_acceleration.x, orientation.w

        Refuses topics larger than ``LARGE_TOPIC_THRESHOLD`` (1 M
        messages by default) unless ``force=True`` — see the README
        "Performance contract" section. For larger topics use
        ``iter_chunks()`` or ``materialize_ipc_cache()`` instead.

        Raises:
            LargeTopicError: if message_count > LARGE_TOPIC_THRESHOLD
                and force is False.
        """
        if self._cached_df is not None:
            return self._cached_df

        if (
            not force
            and self._topic_info.message_count > LARGE_TOPIC_THRESHOLD
        ):
            raise LargeTopicError(
                topic_name=self._topic_name,
                message_count=self._topic_info.message_count,
                threshold=LARGE_TOPIC_THRESHOLD,
            )

        # Fold chunks one at a time instead of building a list — even
        # when force=True, we shouldn't double-buffer (raw chunks list
        # plus the concatenated DataFrame).
        result: pl.DataFrame | None = None
        for chunk in self.iter_chunks():
            if chunk.height == 0:
                continue
            result = chunk if result is None else pl.concat(
                [result, chunk], how="diagonal_relaxed"
            )

        if result is None:
            result = pl.DataFrame({"timestamp_ns": []})

        self._cached_df = result
        return result

    def to_pandas(self, force: bool = False):
        """Convert topic messages to a Pandas DataFrame.

        Same large-topic guard as ``to_polars``. Pass ``force=True`` to
        opt in past the threshold.
        """
        return self.to_polars(force=force).to_pandas()

    def to_numpy(self, force: bool = False) -> dict[str, np.ndarray]:
        """Convert numeric columns to numpy arrays.

        Columns that cannot be converted (e.g., nested lists of varying length)
        are skipped and their names collected in the returned dict's
        ``__skipped__`` key so callers can inspect what was dropped.

        Same large-topic guard as ``to_polars``. Pass ``force=True`` to
        opt in past the threshold.
        """
        df = self.to_polars(force=force)
        result: dict[str, np.ndarray] = {}
        skipped: list[str] = []
        for col in df.columns:
            try:
                result[col] = df[col].to_numpy()
            except Exception as e:
                skipped.append(f"{col}: {type(e).__name__}: {e}")
        if skipped:
            result["__skipped__"] = np.array(skipped, dtype=object)
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
        """Run health checks and return a detailed report.

        Streaming implementation: maintains a small per-topic
        ``TopicHealthState`` and updates it message-by-message instead
        of accumulating timestamp lists. Memory is bounded by
        ``num_topics * constant``, regardless of bag size.
        """
        if self._health_report is not None:
            return self._health_report

        from resurrector.ingest.health_check import (
            TopicHealthState, update_state,
        )

        checker = HealthChecker()
        config = checker.config

        # Pre-seed expected_intervals_ns from the metadata frequencies
        # so the inline gap/rate checks can fire from message #1.
        # `topic_info.frequency_hz` is computed by the parser at metadata
        # time and is more accurate than a bootstrap running estimate.
        expected_intervals_ns: dict[str, float] = {}
        expected_frequencies: dict[str, float] = {}
        for ti in self.metadata.topics:
            if ti.frequency_hz and ti.frequency_hz > 0:
                expected_intervals_ns[ti.name] = 1e9 / ti.frequency_hz
                expected_frequencies[ti.name] = ti.frequency_hz

        states: dict[str, TopicHealthState] = {}
        for ti in self.metadata.topics:
            states[ti.name] = TopicHealthState()

        for msg in self._parser.read_messages():
            state = states.get(msg.topic)
            if state is None:
                # Topic not in metadata (shouldn't happen normally) — skip.
                continue
            update_state(
                state=state,
                topic=msg.topic,
                timestamp_ns=msg.timestamp_ns,
                message_size=len(msg.raw_data) if msg.raw_data else None,
                config=config,
                expected_interval_ns=expected_intervals_ns.get(msg.topic),
            )

        self._health_report = checker.run_streaming(
            states=states,
            bag_start_ns=self.metadata.start_time_ns,
            bag_end_ns=self.metadata.end_time_ns,
            expected_frequencies=expected_frequencies,
        )
        return self._health_report

    def sync(
        self,
        topics: list[str],
        method: str = "nearest",
        tolerance_ms: float = 50.0,
        anchor: str | None = None,
        *,
        engine: str = "auto",
        out_of_order: str = "error",
        boundary: str = "null",
        max_buffer_messages: int = 100_000,
        max_lateness_ms: float = 0.0,
    ) -> pl.DataFrame:
        """Synchronize multiple topics by timestamp.

        Args:
            topics: List of topic names to synchronize.
            method: ``"nearest"``, ``"interpolate"``, or ``"sample_and_hold"``.
            tolerance_ms: Maximum time difference for matching (ms).
            anchor: Topic to use as the time reference. Defaults to the
                highest-frequency topic.
            engine: ``"eager"`` (load every topic, v0.3.x behavior),
                ``"streaming"`` (per-topic bounded buffers), or
                ``"auto"`` (eager when all topics are under
                ``LARGE_TOPIC_THRESHOLD``, streaming otherwise — the default).
            out_of_order: streaming-only. ``"error"`` (raise on
                regression — the default), ``"warn_drop"`` (drop the
                regression with a log warning), or ``"reorder"``
                (bounded watermark reorder buffer).
            boundary: streaming-only, interpolate-only. ``"null"``
                (default), ``"drop"``, ``"hold"``, or ``"error"`` —
                see ``synchronize()`` for details.
            max_buffer_messages: streaming-only, per-topic cap. Tripped
                raises :class:`SyncBufferExceededError`.
            max_lateness_ms: streaming-only. Watermark lateness window
                for the ``reorder`` policy. Ignored otherwise.

        Returns:
            A unified Polars DataFrame with columns prefixed by topic name.
        """
        from resurrector.core.sync import synchronize
        topic_views = {name: self[name] for name in topics}
        return synchronize(
            topic_views,
            method=method,
            tolerance_ms=tolerance_ms,
            anchor=anchor,
            engine=engine,
            out_of_order=out_of_order,
            boundary=boundary,
            max_buffer_messages=max_buffer_messages,
            max_lateness_ms=max_lateness_ms,
        )

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

    def _repr_html_(self) -> str:
        """Rich HTML representation for Jupyter notebooks."""
        from resurrector.core.topic_groups import get_topic_group

        meta = self.metadata
        try:
            health = self.health_report()
            score = health.score
            n_warnings = len(health.warnings)
        except Exception:
            score = None
            n_warnings = 0

        # Health badge color
        if score is not None:
            if score >= 90:
                badge_color, badge_bg = "#fff", "#28a745"
            elif score >= 70:
                badge_color, badge_bg = "#000", "#ffc107"
            else:
                badge_color, badge_bg = "#fff", "#dc3545"
            badge = (
                f'<span style="background:{badge_bg};color:{badge_color};'
                f'padding:2px 8px;border-radius:4px;font-weight:bold">'
                f'{score}/100</span>'
            )
        else:
            badge = '<span style="color:#888">?</span>'

        # Header
        html = f"""
        <div style="font-family:sans-serif;border:1px solid #ddd;border-radius:8px;padding:16px;max-width:800px">
        <h3 style="margin:0 0 8px 0">🤖 {meta.path.name}</h3>
        <div style="margin-bottom:12px">
            Health: {badge}
            &nbsp;|&nbsp; Duration: <b>{meta.duration_sec:.1f}s</b>
            &nbsp;|&nbsp; Size: <b>{_format_size(self._path.stat().st_size)}</b>
            &nbsp;|&nbsp; Topics: <b>{len(meta.topics)}</b>
            &nbsp;|&nbsp; Messages: <b>{meta.message_count:,}</b>
        </div>
        <table style="border-collapse:collapse;width:100%;font-size:13px">
        <tr style="background:#f8f9fa">
            <th style="text-align:left;padding:6px;border-bottom:2px solid #dee2e6">Topic</th>
            <th style="text-align:left;padding:6px;border-bottom:2px solid #dee2e6">Type</th>
            <th style="text-align:left;padding:6px;border-bottom:2px solid #dee2e6">Group</th>
            <th style="text-align:right;padding:6px;border-bottom:2px solid #dee2e6">Count</th>
            <th style="text-align:right;padding:6px;border-bottom:2px solid #dee2e6">Hz</th>
            <th style="text-align:center;padding:6px;border-bottom:2px solid #dee2e6">Health</th>
        </tr>
        """

        for topic in meta.topics:
            freq = f"{topic.frequency_hz:.1f}" if topic.frequency_hz else "?"
            group = get_topic_group(topic.name)
            if score is not None:
                th = health.topic_scores.get(topic.name)
                if th and th.score >= 90:
                    h_icon = "✅"
                elif th and th.score >= 70:
                    h_icon = f"⚠️ {th.score}"
                elif th:
                    h_icon = f"❌ {th.score}"
                else:
                    h_icon = "?"
            else:
                h_icon = "?"

            html += f"""
            <tr style="border-bottom:1px solid #eee">
                <td style="padding:4px 6px;font-family:monospace">{topic.name}</td>
                <td style="padding:4px 6px;color:#666">{topic.message_type}</td>
                <td style="padding:4px 6px;color:#888;font-size:12px">{group}</td>
                <td style="padding:4px 6px;text-align:right">{topic.message_count:,}</td>
                <td style="padding:4px 6px;text-align:right">{freq}</td>
                <td style="padding:4px 6px;text-align:center">{h_icon}</td>
            </tr>"""

        html += "</table></div>"
        return html


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
