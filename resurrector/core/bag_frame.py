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
    """Lazy view of a single topic, returned by ``BagFrame[topic_name]``.

    Supports conversion to Polars/Pandas/NumPy, message-level iteration,
    and bounded-memory chunked streaming for large topics. All eager
    conversion methods (``to_polars`` etc.) refuse topics larger than
    ``LARGE_TOPIC_THRESHOLD`` (1 M messages by default) unless you pass
    ``force=True`` — see the README "Performance contract".

    Example::

        from resurrector import BagFrame
        bf = BagFrame("experiment.mcap")
        imu = bf["/imu/data"]                            # TopicView (lazy)
        df = imu.to_polars()                             # Polars DataFrame
        for chunk in imu.iter_chunks(chunk_size=10_000): # bounded-memory
            process(chunk)
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
        """The topic name as recorded in the bag (e.g. ``"/imu/data"``)."""
        return self._topic_name

    @property
    def message_type(self) -> str:
        """ROS message type string (e.g. ``"sensor_msgs/msg/Imu"``)."""
        return self._topic_info.message_type

    @property
    def message_count(self) -> int:
        """Total number of messages on this topic in the bag."""
        return self._topic_info.message_count

    @property
    def frequency_hz(self) -> float | None:
        """Average publish frequency in Hz, or ``None`` if not computable."""
        return self._topic_info.frequency_hz

    @property
    def is_image_topic(self) -> bool:
        """True iff the topic is ``sensor_msgs/msg/Image`` or ``CompressedImage``."""
        return self._topic_info.message_type in _IMAGE_TYPES

    def iter_messages(self) -> Iterator[Message]:
        """Yield raw decoded messages one at a time. Memory bounded by message size.

        Each yielded ``Message`` has ``.topic``, ``.timestamp_ns``,
        ``.data`` (decoded fields as a dict), and ``.raw_data`` (the
        underlying serialized bytes). Use this when you need access to
        the raw schema-decoded fields rather than a flattened DataFrame.

        Example::

            for msg in bf["/imu/data"].iter_messages():
                accel_x = msg.data["linear_acceleration"]["x"]
                t = msg.timestamp_ns
        """
        parser = parse_bag(self._bag_path)
        yield from parser.read_messages(
            topics=[self._topic_name],
            start_time_ns=self._start_time_ns,
            end_time_ns=self._end_time_ns,
        )

    def iter_images(self) -> Iterator[tuple[int, np.ndarray]]:
        """Yield ``(timestamp_ns, image_array)`` pairs for an image topic.

        Decodes each frame to an HxWxC NumPy array (uint8). Handles both
        ``sensor_msgs/msg/Image`` (raw, encoding-aware) and
        ``sensor_msgs/msg/CompressedImage`` (JPEG/PNG via OpenCV).

        Raises:
            TypeError: If this topic is not an image type — see
                :attr:`is_image_topic`.

        Example::

            for ts, frame in bf["/camera/rgb"].iter_images():
                cv2.imwrite(f"frame_{ts}.png", frame)
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
        """Yield the topic as Polars DataFrames in fixed-size chunks. Bounded memory.

        This is the core streaming primitive. Memory usage is bounded
        by ``chunk_size`` regardless of total topic size — open a 100 GB
        bag without OOMing. Nested message fields are flattened with
        dot notation (``linear_acceleration.x``, ``orientation.w``).

        Args:
            chunk_size: Rows per yielded DataFrame. Default 50_000.
                Lower for tighter RSS budgets; raise to amortize the
                per-chunk overhead on fast disks.

        Yields:
            ``pl.DataFrame`` with up to ``chunk_size`` rows and a
            ``timestamp_ns`` column plus one column per leaf message field.

        Example::

            # Streaming downsample of a large topic to 1 Hz
            buckets = []
            for chunk in bf["/imu/data"].iter_chunks(chunk_size=10_000):
                ds = chunk.group_by_dynamic("timestamp_ns", every="1s").mean()
                buckets.append(ds)
            full = pl.concat(buckets)
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
        """Materialize the entire topic as one Polars DataFrame.

        Flattens nested message fields with dot notation (e.g.
        ``linear_acceleration.x``, ``orientation.w``). Cached after
        the first call so repeated access is free.

        Args:
            force: Bypass the ``LARGE_TOPIC_THRESHOLD`` (1 M messages)
                guard and materialize anyway. Use only when you've
                confirmed the topic fits in RAM. For larger topics
                prefer :meth:`iter_chunks` or
                :meth:`materialize_ipc_cache`.

        Returns:
            ``pl.DataFrame`` with a ``timestamp_ns`` column plus one
            column per leaf message field.

        Raises:
            LargeTopicError: if ``message_count > LARGE_TOPIC_THRESHOLD``
                and ``force`` is False. The exception message points at
                the streaming alternatives.

        Example::

            df = bf["/imu/data"].to_polars()
            big = bf["/camera/rgb"].to_polars(force=True)  # only if you have RAM
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
        """Materialize the entire topic as a Pandas DataFrame.

        Convenience wrapper around :meth:`to_polars` for users who
        prefer Pandas. Same large-topic guard applies; pass
        ``force=True`` to bypass it.

        Returns:
            ``pandas.DataFrame``.

        Example::

            df_pd = bf["/imu/data"].to_pandas()
            df_pd.plot.line(x="timestamp_ns", y="linear_acceleration.x")
        """
        return self.to_polars(force=force).to_pandas()

    def to_numpy(self, force: bool = False) -> dict[str, np.ndarray]:
        """Materialize the topic as a dict of column-name → numpy array.

        Columns that cannot be converted to a numeric ndarray (nested
        lists of varying length, structured fields, etc.) are skipped
        and their names collected under the ``__skipped__`` key in the
        return dict so callers can audit what was dropped.

        Same ``LARGE_TOPIC_THRESHOLD`` guard as :meth:`to_polars`.

        Returns:
            ``dict[str, np.ndarray]``. Always contains ``timestamp_ns``;
            also contains one entry per convertible flat field.

        Example::

            arrays = bf["/imu/data"].to_numpy()
            t = arrays["timestamp_ns"]
            ax = arrays["linear_acceleration.x"]
            print(arrays.get("__skipped__"))   # any columns that didn't convert
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
    """Pandas-like front door for a rosbag file. The main entry point of the library.

    Lazy by default — construction reads only metadata, not message
    payloads. Topics are accessed with bracket-indexing (``bf[topic]``)
    which returns a :class:`TopicView`. From there you can convert to
    Polars / Pandas / NumPy, iterate in bounded-memory chunks, or feed
    into :meth:`sync` / :meth:`export`.

    Args:
        path: Path to a bag file. Supports ``.mcap`` natively;
            ``.bag`` (ROS 1) and ``.db3`` (ROS 2 SQLite) are
            auto-converted to MCAP via the official ``mcap`` and
            ``ros2 bag convert`` CLIs respectively.

    Example::

        from resurrector import BagFrame

        bf = BagFrame("experiment.mcap")
        bf.info()                                          # rich overview
        df = bf["/imu/data"].to_polars()                   # Polars DataFrame
        report = bf.health_report()                        # 0-100 quality score
        synced = bf.sync(["/imu/data", "/joint_states"],   # multi-stream sync
                         method="nearest", tolerance_ms=50)
        bf.export(topics=["/imu/data"], format="parquet",  # ML-ready export
                  output="./data")
    """

    def __init__(self, path: str | Path):
        """Open a bag for analysis. Reads metadata only — message payloads stay on disk.

        Raises:
            FileNotFoundError: If the path doesn't exist.

        Example::

            bf = BagFrame("~/.resurrector/demo_sample.mcap")
        """
        self._path = Path(path)
        if not self._path.exists():
            raise FileNotFoundError(f"Bag file not found: {self._path}")

        self._parser = parse_bag(self._path)
        self._metadata: BagMetadata | None = None
        self._health_report: BagHealthReport | None = None

    @property
    def path(self) -> Path:
        """Filesystem path to the bag file."""
        return self._path

    @property
    def metadata(self) -> BagMetadata:
        """Cached :class:`BagMetadata` — start/end time, topics, schemas, etc."""
        if self._metadata is None:
            self._metadata = self._parser.get_metadata()
        return self._metadata

    @property
    def topics(self) -> list[TopicInfo]:
        """List of :class:`TopicInfo` records (name, type, count, frequency)."""
        return self.metadata.topics

    @property
    def topic_names(self) -> list[str]:
        """List of topic names as strings — convenient for iteration."""
        return [t.name for t in self.topics]

    @property
    def duration_sec(self) -> float:
        """Bag duration in seconds (end - start)."""
        return self.metadata.duration_sec

    @property
    def message_count(self) -> int:
        """Total number of messages across all topics."""
        return self.metadata.message_count

    def __getitem__(self, topic_name: str) -> TopicView:
        """Return a lazy :class:`TopicView` for ``topic_name`` — the main accessor.

        Raises:
            KeyError: If the topic is not present. The error message
                lists the available topics.

        Example::

            imu = bf["/imu/data"]               # TopicView, no I/O yet
            df = imu.to_polars()                # actual read happens here
        """
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
        """Print and return a human-readable summary of the bag.

        Like ``pandas.DataFrame.info()`` — shows the bag's name, health
        score, duration, size, and a per-topic table (name, type, count,
        frequency, health). Side effect: prints to stdout. Also returns
        the same text as a string for convenience.

        Returns:
            The full summary as a single string (already printed).

        Example::

            bf.info()
        """
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
        """Return a view of the bag restricted to a time range.

        The returned ``TimeslicedBagFrame`` quacks like a ``BagFrame``
        for the API surface used in analysis (``[topic]``, ``sync``,
        ``topic_names``, etc.) but every read is bounded to the
        ``[start, end)`` window. Useful for "I only care about the
        manipulation segment" workflows without re-recording.

        Args:
            start: Start time, relative to bag start. Accepts a float
                (seconds) or a string like ``"10s"``, ``"1.5min"``,
                ``"500ms"``, ``"2h"``.
            end: End time, same format.

        Returns:
            A time-restricted view that supports the standard analysis API.

        Example::

            chunk = bf.time_slice("10s", "30s")
            df = chunk["/imu/data"].to_polars()
            chunk.sync(["/imu/data", "/joint_states"])
        """
        start_sec = _parse_time(start)
        end_sec = _parse_time(end)
        return TimeslicedBagFrame(self, start_sec, end_sec)

    def health_report(self) -> BagHealthReport:
        """Compute a 0-100 quality score for the bag plus per-topic breakdowns.

        Detects dropped messages, time gaps, out-of-order timestamps,
        frequency drift, and message-size anomalies. Cached after the
        first call. Streaming implementation: memory is bounded by
        ``num_topics × constant`` regardless of bag size.

        Returns:
            ``BagHealthReport`` with ``.score`` (0-100), ``.issues`` (list
            of typed findings), ``.recommendations`` (suggested next steps),
            and ``.topic_scores`` (per-topic breakdown).

        Example::

            r = bf.health_report()
            print(f"Score: {r.score}/100 — {len(r.issues)} issues")
            for issue in r.issues:
                print(f"  [{issue.severity.value}] {issue.message}")
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
        """Export bag data to ML-friendly formats — the main bulk-export entry point.

        Streams topic data through the chosen format writer; chunk-streaming
        formats (Parquet, HDF5, CSV, Zarr, LeRobot, RLDS) are bounded
        by chunk size, not topic size. NumPy ``.npz`` materializes
        per-topic and refuses topics over 1 M messages with a clear
        :class:`LargeTopicError`.

        Args:
            topics: Topics to export. ``None`` means every non-image topic.
            format: One of ``parquet`` (default, columnar, best for ML),
                ``hdf5``, ``csv``, ``numpy``, ``zarr`` (needs
                ``[all-exports]``), ``lerobot`` / ``rlds`` (needs
                ``[all-exports]``, training-pipeline-ready).
            output: Output directory. Created if missing.
            sync: When True, time-align all topics before writing using
                ``sync_method``.
            sync_method: ``nearest`` / ``interpolate`` / ``sample_and_hold``.
                Only used when ``sync`` is True.
            downsample_hz: Resample to this rate before writing. Useful
                for shrinking a 1 kHz IMU to 50 Hz training data.

        Returns:
            ``Path`` to the output directory.

        Raises:
            LargeTopicError: Per-format thresholds (NumPy hard cap at 1 M).

        Example::

            # Quick Parquet snapshot of two topics
            bf.export(topics=["/imu/data", "/joint_states"], format="parquet",
                      output="./parquet_out")

            # Time-synced HDF5 at 50 Hz for ML training
            bf.export(topics=["/imu/data", "/joint_states"],
                      format="hdf5", output="./training",
                      sync=True, sync_method="nearest", downsample_hz=50)

            # LeRobot-formatted dataset for direct use in robot-learning pipelines
            bf.export(format="lerobot", output="./lerobot_data")
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
