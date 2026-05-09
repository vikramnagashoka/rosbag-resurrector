"""N-bag concatenated stream API (v0.6.0 — Sub-feature B.2).

Lets you treat a sequence of bags as a single time-aligned stream:

    import resurrector
    bf = resurrector.concatenate_bags(["a.mcap", "b.mcap", "c.mcap"], mode="time")
    df = bf["/imu/data"].to_polars()  # rows from all 3 bags, sorted by ts

Two modes:

- ``mode="time"`` — order bags by their ``start_time_ns``; messages
  flow in absolute-time order (assumes the bags are time-disjoint;
  overlapping bags' messages will appear interleaved by their per-bag
  iteration order, which may not be globally sorted).

- ``mode="index"`` — preserve the user-supplied list order regardless
  of timestamps. Useful when bags share a logical sequence (e.g.
  trial 1 / trial 2 / trial 3) that doesn't align with wall-clock.

Memory-bounded: composes ``BagFrame.iter_chunks()`` per bag, so the
peak memory is bounded by ``chunk_size`` × max bag size. Doesn't
materialize anything across bags ahead of time.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

import polars as pl

from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.parser import Message, TopicInfo


class ConcatenatedTopicView:
    """A virtual TopicView that spans multiple bags' instances of one topic.

    Mirrors the subset of :class:`resurrector.core.bag_frame.TopicView`
    surface that's safe to compose across bags: ``iter_chunks``,
    ``to_polars``, ``message_count``, ``message_type``. Eager bulk
    operations like ``to_numpy`` aren't supported because they'd
    require concatenating across bags in memory — explicit caller
    intent should drive that.
    """

    def __init__(self, parent: "ConcatenatedBagFrame", topic: str):
        self._parent = parent
        self._topic = topic
        # Find the per-bag views that have this topic; bags missing the
        # topic are silently skipped (the schema may differ across bags).
        self._views = []
        for bf in parent._bag_frames:
            if topic in {ti.name for ti in bf.metadata.topics}:
                self._views.append(bf[topic])

    @property
    def name(self) -> str:
        return self._topic

    @property
    def message_count(self) -> int:
        return sum(v.message_count for v in self._views)

    @property
    def message_type(self) -> str:
        # Pick the first view's type. Cross-bag schema drift is a real
        # concern; if message_types differ across bags this returns the
        # first one and the caller is responsible for handling it
        # (use bag-side QC tool from Sub-feature B.3 to detect drift).
        return self._views[0].message_type if self._views else "unknown"

    @property
    def n_bags_with_topic(self) -> int:
        return len(self._views)

    def iter_chunks(self, chunk_size: int = 50_000) -> Iterator[pl.DataFrame]:
        """Yield DataFrames from each bag's view in concatenation order.

        Each yielded DataFrame is a chunk from a single bag — chunks
        don't span bag boundaries. If you want a single contiguous
        DataFrame, call ``to_polars()`` (which collects + concats).
        """
        for view in self._views:
            for chunk in view.iter_chunks(chunk_size=chunk_size):
                yield chunk

    def to_polars(self) -> pl.DataFrame:
        """Concatenate all bags' messages into one DataFrame.

        Memory: O(sum of message counts × per-row size). For very large
        N-bag concatenations, prefer ``iter_chunks()`` and process
        chunk-by-chunk.
        """
        chunks = list(self.iter_chunks())
        if not chunks:
            return pl.DataFrame()
        return pl.concat(chunks, how="diagonal_relaxed")

    def __len__(self) -> int:
        return self.message_count

    def __repr__(self) -> str:
        return (
            f"<ConcatenatedTopicView name='{self._topic}' "
            f"bags={self.n_bags_with_topic} "
            f"messages={self.message_count}>"
        )


class ConcatenatedBagFrame:
    """N bags presented as a single time-aligned stream.

    Built by :func:`concatenate_bags`. Read-only — mutation operations
    (close, scan, etc.) aren't propagated to the underlying bags.

    Surface: ``[topic]`` returns a :class:`ConcatenatedTopicView`,
    ``topics`` is the union of all underlying bags' topics, ``mode``
    is the ordering strategy used at construction.
    """

    def __init__(
        self,
        bag_paths: list[str | Path],
        mode: str = "time",
        bag_ids: list[int] | None = None,
        labels: list[str] | None = None,
    ):
        if not bag_paths:
            raise ValueError("concatenate_bags requires at least one bag")
        if mode not in ("time", "index"):
            raise ValueError(
                f"mode must be 'time' or 'index'; got {mode!r}"
            )
        self._mode = mode
        self._bag_paths = [Path(p) for p in bag_paths]
        self._bag_frames = [BagFrame(p) for p in self._bag_paths]
        self._bag_ids = list(bag_ids) if bag_ids else list(range(len(self._bag_paths)))
        self._labels = (
            list(labels) if labels else [p.stem for p in self._bag_paths]
        )

        # Sort by mode
        if mode == "time":
            order = sorted(
                range(len(self._bag_frames)),
                key=lambda i: self._bag_frames[i].metadata.start_time_ns,
            )
            self._bag_frames = [self._bag_frames[i] for i in order]
            self._bag_paths = [self._bag_paths[i] for i in order]
            self._bag_ids = [self._bag_ids[i] for i in order]
            self._labels = [self._labels[i] for i in order]
        # "index" mode: already in user-supplied order

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def n_bags(self) -> int:
        return len(self._bag_frames)

    @property
    def topics(self) -> list[str]:
        """Union of every underlying bag's topic names, sorted."""
        out: set[str] = set()
        for bf in self._bag_frames:
            for ti in bf.metadata.topics:
                out.add(ti.name)
        return sorted(out)

    @property
    def topic_info(self) -> list[TopicInfo]:
        """Aggregate TopicInfo per topic.

        message_count is the sum across bags. message_type is the
        first bag's type — drift across bags is the caller's problem
        (run the bag-side QC tool to detect it).
        """
        # topic name -> (first message_type, summed message_count, first encoding, first schema)
        agg: dict[str, dict] = {}
        for bf in self._bag_frames:
            for ti in bf.metadata.topics:
                if ti.name not in agg:
                    agg[ti.name] = {
                        "message_type": ti.message_type,
                        "message_count": ti.message_count,
                        "schema_encoding": ti.schema_encoding,
                        "schema_data": ti.schema_data,
                        "frequency_hz": ti.frequency_hz,
                    }
                else:
                    agg[ti.name]["message_count"] += ti.message_count
        return [
            TopicInfo(
                name=name,
                message_type=v["message_type"],
                message_count=v["message_count"],
                schema_encoding=v["schema_encoding"],
                schema_data=v["schema_data"],
                frequency_hz=v["frequency_hz"],
            )
            for name, v in sorted(agg.items())
        ]

    @property
    def bag_paths(self) -> list[Path]:
        return list(self._bag_paths)

    @property
    def labels(self) -> list[str]:
        return list(self._labels)

    @property
    def total_duration_sec(self) -> float:
        """Sum of underlying bag durations (no overlap detection)."""
        return sum(bf.metadata.duration_sec for bf in self._bag_frames)

    def __getitem__(self, topic: str) -> ConcatenatedTopicView:
        if topic not in self.topics:
            raise KeyError(
                f"Topic {topic!r} not found in any of the {self.n_bags} bags"
            )
        return ConcatenatedTopicView(self, topic)

    def __contains__(self, topic: str) -> bool:
        return topic in self.topics

    def iter_messages(self, topic: str) -> Iterator[Message]:
        """Stream every message on a topic from every bag in order."""
        for bf in self._bag_frames:
            if topic in {ti.name for ti in bf.metadata.topics}:
                yield from bf[topic].iter_messages()

    def __repr__(self) -> str:
        return (
            f"<ConcatenatedBagFrame mode='{self._mode}' "
            f"bags={self.n_bags} topics={len(self.topics)} "
            f"total_duration={self.total_duration_sec:.1f}s>"
        )


def concatenate_bags(
    bags: list[str | Path],
    mode: str = "time",
    labels: list[str] | None = None,
) -> ConcatenatedBagFrame:
    """Build a ConcatenatedBagFrame from N bag paths.

    Args:
        bags: List of bag file paths.
        mode: ``"time"`` (sort by start_time_ns) or ``"index"`` (preserve
            user order). Default: ``"time"``.
        labels: Optional human-friendly labels per bag, paired by index.
            Defaults to bag-file stems.

    Returns:
        :class:`ConcatenatedBagFrame` — supports ``[topic]`` →
        :class:`ConcatenatedTopicView`, ``topics``, ``iter_messages``.

    Raises:
        ValueError: If ``bags`` is empty or ``mode`` is invalid.

    Example::

        from resurrector import concatenate_bags

        bf = concatenate_bags(["session1.mcap", "session2.mcap"])
        df = bf["/imu/data"].to_polars()  # rows from both bags
        print(f"{len(df)} total IMU messages across {bf.n_bags} bags")
    """
    return ConcatenatedBagFrame(bag_paths=bags, mode=mode, labels=labels)
