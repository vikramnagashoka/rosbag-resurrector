"""Bridge recorder: write a fresh MCAP file from messages flowing through the bridge.

Hooks into the same per-message callback used to fan out to WebSocket
clients. The recorder lives alongside the WebSocket fanout — write to
disk while relaying live, no extra parser pass over the source bag.

Use cases:
- **Live mode** (primary): bridge subscribes to a real ROS 2 system; the
  recorder captures everything to disk for later analysis. Without this,
  you'd need a separate ros2 bag record process running in parallel.
- **Playback mode** (secondary): mostly redundant since you have the
  source bag, but useful when applying server-side filters or transforms
  (the recorder captures the post-filter stream).

Schema sourcing
---------------
For playback mode, schemas come from the source bag's parser metadata
(``TopicInfo.schema_encoding`` + ``TopicInfo.schema_data``). The recorder
takes the topic list at construction time and registers schemas + channels
lazily on first message per topic.

For live mode (rclpy-based), the live subscriber must populate the same
``TopicInfo`` shape with schema info from rclpy introspection. v0.5.0
ships the recorder layer; live wiring lands as a follow-up.

Clean shutdown
--------------
``close()`` calls ``mcap.writer.Writer.finish()`` to flush index + footer.
If the process dies without close(), the resulting MCAP is recoverable but
won't have a summary index — readers can still iterate messages, but
``get_summary()`` returns ``None``. Always call close() in a try/finally.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import IO

from resurrector.ingest.parser import TopicInfo

logger = logging.getLogger("resurrector.bridge.recorder")


class BridgeRecorder:
    """Append-mode MCAP writer for messages relayed by the bridge.

    Lifecycle:

    1. Construct with the destination path + topic metadata.
    2. Call ``record(topic, timestamp_ns, raw_data)`` per message.
    3. Call ``close()`` when done. Idempotent.

    Construction does NOT open the file — that happens on the first
    ``record()`` call, so a recorder constructed and never used leaves
    no zero-byte file behind.

    Args:
        output_path: Destination ``.mcap`` path. Parent dir is created.
        topic_info: List of :class:`TopicInfo` describing every topic
            that will be recorded. Schema_encoding/schema_data must be
            populated; live-mode wiring is responsible for sourcing
            these from rclpy.
        sequence_per_topic: When True, maintain a per-topic sequence
            counter. False: every message gets sequence=0 (sufficient
            for most readers; PlotJuggler doesn't use sequence at all).
    """

    def __init__(
        self,
        output_path: str | Path,
        topic_info: list[TopicInfo],
        sequence_per_topic: bool = False,
    ):
        self._output_path = Path(output_path)
        self._topic_info_by_name: dict[str, TopicInfo] = {
            t.name: t for t in topic_info
        }
        self._sequence_per_topic = sequence_per_topic
        self._sequences: dict[str, int] = {}
        self._messages_written = 0
        self._closed = False

        self._fp: IO[bytes] | None = None
        self._writer = None  # type: ignore[var-annotated]
        self._schema_id_by_topic: dict[str, int] = {}
        self._channel_id_by_topic: dict[str, int] = {}

    @property
    def output_path(self) -> Path:
        return self._output_path

    @property
    def messages_written(self) -> int:
        return self._messages_written

    def register_topic_info(self, info: TopicInfo) -> None:
        """Add or update a topic's metadata after construction.

        Used by live mode where topics are discovered as messages arrive
        — the LiveSubscriber introspects the rclpy message class and
        calls this so the recorder can register the topic's MCAP schema
        + channel on the next ``record()`` call. Idempotent: re-registering
        the same topic with new info just updates the cached entry.

        If the topic was previously marked as un-recordable (e.g. seen
        without a schema), this clears that mark so it gets re-tried.
        """
        self._topic_info_by_name[info.name] = info
        self._schema_id_by_topic.pop(info.name, None)
        self._channel_id_by_topic.pop(info.name, None)

    def _open(self) -> None:
        """Lazy-init the file + Writer on first record() call."""
        from mcap.writer import Writer

        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = open(self._output_path, "wb")
        self._writer = Writer(self._fp)
        self._writer.start()
        logger.info("BridgeRecorder opened %s", self._output_path)

    def _ensure_topic_registered(self, topic: str) -> tuple[int, int] | None:
        """Register schema + channel for ``topic`` if not already. Returns (schema_id, channel_id) or None if topic isn't known."""
        if topic in self._channel_id_by_topic:
            return self._schema_id_by_topic[topic], self._channel_id_by_topic[topic]

        ti = self._topic_info_by_name.get(topic)
        if ti is None:
            # Unknown topic — happens if the live subscriber discovers a
            # topic mid-stream that wasn't in the construction-time list.
            # Drop the message rather than fabricating a schema; log once
            # per topic.
            logger.warning(
                "BridgeRecorder: dropping message on unknown topic %r "
                "(not in topic_info passed at construction)", topic,
            )
            self._schema_id_by_topic[topic] = -1
            self._channel_id_by_topic[topic] = -1
            return None

        if not ti.schema_encoding or not ti.schema_data:
            logger.warning(
                "BridgeRecorder: dropping message on topic %r — "
                "schema_encoding or schema_data missing on TopicInfo",
                topic,
            )
            self._schema_id_by_topic[topic] = -1
            self._channel_id_by_topic[topic] = -1
            return None

        # Register schema first
        schema_data = ti.schema_data
        if isinstance(schema_data, str):
            schema_data = schema_data.encode("utf-8")
        schema_id = self._writer.register_schema(  # type: ignore[union-attr]
            name=ti.message_type,
            encoding=ti.schema_encoding,
            data=schema_data,
        )
        # Then channel
        channel_id = self._writer.register_channel(  # type: ignore[union-attr]
            topic=topic,
            message_encoding="cdr",  # ROS 2 default; live wiring may override
            schema_id=schema_id,
        )
        self._schema_id_by_topic[topic] = schema_id
        self._channel_id_by_topic[topic] = channel_id
        return schema_id, channel_id

    def record(self, topic: str, timestamp_ns: int, raw_data: bytes) -> None:
        """Append one message to the output MCAP.

        Args:
            topic: Topic name (use the un-namespaced name; if you're
                recording a multi-bag stream, the namespacing convention
                is the caller's choice).
            timestamp_ns: Message timestamp (used as both ``log_time`` and
                ``publish_time``).
            raw_data: Serialized message bytes (CDR-encoded for ROS 2).
                If the bridge layer doesn't have raw bytes available,
                drop the message rather than re-serializing.

        Silently drops messages on topics that weren't supplied at
        construction time, or when ``raw_data`` is empty. Both cases
        log a one-time warning per topic.
        """
        if self._closed:
            raise RuntimeError("BridgeRecorder.record() called after close()")
        if not raw_data:
            return  # Nothing to write
        if self._writer is None:
            self._open()
        ids = self._ensure_topic_registered(topic)
        if ids is None or ids == (-1, -1):
            return
        _, channel_id = ids

        if self._sequence_per_topic:
            seq = self._sequences.get(topic, 0)
            self._sequences[topic] = seq + 1
        else:
            seq = 0

        self._writer.add_message(  # type: ignore[union-attr]
            channel_id=channel_id,
            log_time=timestamp_ns,
            publish_time=timestamp_ns,
            data=raw_data,
            sequence=seq,
        )
        self._messages_written += 1

    def close(self) -> None:
        """Finish + close the writer. Idempotent."""
        if self._closed:
            return
        if self._writer is not None:
            try:
                self._writer.finish()
            except Exception as e:
                logger.error("Error finishing MCAP writer: %s", e)
            self._writer = None
        if self._fp is not None:
            self._fp.close()
            self._fp = None
        self._closed = True
        logger.info(
            "BridgeRecorder closed %s after %d messages",
            self._output_path, self._messages_written,
        )

    def __enter__(self) -> "BridgeRecorder":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self) -> None:
        # Best-effort cleanup if the user forgot close(). The MCAP may
        # still lack a summary index (no finish() call) but at least
        # the file handle gets released.
        if not self._closed and self._fp is not None:
            try:
                self._fp.close()
            except Exception:
                pass
