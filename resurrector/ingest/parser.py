"""Unified parser for MCAP (ROS2) and legacy rosbag (ROS1) formats.

Provides a common interface to read topics, metadata, and messages
from any supported bag format without requiring a ROS installation.
"""

from __future__ import annotations

import logging
import os
import struct
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import numpy as np

logger = logging.getLogger("resurrector.ingest.parser")

from mcap.reader import make_reader

from resurrector.core.exceptions import UndecodableTopic
from resurrector.ingest import custom_decoders, fast_mcap
from resurrector.ingest.custom_decoders import load_decoders  # noqa: F401  (re-exported)
from resurrector.ingest.ros2msg import (
    CDRDecodeError, UnsupportedSchema, compile_decoder, compile_row_decoder,
)


@dataclass
class TopicInfo:
    """Metadata about a topic in a bag file."""
    name: str
    message_type: str
    message_count: int
    frequency_hz: float | None = None
    schema_encoding: str = ""
    schema_data: str = ""


@dataclass
class BagMetadata:
    """Metadata about an entire bag file."""
    path: Path
    format: str  # "mcap", "ros1bag", "ros2db3"
    duration_sec: float
    start_time_ns: int
    end_time_ns: int
    message_count: int
    topics: list[TopicInfo] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def start_time_sec(self) -> float:
        return self.start_time_ns / 1e9

    @property
    def end_time_sec(self) -> float:
        return self.end_time_ns / 1e9


@dataclass
class Message:
    """A single deserialized message from a bag."""
    topic: str
    timestamp_ns: int
    data: dict[str, Any]
    raw_data: bytes | None = None
    sequence: int = 0

    @property
    def timestamp_sec(self) -> float:
        return self.timestamp_ns / 1e9


_METADATA_DECODERS: set = set()  # filled with the built-in metadata-only decoders
_SUMMARIES: "OrderedDict[tuple, Any]" = OrderedDict()
_SUMMARIES_LOCK = threading.Lock()
_SUMMARIES_MAX = 2


def _summary(f, reader):
    """``reader.get_summary()``, cached for the last couple of files.

    Iterating topic by topic opens the file once per topic, and a summary
    with thousands of chunk indexes takes tens of milliseconds to parse
    each time. Keyed on size, mtime and inode, so a rewritten file is
    read again.
    """
    st = os.fstat(f.fileno())
    key = (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
    with _SUMMARIES_LOCK:
        hit = _SUMMARIES.get(key)
        if hit is not None:
            _SUMMARIES.move_to_end(key)
            return hit
    summary = reader.get_summary()
    if summary is not None:
        with _SUMMARIES_LOCK:
            _SUMMARIES[key] = summary
            while len(_SUMMARIES) > _SUMMARIES_MAX:
                _SUMMARIES.popitem(last=False)
    return summary


class MCAPParser:
    """Parser for MCAP format files (ROS2 default)."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"MCAP file not found: {self.path}")
        self._bodies: dict[str, str] | None = None

    def _bag_bodies(self) -> dict[str, str]:
        """``{"pkg/Type": fields}`` from every ``ros2msg`` definition the bag
        embeds: what definitions loaded with :func:`load_decoders` borrow
        the types they use from. Read on first use, only when one is needed."""
        if self._bodies is None:
            with open(self.path, "rb") as f:
                summary = _summary(f, make_reader(f))
            self._bodies = custom_decoders.bodies_from_schemas(
                summary.schemas.values() if summary else ()
            )
        return self._bodies

    def undecodable_topics(self, topics: list[str] | None = None) -> list[UndecodableTopic]:
        """Topics (of ``topics``, default all) whose message type has no
        decoder: no registered one, no usable embedded definition, and
        nothing loaded with :func:`load_decoders`. Reads only the summary."""
        with open(self.path, "rb") as f:
            summary = _summary(f, make_reader(f))
        if summary is None:
            return []
        wanted = None if topics is None else set(topics)
        found: dict[str, UndecodableTopic] = {}
        for channel in summary.channels.values():
            if wanted is not None and channel.topic not in wanted:
                continue
            if channel.topic in found:
                continue
            schema = summary.schemas.get(channel.schema_id) if channel.schema_id else None
            msg_type = schema.name if schema else "unknown"
            _, _, reason = _choose_decoder(msg_type, schema, self._bag_bodies)
            if reason is not None:
                found[channel.topic] = UndecodableTopic(channel.topic, msg_type, reason)
        return [found[t] for t in sorted(found)]

    def get_metadata(self) -> BagMetadata:
        """Read bag metadata without loading messages."""
        with open(self.path, "rb") as f:
            reader = make_reader(f)
            summary = _summary(f, reader)

            if summary is None:
                raise ValueError(f"Could not read summary from {self.path}")

            # Build topic info from channels and statistics
            topics: list[TopicInfo] = []
            schemas_by_id = {s.id: s for s in summary.schemas.values()}

            # Get per-channel message counts from statistics
            channel_msg_counts: dict[int, int] = {}
            if summary.statistics:
                channel_msg_counts = dict(summary.statistics.channel_message_counts)

            start_time = summary.statistics.message_start_time if summary.statistics else 0
            end_time = summary.statistics.message_end_time if summary.statistics else 0
            total_count = summary.statistics.message_count if summary.statistics else 0
            duration_ns = end_time - start_time if end_time > start_time else 0
            duration_sec = duration_ns / 1e9

            for channel in summary.channels.values():
                schema = schemas_by_id.get(channel.schema_id)
                msg_count = channel_msg_counts.get(channel.id, 0)

                freq = None
                if duration_sec > 0 and msg_count > 1:
                    freq = round(msg_count / duration_sec, 2)

                topics.append(TopicInfo(
                    name=channel.topic,
                    message_type=schema.name if schema else "unknown",
                    message_count=msg_count,
                    frequency_hz=freq,
                    schema_encoding=schema.encoding if schema else "",
                    schema_data=schema.data.decode("utf-8", errors="replace") if schema else "",
                ))

            # Sort topics by name
            topics.sort(key=lambda t: t.name)

            # Read MCAP-level metadata
            extra_metadata: dict[str, Any] = {}
            # metadata_indexes contains index records; reading full metadata
            # requires iterating messages, so we skip for summary-only reads

            return BagMetadata(
                path=self.path,
                format="mcap",
                duration_sec=duration_sec,
                start_time_ns=start_time,
                end_time_ns=end_time,
                message_count=total_count,
                topics=topics,
                metadata=extra_metadata,
            )

    def _raw(
        self,
        topics: list[str] | None,
        start_time_ns: int | None,
        end_time_ns: int | None,
        prefix_for: Callable[[Any, Any], int | None] | None = None,
    ) -> Iterator[tuple[Any, Any, int, int, bytes, int, int]]:
        """``(channel, schema, log_time, sequence, data, size, data_offset)``
        per message, in log-time order: the fast indexed reader when the
        file allows it, else the mcap reader. Both yield the same messages
        in the same order.

        ``prefix_for(channel, schema)`` may name how many payload bytes a
        channel's caller needs; the fast reader then may cut ``data`` to
        that (``size`` stays the full length, ``data_offset`` is where the
        payload starts in the file, -1 if it can't be re-read from there).
        """
        if isinstance(topics, str):
            topics = [topics]
        with open(self.path, "rb") as f:
            reader = make_reader(f)
            summary = _summary(f, reader)
            if fast_mcap.can_read(summary):
                schemas = summary.schemas
                info = {
                    cid: (ch, schemas.get(ch.schema_id) if ch.schema_id else None)
                    for cid, ch in summary.channels.items()
                }
                prefix = None
                if prefix_for is not None:
                    prefix = {cid: n for cid, (ch, sc) in info.items()
                              if (n := prefix_for(ch, sc)) is not None}
                for m in fast_mcap.iter_raw(
                    f, summary, _channel_ids(summary, topics), start_time_ns, end_time_ns,
                    prefix or None,
                ):
                    channel, schema = info[m.channel_id]
                    yield channel, schema, m.log_time, m.sequence, m.data, m.size, m.data_offset
                return
            for schema, channel, msg in reader.iter_messages(
                topics=topics, start_time=start_time_ns, end_time=end_time_ns,
            ):
                yield channel, schema, msg.log_time, msg.sequence, msg.data, len(msg.data), -1

    def read_messages(
        self,
        topics: list[str] | None = None,
        start_time_ns: int | None = None,
        end_time_ns: int | None = None,
    ) -> Iterator[Message]:
        """Read messages from the bag, optionally filtered.

        Yields raw Message objects with CDR-encoded data in raw_data
        and a best-effort parsed dict in data, in log-time order.

        Each topic's decoder is picked once: a registered decoder
        (built-in or :func:`register_decoder`) if there is one, else one
        compiled from the definition the bag embeds for that type
        (:mod:`resurrector.ingest.ros2msg`), else one compiled from
        definitions loaded with :func:`load_decoders`. A type with none
        of these comes back as an ``_unparsed`` placeholder, raw bytes
        included; :meth:`undecodable_topics` lists those topics up front.
        """
        warned: set[str] = set()
        failures = _FailureLog()
        decoders: dict[int, tuple[str, DecoderFn | None, str | None]] = {}
        try:
            for channel, schema, log_time, sequence, data, _, _ in self._raw(
                topics, start_time_ns, end_time_ns,
            ):
                entry = decoders.get(channel.id)
                if entry is None:
                    msg_type = schema.name if schema else "unknown"
                    decoder, _, reason = _choose_decoder(msg_type, schema, self._bag_bodies)
                    entry = decoders[channel.id] = (msg_type, decoder, reason)
                msg_type, decoder, reason = entry
                parsed = _decode_with(decoder, msg_type, data, failures)
                if parsed.get("_unparsed") and msg_type not in warned:
                    _warn_unparsed(channel.topic, msg_type, reason)
                    warned.add(msg_type)
                yield Message(topic=channel.topic, timestamp_ns=log_time, data=parsed,
                              raw_data=data, sequence=sequence)
        finally:
            failures.close()

    def iter_raw(
        self,
        topics: list[str] | None = None,
        start_time_ns: int | None = None,
        end_time_ns: int | None = None,
    ) -> Iterator[tuple[str, int, bytes]]:
        """Yield ``(topic, log_time_ns, cdr_bytes)`` without decoding anything.

        Same messages and order as :meth:`read_messages`, for callers that
        only need timestamps and sizes (health checks, density).
        """
        for channel, _schema, log_time, _seq, data, _, _ in self._raw(
            topics, start_time_ns, end_time_ns,
        ):
            yield channel.topic, log_time, data

    def iter_sizes(
        self,
        topics: list[str] | None = None,
        start_time_ns: int | None = None,
        end_time_ns: int | None = None,
    ) -> Iterator[tuple[str, int, int]]:
        """Yield ``(topic, log_time_ns, payload_size)``, reading as little as possible.

        For health checks and density, which need nothing else: in an
        indexed, uncompressed file only each message's record header is
        read, so a bag of camera frames costs a fraction of its size.
        """
        for channel, _schema, log_time, _seq, _data, size, _ in self._raw(
            topics, start_time_ns, end_time_ns, prefix_for=lambda ch, sc: 0,
        ):
            yield channel.topic, log_time, size

    def iter_rows(
        self,
        topics: list[str] | None = None,
        start_time_ns: int | None = None,
        end_time_ns: int | None = None,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """Yield ``(topic, row)``: the flat row ``iter_chunks`` builds a frame from.

        ``row`` is ``{"timestamp_ns": t}`` filled by :func:`flatten_into`
        from :meth:`read_messages`' dict. For a type decoded from its
        embedded definition the row is generated directly, without the
        dict, which is most of what makes ``iter_chunks`` fast.
        """
        warned: set[str] = set()
        failures = _FailureLog()
        makers: dict[int, Callable[[bytes, int], dict]] = {}
        fd = -1
        try:
            for channel, schema, log_time, _seq, data, size, offset in self._raw(
                topics, start_time_ns, end_time_ns, prefix_for=_row_prefix,
            ):
                make = makers.get(channel.id)
                if make is None:
                    make = makers[channel.id] = _row_maker(
                        channel.topic, schema, warned, failures, self._bag_bodies,
                    )
                row = make(data, log_time)
                if len(row) == 1 and size > len(data) and offset >= 0:
                    # The metadata didn't fit in the prefix: decode it all.
                    if fd < 0:
                        fd = os.open(self.path, os.O_RDONLY)
                    row = make(os.pread(fd, size, offset), log_time)
                yield channel.topic, row
        finally:
            failures.close()
            if fd >= 0:
                os.close(fd)


class CDRParseError(ValueError):
    """Raised when a CDR-encoded message is truncated or malformed.

    Indicates the message's declared field counts or string lengths exceed
    the actual buffer size — typically from a corrupted recording or
    a mismatched schema.
    """

    def __init__(self, msg_type: str, offset: int, needed: int, available: int, detail: str = ""):
        self.msg_type = msg_type
        self.offset = offset
        self.needed = needed
        self.available = available
        super().__init__(
            f"CDR parse failed for {msg_type} at offset {offset}: "
            f"needed {needed} bytes, buffer has {available}"
            f"{' — ' + detail if detail else ''}"
        )


def _safe_unpack(fmt: str, buf: bytes, offset: int, msg_type: str = "?") -> tuple:
    """struct.unpack_from with explicit bounds checking.

    Raises CDRParseError with actionable context instead of letting
    struct.error propagate with opaque positional details.

    A zero-byte read (e.g. ``<0d`` for an empty array) is always allowed
    even if offset is at end of buffer, matching struct.unpack_from's
    behavior so that empty trailing arrays don't trip the bounds check.
    """
    needed = struct.calcsize(fmt)
    if needed == 0:
        return ()
    if offset + needed > len(buf):
        raise CDRParseError(msg_type, offset, needed, len(buf) - offset)
    return struct.unpack_from(fmt, buf, offset)


def _safe_read_string(buf: bytes, offset: int, msg_type: str = "?") -> tuple[str, int]:
    """Read a length-prefixed CDR string with bounds validation.

    Returns (string, new_offset). Raises CDRParseError if the declared
    string length would read past the buffer.
    """
    (str_len,) = _safe_unpack("<I", buf, offset, msg_type)
    offset += 4
    if offset + str_len > len(buf):
        raise CDRParseError(
            msg_type, offset, str_len, len(buf) - offset,
            detail=f"string length {str_len} exceeds buffer",
        )
    s = buf[offset:offset + str_len].decode("utf-8", errors="replace").rstrip("\x00")
    offset += str_len
    # Align to 4 bytes
    offset = (offset + 3) & ~3
    return s, offset


# ---------------------------------------------------------------------------
# Pluggable decoder registry (v0.6.0 — Sub-feature B.1)
#
# Decoder functions take the full CDR-encoded message bytes (including
# the 4-byte encapsulation header) and return a dict that becomes
# Message.data. Raising any exception indicates parse failure — the
# parser catches it and returns a {_parse_error: True, ...} placeholder
# so one bad message doesn't abort an entire bag scan.
# ---------------------------------------------------------------------------

from typing import Callable

DecoderFn = Callable[[bytes], dict[str, Any]]

# msg_type → decoder function. Mutated at import time below to seed the
# built-in decoders, and at runtime by register_decoder().
_DECODER_REGISTRY: dict[str, DecoderFn] = {}


def register_decoder(msg_type: str, decoder: DecoderFn) -> None:
    """Register a custom CDR decoder for a ROS 2 message type.

    Types the bag embeds a ``ros2msg`` definition for decode without
    this, and :func:`load_decoders` covers types with ``.msg`` files.
    Register a decoder for a type neither can describe (otherwise APIs
    that read fields raise :class:`UndecodableTopicError` for it), or to
    override the built-in or embedded decoder.

    Args:
        msg_type: Fully-qualified type string, e.g.
            ``"my_pkg/msg/CustomThing"``. Must exactly match what the
            bag's schema reports.
        decoder: Function ``(raw_data: bytes) -> dict``. Receives the
            full CDR-encoded message bytes including the 4-byte CDR
            encapsulation header. Should return a flat or nested dict;
            nested dicts get dot-flattened by ``BagFrame.iter_chunks()``.
            Raise any exception to indicate parse failure — the parser
            catches it and surfaces a ``_parse_error`` placeholder.

    Re-registration is allowed and replaces the existing decoder.

    Example::

        from resurrector.ingest.parser import register_decoder
        import struct

        def decode_my_thing(raw):
            buf = raw[4:]  # skip CDR header
            x, y, z = struct.unpack_from('<3d', buf, 0)
            return {'x': x, 'y': y, 'z': z}

        register_decoder('my_pkg/msg/CustomThing', decode_my_thing)
    """
    if not callable(decoder):
        raise TypeError(f"decoder for {msg_type!r} must be callable")
    if not msg_type:
        raise ValueError("msg_type cannot be empty")
    _DECODER_REGISTRY[msg_type] = decoder


def unregister_decoder(msg_type: str) -> bool:
    """Remove a registered decoder. Returns True if one was present.

    Useful for tests or hot-reload scenarios. Removing a built-in
    decoder is allowed: that type then decodes from the definition the
    bag embeds, or comes back as ``_unparsed`` placeholders if it has
    none, until another decoder is registered.
    """
    return _DECODER_REGISTRY.pop(msg_type, None) is not None


def list_decoders() -> list[str]:
    """Return all message types with a registered decoder, sorted."""
    return sorted(_DECODER_REGISTRY.keys())


def _parse_cdr_message(msg_type: str, data: bytes) -> dict[str, Any]:
    """Dispatch CDR decoding via the decoder registry.

    Built-in decoders (Imu, JointState, Image, LaserScan,
    CompressedImage, TFMessage, PointCloud2, Marker, MarkerArray) are
    registered at module import. Users add their own via
    :func:`register_decoder`. On malformed input, a CDRParseError is
    caught here and surfaced in the returned dict rather than
    propagating up — so one bad message doesn't abort a whole bag scan.
    """
    return _decode_with(_DECODER_REGISTRY.get(msg_type), msg_type, data)


def _channel_ids(summary, topics: list[str] | None) -> list[int] | None:
    """Channel ids carrying ``topics`` (None means every channel)."""
    if topics is None:
        return None
    wanted = set(topics)
    return [cid for cid, ch in summary.channels.items() if ch.topic in wanted]


def _no_bodies() -> dict[str, str]:
    return {}


def _choose_decoder(
    msg_type: str, schema, bag_bodies: Callable[[], dict[str, str]] = _no_bodies,
) -> tuple[DecoderFn | None, str | bytes | None, str | None]:
    """``(decoder, definition, reason)`` for one channel.

    In order: the registered decoder for ``msg_type``; one compiled from
    the definition the bag embeds (``schema``); one compiled from
    definitions loaded with :func:`load_decoders`, with types it uses
    filled in from ``bag_bodies()``. ``definition`` is the text a
    compiled decoder came from (None for a registered one). With no
    decoder, ``reason`` says why, for :class:`UndecodableTopicError`.
    """
    decoder = _DECODER_REGISTRY.get(msg_type)
    if decoder is not None:
        return _layout_adapter(msg_type, decoder, schema) or decoder, None, None
    embedded_error = None
    if schema is not None and schema.encoding == "ros2msg" and schema.data:
        try:
            return compile_decoder(msg_type, schema.data), schema.data, None
        except UnsupportedSchema as exc:
            embedded_error = exc
    loaded = custom_decoders.definition_for(msg_type, bag_bodies)
    if loaded is not None:
        try:
            return compile_decoder(msg_type, loaded), loaded, None
        except UnsupportedSchema as exc:
            return None, None, f"its loaded definition can't be decoded ({exc})"
    if embedded_error is not None:
        reason = f"its definition can't be decoded ({embedded_error})"
    elif schema is None or not schema.data:
        reason = "the bag has no definition of it"
    else:
        reason = f"the bag has only a {schema.encoding} definition of it"
    return None, None, reason


def _decoder_for(msg_type: str, schema) -> DecoderFn | None:
    """The decoder :func:`_choose_decoder` picks, without the bag's other
    definitions to fill in from."""
    return _choose_decoder(msg_type, schema)[0]


def _layout_adapter(msg_type: str, decoder: DecoderFn, schema) -> DecoderFn | None:
    """A schema-based stand-in for a built-in decoder whose fixed field
    order doesn't match the bag's definition, or None if it matches.

    The Marker parser reads ROS 2 Humble's field order; Iron and later add
    fields mid-message, which it would misread. With the bag's definition
    at hand, decode from that and convert to the same output instead.
    A user-registered decoder is never replaced.
    """
    if decoder not in (_parse_marker, _parse_marker_array):
        return None
    if schema is None or schema.encoding != "ros2msg" or not schema.data:
        return None
    from resurrector.core.scene import HUMBLE_MARKER_FIELDS, marker_from_fields
    from resurrector.ingest.ros2msg import parse_definitions

    try:
        defs = parse_definitions(msg_type, schema.data.decode("utf-8", "replace"))
        fields = tuple(f.name for f in defs.get("visualization_msgs/Marker", ()))
        if fields == HUMBLE_MARKER_FIELDS:
            return None
        generic = compile_decoder(msg_type, schema.data)
    except UnsupportedSchema:
        return None

    if decoder is _parse_marker:
        return lambda data: {"marker": marker_from_fields(generic(data)).to_dict()}

    def decode_array(data: bytes) -> dict[str, Any]:
        markers = [marker_from_fields(m).to_dict() for m in generic(data)["markers"]]
        return {"markers": markers, "_count": len(markers)}
    return decode_array


def _decode_with(
    decoder: DecoderFn | None, msg_type: str, data: bytes, failures: "_FailureLog | None" = None,
) -> dict[str, Any]:
    """Decode one message, turning failures into placeholders (see above)."""
    if len(data) < 4:
        return {}

    if decoder is None:
        logger.debug("No CDR parser for message type '%s' (%d bytes)", msg_type, len(data))
        return {"_unparsed": True, "_msg_type": msg_type, "_raw_size": len(data)}

    try:
        return decoder(data)
    except Exception as exc:
        return _decode_failure(msg_type, data, exc, failures)


def _decode_failure(
    msg_type: str, data: bytes, exc: Exception, failures: "_FailureLog | None" = None,
) -> dict[str, Any]:
    """Log a message that didn't decode; return its ``_parse_error`` placeholder."""
    if failures is None or failures.first(msg_type):
        logger.warning("CDR parse error for '%s' (%d bytes): %s", msg_type, len(data), exc)
    if isinstance(exc, (CDRParseError, CDRDecodeError)):
        return {"_parse_error": True, "_msg_type": msg_type, "_raw_size": len(data),
                "_error": str(exc)}
    return {"_parse_error": True, "_msg_type": msg_type, "_raw_size": len(data)}


class _FailureLog:
    """Messages that failed to decode during one read.

    The first failure of each type is logged with its error; the rest are
    counted and logged as one line when the read ends, so a topic whose
    every message fails leaves two lines in the log, not one per message.
    """

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def first(self, msg_type: str) -> bool:
        n = self.counts.get(msg_type, 0)
        self.counts[msg_type] = n + 1
        return n == 0

    def close(self) -> None:
        for msg_type, n in self.counts.items():
            if n > 1:
                logger.warning(
                    "%d messages of type '%s' failed to decode (the first error is above)",
                    n, msg_type,
                )
        self.counts.clear()


def _warn_unparsed(topic: str, msg_type: str, reason: str | None = None) -> None:
    logger.warning(
        "Topic '%s': can't decode %s%s, so its messages carry raw bytes only. "
        "Load its definitions with resurrector.load_decoders(path).",
        topic, msg_type, f" ({reason})" if reason else "",
    )


# Built-in decoders that read only a message's leading metadata (the pixels
# and points stay in raw_data), so a row never needs more than this much.
_METADATA_ONLY = {
    "sensor_msgs/msg/Image", "sensor_msgs/msg/CompressedImage", "sensor_msgs/msg/PointCloud2",
}
_METADATA_PREFIX = 65536


def _row_prefix(channel, schema) -> int | None:
    """How much of a channel's payload :meth:`MCAPParser.iter_rows` needs."""
    msg_type = schema.name if schema else ""
    if msg_type in _METADATA_ONLY and _DECODER_REGISTRY.get(msg_type) in _METADATA_DECODERS:
        return _METADATA_PREFIX
    return None


def _row_maker(
    topic: str, schema, warned: set[str], failures: _FailureLog,
    bag_bodies: Callable[[], dict[str, str]] = _no_bodies,
) -> Callable[[bytes, int], dict]:
    """``make(data, ts) -> row`` for one channel, as :meth:`MCAPParser.iter_rows` uses it.

    A registered decoder's dict is flattened. A type decoded from a
    definition (embedded, or loaded with :func:`load_decoders`) gets the
    generated row function, which builds the same row without the dict.
    Placeholders (undecodable, or a message that fails) flatten to the
    timestamp alone.
    """
    msg_type = schema.name if schema else "unknown"
    # The same choice read_messages makes.
    decoder, definition, reason = _choose_decoder(msg_type, schema, bag_bodies)
    row_fn = None
    if decoder is not None and definition is not None:
        row_fn = compile_row_decoder(msg_type, definition)

    if row_fn is not None:
        def make(data: bytes, ts: int) -> dict:
            if len(data) < 4:
                return {"timestamp_ns": ts}
            try:
                return row_fn(data, ts)
            except Exception as exc:
                _decode_failure(msg_type, data, exc, failures)
                return {"timestamp_ns": ts}
        return make

    flat: list = [None]  # compiled flattener, made from the first decoded message

    def make(data: bytes, ts: int) -> dict:
        parsed = _decode_with(decoder, msg_type, data, failures)
        if parsed.get("_unparsed") and msg_type not in warned:
            _warn_unparsed(topic, msg_type, reason)
            warned.add(msg_type)
        row = {"timestamp_ns": ts}
        fn = flat[0]
        if fn is None and parsed and "_parse_error" not in parsed and "_unparsed" not in parsed:
            fn = flat[0] = _compile_flattener(parsed)
        if fn is None or not fn(parsed, row):
            row = {"timestamp_ns": ts}
            flatten_into(parsed, row)
        return row
    return make


def _compile_flattener(sample: dict) -> Callable[[dict, dict], bool]:
    """A function filling ``row`` exactly as :func:`flatten_into` would, for
    dicts shaped like ``sample`` (same keys, nested the same way).

    A decoder returns the same shape for every valid message of a type, so
    the dict walk, the type checks and the key strings can be fixed once.
    The function checks the shape as it goes and returns False at the
    first difference (an error placeholder, a longer list than 20, a
    non-number in a list), and the caller falls back to ``flatten_into``.
    """
    lines = ["def flat(d, row):"]
    env: dict[str, Any] = {"_num": (int, float)}
    counter = [0]

    def emit(node: dict, var: str, prefix: str, depth: int) -> None:
        pad = "    "
        lines.append(f"{pad}if type({var}) is not dict or len({var}) != {len(node)}: return False")
        for key, value in node.items():
            full = f"{prefix}.{key}" if prefix else key
            if key.startswith("_"):
                # Not in rows, but its presence still has to match.
                lines.append(f"{pad}if {key!r} not in {var}: return False")
                continue
            counter[0] += 1
            v = f"x{counter[0]}"
            lines.append(f"{pad}{v} = {var}[{key!r}]")
            cls = type(value)
            if cls is dict:
                emit(value, v, full, depth + 1)
            elif cls is list:
                keys = f"K{counter[0]}"
                env[keys] = tuple(f"{full}.{i}" for i in range(20))
                lines.extend([
                    f"{pad}if type({v}) is not list: return False",
                    f"{pad}if len({v}) <= 20:",
                    f"{pad}    for e in {v}:",
                    f"{pad}        if not isinstance(e, _num): break",
                    f"{pad}    else:",
                    f"{pad}        for i, e in enumerate({v}): row[{keys}[i]] = e",
                ])
            elif isinstance(value, (np.ndarray, bytes, bytearray, memoryview)):
                lines.append(f"{pad}if not isinstance({v}, _skip): return False")
                env["_skip"] = (np.ndarray, bytes, bytearray, memoryview)
            else:
                lines.append(f"{pad}if type({v}) in _special: return False")
                lines.append(f"{pad}row[{full!r}] = {v}")
                env["_special"] = (dict, list, np.ndarray, bytes, bytearray, memoryview)

    emit(sample, "d", "", 0)
    lines.append("    return True")
    exec(compile("\n".join(lines) + "\n", "<flattener>", "exec"), env)
    return env["flat"]


def flatten_into(d: dict, out: dict, prefix: str = "") -> None:
    """Flatten a nested dict into ``out`` with dot notation, skipping internal keys.

    The rules every DataFrame column comes from: keys starting with ``_``
    are dropped; nested dicts become ``parent.child``; numeric lists of up
    to 20 values become ``field.0``, ``field.1`` ...; longer lists, lists
    of anything else, NumPy arrays and byte blobs are left out (they stay
    available from ``iter_messages()``).
    """
    for key, value in d.items():
        if key.startswith("_"):
            continue
        full_key = f"{prefix}.{key}" if prefix else key
        cls = type(value)
        if cls is dict:
            flatten_into(value, out, full_key)
        elif cls is list:
            if len(value) <= 20 and all(isinstance(v, (int, float)) for v in value):
                for i, v in enumerate(value):
                    out[f"{full_key}.{i}"] = v
        elif isinstance(value, dict):  # dict subclasses
            flatten_into(value, out, full_key)
        elif isinstance(value, list):
            if len(value) <= 20 and all(isinstance(v, (int, float)) for v in value):
                for i, v in enumerate(value):
                    out[f"{full_key}.{i}"] = v
        elif isinstance(value, (np.ndarray, bytes, bytearray, memoryview)):
            continue
        else:
            out[full_key] = value


# Built-in decoders — wrap the existing helpers (some take CDR-stripped
# buffers, some take the full data including header) into a uniform
# (raw_data: bytes) → dict signature.

def _decode_imu(data: bytes) -> dict[str, Any]:
    return _parse_imu(data[4:])


def _decode_joint_state(data: bytes) -> dict[str, Any]:
    return _parse_joint_state(data[4:])


def _decode_image(data: bytes) -> dict[str, Any]:
    return _parse_image(data[4:])


def _decode_laser_scan(data: bytes) -> dict[str, Any]:
    return _parse_laser_scan(data[4:])


def _decode_compressed_image(data: bytes) -> dict[str, Any]:
    return _parse_compressed_image(data[4:])


# These ones already accept the full data including CDR header
def _register_builtin_decoders() -> None:
    """Seed the registry with the built-in CDR decoders. Idempotent."""
    register_decoder("sensor_msgs/msg/Imu", _decode_imu)
    register_decoder("sensor_msgs/msg/JointState", _decode_joint_state)
    register_decoder("sensor_msgs/msg/Image", _decode_image)
    register_decoder("sensor_msgs/msg/LaserScan", _decode_laser_scan)
    register_decoder("sensor_msgs/msg/CompressedImage", _decode_compressed_image)
    register_decoder("tf2_msgs/msg/TFMessage", _parse_tf_message)
    register_decoder("sensor_msgs/msg/PointCloud2", _parse_pointcloud2)
    register_decoder("visualization_msgs/msg/Marker", _parse_marker)
    register_decoder("visualization_msgs/msg/MarkerArray", _parse_marker_array)
    _METADATA_DECODERS.update((_decode_image, _decode_compressed_image, _parse_pointcloud2))


def _read_string_unpadded(buf: bytes, offset: int, msg_type: str = "?") -> tuple[str, int]:
    """Like :func:`_safe_read_string`, but returns the offset right after the
    string's bytes. CDR pads only to align the *next* field, so a 1-byte
    field after a string starts immediately."""
    (str_len,) = _safe_unpack("<I", buf, offset, msg_type)
    offset += 4
    if offset + str_len > len(buf):
        raise CDRParseError(
            msg_type, offset, str_len, len(buf) - offset,
            detail=f"string length {str_len} exceeds buffer",
        )
    s = buf[offset:offset + str_len].decode("utf-8", errors="replace").rstrip("\x00")
    return s, offset + str_len


def _read_header(buf: bytes, offset: int, msg_type: str = "?") -> tuple[int, int, str, int]:
    """Read a std_msgs/Header from CDR buffer.

    Returns (sec, nsec, frame_id, new_offset). Bounds-checked.
    """
    sec, nsec = _safe_unpack("<II", buf, offset, msg_type)
    offset += 8
    frame_id, offset = _safe_read_string(buf, offset, msg_type)
    return sec, nsec, frame_id, offset


def _parse_imu(buf: bytes) -> dict[str, Any]:
    """Parse sensor_msgs/Imu from CDR buffer."""
    mt = "sensor_msgs/msg/Imu"
    sec, nsec, frame_id, off = _read_header(buf, 0, mt)
    # orientation: 4 x float64, aligned to 8 (frame_id can end on any
    # 4-byte boundary, so this padding is 0 or 4 bytes)
    off = (off + 7) & ~7
    qx, qy, qz, qw = _safe_unpack("<4d", buf, off, mt)
    off += 32
    # orientation_covariance: 9 x float64
    off += 72
    if off > len(buf):
        raise CDRParseError(mt, off, 0, 0, detail="truncated before angular_velocity")
    # angular_velocity: 3 x float64
    gx, gy, gz = _safe_unpack("<3d", buf, off, mt)
    off += 24
    # angular_velocity_covariance: 9 x float64
    off += 72
    if off > len(buf):
        raise CDRParseError(mt, off, 0, 0, detail="truncated before linear_acceleration")
    # linear_acceleration: 3 x float64
    ax, ay, az = _safe_unpack("<3d", buf, off, mt)

    return {
        "header": {"stamp_sec": sec, "stamp_nsec": nsec, "frame_id": frame_id},
        "orientation": {"x": qx, "y": qy, "z": qz, "w": qw},
        "angular_velocity": {"x": gx, "y": gy, "z": gz},
        "linear_acceleration": {"x": ax, "y": ay, "z": az},
    }


# Upper bounds to guard against malicious / corrupted n-prefixes before
# we try to allocate or read. 10M fields/names is far larger than any
# real ROS message; past this we reject rather than attempt to parse.
_MAX_ARRAY_LEN = 10_000_000


_U32 = struct.Struct("<I")
_U32X3 = struct.Struct("<III")


def _parse_joint_state(buf: bytes) -> dict[str, Any]:
    """Parse sensor_msgs/JointState from CDR buffer (encapsulation header stripped).

    A float64 array is padded to 8 bytes only when it has elements, as
    Fast-CDR writes it. Many drivers publish positions only, so an empty
    ``velocity`` or ``effort`` is common; padding it anyway (as this
    decoder once did) misreads every such message.
    """
    mt = "sensor_msgs/msg/JointState"
    end = len(buf)
    if end < 12:
        raise CDRParseError(mt, 0, 12, end)
    sec, nsec, n = _U32X3.unpack_from(buf, 0)
    off = 12
    if off + n > end:
        raise CDRParseError(mt, off, n, end - off, detail=f"string length {n} exceeds buffer")
    frame_id = buf[off:off + n].decode("utf-8", errors="replace").rstrip("\x00")
    off = (off + n + 3) & ~3

    if off + 4 > end:
        raise CDRParseError(mt, off, 4, end - off)
    (n_names,) = _U32.unpack_from(buf, off)
    off += 4
    if n_names > _MAX_ARRAY_LEN:
        raise CDRParseError(mt, off - 4, 0, 0, detail=f"n_names={n_names} exceeds max")
    names: list[str] = []
    for _ in range(n_names):
        if off + 4 > end:
            raise CDRParseError(mt, off, 4, end - off)
        (n,) = _U32.unpack_from(buf, off)
        off += 4
        if off + n > end:
            raise CDRParseError(mt, off, n, end - off, detail=f"string length {n} exceeds buffer")
        names.append(buf[off:off + n].decode("utf-8", errors="replace").rstrip("\x00"))
        off = (off + n + 3) & ~3

    arrays: list[list[float]] = []
    for _ in range(3):  # position, velocity, effort
        if off + 4 > end:
            raise CDRParseError(mt, off, 4, end - off)
        (n,) = _U32.unpack_from(buf, off)
        off += 4
        if n > _MAX_ARRAY_LEN:
            raise CDRParseError(mt, off - 4, 0, 0, detail=f"array length {n} exceeds max")
        if not n:
            arrays.append([])
            continue
        off = (off + 7) & ~7
        if off + 8 * n > end:
            raise CDRParseError(mt, off, 8 * n, end - off)
        arrays.append(list(struct.unpack_from(f"<{n}d", buf, off)))
        off += 8 * n

    return {
        "header": {"stamp_sec": sec, "stamp_nsec": nsec, "frame_id": frame_id},
        "name": names,
        "position": arrays[0],
        "velocity": arrays[1],
        "effort": arrays[2],
    }


def _parse_image(buf: bytes) -> dict[str, Any]:
    """Parse sensor_msgs/Image from CDR buffer (metadata only, not pixel data)."""
    mt = "sensor_msgs/msg/Image"
    sec, nsec, frame_id, off = _read_header(buf, 0, mt)
    height, width = _safe_unpack("<II", buf, off, mt)
    off += 8
    # is_bigendian is a uint8, so it sits right after the encoding string's
    # bytes; padding comes only before step (uint32). Reading it at the
    # next 4-byte boundary (as this once did) shifted step,
    # data_length and the pixel offset by up to 3 bytes on real bags.
    encoding, off = _read_string_unpadded(buf, off, mt)
    (is_bigendian,) = _safe_unpack("<B", buf, off, mt)
    off += 1
    off = (off + 3) & ~3
    (step,) = _safe_unpack("<I", buf, off, mt)
    off += 4
    (data_len,) = _safe_unpack("<I", buf, off, mt)
    off += 4
    # Don't store pixel data in the dict — too large
    return {
        "header": {"stamp_sec": sec, "stamp_nsec": nsec, "frame_id": frame_id},
        "height": height,
        "width": width,
        "encoding": encoding,
        "is_bigendian": bool(is_bigendian),
        "step": step,
        "data_length": data_len,
        "_pixel_data_offset": off,  # Offset into raw CDR data where pixels start
    }


def _parse_laser_scan(buf: bytes) -> dict[str, Any]:
    """Parse sensor_msgs/LaserScan from CDR buffer."""
    mt = "sensor_msgs/msg/LaserScan"
    sec, nsec, frame_id, off = _read_header(buf, 0, mt)
    angle_min, angle_max, angle_inc, time_inc, scan_time = _safe_unpack("<5f", buf, off, mt)
    off += 20
    range_min, range_max = _safe_unpack("<2f", buf, off, mt)
    off += 8
    (n_ranges,) = _safe_unpack("<I", buf, off, mt)
    off += 4
    if n_ranges > _MAX_ARRAY_LEN:
        raise CDRParseError(mt, off - 4, 0, 0, detail=f"n_ranges={n_ranges} exceeds max")
    ranges = list(_safe_unpack(f"<{n_ranges}f", buf, off, mt)) if n_ranges > 0 else []
    off += n_ranges * 4
    (n_intensities,) = _safe_unpack("<I", buf, off, mt)
    off += 4
    if n_intensities > _MAX_ARRAY_LEN:
        raise CDRParseError(mt, off - 4, 0, 0, detail=f"n_intensities={n_intensities} exceeds max")
    intensities = list(_safe_unpack(f"<{n_intensities}f", buf, off, mt)) if n_intensities > 0 else []

    return {
        "header": {"stamp_sec": sec, "stamp_nsec": nsec, "frame_id": frame_id},
        "angle_min": angle_min,
        "angle_max": angle_max,
        "angle_increment": angle_inc,
        "time_increment": time_inc,
        "scan_time": scan_time,
        "range_min": range_min,
        "range_max": range_max,
        "ranges": ranges,
        "intensities": intensities,
    }


# sensor_msgs/Image encodings get_image_array decodes; it returns None for
# any other (mono16, 16UC1, 32FC1, rgb16, bayer_*, yuv422, ...).
RAW_IMAGE_ENCODINGS = ("rgb8", "bgr8", "rgba8", "bgra8", "mono8")


def get_image_array(msg: Message) -> np.ndarray | None:
    """Extract image data from a parsed Image message as a numpy array.

    None when the encoding isn't one of :data:`RAW_IMAGE_ENCODINGS` or the
    pixel data is shorter than height x width x channels.
    """
    if msg.raw_data is None or "height" not in msg.data:
        return None
    offset = msg.data.get("_pixel_data_offset")
    if offset is None:
        return None
    height = msg.data["height"]
    width = msg.data["width"]
    encoding = msg.data["encoding"]

    raw = msg.raw_data[4:]  # Skip CDR header
    pixel_data = raw[offset:]

    if encoding in ("rgb8", "bgr8"):
        channels = 3
    elif encoding in ("rgba8", "bgra8"):
        channels = 4
    elif encoding == "mono8":
        channels = 1
    else:
        return None

    expected_size = height * width * channels
    if len(pixel_data) < expected_size:
        return None

    arr = np.frombuffer(pixel_data[:expected_size], dtype=np.uint8)
    if channels == 1:
        return arr.reshape(height, width)
    return arr.reshape(height, width, channels)


def _parse_compressed_image(buf: bytes) -> dict[str, Any]:
    """Parse sensor_msgs/CompressedImage from CDR buffer.

    CDR layout: Header header + string format + uint8[] data
    """
    mt = "sensor_msgs/msg/CompressedImage"
    sec, nsec, frame_id, off = _read_header(buf, 0, mt)
    fmt, off = _safe_read_string(buf, off, mt)
    # compressed data: uint8[] (length-prefixed)
    (data_len,) = _safe_unpack("<I", buf, off, mt)
    off += 4
    return {
        "header": {"stamp_sec": sec, "stamp_nsec": nsec, "frame_id": frame_id},
        "format": fmt,
        "data_length": data_len,
        "_compressed_data_offset": off,
    }


def get_compressed_image_array(msg: Message) -> np.ndarray | None:
    """Decode a CompressedImage message (JPEG/PNG) to a numpy array.

    Uses Pillow, a base dependency.
    """
    if msg.raw_data is None or "format" not in msg.data:
        return None
    offset = msg.data.get("_compressed_data_offset")
    if offset is None:
        return None

    try:
        from PIL import Image as PILImage
    except ImportError as e:
        # Only reachable on a partial install (e.g. pip --no-deps), so
        # point at the missing package, not an extra.
        raise ImportError(
            "CompressedImage decoding requires Pillow, which "
            "rosbag-resurrector depends on but this environment lacks. "
            "Install with: pip install Pillow"
        ) from e

    import io

    data_len = msg.data["data_length"]
    raw = msg.raw_data[4:]  # Skip CDR encapsulation header
    compressed_bytes = raw[offset:offset + data_len]

    if not compressed_bytes:
        return None

    try:
        img = PILImage.open(io.BytesIO(compressed_bytes))
        return np.array(img)
    except Exception:
        logger.warning("Failed to decode CompressedImage (%s, %d bytes)", msg.data["format"], data_len)
        return None


def _parse_tf_message(raw_data: bytes) -> dict[str, Any]:
    """Parse a tf2_msgs/TFMessage CDR payload into the message dict.

    Stores the decoded TransformStamped list under ``transforms``.
    Each entry has parent_frame, child_frame, translation (xyz),
    rotation (xyzw), timestamp_ns. The full ``raw_data`` is also kept
    on the Message so the dashboard's TFTree builder can re-decode if
    it needs the static-vs-dynamic distinction (which depends on
    topic name, not payload).
    """
    from resurrector.core.scene import parse_tf_message

    transforms = parse_tf_message(raw_data, is_static=False)
    return {
        "transforms": [t.to_dict() for t in transforms],
        "_count": len(transforms),
    }


def _parse_marker(raw_data: bytes) -> dict[str, Any]:
    """Parse a single visualization_msgs/Marker into the message dict."""
    from resurrector.core.scene import parse_marker
    m = parse_marker(raw_data)
    if m is None:
        return {"_parse_error": True, "_msg_type": "visualization_msgs/msg/Marker"}
    return {"marker": m.to_dict()}


def _parse_marker_array(raw_data: bytes) -> dict[str, Any]:
    """Parse a visualization_msgs/MarkerArray into the message dict."""
    from resurrector.core.scene import parse_marker_array
    markers = parse_marker_array(raw_data)
    return {"markers": [m.to_dict() for m in markers], "_count": len(markers)}


def _parse_pointcloud2(raw_data: bytes) -> dict[str, Any]:
    """Parse PointCloud2 CDR header (metadata only — points stay in raw_data).

    The frontend / API layer pulls the actual points via
    ``decode_pointcloud2_xyz(msg.raw_data, meta)`` to avoid copying
    the (potentially large) payload into the message dict during the
    bag scan.
    """
    from resurrector.core.scene import parse_pointcloud2_meta

    meta = parse_pointcloud2_meta(raw_data)
    if meta is None:
        return {"_parse_error": True, "_msg_type": "sensor_msgs/msg/PointCloud2"}
    return {
        "header": {"frame_id": meta.frame_id},
        "height": meta.height,
        "width": meta.width,
        "point_step": meta.point_step,
        "row_step": meta.row_step,
        "is_bigendian": meta.is_bigendian,
        "fields": meta.fields,
        "_pointcloud_data_offset": meta.data_offset,
        "_pointcloud_data_length": meta.data_length,
    }


def resolve_bag_path(path: str | Path) -> Path:
    """The file to open for ``path``.

    A rosbag2 directory recorded with MCAP storage opens as the ``.mcap``
    file inside it. Anything else is returned unchanged.

    Raises:
        ValueError: the directory is split into several MCAP files; each
            is a bag of its own here, so the caller names one.
    """
    from resurrector.ingest.scanner import ros2_mcap_files

    path = Path(path)
    if not path.is_dir():
        return path
    files = ros2_mcap_files(path)
    if len(files) == 1:
        return files[0]
    if files:
        names = ", ".join(f.name for f in files[:3]) + (", ..." if len(files) > 3 else "")
        raise ValueError(
            f"{path.name} is split into {len(files)} MCAP files ({names}); "
            f"open one of them"
        )
    return path


def parse_bag(path: str | Path, auto_convert: bool = True) -> MCAPParser:
    """Create a parser for the given bag file.

    Natively supports MCAP. For legacy ``.bag`` (ROS 1) and ``.db3``
    (ROS 2 SQLite) files we shell out to the official converters
    (``mcap convert`` / ``ros2 bag convert``) and open the resulting
    MCAP. The converted file is kept alongside the original so the cost
    is paid once.

    Args:
        path: Path to the bag file.
        auto_convert: If True (default), legacy formats are converted
            automatically. Set False to force a ``NotImplementedError``
            — useful when the caller wants to handle conversion itself.
    """
    path = resolve_bag_path(path)
    ext = path.suffix.lower()

    if ext == ".mcap":
        return MCAPParser(path)

    if ext in {".bag", ".db3"} and auto_convert:
        from resurrector.ingest.convert import convert_to_mcap, ConversionError
        converted = path.with_suffix(".mcap")
        if not converted.exists():
            try:
                converted = convert_to_mcap(path)
            except ConversionError as e:
                raise NotImplementedError(
                    f"Could not auto-convert {path.name}: {e}"
                ) from e
        return MCAPParser(converted)

    if ext == ".bag":
        raise NotImplementedError(
            "ROS1 .bag format requires the mcap CLI for auto-convert "
            "(https://mcap.dev/guides/cli) or the 'rosbags' package "
            "(pip install 'rosbag-resurrector[ros1]')."
        )
    if ext == ".db3":
        raise NotImplementedError(
            "ROS2 .db3 (SQLite) format requires the ros2 CLI for auto-convert "
            "(provided by any ROS 2 install)."
        )
    raise ValueError(
        f"Unsupported file format: {ext or path.name!r} (expected .mcap, .bag or .db3)"
    )


# Register the built-in decoders now that all helper functions are defined.
_register_builtin_decoders()
