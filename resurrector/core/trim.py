"""Time-range trim of a bag file.

Produces a new MCAP (or other format) containing only the messages
inside [start_sec, end_sec] for the selected topics. Used by the
dashboard's brush-to-trim UX and the new ``resurrector trim`` CLI.

For MCAP output the original schemas, channels, and message bytes are
preserved byte-for-byte — this is critical because the trimmed file
needs to be re-openable by any MCAP-aware tool (PlotJuggler, Foxglove,
ros2 bag) without quirks.

For other formats (Parquet, CSV, MP4) we delegate to the existing
``Exporter`` after time-slicing.
"""

from __future__ import annotations

import logging
from pathlib import Path

from resurrector.ingest.parser import parse_bag

logger = logging.getLogger("resurrector.core.trim")


SUPPORTED_FORMATS = {"mcap", "parquet", "csv", "hdf5", "numpy", "zarr", "mp4"}


def trim_to_mcap(
    source_path: str | Path,
    output_path: str | Path,
    start_sec: float,
    end_sec: float,
    topics: list[str] | None = None,
) -> Path:
    """Copy a time-range slice of a bag to a new MCAP, preserving schemas.

    The source's original encoding, message_encoding, channels, and
    raw message bytes are written verbatim — no decode/re-encode round
    trip — so the output is bit-identical to a recording made over the
    same window.

    Args:
        source_path: Source bag (MCAP; legacy formats auto-converted).
        output_path: Destination .mcap path.
        start_sec: Inclusive start, in seconds from the source's
            ``start_time_ns``.
        end_sec: Exclusive end, same reference.
        topics: Optional allowlist. If None, every topic is preserved.

    Returns:
        The output path.
    """
    from mcap.reader import make_reader
    from mcap.writer import Writer

    if end_sec <= start_sec:
        raise ValueError(
            f"end_sec ({end_sec}) must be greater than start_sec ({start_sec})"
        )

    source = Path(source_path)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    # Resolve via parse_bag so legacy formats are auto-converted to MCAP.
    parser = parse_bag(source)
    metadata = parser.get_metadata()
    bag_start_ns = metadata.start_time_ns
    target_start_ns = bag_start_ns + int(start_sec * 1e9)
    target_end_ns = bag_start_ns + int(end_sec * 1e9)

    topic_filter = set(topics) if topics else None
    written = 0

    # parse_bag may have produced a converted MCAP file; use the parser's
    # path attribute so we read from the converted file.
    mcap_path = parser.path

    with open(mcap_path, "rb") as src_f, open(output, "wb") as dst_f:
        reader = make_reader(src_f)
        writer = Writer(dst_f)
        writer.start()

        # Copy schemas first; we register a schema in the destination
        # for every distinct (encoding, name) pair we encounter, mapping
        # source schema_id -> dest schema_id.
        summary = reader.get_summary()
        if summary is None:
            raise RuntimeError(f"Cannot read summary from {mcap_path}")

        src_schema_to_dst: dict[int, int] = {}
        for sch in summary.schemas.values():
            dst_id = writer.register_schema(
                name=sch.name,
                encoding=sch.encoding,
                data=sch.data,
            )
            src_schema_to_dst[sch.id] = dst_id

        src_channel_to_dst: dict[int, int] = {}
        for ch in summary.channels.values():
            if topic_filter is not None and ch.topic not in topic_filter:
                continue
            dst_id = writer.register_channel(
                topic=ch.topic,
                message_encoding=ch.message_encoding,
                schema_id=src_schema_to_dst.get(ch.schema_id, 0),
                metadata=dict(ch.metadata),
            )
            src_channel_to_dst[ch.id] = dst_id

        # Iterate messages in the time window. The MCAP reader supports
        # native start_time/end_time filtering so this is much faster
        # than streaming everything and discarding.
        for sch, ch, msg in reader.iter_messages(
            topics=list(topic_filter) if topic_filter else None,
            start_time=target_start_ns,
            end_time=target_end_ns,
        ):
            dst_channel_id = src_channel_to_dst.get(ch.id)
            if dst_channel_id is None:
                continue
            writer.add_message(
                channel_id=dst_channel_id,
                log_time=msg.log_time,
                publish_time=msg.publish_time,
                data=msg.data,
                sequence=msg.sequence,
            )
            written += 1

        writer.finish()

    logger.info(
        "Trimmed %s [%s..%s sec] -> %s (%d messages)",
        source.name, start_sec, end_sec, output, written,
    )
    return output


def trim_to_format(
    source_path: str | Path,
    output_path: str | Path,
    start_sec: float,
    end_sec: float,
    topics: list[str],
    format: str,
) -> Path:
    """Trim a bag and export the slice to any supported format.

    For ``mcap`` we copy raw messages via :func:`trim_to_mcap`. For
    every other format we time-slice the BagFrame and run the existing
    Exporter.
    """
    if format not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported trim format: {format}. "
            f"Supported: {sorted(SUPPORTED_FORMATS)}"
        )

    if end_sec <= start_sec:
        raise ValueError(
            f"end_sec ({end_sec}) must be greater than start_sec ({start_sec})"
        )

    if format == "mcap":
        return trim_to_mcap(source_path, output_path, start_sec, end_sec, topics)

    from resurrector.core.bag_frame import BagFrame

    bf = BagFrame(source_path)
    sliced = bf.time_slice(start_sec, end_sec)

    if format == "mp4":
        # Video export takes a single image topic.
        from resurrector.core.export import Exporter
        if len(topics) != 1:
            raise ValueError(
                f"mp4 export requires exactly one image topic, got {len(topics)}"
            )
        topic = topics[0]
        view = sliced[topic]
        if not view.is_image_topic:
            raise ValueError(
                f"Topic '{topic}' is not an image topic; cannot export to MP4"
            )
        return Exporter().export_video(view, output_path)

    # All other formats go through the standard streaming exporter.
    from resurrector.core.export import Exporter
    output_dir = Path(output_path)
    if output_dir.suffix:
        # Caller passed a file path; we need a directory for the multi-topic
        # exporter. Use the parent and let it write per-topic files.
        output_dir = output_dir.parent
    Exporter().export(
        bag_frame=sliced,
        topics=topics,
        format=format,
        output_dir=str(output_dir),
    )
    return output_dir
