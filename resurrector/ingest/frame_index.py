"""Frame offset cache build + lookup.

    First request                Subsequent requests
    ─────────────                ───────────────────

   build_frame_offsets()         get_frame_at()
         │                              │
         ▼                              ▼
   iter MCAP messages            SELECT timestamp_ns FROM
   on image topic(s)             frame_offsets WHERE (bag,
   and bulk-insert               topic, idx)  ← O(1)
   (frame_index,                       │
    timestamp_ns)                      ▼
         │                        reader.iter_messages(
         ▼                          start_time = ts,
   O(N) scan per                    topics=[topic])
   (bag, topic)                  → first message is the frame
         │                              │
         ▼                              ▼
   CACHED                         JPEG encode & return

Tests live in `tests/test_frame_offsets_annotations.py` for the index
CRUD and `tests/test_frame_index.py` for the build/lookup pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

from resurrector.ingest.indexer import BagIndex

logger = logging.getLogger("resurrector.ingest.frame_index")

IMAGE_TOPIC_TYPES = {
    "sensor_msgs/msg/Image",
    "sensor_msgs/msg/CompressedImage",
}


def image_topics(bag_path: str | Path) -> list[str]:
    """Return names of image topics in a bag (raw or compressed)."""
    from resurrector.ingest.parser import parse_bag
    parser = parse_bag(bag_path)
    meta = parser.get_metadata()
    return [t.name for t in meta.topics if t.message_type in IMAGE_TOPIC_TYPES]


def build_frame_offsets(
    index: BagIndex,
    bag_id: int,
    bag_path: str | Path,
    topics: Iterable[str] | None = None,
) -> dict[str, int]:
    """Scan a bag once and cache (frame_index, timestamp_ns) per image topic.

    Idempotent — topics already fully cached are skipped.

    Returns a map of topic -> frame count for topics that were built
    during this call. Topics that were already cached return 0.
    """
    from resurrector.ingest.parser import parse_bag

    target_topics = list(topics) if topics is not None else image_topics(bag_path)
    if not target_topics:
        return {}

    # Filter out topics that are already cached
    to_build = [t for t in target_topics if not index.has_frame_offsets(bag_id, t)]
    if not to_build:
        return {t: 0 for t in target_topics}

    logger.info(
        "Building frame offsets for bag %d (%s), topics: %s",
        bag_id, bag_path, to_build,
    )

    parser = parse_bag(bag_path)
    per_topic_counter: dict[str, int] = {t: 0 for t in to_build}
    per_topic_offsets: dict[str, list[tuple[int, int]]] = {t: [] for t in to_build}

    for msg in parser.read_messages(topics=to_build):
        counter = per_topic_counter[msg.topic]
        per_topic_offsets[msg.topic].append((counter, msg.timestamp_ns))
        per_topic_counter[msg.topic] = counter + 1

    # Single transaction per topic.
    for topic, offsets in per_topic_offsets.items():
        index.insert_frame_offsets(bag_id, topic, offsets)
        logger.info("Cached %d frames for %s on bag %d", len(offsets), topic, bag_id)

    # Merge results: already-cached topics report 0, newly built report their count.
    return {t: per_topic_counter.get(t, 0) for t in target_topics}


def get_frame_timestamp(
    index: BagIndex,
    bag_id: int,
    bag_path: str | Path,
    topic: str,
    frame_index: int,
) -> int | None:
    """Return the timestamp_ns for frame N on a topic.

    Lazily builds the offset cache on first access for this (bag, topic).
    Callers that want explicit control can call ``build_frame_offsets``
    first and rely on the returned timestamp being O(1).
    """
    ts = index.get_frame_timestamp(bag_id, topic, frame_index)
    if ts is not None:
        return ts

    if not index.has_frame_offsets(bag_id, topic):
        build_frame_offsets(index, bag_id, bag_path, topics=[topic])
        ts = index.get_frame_timestamp(bag_id, topic, frame_index)

    return ts


def read_single_frame(
    bag_path: str | Path, topic: str, timestamp_ns: int,
):
    """Read a single message at the given timestamp and decode as image.

    Uses MCAP's native start_time filter so we don't scan the whole bag.
    """
    from resurrector.ingest.parser import (
        parse_bag, get_image_array, get_compressed_image_array,
    )

    parser = parse_bag(bag_path)
    # Look in a 1-second window starting at the timestamp.
    # Different recorders have slightly different time bases so a tight
    # range is safer than an exact match.
    for msg in parser.read_messages(
        topics=[topic],
        start_time_ns=timestamp_ns,
        end_time_ns=timestamp_ns + 1_000_000_000,
    ):
        if msg.topic != topic:
            continue
        # Try both decoders; whichever succeeds wins.
        if "_compressed_data_offset" in msg.data:
            return get_compressed_image_array(msg), msg.timestamp_ns
        if "_pixel_data_offset" in msg.data:
            return get_image_array(msg), msg.timestamp_ns
    return None, None
