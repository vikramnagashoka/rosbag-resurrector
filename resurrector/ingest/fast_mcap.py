"""Fast raw message iteration over indexed MCAP files.

The mcap library's reader is correct but slow on real bags: for every chunk
it parses each record into a Python object, filters by topic afterwards, and
pushes every message through a heap to keep log-time order. Reading one
topic of a 20-topic bag therefore costs about as much as reading all of it,
and a full read spends most of its time in that bookkeeping.

This reader yields exactly what ``McapReader.iter_messages(log_time_order=True)``
yields (the same messages, in the same order: log time, then chunk file
offset, then position in the chunk) with less work:

- **Channel filter first.** A record's channel id is checked before anything
  is built for it.
- **Message index for topic subsets.** When only some of a chunk's channels
  are wanted, the chunk's message index gives their record offsets, and for
  an uncompressed chunk only those records are read from disk.
- **Merge over chunk cursors, not messages.** Chunks are visited in start-time
  order and a heap holds one cursor per open chunk. A cursor yields every
  message that sorts before the next cursor or unopened chunk without
  touching the heap, so a bag whose chunks don't overlap in time (the usual
  rosbag2 layout) is read chunk by chunk with no per-message heap work.

Memory is bounded by the chunks open at once: one, unless chunks overlap in
time. Files without a summary or chunk index, chunks without a message index,
and unknown compression are left to the mcap reader: :func:`iter_raw`
returns None for them and the caller falls back.
"""

from __future__ import annotations

import heapq
import os
import struct
from typing import IO, Iterable, Iterator, NamedTuple

_OP_MESSAGE = 0x05
_OP_CHUNK = 0x06
_OP_MESSAGE_INDEX = 0x07

_RECORD = struct.Struct("<BQ")              # opcode, content length
_MESSAGE = struct.Struct("<HIQQ")           # channel_id, sequence, log_time, publish_time
_MESSAGE_HEAD = _RECORD.size + _MESSAGE.size  # 31 bytes before a message's data
_CHUNK = struct.Struct("<QQQII")            # start, end, uncompressed size, crc, compression len
_INDEX_HEAD = struct.Struct("<HI")          # channel_id, entries byte length

# One chunk's wanted messages: (log_time, position, channel_id, sequence, publish_time, data).
_Entry = tuple

SUPPORTED_COMPRESSION = ("", "zstd", "lz4")


class RawMessage(NamedTuple):
    channel_id: int
    sequence: int
    log_time: int
    publish_time: int
    data: bytes


class _Unreadable(ValueError):
    """The file doesn't look the way the summary says; use the mcap reader."""


def can_read(summary) -> bool:
    """Whether :func:`iter_raw` can read this file (else use the mcap reader)."""
    if summary is None or not summary.chunk_indexes:
        return False
    return all(
        ci.message_index_offsets and ci.compression in SUPPORTED_COMPRESSION
        for ci in summary.chunk_indexes
    )


def iter_raw(
    f: IO[bytes],
    summary,
    channel_ids: Iterable[int] | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
) -> Iterator[RawMessage]:
    """Yield the messages of ``channel_ids`` (all if None) in log-time order.

    ``start_time`` is inclusive and ``end_time`` exclusive, both on log
    time, as in the mcap reader. Call :func:`can_read` first; a file it
    rejects raises ``ValueError`` here.
    """
    if not can_read(summary):
        raise ValueError("file has no usable chunk index; use the mcap reader")
    wanted = None if channel_ids is None else frozenset(channel_ids)
    chunks = [
        ci for ci in summary.chunk_indexes
        if (start_time is None or ci.message_end_time >= start_time)
        and (end_time is None or ci.message_start_time < end_time)
        and (wanted is None or not wanted.isdisjoint(ci.message_index_offsets))
    ]
    chunks.sort(key=lambda ci: (ci.message_start_time, ci.chunk_start_offset))
    fd = f.fileno()

    # Heap entries: (log_time, chunk_offset, position, entries, next_index).
    # Keys never tie across chunks (offsets differ), so the list is never compared.
    heap: list = []
    i = 0
    n_chunks = len(chunks)
    while True:
        # Open every chunk that sorts before the smallest pending message.
        while i < n_chunks and (
            not heap
            or (chunks[i].message_start_time, chunks[i].chunk_start_offset)
            < (heap[0][0], heap[0][1])
        ):
            ci = chunks[i]
            i += 1
            entries = _read_chunk(fd, ci, wanted, start_time, end_time)
            if entries:
                first = entries[0]
                heapq.heappush(heap, (first[0], ci.chunk_start_offset, first[1], entries, 0))
        if not heap:
            return
        _, offset, _, entries, k = heapq.heappop(heap)
        # Yield from this chunk until something else sorts first.
        if heap:
            bound = (heap[0][0], heap[0][1])
        else:
            bound = None
        if i < n_chunks:
            nxt = (chunks[i].message_start_time, chunks[i].chunk_start_offset)
            bound = nxt if bound is None or nxt < bound else bound
        n = len(entries)
        while k < n:
            e = entries[k]
            if bound is not None and (e[0], offset) > bound:
                break
            yield RawMessage(e[2], e[3], e[0], e[4], e[5])
            k += 1
        if k < n:
            e = entries[k]
            heapq.heappush(heap, (e[0], offset, e[1], entries, k))


def _read_chunk(fd: int, ci, wanted, start_time, end_time) -> list[_Entry]:
    """One chunk's wanted messages, sorted by (log_time, position)."""
    compression = ci.compression
    head = _RECORD.size + _CHUNK.size + len(compression.encode()) + 8
    records_at = ci.chunk_start_offset + head
    in_chunk = ci.message_index_offsets
    partial = wanted is not None and not wanted.issuperset(in_chunk)

    if compression == "" and partial:
        entries = _read_indexed_uncompressed(fd, ci, records_at, wanted)
    else:
        chunk = os.pread(fd, ci.chunk_length, ci.chunk_start_offset)
        op, _ = _RECORD.unpack_from(chunk, 0)
        if op != _OP_CHUNK or len(chunk) != ci.chunk_length:
            raise _Unreadable(f"no chunk record at offset {ci.chunk_start_offset}")
        (records_len,) = struct.unpack_from("<Q", chunk, head - 8)
        records = memoryview(chunk)[head:head + records_len]
        if len(records) != records_len:
            raise _Unreadable(f"chunk at offset {ci.chunk_start_offset} is cut short")
        if compression:
            records = memoryview(_decompress(compression, records, ci.uncompressed_size))
        if partial:
            index = os.pread(fd, ci.message_index_length, ci.chunk_start_offset + ci.chunk_length)
            _, offsets = _index_offsets(index, ci, wanted)
            entries = _messages_at(records, offsets, wanted)
        else:
            entries = _scan(records, wanted)

    if start_time is not None or end_time is not None:
        entries = [
            e for e in entries
            if (start_time is None or e[0] >= start_time) and (end_time is None or e[0] < end_time)
        ]
    # (log_time, position) is unique within a chunk, so tuple order never reaches
    # the data. Near-linear: recordings are almost always already in order.
    entries.sort()
    return entries


def _scan(records: memoryview, wanted) -> list[_Entry]:
    """Every message record in a chunk's records, filtered by channel."""
    out = []
    append = out.append
    unpack_record = _RECORD.unpack_from
    unpack_message = _MESSAGE.unpack_from
    pos, end = 0, len(records)
    while pos < end:
        op, length = unpack_record(records, pos)
        if op == _OP_MESSAGE:
            cid, seq, log_time, publish_time = unpack_message(records, pos + 9)
            if wanted is None or cid in wanted:
                append((log_time, pos, cid, seq, publish_time,
                        bytes(records[pos + _MESSAGE_HEAD:pos + 9 + length])))
        pos += 9 + length
    if pos != end:
        raise _Unreadable("chunk records overrun their length")
    return out


def _index_offsets(index: bytes, ci, wanted) -> tuple[list[int], list[int]]:
    """(every indexed record offset, the wanted channels' offsets), both
    sorted, from a chunk's message index region."""
    base = ci.chunk_start_offset + ci.chunk_length
    every: list[int] = []
    mine: list[int] = []
    for cid, at in ci.message_index_offsets.items():
        pos = at - base
        op, _ = _RECORD.unpack_from(index, pos)
        ch, nbytes = _INDEX_HEAD.unpack_from(index, pos + 9)
        if op != _OP_MESSAGE_INDEX or ch != cid:
            raise _Unreadable(f"no message index for channel {cid} at offset {at}")
        n = nbytes // 16
        pairs = struct.unpack_from(f"<{2 * n}Q", index, pos + 9 + _INDEX_HEAD.size)
        offsets = pairs[1::2]
        every.extend(offsets)
        if cid in wanted:
            mine.extend(offsets)
    every.sort()
    mine.sort()
    return every, mine


def _messages_at(records, offsets: list[int], wanted) -> list[_Entry]:
    out = []
    unpack_record = _RECORD.unpack_from
    unpack_message = _MESSAGE.unpack_from
    for pos in offsets:
        op, length = unpack_record(records, pos)
        if op != _OP_MESSAGE:
            raise _Unreadable(f"message index points at opcode {op}")
        cid, seq, log_time, publish_time = unpack_message(records, pos + 9)
        if cid in wanted:
            out.append((log_time, pos, cid, seq, publish_time,
                        bytes(records[pos + _MESSAGE_HEAD:pos + 9 + length])))
    return out


def _read_indexed_uncompressed(fd: int, ci, records_at: int, wanted) -> list[_Entry]:
    """Wanted messages of an uncompressed chunk, reading only their records.

    A record's extent comes from the next indexed record's offset (or the
    end of the records), so runs of consecutive wanted records are read
    with one ``pread`` each and nothing else in the chunk is touched.
    """
    index = os.pread(fd, ci.message_index_length, ci.chunk_start_offset + ci.chunk_length)
    every, mine_sorted = _index_offsets(index, ci, wanted)
    if not mine_sorted:
        return []
    mine = set(mine_sorted)
    bounds = every
    bounds.append(ci.uncompressed_size)  # records end == uncompressed size here

    out = []
    unpack_record = _RECORD.unpack_from
    unpack_message = _MESSAGE.unpack_from
    j, n = bounds.index(mine_sorted[0]), len(bounds) - 1
    last = bounds.index(mine_sorted[-1])
    while j <= last:
        if bounds[j] not in mine:
            j += 1
            continue
        run_start = j
        while j < n and bounds[j] in mine:
            j += 1
        lo, hi = bounds[run_start], bounds[j]
        buf = os.pread(fd, hi - lo, records_at + lo)
        if len(buf) != hi - lo:
            raise _Unreadable("short read inside a chunk")
        for pos in bounds[run_start:j]:
            rel = pos - lo
            op, length = unpack_record(buf, rel)
            if op != _OP_MESSAGE:
                raise _Unreadable(f"message index points at opcode {op}")
            cid, seq, log_time, publish_time = unpack_message(buf, rel + 9)
            if cid in wanted:
                out.append((log_time, pos, cid, seq, publish_time,
                            buf[rel + _MESSAGE_HEAD:rel + 9 + length]))
    return out


def _decompress(compression: str, data, size: int) -> bytes:
    if compression == "zstd":
        import zstandard
        return zstandard.ZstdDecompressor().decompress(bytes(data), max_output_size=size)
    if compression == "lz4":
        import lz4.frame
        return lz4.frame.decompress(bytes(data))
    raise _Unreadable(f"unsupported compression {compression!r}")
