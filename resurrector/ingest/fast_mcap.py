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
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import IO, Iterable, Iterator, NamedTuple

_OP_MESSAGE = 0x05
_OP_CHUNK = 0x06
_OP_MESSAGE_INDEX = 0x07

_RECORD = struct.Struct("<BQ")              # opcode, content length
_MESSAGE = struct.Struct("<HIQQ")           # channel_id, sequence, log_time, publish_time
_MESSAGE_HEAD = _RECORD.size + _MESSAGE.size  # 31 bytes before a message's data
_CHUNK = struct.Struct("<QQQII")            # start, end, uncompressed size, crc, compression len
_INDEX_HEAD = struct.Struct("<HI")          # channel_id, entries byte length

# One chunk's wanted messages:
# (log_time, position, channel_id, sequence, publish_time, data, size, data_offset).
_Entry = tuple

# A record is read only up to its prefix when that saves more than this.
_PREFIX_SLACK = 4096

# How many chunks ahead to ask the OS to read in the background. For a bag
# bigger than the page cache this keeps the disk busy while the caller
# decodes, instead of waiting on one small read at a time (3x faster health
# checks on a 55 GB bag read cold). A hint, not a read: nothing is copied,
# no memory is held, and it costs almost nothing when the data is cached.
READ_AHEAD = 8

# A bag bigger than physical memory can never sit in the page cache whole,
# so its reads always reach the disk. For those, worker threads also read
# the next PREFETCH chunks themselves (pread releases the GIL), which keeps
# several reads in flight: 3x faster health checks and 2.2x faster topic
# reads on a 55 GB bag on a 24 GB machine, where hints alone give 1.4x.
# Smaller bags skip the threads, which cost up to 50% on cached per-topic
# passes. Memory: PREFETCH chunks' wanted messages.
PREFETCH = 4

_F_RDADVISE = 44  # macOS <sys/fcntl.h>; Python's fcntl module doesn't name it


def _physical_memory() -> int:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        return 0


def _advise(fd: int, offset: int, length: int) -> None:
    """Ask the OS to start reading ``[offset, offset + length)`` now."""
    global _advise
    try:
        if sys.platform == "darwin":
            import fcntl
            fcntl.fcntl(fd, _F_RDADVISE, struct.pack("qi4x", offset, min(length, 2**31 - 1)))
        elif hasattr(os, "posix_fadvise"):
            os.posix_fadvise(fd, offset, length, os.POSIX_FADV_WILLNEED)
    except OSError:
        _advise = lambda fd, offset, length: None  # unsupported here; stop trying

SUPPORTED_COMPRESSION = ("", "zstd", "lz4")


class RawMessage(NamedTuple):
    channel_id: int
    sequence: int
    log_time: int
    publish_time: int
    data: bytes
    size: int = -1        # the payload's full length; > len(data) if cut to a prefix
    data_offset: int = -1  # file offset of the payload, when it can be re-read from there


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
    prefix: dict[int, int] | None = None,
) -> Iterator[RawMessage]:
    """Yield the messages of ``channel_ids`` (all if None) in log-time order.

    ``start_time`` is inclusive and ``end_time`` exclusive, both on log
    time, as in the mcap reader. Call :func:`can_read` first; a file it
    rejects raises ``ValueError`` here.

    ``prefix`` maps a channel id to how many payload bytes its caller
    needs. In an uncompressed chunk, a larger message of that channel is
    then read only that far (``data`` is cut short, ``size`` is the full
    length and ``data_offset`` says where to re-read it): a camera frame
    costs a header read instead of a megabyte copy when only its metadata
    is wanted. ``{cid: 0}`` reads nothing past the record header.
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
    n_chunks = len(chunks)
    advised = [0]  # chunks[:advised[0]] have been hinted
    ram = _physical_memory()
    pool = None
    if PREFETCH > 0 and n_chunks > 1 and ram and os.fstat(fd).st_size > ram:
        pool = ThreadPoolExecutor(PREFETCH, thread_name_prefix="mcap-read")
    pending: dict[int, object] = {}

    def read(idx: int) -> list[_Entry]:
        """Chunk ``idx``'s entries, after starting the reads of the next ones."""
        stop = min(idx + 1 + READ_AHEAD, n_chunks)
        for j in range(max(advised[0], idx + 1), stop):
            ci = chunks[j]
            _advise(fd, ci.chunk_start_offset, ci.chunk_length + ci.message_index_length)
        advised[0] = max(advised[0], stop)
        if pool is not None:
            for j in range(idx + 1, min(idx + 1 + PREFETCH, n_chunks)):
                if j not in pending:
                    pending[j] = pool.submit(
                        _read_chunk, fd, chunks[j], wanted, start_time, end_time, prefix)
            future = pending.pop(idx, None)
            if future is not None:
                return future.result()
        return _read_chunk(fd, chunks[idx], wanted, start_time, end_time, prefix)

    try:
        yield from _merge(chunks, read)
    finally:
        if pool is not None:
            pool.shutdown(wait=True, cancel_futures=True)


def _merge(chunks: list, read) -> Iterator[RawMessage]:
    """Yield the chunks' messages in (log_time, chunk offset, position) order,
    opening chunk ``i`` (via ``read(i)``) only when it could hold the next one."""

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
            entries = read(i)
            i += 1
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
            yield RawMessage(e[2], e[3], e[0], e[4], e[5], e[6], e[7])
            k += 1
        if k < n:
            e = entries[k]
            heapq.heappush(heap, (e[0], offset, e[1], entries, k))


def _read_chunk(fd: int, ci, wanted, start_time, end_time, prefix=None) -> list[_Entry]:
    """One chunk's wanted messages, sorted by (log_time, position)."""
    compression = ci.compression
    head = _RECORD.size + _CHUNK.size + len(compression.encode()) + 8
    records_at = ci.chunk_start_offset + head
    in_chunk = ci.message_index_offsets
    partial = wanted is not None and not wanted.issuperset(in_chunk)

    if compression == "" and (partial or prefix):
        mine = wanted if wanted is not None else frozenset(in_chunk)
        entries = _read_indexed_uncompressed(fd, ci, records_at, mine, prefix)
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
            entries = _scan(records, wanted, records_at if not compression else None)

    if start_time is not None or end_time is not None:
        entries = [
            e for e in entries
            if (start_time is None or e[0] >= start_time) and (end_time is None or e[0] < end_time)
        ]
    # (log_time, position) is unique within a chunk, so tuple order never reaches
    # the data. Near-linear: recordings are almost always already in order.
    entries.sort()
    return entries


def _scan(records: memoryview, wanted, records_at: int | None = None) -> list[_Entry]:
    """Every message record in a chunk's records, filtered by channel.
    ``records_at`` is the records' file offset (None if decompressed)."""
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
                        bytes(records[pos + _MESSAGE_HEAD:pos + 9 + length]), length - 22,
                        -1 if records_at is None else records_at + pos + _MESSAGE_HEAD))
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
                        bytes(records[pos + _MESSAGE_HEAD:pos + 9 + length]), length - 22, -1))
    return out


def _read_indexed_uncompressed(fd: int, ci, records_at: int, wanted, prefix=None) -> list[_Entry]:
    """Wanted messages of an uncompressed chunk, reading only their records.

    A record's extent comes from the next indexed record's offset (or the
    end of the records), so runs of consecutive wanted records are read
    with one ``pread`` each and nothing else in the chunk is touched. A
    record of a ``prefix`` channel that is much longer than its prefix is
    read on its own, only that far.
    """
    index = os.pread(fd, ci.message_index_length, ci.chunk_start_offset + ci.chunk_length)
    every, mine_sorted, owner = _index_owners(index, ci, wanted)
    if not mine_sorted:
        return []
    bounds = every
    bounds.append(ci.uncompressed_size)  # records end == uncompressed size here
    at = {pos: i for i, pos in enumerate(bounds)}

    # (lo, hi) byte ranges to read, each covering whole records or one cut record.
    reads: list[tuple[int, int]] = []
    run_lo = run_hi = -1
    for pos in mine_sorted:
        nxt = bounds[at[pos] + 1]
        limit = None
        if prefix:
            want = prefix.get(owner[pos])
            if want is not None and nxt - pos > _MESSAGE_HEAD + want + _PREFIX_SLACK:
                limit = _MESSAGE_HEAD + want
        if limit is not None:
            if run_lo >= 0:
                reads.append((run_lo, run_hi))
                run_lo = -1
            reads.append((pos, pos + limit))
        elif run_lo >= 0 and run_hi == pos:
            run_hi = nxt
        else:
            if run_lo >= 0:
                reads.append((run_lo, run_hi))
            run_lo, run_hi = pos, nxt
    if run_lo >= 0:
        reads.append((run_lo, run_hi))

    out = []
    unpack_record = _RECORD.unpack_from
    unpack_message = _MESSAGE.unpack_from
    i, n_mine = 0, len(mine_sorted)
    for lo, hi in reads:
        buf = os.pread(fd, hi - lo, records_at + lo)
        if len(buf) != hi - lo:
            raise _Unreadable("short read inside a chunk")
        while i < n_mine and mine_sorted[i] < hi:
            pos = mine_sorted[i]
            i += 1
            rel = pos - lo
            op, length = unpack_record(buf, rel)
            if op != _OP_MESSAGE:
                raise _Unreadable(f"message index points at opcode {op}")
            cid, seq, log_time, publish_time = unpack_message(buf, rel + 9)
            out.append((log_time, pos, cid, seq, publish_time,
                        buf[rel + _MESSAGE_HEAD:rel + 9 + length], length - 22,
                        records_at + pos + _MESSAGE_HEAD))
    return out


def _index_owners(index: bytes, ci, wanted) -> tuple[list[int], list[int], dict[int, int]]:
    """Like :func:`_index_offsets`, plus each wanted offset's channel id."""
    base = ci.chunk_start_offset + ci.chunk_length
    every: list[int] = []
    mine: list[int] = []
    owner: dict[int, int] = {}
    for cid, at in ci.message_index_offsets.items():
        pos = at - base
        op, _ = _RECORD.unpack_from(index, pos)
        ch, nbytes = _INDEX_HEAD.unpack_from(index, pos + 9)
        if op != _OP_MESSAGE_INDEX or ch != cid:
            raise _Unreadable(f"no message index for channel {cid} at offset {at}")
        n = nbytes // 16
        offsets = struct.unpack_from(f"<{2 * n}Q", index, pos + 9 + _INDEX_HEAD.size)[1::2]
        every.extend(offsets)
        if cid in wanted:
            mine.extend(offsets)
            owner.update(dict.fromkeys(offsets, cid))
    every.sort()
    mine.sort()
    return every, mine, owner


def _decompress(compression: str, data, size: int) -> bytes:
    if compression == "zstd":
        import zstandard
        return zstandard.ZstdDecompressor().decompress(bytes(data), max_output_size=size)
    if compression == "lz4":
        import lz4.frame
        return lz4.frame.decompress(bytes(data))
    raise _Unreadable(f"unsupported compression {compression!r}")
