"""Durable append-only segmented commit log.

A record is framed as::

    +--------+----------------+------------+-------------+-----------+---------+
    | magic4 | payload_len u32| offset u64 | ts i64      | crc32 u32 | payload |
    +--------+----------------+------------+-------------+-----------+---------+

Records are appended to size-rolled segment files (``<base_offset:020d>.log``)
with a sparse sidecar index (``.index``, one entry every ``index_interval_bytes``
of ``(relative_offset, byte_position)``). On startup each segment replays its
records validating CRCs and truncates any torn/corrupt tail, so a crash mid-write
can never corrupt previously committed records.
"""

from __future__ import annotations

import mmap
import os
import struct
import time
import zlib
from bisect import bisect_right
from dataclasses import dataclass

MAGIC = b"PLG1"

_HEADER = struct.Struct("<4sIQqI")
_INDEX_ENTRY = struct.Struct("<QQ")

MAX_RECORD_BYTES = 64 * 1024 * 1024


class CorruptRecord(Exception):
    """Raised when a record fails validation (torn write or corruption)."""


@dataclass(frozen=True)
class Record:
    __slots__ = ("offset", "timestamp", "payload")

    offset: int
    timestamp: int
    payload: bytes


def encode_record(offset: int, payload: bytes, timestamp: int = 0) -> bytes:
    return _HEADER.pack(MAGIC, len(payload), offset, timestamp, zlib.crc32(payload)) + payload


def read_record(f) -> Record | None:
    header = f.read(_HEADER.size)
    if not header:
        return None
    if len(header) < _HEADER.size:
        raise CorruptRecord("truncated record header")
    magic, length, offset, timestamp, crc = _HEADER.unpack(header)
    if magic != MAGIC:
        raise CorruptRecord("bad magic")
    if length > MAX_RECORD_BYTES:
        raise CorruptRecord("implausible payload length")
    payload = f.read(length)
    if len(payload) < length:
        raise CorruptRecord("truncated payload")
    if zlib.crc32(payload) != crc:
        raise CorruptRecord("crc mismatch")
    return Record(offset=offset, timestamp=timestamp, payload=payload)


class Segment:
    """One ``.log`` file plus its sparse ``.index`` sidecar."""

    def __init__(self, dir_path: str, base_offset: int, index_interval_bytes: int = 4096) -> None:
        self.dir_path = dir_path
        self.base_offset = base_offset
        self.index_interval_bytes = max(1, int(index_interval_bytes))
        stem = f"{base_offset:020d}"
        self.log_path = os.path.join(dir_path, stem + ".log")
        self.index_path = os.path.join(dir_path, stem + ".index")
        self.index: list[tuple[int, int]] = []
        self.size = 0
        self.last_indexed_position = -1
        self.max_timestamp = 0
        self.next_offset = base_offset
        self._log_fh = None
        self._index_fh = None
        self._read_fh = None
        self._mmap = None
        self._mmap_size = 0

    def _append_handle(self):
        if self._log_fh is None or self._log_fh.closed:
            self._log_fh = open(self.log_path, "ab", buffering=0)
        return self._log_fh

    def _index_handle(self):
        if self._index_fh is None or self._index_fh.closed:
            self._index_fh = open(self.index_path, "ab", buffering=0)
        return self._index_fh

    def _read_handle(self):
        if self._read_fh is None or self._read_fh.closed:
            self._read_fh = open(self.log_path, "rb")
        return self._read_fh

    def _mmap_handle(self):
        """mmap for fast random reads — falls back to file handle on failure or empty file."""
        try:
            size = os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0
            if size == 0:
                return None
            if self._mmap is None or self._mmap.closed or self._mmap_size != size:
                if self._mmap is not None and not self._mmap.closed:
                    self._mmap.close()
                fh = open(self.log_path, "rb")
                self._mmap = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
                self._mmap_size = size
                fh.close()
            return self._mmap
        except Exception:
            return None

    def _maybe_index(self, abs_offset: int, position: int) -> None:
        rel = abs_offset - self.base_offset
        if not self.index or (position - self.last_indexed_position) >= self.index_interval_bytes:
            self.index.append((rel, position))
            fh = self._index_handle()
            fh.write(_INDEX_ENTRY.pack(rel, position))
            fh.flush()
            self.last_indexed_position = position

    def recover(self) -> int:
        self.close_handles()
        self.index = []
        self.size = 0
        self.last_indexed_position = -1
        self.max_timestamp = 0
        self.next_offset = self.base_offset

        if not os.path.exists(self.log_path):
            open(self.log_path, "wb").close()
            open(self.index_path, "wb").close()
            return self.base_offset

        next_offset = self.base_offset
        good_size = 0
        with open(self.log_path, "rb") as f:
            while True:
                position = f.tell()
                try:
                    rec = read_record(f)
                except CorruptRecord:
                    break
                if rec is None:
                    break
                self._maybe_index(rec.offset, position)
                self.max_timestamp = max(self.max_timestamp, rec.timestamp)
                good_size = f.tell()
                next_offset = rec.offset + 1

        if good_size < os.path.getsize(self.log_path):
            with open(self.log_path, "r+b") as f:
                f.truncate(good_size)

        self.size = good_size
        self.next_offset = next_offset
        self._rewrite_index()
        return next_offset

    def _rewrite_index(self) -> None:
        with open(self.index_path, "wb") as f:
            for rel, position in self.index:
                f.write(_INDEX_ENTRY.pack(rel, position))

    def append(
        self, offset: int, record_bytes: bytes, timestamp: int = 0, fsync: bool = False
    ) -> None:
        position = self.size
        fh = self._append_handle()
        fh.write(record_bytes)
        if fsync:
            os.fsync(fh.fileno())
        self.size += len(record_bytes)
        if timestamp > self.max_timestamp:
            self.max_timestamp = timestamp
        self.next_offset = offset + 1
        rel = offset - self.base_offset
        if not self.index or (position - self.last_indexed_position) >= self.index_interval_bytes:
            self.index.append((rel, position))
            idx_fh = self._index_handle()
            idx_fh.write(_INDEX_ENTRY.pack(rel, position))
            self.last_indexed_position = position

    def read(self, abs_offset: int) -> Record | None:
        if abs_offset < self.base_offset or abs_offset >= self.next_offset:
            return None
        rel_target = abs_offset - self.base_offset
        lo, hi = 0, len(self.index)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.index[mid][0] <= rel_target:
                lo = mid + 1
            else:
                hi = mid
        start_rel, start_pos = self.index[lo - 1] if lo > 0 else (0, 0)
        # mmap fast path — zero-copy page-cache reads
        mm = self._mmap_handle()
        if mm is not None:
            try:
                mm.seek(start_pos)
                while True:
                    try:
                        rec = read_record(mm)
                    except CorruptRecord:
                        return None
                    if rec is None:
                        return None
                    if rec.offset == abs_offset:
                        return rec
                    if rec.offset > abs_offset:
                        return None
            except Exception:
                pass
        f = self._read_handle()
        f.seek(start_pos)
        while True:
            try:
                rec = read_record(f)
            except CorruptRecord:
                return None
            if rec is None:
                return None
            if rec.offset == abs_offset:
                return rec
            if rec.offset > abs_offset:
                return None

    def position_of(self, abs_offset: int) -> int | None:
        """Byte position where the record with ``abs_offset`` starts."""
        if abs_offset < self.base_offset or abs_offset >= self.next_offset:
            return None
        rel_target = abs_offset - self.base_offset
        lo, hi = 0, len(self.index)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.index[mid][0] <= rel_target:
                lo = mid + 1
            else:
                hi = mid
        _, start_pos = self.index[lo - 1] if lo > 0 else (0, 0)
        f = self._read_handle()
        f.seek(start_pos)
        while True:
            position = f.tell()
            try:
                rec = read_record(f)
            except CorruptRecord:
                return None
            if rec is None:
                return None
            if rec.offset == abs_offset:
                return position
            if rec.offset > abs_offset:
                return None

    def truncate_from(self, abs_offset: int) -> bool:
        """Drop records with offset >= abs_offset. Returns True if anything changed."""
        if abs_offset <= self.base_offset:
            changed = self.size > 0 or self.next_offset != self.base_offset
            self.close_handles()
            open(self.log_path, "wb").close()
            self.index = []
            self.size = 0
            self.last_indexed_position = -1
            self.max_timestamp = 0
            self.next_offset = self.base_offset
            self._rewrite_index()
            return changed
        position = self.position_of(abs_offset)
        if position is None:
            return False
        self.close_handles()
        with open(self.log_path, "r+b") as f:
            f.truncate(position)
        self.size = position
        self.next_offset = abs_offset
        rel_cut = abs_offset - self.base_offset
        self.index = [e for e in self.index if e[0] < rel_cut]
        self.last_indexed_position = self.index[-1][1] if self.index else -1
        self._rewrite_index()
        return True

    @property
    def is_empty(self) -> bool:
        return self.size == 0

    def close_handles(self) -> None:
        for attr in ("_log_fh", "_index_fh", "_read_fh"):
            fh = getattr(self, attr)
            if fh is not None and not fh.closed:
                fh.close()
        if self._mmap is not None and not self._mmap.closed:
            try:
                self._mmap.close()
            except Exception:
                pass
        self._mmap = None
        self._mmap_size = 0

    def delete(self) -> None:
        self.close_handles()
        for path in (self.log_path, self.index_path):
            if os.path.exists(path):
                os.remove(path)


class CommitLog:
    """A multi-segment durable commit log with rolling and recovery."""

    def __init__(
        self,
        dir_path: str,
        max_segment_bytes: int = 128 * 1024 * 1024,
        index_interval_bytes: int = 4096,
    ) -> None:
        self.dir_path = dir_path
        self.max_segment_bytes = max(1, int(max_segment_bytes))
        self.index_interval_bytes = index_interval_bytes
        os.makedirs(dir_path, exist_ok=True)
        self.segments: list[Segment] = []
        self.next_offset = self.recover()

    def _new_segment(self, base_offset: int) -> Segment:
        seg = Segment(self.dir_path, base_offset, self.index_interval_bytes)
        seg.recover()
        self.segments.append(seg)
        return seg

    def recover(self) -> int:
        stems = sorted(
            int(name[: -len(".log")])
            for name in os.listdir(self.dir_path)
            if name.endswith(".log") and name[: -len(".log")].isdigit()
        )
        self.segments = [Segment(self.dir_path, b, self.index_interval_bytes) for b in stems]
        if not self.segments:
            self._new_segment(0)
            return 0
        for seg in self.segments:
            seg.recover()
        return self.segments[-1].next_offset

    def _active(self) -> Segment:
        return self.segments[-1]

    def append(self, payload: bytes, timestamp: int | None = None, fsync: bool = False) -> int:
        offset = self.next_offset
        ts = int(time.time() * 1000) if timestamp is None else timestamp
        active = self._active()
        if active.size >= self.max_segment_bytes:
            active.close_handles()
            self._new_segment(offset)
            active = self._active()
        active.append(offset, encode_record(offset, payload, ts), timestamp=ts, fsync=fsync)
        self.next_offset = offset + 1
        return offset

    def append_many(
        self,
        payloads: list[bytes],
        timestamps: list[int] | None = None,
        fsync: bool = False,
    ) -> list[int]:
        """Group commit: batches records into one write syscall (+one optional fsync)."""
        if not payloads:
            return []
        n = len(payloads)
        offsets = list(range(self.next_offset, self.next_offset + n))
        if timestamps is None:
            now = int(time.time() * 1000)
            timestamps = [now] * n

        done: list[int] = []
        i = 0
        while i < n:
            active = self._active()
            blob = bytearray()
            positions = []
            pos = active.size
            j = i
            while j < n:
                rec = encode_record(offsets[j], payloads[j], timestamps[j])
                if j > i and pos + len(rec) > self.max_segment_bytes:
                    break
                positions.append(pos)
                blob.extend(rec)
                pos += len(rec)
                j += 1
            fh = active._append_handle()
            fh.write(bytes(blob))
            for k in range(i, j):
                active._maybe_index(offsets[k], positions[k - i])
                active.next_offset = offsets[k] + 1
                if timestamps[k] > active.max_timestamp:
                    active.max_timestamp = timestamps[k]
            active.size = pos
            if fsync:
                os.fsync(fh.fileno())
            done.extend(offsets[i:j])
            if j < n:
                active.close_handles()
                self._new_segment(offsets[j])
            i = j
        self.next_offset = offsets[-1] + 1
        return offsets

    def read(self, offset: int) -> Record | None:
        bases = [s.base_offset for s in self.segments]
        idx = bisect_right(bases, offset) - 1
        if idx < 0:
            return None
        rec = self.segments[idx].read(offset)
        if rec is None and idx > 0:
            return self.segments[idx - 1].read(offset)
        return rec

    def read_range(self, start: int, limit: int = 100) -> list[Record]:
        out = []
        offset = start
        while len(out) < limit:
            rec = self.read(offset)
            if rec is None:
                break
            out.append(rec)
            offset += 1
        return out

    def truncate_from(self, offset: int) -> bool:
        """Drop all records with offset >= ``offset`` across segments."""
        bases = [s.base_offset for s in self.segments]
        idx = bisect_right(bases, offset) - 1
        changed = False
        if idx < 0:
            return False
        for seg in self.segments[idx + 1 :]:
            seg.delete()
            changed = True
        self.segments = self.segments[: idx + 1]
        if self.segments and self.segments[idx].truncate_from(offset):
            changed = True
        if self.segments:
            self.next_offset = self.segments[-1].next_offset
        else:
            self._new_segment(0)
            self.next_offset = 0
            changed = True
        return changed

    def truncate_segments_before(self, min_base_offset: int) -> int:
        removed = 0
        kept = []
        for i, seg in enumerate(self.segments):
            if seg.is_empty and i < len(self.segments) - 1:
                seg.delete()
                removed += 1
                continue
            if seg.base_offset < min_base_offset and i < len(self.segments) - 1:
                seg.delete()
                removed += 1
                continue
            kept.append(seg)
        self.segments = kept
        return removed

    def stats(self) -> dict:
        return {
            "next_offset": self.next_offset,
            "segments": len(self.segments),
            "size_bytes": sum(s.size for s in self.segments),
            "first_offset": self.segments[0].base_offset if self.segments else 0,
        }

    def close(self) -> None:
        for seg in self.segments:
            seg.close_handles()
