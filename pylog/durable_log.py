"""Raft log durability backed by the CommitLog.

Every raft entry is stored as a CommitLog record whose payload is an 8-byte
big-endian term prefix followed by the caller payload, so raft index ``i`` lives
at CommitLog offset ``i - 1`` and gets the same CRC / segment / crash-recovery
guarantees as any other pylog data. An in-memory mirror is rebuilt from disk on
open and kept exactly coherent: after every mutation the CommitLog is brought to
match the cache (appends batched via group-commit, conflict tails physically
truncated), so ``disk.next_offset == last_index`` is an invariant.
"""

from __future__ import annotations

import struct

from .log import CommitLog
from .raft import RaftLog

_TERM = struct.Struct(">Q")


def encode_entry_payload(term: int, payload) -> bytes:
    return _TERM.pack(term) + bytes(payload)


def decode_entry_payload(blob: bytes) -> tuple[int, bytes]:
    term = _TERM.unpack_from(blob, 0)[0]
    return term, blob[_TERM.size :]


class DurableRaftLog(RaftLog):
    def __init__(self, dir_path: str, max_segment_bytes: int = 64 * 1024 * 1024) -> None:
        super().__init__()
        self.disk = CommitLog(dir_path, max_segment_bytes=max_segment_bytes)
        self.entries = []
        self.first_index = 1
        for rec in self.disk.read_range(0, limit=self.disk.next_offset):
            term, payload = decode_entry_payload(rec.payload)
            self.entries.append({"index": rec.offset + 1, "term": term, "payload": payload})

    def append_entry(self, term: int, payload) -> dict:
        entry = {"index": self.last_index + 1, "term": term, "payload": payload}
        self.entries.append(entry)
        self.disk.append_many([encode_entry_payload(term, payload)])
        return entry

    def truncate_disk_through(self, raft_index: int) -> None:
        """After a snapshot install: keep only suffix entries (raft index > raft_index)."""
        self.disk.truncate_from(raft_index)

    def merge(self, prev_index: int, prev_term: int, incoming: list[dict]):
        """AppendEntries against cache + disk, keeping both byte-identical."""
        if prev_index > self.last_index:
            return False, 0, self.last_index + 1
        local_term = self.term_at(prev_index)
        if local_term != prev_term:
            hint = self.conflict_term_first_index(local_term, prev_index)
            return False, 0, hint

        cut = None
        for k, inc in enumerate(incoming):
            idx = prev_index + 1 + k
            local = self.term_at(idx)
            if local == -1 or local != inc["term"]:
                cut = idx
                break

        changed = False
        if cut is not None:
            self.entries = [e for e in self.entries if e["index"] < cut]
            self.disk.truncate_from(cut - 1)
            changed = True

        existing_last = self.last_index
        to_add = []
        for inc in incoming:
            if inc["index"] > existing_last:
                entry = {
                    "index": existing_last + 1 + len(to_add),
                    "term": inc["term"],
                    "payload": inc.get("payload"),
                }
                self.entries.append(entry)
                to_add.append(entry)

        if to_add:
            payloads = [encode_entry_payload(e["term"], e["payload"]) for e in to_add]
            self.disk.append_many(payloads)
            changed = True
        _ = changed

        match = prev_index + len(incoming)
        return True, match, 0

    @property
    def last_index(self) -> int:
        return self.entries[-1]["index"] if self.entries else 0

    def stats(self) -> dict:
        return {
            "raft_last_index": self.last_index,
            "disk_records": self.disk.next_offset,
            "disk_bytes": self.disk.stats()["size_bytes"],
            "segments": len(self.disk.segments),
        }
