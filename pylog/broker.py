"""Broker: topics → partitions, key-hash routing, consumer groups, retention."""

from __future__ import annotations

import json
import os
import time
import zlib

from .log import CommitLog, Record


class Partition:
    def __init__(
        self,
        dir_path: str,
        max_segment_bytes: int,
        index_interval_bytes: int,
    ) -> None:
        self.log = CommitLog(
            dir_path,
            max_segment_bytes=max_segment_bytes,
            index_interval_bytes=index_interval_bytes,
        )

    @property
    def high_watermark(self) -> int:
        return self.log.next_offset

    @property
    def size_bytes(self) -> int:
        return self.log.stats()["size_bytes"]

    def append(self, payload: bytes, timestamp: int) -> int:
        return self.log.append(payload, timestamp=timestamp)

    def read(self, offset: int) -> Record | None:
        return self.log.read(offset)

    def read_after(self, offset: int, limit: int = 100) -> list[Record]:
        out = []
        pos = offset + 1 if offset >= 0 else 0
        while len(out) < limit:
            rec = self.log.read(pos)
            if rec is None:
                break
            out.append(rec)
            pos += 1
        return out

    def retention_sweep(
        self,
        now_ms: int,
        max_age_ms: int | None,
        max_total_bytes: int | None,
    ) -> int:
        removed = 0
        segments = self.log.segments
        if len(segments) > 1:
            expired_through = -1
            for i, seg in enumerate(segments[:-1]):
                if max_age_ms is not None and (now_ms - seg.max_timestamp) > max_age_ms:
                    expired_through = i
                else:
                    break
            if expired_through >= 0:
                cut = segments[expired_through].base_offset + 1
                removed += self.log.truncate_segments_before(cut)

        if max_total_bytes is not None:
            while len(self.log.segments) > 1:
                total = sum(s.size for s in self.log.segments)
                if total <= max_total_bytes:
                    break
                self.log.truncate_segments_before(self.log.segments[1].base_offset + 1)
                removed += 1
        return removed


class ConsumerGroup:
    def __init__(self, name: str, path: str) -> None:
        self.name = name
        self.path = path
        self.offsets: dict[str, int] = {}
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as f:
                self.offsets = {k: int(v) for k, v in json.load(f).items()}

    def commit(self, topic: str, partition: int, offset: int) -> None:
        key = f"{topic}/{partition}"
        if offset < self.offsets.get(key, -1):
            return
        self.offsets[key] = offset
        tmp = self.path + ".tmp"
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.offsets, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def committed(self, topic: str, partition: int) -> int:
        return self.offsets.get(f"{topic}/{partition}", -1)


class Broker:
    def __init__(
        self,
        data_dir: str,
        default_partitions: int = 3,
        max_segment_bytes: int = 128 * 1024 * 1024,
        index_interval_bytes: int = 4096,
        retention_max_age_ms: int | None = None,
        retention_max_partition_bytes: int | None = None,
    ) -> None:
        self.data_dir = data_dir
        self.default_partitions = default_partitions
        self.max_segment_bytes = max_segment_bytes
        self.index_interval_bytes = index_interval_bytes
        self.retention_max_age_ms = retention_max_age_ms
        self.retention_max_partition_bytes = retention_max_partition_bytes
        self.topics_dir = os.path.join(data_dir, "topics")
        self.groups_dir = os.path.join(data_dir, "groups")
        os.makedirs(self.topics_dir, exist_ok=True)
        os.makedirs(self.groups_dir, exist_ok=True)
        self.topics: dict[str, list[Partition]] = {}
        self.groups: dict[str, ConsumerGroup] = {}
        self._rr = 0
        for name in sorted(os.listdir(self.topics_dir)):
            pdir = os.path.join(self.topics_dir, name)
            if os.path.isdir(pdir):
                count = len([d for d in os.listdir(pdir) if d.startswith("p")])
                if count:
                    self._open_topic(name, count)

    def _open_topic(self, name: str, num_partitions: int) -> None:
        parts = []
        for i in range(num_partitions):
            parts.append(
                Partition(
                    os.path.join(self.topics_dir, name, f"p{i}"),
                    self.max_segment_bytes,
                    self.index_interval_bytes,
                )
            )
        self.topics[name] = parts

    def create_topic(self, name: str, num_partitions: int | None = None) -> None:
        if name in self.topics:
            raise ValueError(f"topic exists: {name}")
        n = num_partitions or self.default_partitions
        if n < 1:
            raise ValueError("num_partitions must be >= 1")
        self._open_topic(name, n)

    def produce(
        self, topic: str, payload: bytes, key: str | None = None, timestamp: int | None = None
    ):
        parts = self.topics.get(topic)
        if parts is None:
            self.create_topic(topic)
            parts = self.topics[topic]
        if key is not None:
            idx = zlib.crc32(key.encode("utf-8")) % len(parts)
        else:
            idx = self._rr % len(parts)
            self._rr += 1
        ts = timestamp if timestamp is not None else int(time.time() * 1000)
        offset = parts[idx].append(payload, ts)
        return {"topic": topic, "partition": idx, "offset": offset}

    def fetch(self, topic: str, partition: int, after_offset: int = -1, limit: int = 100):
        parts = self.topics.get(topic)
        if parts is None or not (0 <= partition < len(parts)):
            raise KeyError(f"no such topic/partition: {topic}[{partition}]")
        recs = parts[partition].read_after(after_offset, limit)
        return [
            {
                "offset": r.offset,
                "timestamp": r.timestamp,
                "payload": r.payload.decode("utf-8", "replace"),
            }
            for r in recs
        ]

    def group(self, name: str) -> ConsumerGroup:
        g = self.groups.get(name)
        if g is None:
            g = ConsumerGroup(name, os.path.join(self.groups_dir, f"{name}.json"))
            self.groups[name] = g
        return g

    def commit(self, group_name: str, topic: str, partition: int, offset: int) -> None:
        self.group(group_name).commit(topic, partition, offset)

    def lag(self, group_name: str, topic: str, partition: int) -> int:
        parts = self.topics.get(topic)
        if parts is None:
            return 0
        hw = parts[partition].high_watermark
        return max(0, hw - 1 - self.group(group_name).committed(topic, partition))

    def retention_sweep(self, now_ms: int | None = None) -> int:
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        removed = 0
        for parts in self.topics.values():
            for p in parts:
                removed += p.retention_sweep(
                    now, self.retention_max_age_ms, self.retention_max_partition_bytes
                )
        return removed

    def stats(self) -> dict:
        topics = {}
        for name, parts in self.topics.items():
            topics[name] = {
                "partitions": len(parts),
                "high_watermarks": [p.high_watermark for p in parts],
                "size_bytes": sum(p.size_bytes for p in parts),
            }
        return {
            "topics": topics,
            "groups": sorted(self.groups),
            "total_size_bytes": sum(t["size_bytes"] for t in topics.values()),
        }

    def close(self) -> None:
        for parts in self.topics.values():
            for p in parts:
                p.log.close()
