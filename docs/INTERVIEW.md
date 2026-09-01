# pylog — Code Walkthrough & Interview Guide

> **Folder:** `pylog/` + `Desktop/projects/pylog`
> **One line:** A **Kafka-style durable, partitioned, replicated commit log** with full **Raft consensus** — pure stdlib Python.

## 30-second pitch

> "pylog is a Kafka-inspired commit log I built bottom-up. It starts with a durable **append-only segmented log** with CRC + crash recovery, adds topics/partitions + consumer groups, and replicates via **Raft** — leader election, log replication with matching/safety, PreVote + CheckQuorum. It does ~150k appends/sec and 5-node election in ~150 ms virtual clock, 60 tests."

## Big picture

```
Producer ─► Broker (topics → partitions) ─► append-only segmented log (durable)
 │ │ CRC + crash recovery
Consumer ◄─────┘ ▼
 Raft: elect → replicate → commit
```

Layers: **durable log → broker → replication → Raft** `ARCHITECTURE.md:5`

## Module map

- `pylog/log.py:27` — Commitment: `PLG1` magic, `len+crc`, sparse index `log.py:103`, `recover:112` truncates torn tail, `append_many:323` group commit
- `pylog/broker.py` — topics/partitions `crc32(key)`, consumer groups `commit`, retention
- `pylog/net.py` / `api.py:10` — length-prefixed JSON, `HttpGateway` dashboard `DASHBOARD_HTML:10` + `/metrics`
- `pylog/raft.py:146` — `RaftNode` transport-agnostic `handle:354` + `tick:226`, `DurableState:115` fsync, `RaftLog:26` `merge:88`
- `pylog/durable_log.py:31` — Raft entries in CommitLog `term prefix + payload`, invariant `disk.next_offset == last_index`
- `pylog/sim.py:27` — virtual clock `heapq`, `crash/restart/partition` `sim.py:100`, same `RaftNode` code

## Q&A

**Q: Why append-only?** Sequential I/O fast, total order for replay — log is source of truth.

**Q: Crash mid-write?** `read_record:50` CRC fails → `recover:132` `CorruptRecord` break → `truncate(good_size):142` — lose last torn record only.

**Q: Raft election?** Random 150-300ms `raft.py:212`, PreVote `prevote:355` denies if leader alive `361`, vote once per term `voted_for:391`, majority wins `maybe_win:300`.

**Q: Log consistency?** `AppendEntries` `prev_index/term` `merge:88` rejects on mismatch with `conflict_hint:92`, leader backs up `next_index:459`, commit only current-term `advance_commit:577` §5.4.2 + no-op `become_leader:319`.

**Q: Deterministic test?** `sim.py:133` virtual clock events, fault injection, `run_until:157` — 5-node election ~150 ms virtual, no flakes, `test_jepsen.py` 5×200 random ops.

See `docs/WALKTHROUGH.md` for line-by-line `log.py`.
