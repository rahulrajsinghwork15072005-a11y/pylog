# pylog

A Kafka-style **durable, partitioned, replicated commit log** built from scratch in
pure-stdlib Python — with a full **Raft consensus** layer whose log is backed by the
same crash-safe CommitLog that powers the broker.

> **Zero dependencies.** TCP sockets + threading + a custom HTTP server.
> Deterministic cluster simulator with a virtual clock — distributed behaviour tested
> without sleeps or flakes. Runs as one process or as N independent OS processes/hosts.

```
commit-log appends/sec          |    180,485
commit-log group-commit/sec     |  1,489,089   (batches of 256, single write syscall)
broker produce/sec              |     ~45,000
raft election (virtual clock)   |      <300 ms simulated · <1 s wall
multi-process raft cluster      |      verified: 3 OS processes, full heartbeat mesh
```

## Features

**Durable log** (`pylog/log.py`)
- Append-only segmented design (`00000000000000000000.log`), size-rolled
- CRC32 per record; crash recovery replays and **truncates torn tails**
- Sparse index sidecar — O(log n) seeks, never a whole-file scan
- `append_many()` group commit: one syscall (+one optional fsync) per batch
- Physical truncation at any offset (`truncate_from`) for replication repair

**Broker** (`pylog/broker.py`)
- Topics → partitions; routing by `crc32(key)` (sticky per-key order) or round-robin
- Consumer groups with fsync'd committed offsets; lag metric
- Retention sweeping by age and partition byte budget

**Networking** (`pylog/net.py`, `pylog/api.py`)
- Length-prefixed JSON framing; thread-per-connection server with handler isolation
- Thread-safe pooled client connections (mutex-guarded request/response)
- HTTP REST gateway: produce / consume / commit / stats / health + live dashboard

**Raft consensus** (`pylog/raft.py`) — transport-agnostic state machine
- Randomised elections with up-to-date vote restriction (snapshot-aware)
- AppendEntries: log-matching check, conflict-term back-off, majority commit
- §5.4.2 safety: only current-term entries advance commit; auto no-op on election
- **PreVote**, **CheckQuorum**, **leadership transfer**
- **Snapshots**: fsync'd JSON state-machine checkpoints + log compaction +
  InstallSnapshot RPC for lagging followers (with disk re-sync)
- **Lease-based linearizable reads**: leader serves reads only while a majority
  acknowledged within one election timeout
- term/votedFor persisted atomically (tmp + fsync + rename) before use

**Durable Raft log** (`pylog/durable_log.py`)
- Raft entries stored *in* the CommitLog: 8-byte big-endian term prefix + payload,
  raft index `i` ↔ CommitLog offset `i - 1`
- Invariant: `disk.next_offset == raft last_index` after every mutation
- Conflict truncation physically rewrites segments; restart rebuilds from disk
- Crash-recovery guarantees apply to the replicated log itself

**Deterministic simulator** (`pylog/sim.py`)
- Virtual discrete-event clock; crash/restart/partition/isolate fault injection
- Same RaftNode code as production; identical seed ⇒ identical run

## Quick start

```bash
pip install -e .
python -m pytest          # 52 tests
python demo.py            # durable log walkthrough incl. torn-write recovery
python raft_demo.py       # live 3-node durable raft cluster over real sockets
python cli.py replicate   # election → replicate → snapshot → leader crash → re-election
python bench.py           # benchmarks above
```

Single-node broker:

```bash
python cli.py serve --data-dir ./data --http-port 8787 --tcp-port 8788
# dashboard: http://127.0.0.1:8787/
python cli.py produce orders "pizza" --key u1
python cli.py consume orders --follow
python cli.py status
```

Real multi-process cluster (one process = one raft member; put each on its own host
by changing the addresses):

```bash
python cli.py raft-node --id n1 --peer n1=127.0.0.1:9711 --peer n2=127.0.0.1:9712 --peer n3=127.0.0.1:9713 --state-dir ./n1
python cli.py raft-node --id n2 --peer n1=127.0.0.1:9711 --peer n2=127.0.0.1:9712 --peer n3=127.0.0.1:9713 --state-dir ./n2
python cli.py raft-node --id n3 --peer n1=127.0.0.1:9711 --peer n2=127.0.0.1:9712 --peer n3=127.0.0.1:9713 --state-dir ./n3
```

Library:

```python
from pylog import CommitLog
from pylog.durable_log import DurableRaftLog

log = CommitLog("./data")
off = log.append(b"hello")
print(log.read(off).payload)

raft_log = DurableRaftLog("./raft-wal")     # survives crashes like any pylog data
```

## Design

See [ARCHITECTURE.md](ARCHITECTURE.md) for layer-by-layer design and the reasoning
behind every choice (record framing, sparse indexing, recovery semantics, Raft safety
rules, snapshot protocol, read linearizability).

## The core idea

Everything in Kafka and Raft rests on one abstraction: an **append-only log**.
Writes only ever append → sequential I/O is fast and events get a total order.
Each record carries a CRC32; on restart the log replays and truncates at the first
checksum failure, so a crash mid-write can never corrupt committed data. Raft
replicates this same log across nodes and commits entries once a majority stores
them — and in pylog, Raft's own log *is* that same durable structure.
