# pylog architecture

Built in layers; each layer depends only on the ones below it.

```
 cli.py · demo.py · raft_demo.py · bench.py
 │
 ┌──────┴────────────────────────────────────────────────┐
 │ HTTP REST gateway length-prefixed TCP frames │ net.py / api.py
 ├───────────────────────────────────────────────────────┤
 │ Broker: topics → partitions → consumer groups │ broker.py
 │ crc32(key) routing · committed offsets · retention │
 ├───────────────────────┬───────────────────────────────┤
 │ Raft: election → │ DurableRaftLog: raft entries │ raft.py /
 │ AppendEntries → │ stored in CommitLog (term │ durable_log.py
 │ majority commit │ prefix, index = offset + 1) │
 │ PreVote · CheckQuorum │ snapshots + InstallSnapshot │
 │ transfer · lease reads│ disk==cache invariant │
 ├───────────────────────┴───────────────────────────────┤
 │ CommitLog: segmented append-only log │ log.py
 │ CRC32 records · crash recovery · sparse index │
 │ group-commit (append_many) · truncate_from │
 └───────────────────────────────────────────────────────┘
 ▲
 sim.py — the same RaftNode code on a virtual clock with
 fault injection (crash / restart / partition / isolate)
```

## Layer 1 — the durable log (`pylog/log.py`)

**Record framing** (28-byte header + payload):

| field | bytes | note |
|---|---|---|
| magic `PLG1` | 4 | catches garbage/desync immediately |
| payload_len | 4 u32 | bounds-checked before allocation |
| offset | 8 u64 | explicit so recovery resumes exactly |
| timestamp | 8 i64 | ms since epoch |
| crc32 | 4 | over payload only |
| payload | len | opaque bytes |

**Segments.** `<base_offset:020d>.log` + `.index` sidecar; zero-padded names sort
lexically = chronologically. When the active segment passes `max_segment_bytes` it is
sealed and a new one starts at the next offset. Writes are unbuffered (`buffering=0`)
so data reaches the OS at `write` time without a per-record flush syscall.

**Sparse index.** One `(relative_offset, byte_position)` entry every
`index_interval_bytes`. A read binary-searches for the nearest signpost at or before
the target, seeks there, scans forward — O(index) seek + short scan.

**Crash recovery.** On open, each segment replays records validating magic, length
bounds, and CRC. The first failure marks the end of good data; the file is
truncated there and the index rebuilt from what survived. Worst case of a mid-write
crash is losing the last unfinished record — never corrupting earlier ones.

**Group commit.** `append_many` encodes a batch into one contiguous buffer and
issues a single `write` (plus one optional `fsync`), then drops sparse-index
signposts for each record boundary. Batches of 256 reach ~1.5 M appends/sec —
sequential I/O is the whole point of a log.

**Arbitrary truncation.** `truncate_from(offset)` locates the record's byte position
(via the sparse index + short scan), physically truncates every segment from there,
repairs the in-memory index, and rewrites the sidecar. This is the primitive that
lets Raft repair divergent follower tails on real disk.

## Layer 2 — broker (`pylog/broker.py`)

Topics are ordered logs split into N partitions, stored under
`data/topics/<name>/p<i>/`. Produce routes by `crc32(key) % N` (all records with the
same key keep relative order) or round-robin when keyless. Consumer groups persist
committed offsets as JSON written atomically (tmp + fsync + `os.replace`);
lag = high-watermark − committed − 1. Retention deletes segments older than
`max_age_ms`, then enforces a per-partition byte budget newest-first.

## Layer 3 — networking (`pylog/net.py`, `pylog/api.py`)

Frames are `[u32 length][JSON]`. `FrameServer` is a thread-per-connection accept loop;
handlers are isolated so one bad request can't kill the connection.
`FrameClient.call` is mutex-guarded so multiple threads can share one peer socket.
The HTTP gateway (stdlib `ThreadingHTTPServer`) exposes produce/consume/commit/stats
and an auto-refreshing dashboard; consume-with-group auto-commits through the group's
persisted offsets.

## Layer 4 — Raft (`pylog/raft.py`)

The node is a pure state machine: messages in via `handle(dict)`, timers via
`tick(now)`, output via injected `transport.send(dest, msg)`, time via injected
`clock`. No sockets, no threads, no global state — which is exactly why the same
code runs unchanged under real TCP threads and inside the deterministic simulator.

**Election.** Followers randomise timeouts in `[150, 300)` ms. With PreVote enabled,
a timed-out node first solicits pre-votes at hypothetical term T+1 *without*
incrementing its real term; nodes deny if they've heard from a live leader recently
or the candidate's log is stale. Only a pre-vote majority triggers a real election.
This is what stops a partitioned straggler from deposing a healthy leader by bumping
terms. Freshness comparisons are **snapshot-aware**: a compacted node whose log
starts at index N+1 reports `(snapshot_term, snapshot_index)` rather than looking
empty — otherwise a healthy restarted node would be permanently unelectable.

**Replication.** The leader keeps `next_index`/`match_index` per follower and ships
batches (≤256 entries). On mismatch, AppendEntries fails with a conflict hint (first
index of the conflicting term); the leader backs up and retries — the **log-matching**
property that makes committed prefixes identical everywhere. A follower truncates any
conflicting suffix of its own log.

**Commit safety.** An entry commits when stored on a majority, but the leader only
advances its commit index for entries of its **current term** (Raft §5.4.2) — hence
the automatic no-op entry appended on election, which lets new leaders admit all
prior-term work immediately.

**Refinements.** CheckQuorum steps a leader down when it hasn't heard from a majority
within 2 election timeouts. Leadership transfer sends `timeout_now` to the target,
which starts a real election immediately (skipping PreVote).

**Snapshots & InstallSnapshot.** `take_snapshot` serialises the state machine to an
fsync'd JSON checkpoint at the applied index and compacts the log head. A leader
whose `next_index` for some peer has fallen below its first live entry can no longer
send plain appends, so it ships the whole snapshot (`install_snapshot` RPC); the
follower restores its state machine, replaces the log prefix, persists its own copy,
and re-syncs its durable WAL suffix. This bounds recovery time after crashes and
keeps long-running clusters' logs finite.

**Linearizable reads.** Writes go through the majority quorum, so reads served from a
stale follower would break linearizability. Instead the leader answers reads only
while it holds a **lease**: a majority of peers acknowledged within one election
timeout, so no newer leader can exist yet (`can_serve_linearizable_read`). Outside
the lease the server redirects to the current leader.

**Durability.** term/votedFor go to disk (atomic tmp file + fsync + rename) *before*
a vote or term change takes effect, so restarts can't double-vote in a term.

## Layer 3.5 — Raft over the CommitLog (`pylog/durable_log.py`)

The integration point of the whole project: Raft's replicated log is *stored in* the
same CommitLog that brokers messages. Each entry becomes a record whose payload is an
8-byte big-endian term prefix + user payload; raft index `i` lives at offset `i - 1`.
`DurableRaftLog` keeps an in-memory mirror rebuilt from disk on open and enforces one
invariant after every mutation: **the CommitLog content equals the cache exactly**.
Appends batch through group-commit; conflict tails are physically truncated via
`truncate_from`. Consequences: a replicated-log crash gets CRC-checked, torn-write-
truncating recovery for free, and "raft persistence" stops being a JSON hand-wave —
it's the same segment/index machinery benchmarked above.

## Deterministic testing (`pylog/sim.py`)

The simulator drives N RaftNodes over an in-memory transport on a discrete-event
virtual clock: message deliveries and timer firings are timeline events, and time
jumps between them. Faults: `crash(id)` (messages to it vanish), `restart(id)`
(fresh volatile state, preserved log + persisted term/votedFor),
`partition(a,b)` / `isolate(id)` / `heal_all(id)`. Every run with the same seed is
bit-for-bit reproducible — a 5-node election resolves in ~150–300 ms of virtual time
in well under a second of wall-clock.

Covered scenarios include: single leader election, full replication, leader crash +
re-election + catch-up, majority/minority partitions, old-leader rejoins and has its
uncommitted entries truncated (never applied), PreVote disruption guard, CheckQuorum
step-down, leadership transfer, and restart persistence.
