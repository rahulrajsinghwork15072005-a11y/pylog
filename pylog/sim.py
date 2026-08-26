"""Deterministic Raft cluster simulation on a virtual clock.

Time only advances when the simulation says so: message deliveries and timer
firings are events on a discrete timeline, so elections, crashes and partitions
reproduce identically on every run. The same RaftNode code runs here unchanged.
"""

from __future__ import annotations

import heapq
import os
import random

from .raft import DurableState, NOOP, RaftLog, RaftNode
from .statemachine import KVStore


class SimTransport:
    def __init__(self, cluster: SimCluster) -> None:
        self.cluster = cluster

    def send(self, dest: str, msg: dict) -> None:
        c = self.cluster
        heapq.heappush(c.queue, (c.clock + c.latency_ms, c.next_seq(), dest, msg))


class SimCluster:
    def __init__(
        self,
        node_ids: list[str],
        seed: int = 7,
        election_timeout_ms=(150, 300),
        heartbeat_interval_ms=50,
        latency_ms=1.0,
        state_dir: str | None = None,
        use_prevote=True,
    ) -> None:
        self.node_ids = list(node_ids)
        self.rng = random.Random(seed)
        self.clock = 0.0
        self.queue: list = []
        self._seq = 0
        self.latency_ms = latency_ms
        self.partitioned: set[frozenset] = set()
        self.crashed: set[str] = set()
        self.state_dir = state_dir
        self.use_prevote = use_prevote
        self.election_timeout_ms = election_timeout_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms

        self.nodes: dict[str, RaftNode] = {}
        self.logs: dict[str, RaftLog] = {n: RaftLog() for n in node_ids}
        self.state_machines: dict[str, KVStore] = {}
        self.applied: list[tuple] = []

        transport = SimTransport(self)
        self.transport = transport
        for n in self.node_ids:
            sm = KVStore()
            self.state_machines[n] = sm
            node = self._build_node(n, transport, sm)
            self.nodes[n] = node

    def _record_apply(self, node_id: str, index: int, payload, result) -> None:
        self.applied.append((node_id, index, bytes(payload), result))

    def _state_path(self, node_id: str):
        if not self.state_dir:
            return None
        os.makedirs(self.state_dir, exist_ok=True)
        return os.path.join(self.state_dir, f"{node_id}.json")

    def _build_node(self, node_id: str, transport, sm) -> RaftNode:
        node = RaftNode(
            node_id=node_id,
            peers=self.node_ids,
            transport=transport,
            clock=lambda: self.clock,
            rng=random.Random(self.rng.getrandbits(32)),
            persister=DurableState(self._state_path(node_id)),
            state_machine=sm,
            election_timeout_ms=self.election_timeout_ms,
            heartbeat_interval_ms=self.heartbeat_interval_ms,
            use_prevote=self.use_prevote,
            on_apply=lambda nid, idx, payload, res: self._record_apply(nid, idx, payload, res),
        )
        saved_log = self.logs[node_id]
        node.log = saved_log
        node.commit_index = saved_log.last_index
        node.applied_index = 0
        node._apply_committed()
        return node

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    # ------------------------------------------------------------ fault injection

    def crash(self, node_id: str) -> None:
        self.crashed.add(node_id)

    def restart(self, node_id: str) -> None:
        self.crashed.discard(node_id)
        old = self.nodes[node_id]
        # simulate loss of volatile state: create a FRESH state machine so
        # the full-log replay in _apply_committed starts from a clean slate.
        sm = KVStore()
        self.state_machines[node_id] = sm
        node = self._build_node(node_id, self.transport, sm)
        node.persister.current_term = old.persister.current_term
        node.persister.voted_for = old.persister.voted_for
        self.nodes[node_id] = node

    def partition(self, a: str, b: str) -> None:
        self.partitioned.add(frozenset((a, b)))

    def heal(self, a: str, b: str) -> None:
        self.partitioned.discard(frozenset((a, b)))

    def isolate(self, node_id: str) -> None:
        for other in self.node_ids:
            if other != node_id:
                self.partition(node_id, other)

    def heal_all(self, node_id: str) -> None:
        for other in self.node_ids:
            if other != node_id:
                self.heal(node_id, other)

    # ---------------------------------------------------------------- event loop

    def step(self) -> bool:
        """Advance one event (message delivery or timer firing)."""
        live = [n for n in self.node_ids if n not in self.crashed]
        msg_due = self.queue[0][0] if self.queue else float("inf")
        timer_due = min((self.nodes[n].next_event_due() for n in live), default=float("inf"))
        if timer_due == float("inf") and msg_due == float("inf"):
            return False

        if timer_due <= msg_due:
            due_node = min(live, key=lambda n: self.nodes[n].next_event_due())
            self.clock = max(self.clock, self.nodes[due_node].next_event_due())
            self.nodes[due_node].tick()
            return True

        due, _, dest, msg = heapq.heappop(self.queue)
        self.clock = max(self.clock, due)
        if dest in self.crashed:
            return True
        if dest not in self.nodes:
            return True
        src = msg.get("candidate") or msg.get("leader") or msg.get("from")
        if src and frozenset((src, dest)) in self.partitioned:
            return True
        self.nodes[dest].handle(msg)
        return True

    def run_until(self, predicate, max_ms: float = 30000.0) -> bool:
        start = self.clock
        while not predicate():
            if not self.step():
                return False
            if self.clock - start > max_ms:
                raise TimeoutError(f"sim did not converge within {max_ms}ms of virtual time")
        return True

    def run_for(self, ms: float) -> None:
        end = self.clock + ms
        while self.clock < end:
            if not self.step():
                break

    # ------------------------------------------------------------------ helpers

    def leader(self):
        leaders = {
            n.id
            for n in self.nodes.values()
            if n.role.value == "leader" and n.id not in self.crashed
        }
        return leaders.pop() if len(leaders) == 1 else None

    def wait_for_leader(self, max_ms: float = 10000.0) -> str:
        self.run_until(lambda: self.leader() is not None, max_ms=max_ms)
        leader = self.leader()
        assert leader is not None, "no leader emerged"
        return leader

    def quorum_committed_value(self, key: str):
        values = [
            self.state_machines[n].get(key)
            for n in self.node_ids
            if n not in self.crashed and self.state_machines[n].get(key) is not None
        ]
        if len(values) >= (len(self.node_ids)) // 2 + 1:
            return values[0]
        return None

    def propose_via_leader(self, payload: bytes, max_ms: float = 10000.0) -> bool:
        leader = self.leader()
        if leader is None:
            return False
        idx = self.nodes[leader].propose(payload)
        if idx is None:
            return False
        self.run_until(lambda: self.majority_reached(idx), max_ms=max_ms)
        return self.majority_reached(idx)

    def majority_reached(self, index: int) -> bool:
        count = sum(
            1
            for n in self.node_ids
            if n not in self.crashed and self.nodes[n].commit_index >= index
        )
        return count >= len(self.node_ids) // 2 + 1

    def noop_marker_count(self) -> int:
        total = 0
        for log in self.logs.values():
            total += sum(1 for e in log.entries if bytes(e["payload"]) == NOOP.encode())
        return total

