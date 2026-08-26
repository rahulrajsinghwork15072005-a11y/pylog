"""Full Raft consensus: leader election, log replication, PreVote, CheckQuorum,
leadership transfer, and fsync'd durable state.

The node is transport-agnostic: it emits message dicts through an injected
``transport.send(dest_id, msg)`` callable and reads wall-clock time through an
injected ``clock() -> float_ms``. That makes it drivable by real TCP threads *or*
by a deterministic virtual-clock simulator without changing any logic.
"""

from __future__ import annotations

import json
import os
import random
from enum import Enum

NOOP = "__raft_noop__"


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftLog:
    """Ordered entries ``{"index": i, "term": t, "payload": bytes-like}``;
    entry indexes are contiguous, 1-based (index 0 is a virtual sentinel, term 0)."""

    def __init__(self) -> None:
        self.entries: list[dict] = []
        self.first_index = 1
        self.snapshot_last_index = 0
        self.snapshot_last_term = 0

    @property
    def last_index(self) -> int:
        return self.entries[-1]["index"] if self.entries else 0

    def last_term(self) -> int:
        return self.entries[-1]["term"] if self.entries else 0

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        pos = index - self.first_index
        if 0 <= pos < len(self.entries):
            return self.entries[pos]["term"]
        return -1

    def entries_from(self, index: int, limit: int | None = None):
        out = self.entries[max(0, index - self.first_index) :]
        return out[:limit] if limit else out

    def get(self, index: int) -> dict | None:
        pos = index - self.first_index
        if 0 <= pos < len(self.entries):
            return self.entries[pos]
        return None

    def append_entry(self, term: int, payload) -> dict:
        entry = {"index": self.last_index + 1, "term": term, "payload": payload}
        self.entries.append(entry)
        return entry

    def compact(self, last_included_index: int, last_included_term: int) -> bool:
        """Drop entries at or below the snapshot point; keep contiguity above it."""
        if last_included_index < self.first_index - 1:
            return False
        if last_included_index >= self.last_index and self.entries:
            return False
        if not self.entries and last_included_index < self.first_index - 1:
            return False
        self.snapshot_last_index = last_included_index
        self.snapshot_last_term = last_included_term
        self.entries = [e for e in self.entries if e["index"] > last_included_index]
        self.first_index = last_included_index + 1
        return True

    def conflict_term_first_index(self, term: int, from_index: int) -> int:
        i = from_index
        while i > self.first_index - 1 and self.term_at(i) != term:
            i -= 1
        while i > 1 and self.term_at(i - 1) == term:
            i -= 1
        return max(1, i)

    def merge(self, prev_index: int, prev_term: int, incoming: list[dict]):
        """Raft AppendEntries core. Returns (ok, match_index, conflict_hint)."""
        if prev_index > self.last_index:
            return False, 0, self.last_index + 1
        if self.term_at(prev_index) != prev_term:
            hint = self.conflict_term_first_index(self.term_at(prev_index), prev_index)
            return False, 0, hint
        insert_at = None
        for k, inc in enumerate(incoming):
            idx = prev_index + 1 + k
            local = self.term_at(idx)
            if local == -1:
                insert_at = k
                break
            if local != inc["term"]:
                del self.entries[idx - self.first_index :]
                insert_at = k
                break
        if insert_at is not None:
            for k in range(insert_at, len(incoming)):
                inc = dict(incoming[k])
                inc["index"] = prev_index + 1 + k
                self.entries.append(inc)
        match = prev_index + len(incoming)
        return True, match, 0


class DurableState:
    """term + voted_for, written atomically with fsync before use."""

    def __init__(self, path: str | None = None) -> None:
        self.path = path
        if path:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.current_term = 0
        self.voted_for = None
        self._load()

    def _load(self) -> None:
        if self.path and os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            self.current_term = data["term"]
            self.voted_for = data.get("voted_for")

    def save(self, current_term: int, voted_for) -> None:
        self.current_term = current_term
        self.voted_for = voted_for
        if not self.path:
            return
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"term": current_term, "voted_for": voted_for}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        transport,
        clock,
        rng: random.Random | None = None,
        persister: DurableState | None = None,
        state_machine=None,
        election_timeout_ms=(150, 300),
        heartbeat_interval_ms=50,
        quorum_check_ms=None,
        max_entries_per_append=256,
        use_prevote=True,
        on_apply=None,
        log=None,
        snapshot_dir=None,
        auto_snapshot_every=None,
    ) -> None:
        self.id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.config = set([self.id] + self.peers)
        self.old_config: set[str] | None = None
        self.joint_index = 0
        self.transport = transport
        self.clock = clock
        self.rng = rng or random.Random(hash(node_id) & 0xFFFF)
        self.persister = persister or DurableState()
        self.state_machine = state_machine
        self.on_apply = on_apply
        self.election_min, self.election_max = election_timeout_ms
        self.heartbeat_interval = heartbeat_interval_ms
        self.quorum_check_interval = quorum_check_ms if quorum_check_ms is not None else (
            election_timeout_ms[1] * 2
        )
        self.max_batch = max_entries_per_append
        self.use_prevote = use_prevote
        self.log: RaftLog = log if log is not None else RaftLog()
        self.snapshot_dir = snapshot_dir
        self.auto_snapshot_every = auto_snapshot_every
        self.last_snapshot: dict | None = None

        self.role = Role.FOLLOWER
        self.leader_id = None
        self.pre_candidate = False
        self.commit_index = 0
        self.applied_index = 0

        self.election_deadline = self._reset_election_deadline(initial=True)
        self.votes = set()
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.last_peer_ack: dict[str, float] = {}
        self.last_leader_contact = 0.0
        self.next_heartbeat = 0.0
        self.next_quorum_check = 0.0
        self.transfer_target = None
        self.transfer_deadline = 0.0
        self._prevote_term = 0

        if self.snapshot_dir:
            os.makedirs(self.snapshot_dir, exist_ok=True)
            self._load_snapshot_file()

    # ------------------------------------------------------------------ timers

    def _reset_election_deadline(self, initial=False) -> float:
        now = self.clock()
        base = self.rng.uniform(self.election_min, self.election_max)
        if initial:
            base += self.rng.uniform(0, 50)
        self.election_deadline = now + base
        return self.election_deadline

    def _has_majority(self, voters: set[str]) -> bool:
        """Joint consensus: if in joint, need majority in both old and new."""
        if self.old_config is not None:
            old_needed = len(self.old_config) // 2 + 1
            new_needed = len(self.config) // 2 + 1
            old_votes = len(voters & self.old_config)
            new_votes = len(voters & self.config)
            return old_votes >= old_needed and new_votes >= new_needed
        return len(voters) > len(self.config) // 2

    def next_event_due(self) -> float:
        if self.role == Role.LEADER:
            dues = [self.next_heartbeat, self.next_quorum_check]
            if self.transfer_target:
                dues.append(self.transfer_deadline)
            return min(dues)
        return self.election_deadline

    def tick(self, now: float | None = None) -> None:
        now = self.clock() if now is None else now
        if self.role == Role.LEADER:
            if self.transfer_target and now >= self.transfer_deadline:
                # target never took over (crashed / too far behind):
                # abort the transfer so this leader resumes proposing
                self.transfer_target = None
                self.transfer_deadline = 0.0
                self._send_all_appends()
            if now >= self.next_heartbeat:
                self._send_all_appends()
                self.next_heartbeat = now + self.heartbeat_interval
            if now >= self.next_quorum_check:
                self._check_quorum(now)
                self.next_quorum_check = now + self.quorum_check_interval
            return
        if now >= self.election_deadline:
            self._start_prevote_or_election()

    # ------------------------------------------------------------- elections

    def _progress(self) -> tuple[int, int]:
        """(last_term, last_index) snapshot-aware, for vote freshness checks."""
        li = self.log.last_index
        lt = self.log.last_term()
        if li == 0 and self.log.snapshot_last_index > 0:
            return self.log.snapshot_last_term, self.log.snapshot_last_index
        return lt, li

    def _start_prevote_or_election(self) -> None:
        if self.use_prevote and not self.pre_candidate:
            self.pre_candidate = True
            self._prevote_term = self.persister.current_term + 1
            # count ourselves, exactly like a real election: a majority of
            # the CLUSTER must grant, not every remaining peer
            self.votes = {self.id}
            last_term, last_index = self._progress()
            for p in self.peers:
                self.transport.send(
                    p,
                    {
                        "type": "prevote",
                        "term": self._prevote_term,
                        "candidate": self.id,
                        "last_index": last_index,
                        "last_term": last_term,
                    },
                )
            self._reset_election_deadline()
            return
        self.pre_candidate = False
        self._start_election()

    def _start_election(self) -> None:
        self.persister.save(self.persister.current_term + 1, self.id)
        self.role = Role.CANDIDATE
        self.leader_id = None
        self.votes = {self.id}
        self._reset_election_deadline()
        last_term, last_index = self._progress()
        for p in self.peers:
            self.transport.send(
                p,
                {
                    "type": "vote",
                    "term": self.persister.current_term,
                    "candidate": self.id,
                    "last_index": last_index,
                    "last_term": last_term,
                },
            )
        if not self.peers or len(self.votes) > (len(self.peers) + 1) // 2:
            self._become_leader()

    def _maybe_win(self) -> None:
        if self.role == Role.CANDIDATE and self._has_majority(self.votes):
            self._become_leader()

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.id
        self.pre_candidate = False
        self.transfer_target = None
        self.transfer_deadline = 0.0
        now = self.clock()
        self.match_index = {}
        self.next_index = {}
        for p in self.peers:
            self.match_index[p] = 0
            self.next_index[p] = self.log.last_index + 1
            self.last_peer_ack[p] = now
        if self.state_machine is not None:
            self.propose(NOOP.encode())
        self._send_all_appends()
        self.next_heartbeat = now + self.heartbeat_interval
        self.next_quorum_check = now + self.quorum_check_interval

    def _step_down(self, term: int) -> None:
        if term > self.persister.current_term:
            self.persister.save(term, None)
        self.role = Role.FOLLOWER
        self.pre_candidate = False
        self.transfer_target = None
        self.transfer_deadline = 0.0
        self._reset_election_deadline()

    # ------------------------------------------------------------ client API

    def propose(self, payload: bytes):
        if self.role != Role.LEADER or self.transfer_target is not None:
            return None
        entry = self.log.append_entry(self.persister.current_term, payload)
        index = entry["index"]
        self._advance_commit()
        self._send_all_appends()
        return index

    def transfer_leadership(self, target: str) -> bool:
        if self.role != Role.LEADER or target not in self.peers:
            return False
        self.transfer_target = target
        self.transfer_deadline = self.clock() + (self.election_max * 2)
        self.transport.send(target, {"type": "timeout_now", "term": self.persister.current_term})
        return True

    def propose_add_server(self, new_id: str) -> int | None:
        if self.role != Role.LEADER or new_id in self.config or self.old_config is not None:
            return None
        new_servers = set(self.config) | {new_id}
        return self._propose_config(new_servers)

    def propose_remove_server(self, rm_id: str) -> int | None:
        if self.role != Role.LEADER or rm_id not in self.config or rm_id == self.id or self.old_config is not None:
            return None
        if len(self.config) <= 2:
            return None
        new_servers = set(self.config) - {rm_id}
        return self._propose_config(new_servers)

    def _propose_config(self, new_servers: set[str]) -> int | None:
        # Joint consensus: old → joint (old ∪ new) → new
        old = set(self.config)
        joint = old | new_servers
        # Enter joint if not already
        if self.old_config is None:
            self.old_config = old
            self.config = joint
            self.joint_index = self.log.last_index + 1
            # initialise tracking for new peers
            for p in new_servers:
                if p != self.id and p not in self.next_index:
                    self.next_index[p] = self.log.last_index + 1
                    self.match_index[p] = 0
                    self.last_peer_ack[p] = self.clock()
            self.peers = sorted([p for p in self.config if p != self.id])
        payload = b"__cfg:" + json.dumps({"servers": sorted(new_servers)}).encode()
        entry = self.log.append_entry(self.persister.current_term, payload)
        idx = entry["index"]
        self._advance_commit()
        self._send_all_appends()
        return idx

    # -------------------------------------------------------------- inbound

    def handle(self, msg: dict) -> None:
        getattr(self, "_on_" + msg["type"])(msg)

    def _on_prevote(self, m) -> None:
        my_term = self.persister.current_term
        mine = self._progress()
        up_to_date = (m["last_term"], m["last_index"]) >= mine
        leader_alive = (self.clock() - self.last_leader_contact) < (self.election_max * 2)
        grant = m["term"] > my_term and up_to_date and not (
            self.role == Role.FOLLOWER and self.leader_id is not None and leader_alive
        )
        self.transport.send(
            m["candidate"],
            {"type": "prevote_resp", "term": m["term"], "from": self.id, "granted": grant},
        )

    def _on_prevote_resp(self, m) -> None:
        if not self.pre_candidate or m["term"] != self._prevote_term:
            return
        if m["granted"]:
            self.votes.add(m["from"])
        if self._has_majority(self.votes):
            self.pre_candidate = False
            self._start_election()
        elif not m["granted"]:
            self._reset_election_deadline()

    def _on_vote(self, m) -> None:
        my_term = self.persister.current_term
        if m["term"] < my_term:
            denial = {"type": "vote_resp", "term": my_term, "from": self.id, "granted": False}
            self.transport.send(m["candidate"], denial)
            return
        if m["term"] > my_term:
            self._step_down(m["term"])
        mine = self._progress()
        up_to_date = (m["last_term"], m["last_index"]) >= mine
        can_vote = self.persister.voted_for in (None, m["candidate"])
        grant = can_vote and up_to_date
        if grant:
            self.persister.save(self.persister.current_term, m["candidate"])
            self._reset_election_deadline()
        reply = {
            "type": "vote_resp",
            "term": self.persister.current_term,
            "from": self.id,
            "granted": grant,
        }
        self.transport.send(m["candidate"], reply)

    def _on_vote_resp(self, m) -> None:
        if self.role != Role.CANDIDATE or m["term"] != self.persister.current_term:
            return
        if m["granted"]:
            self.votes.add(m["from"])
            self._maybe_win()

    def _on_append(self, m) -> None:
        my_term = self.persister.current_term
        if m["term"] < my_term:
            rejection = {
                "type": "append_resp",
                "term": my_term,
                "from": self.id,
                "success": False,
                "match": 0,
            }
            self.transport.send(m["leader"], rejection)
            return
        if m["term"] > my_term or self.role != Role.FOLLOWER:
            self._step_down(m["term"])
        self.role = Role.FOLLOWER
        self.leader_id = m["leader"]
        self.last_leader_contact = self.clock()
        self._reset_election_deadline()

        ok, match, conflict = self.log.merge(m["prev_index"], m["prev_term"], m["entries"])
        if ok and m["commit"] > self.commit_index:
            self.commit_index = min(m["commit"], self.log.last_index)
            self._apply_committed()
        self.transport.send(
            m["leader"],
            {
                "type": "append_resp",
                "term": self.persister.current_term,
                "from": self.id,
                "success": ok,
                "match": match if ok else 0,
                "conflict": conflict,
            },
        )

    def _on_append_resp(self, m) -> None:
        if self.role != Role.LEADER or m["term"] != self.persister.current_term:
            if m["term"] > self.persister.current_term:
                self._step_down(m["term"])
            return
        peer = m["from"]
        self.last_peer_ack[peer] = self.clock()
        if m["success"]:
            self.match_index[peer] = max(self.match_index[peer], m["match"])
            self.next_index[peer] = self.match_index[peer] + 1
            self._advance_commit()
        else:
            hint = m.get("conflict") or (self.next_index[peer] - 1)
            self.next_index[peer] = max(1, min(self.next_index[peer] - 1, hint))
            self._send_append(peer)

    def _write_snapshot_file(self, idx: int, term: int, state) -> None:
        path = self._snapshot_path(idx)
        if not path:
            return
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {"last_included_index": idx, "last_included_term": term, "state_machine": state},
                f,
            )
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def _on_timeout_now(self, m) -> None:
        if m["term"] >= self.persister.current_term and self.role != Role.LEADER:
            self._start_election()

    def _on_install_snapshot(self, m) -> None:
        if m["term"] < self.persister.current_term:
            rejection = {
                "type": "append_resp",
                "term": self.persister.current_term,
                "from": self.id,
                "success": False,
                "match": 0,
            }
            self.transport.send(m["leader"], rejection)
            return
        if m["term"] > self.persister.current_term or self.role != Role.FOLLOWER:
            self._step_down(m["term"])
        self.leader_id = m["leader"]
        self.last_leader_contact = self.clock()
        self._reset_election_deadline()

        idx = m["last_included_index"]
        term = m["last_included_term"]
        already_have = (
            self.log.term_at(idx) == term
            or self.log.snapshot_last_index >= idx
        )

        if not already_have:
            if self.state_machine is not None:
                self.state_machine.restore(m["data"])
            self.log.entries = [e for e in self.log.entries if e["index"] > idx]
            self.log.first_index = idx + 1
            self.log.snapshot_last_index = idx
            self.log.snapshot_last_term = term
            self._write_snapshot_file(idx, term, m["data"])
            self.last_snapshot = {
                "last_included_index": idx,
                "last_included_term": term,
                "data": m["data"],
            }
            resync = getattr(self.log, "truncate_disk_through", None)
            if resync is not None:
                resync(idx)

        self.commit_index = max(self.commit_index, idx)
        if not already_have:
            self.applied_index = max(self.applied_index, idx)

        ack = {
            "type": "append_resp",
            "term": self.persister.current_term,
            "from": self.id,
            "success": True,
            "match": idx,
        }
        self.transport.send(m["leader"], ack)

    # ------------------------------------------------------------- leader ops

    def _send_all_appends(self) -> None:
        # keep streaming AppendEntries to the transfer target too: it can only
        # take over once its log has caught up; the deadline in tick() aborts
        # the transfer if that never happens.
        for p in self.peers:
            self._send_append(p)

    def _send_append(self, peer: str) -> None:
        ni = self.next_index.get(peer, self.log.last_index + 1)
        if ni < self.log.first_index:
            snap = self.last_snapshot or {}
            self.transport.send(
                peer,
                {
                    "type": "install_snapshot",
                    "term": self.persister.current_term,
                    "leader": self.id,
                    "last_included_index": snap.get(
                        "last_included_index", self.log.snapshot_last_index
                    ),
                    "last_included_term": snap.get(
                        "last_included_term", self.log.snapshot_last_term
                    ),
                    "data": snap.get("data", {}),
                },
            )
            return
        prev = ni - 1
        self.transport.send(
            peer,
            {
                "type": "append",
                "term": self.persister.current_term,
                "leader": self.id,
                "prev_index": prev,
                "prev_term": self.log.term_at(prev),
                "entries": self.log.entries_from(ni, self.max_batch),
                "commit": self.commit_index,
            },
        )

    def _advance_commit(self) -> None:
        # Joint consensus: candidate must be on majority in both old and new when in joint
        for candidate in range(self.log.last_index, self.commit_index, -1):
            if self.log.term_at(candidate) != self.persister.current_term:
                continue
            # count how many in config have this index
            have = 1  # leader itself has it if candidate <= last_index
            for p, mi in self.match_index.items():
                if mi >= candidate:
                    have_config = 1
                else:
                    have_config = 0
                # for joint we need to track separately, but we can count per config below
                _ = have_config
            if self.old_config is not None:
                old_needed = len(self.old_config) // 2 + 1
                new_needed = len(self.config) // 2 + 1
                old_have = (1 if self.id in self.old_config else 0)
                new_have = 1  # leader in new config (config includes leader)
                for p, mi in self.match_index.items():
                    if mi >= candidate:
                        if p in self.old_config:
                            old_have += 1
                        if p in self.config:
                            new_have += 1
                # also account leader if not in config? leader is always in both when in joint (since joint = old ∪ new)
                if old_have >= old_needed and new_have >= new_needed:
                    self.commit_index = candidate
                    self._apply_committed()
                    return
            else:
                # single config
                needed = len(self.config) // 2 + 1
                have = 1
                for p, mi in self.match_index.items():
                    if mi >= candidate and p in self.config:
                        have += 1
                if have >= needed:
                    self.commit_index = candidate
                    self._apply_committed()
                    return

    def _apply_committed(self) -> None:
        # Handle config entries even when state_machine is None
        while self.applied_index < self.commit_index:
            nxt = self.applied_index + 1
            entry = self.log.get(nxt)
            if entry is None:
                if self.state_machine is None:
                    self.applied_index = self.commit_index
                    return
                break
            payload = entry["payload"]
            is_cfg = payload and bytes(payload).startswith(b"__cfg:")
            if is_cfg:
                try:
                    data = json.loads(bytes(payload)[6:].decode())
                    new_servers = set(data.get("servers", []))
                    if new_servers:
                        self.config = set(new_servers)
                        self.peers = sorted([p for p in self.config if p != self.id])
                        # initialise tracking for new peers
                        for p in self.config:
                            if p != self.id and p not in self.next_index:
                                self.next_index[p] = self.log.last_index + 1
                                self.match_index[p] = 0
                                self.last_peer_ack[p] = self.clock()
                        # cleanup removed peers
                        for p in list(self.next_index.keys()):
                            if p not in self.config:
                                self.next_index.pop(p, None)
                                self.match_index.pop(p, None)
                                self.last_peer_ack.pop(p, None)
                        self.old_config = None
                        self.joint_index = 0
                except Exception:
                    pass
                self.applied_index = nxt
                continue
            if self.state_machine is None:
                self.applied_index = nxt
                continue
            result = None
            if payload and not bytes(payload).startswith(b"__raft_noop__"):
                result = self.state_machine.apply(payload)
            self.applied_index = nxt
            if self.on_apply:
                self.on_apply(self.id, entry["index"], entry["payload"], result)
        if (
            self.auto_snapshot_every
            and self.snapshot_dir
            and self.applied_index - (self.last_snapshot or {}).get("last_included_index", 0)
            >= self.auto_snapshot_every
        ):
            self.take_snapshot()

    def _check_quorum(self, now: float) -> None:
        ack_window = self.election_max * 2
        if self.old_config is not None:
            old_alive = (1 if self.id in self.old_config else 0) + sum(
                1 for p in self.old_config if p != self.id and now - self.last_peer_ack.get(p, -1e18) <= ack_window
            )
            new_alive = 1 + sum(
                1 for p in self.config if p != self.id and now - self.last_peer_ack.get(p, -1e18) <= ack_window
            )
            if old_alive <= len(self.old_config) // 2 or new_alive <= len(self.config) // 2:
                self._step_down(self.persister.current_term)
            return
        alive = 1 + sum(
            1 for p in self.config if p != self.id and now - self.last_peer_ack.get(p, -1e18) <= ack_window
        )
        if alive <= len(self.config) // 2:
            self._step_down(self.persister.current_term)

    # ------------------------------------------------------------- snapshots

    def _snapshot_path(self, last_included_index: int) -> str | None:
        if not self.snapshot_dir:
            return None
        return os.path.join(self.snapshot_dir, f"snapshot-{last_included_index:020d}.json")

    def _load_snapshot_file(self) -> None:
        if not self.snapshot_dir or not self.state_machine:
            return
        candidates = sorted(os.listdir(self.snapshot_dir), reverse=True)
        for name in candidates:
            if not name.startswith("snapshot-") or not name.endswith(".json"):
                continue
            with open(os.path.join(self.snapshot_dir, name), encoding="utf-8") as f:
                data = json.load(f)
            idx = data["last_included_index"]
            term = data["last_included_term"]
            self.state_machine.restore(data["state_machine"])
            self.log.compact(idx, term)
            resync = getattr(self.log, "truncate_disk_through", None)
            if resync is not None:
                resync(idx)
            self.commit_index = max(self.commit_index, idx)
            self.applied_index = max(self.applied_index, idx)
            self.last_snapshot = {
                "last_included_index": idx,
                "last_included_term": term,
                "data": data["state_machine"],
            }
            return

    def take_snapshot(self) -> dict | None:
        """Snapshot the state machine and compact the log up to the applied index."""
        if self.state_machine is None or self.applied_index == 0:
            return None
        idx = self.applied_index
        term = self.log.term_at(idx)
        if term < 0 and self.log.snapshot_last_index == idx:
            term = self.log.snapshot_last_term
        state = self.state_machine.snapshot()
        self._write_snapshot_file(idx, term, state)
        self.last_snapshot = {"last_included_index": idx, "last_included_term": term, "data": state}
        if isinstance(self.log, RaftLog):
            self.log.compact(idx, term)
        return self.last_snapshot

    # -------------------------------------------------------- linearizable reads

    def can_serve_linearizable_read(self) -> bool:
        """Leader-lease read: safe only while a majority acknowledged recently (joint-aware)."""
        if self.role != Role.LEADER:
            return False
        now = self.clock()
        window = self.election_min
        if self.old_config is not None:
            old_fresh = sum(
                1 for p in self.old_config if p != self.id and now - self.last_peer_ack.get(p, -1e18) <= window
            ) + (1 if self.id in self.old_config else 0)
            new_fresh = sum(
                1 for p in self.config if p != self.id and now - self.last_peer_ack.get(p, -1e18) <= window
            ) + 1
            return old_fresh > len(self.old_config) // 2 and new_fresh > len(self.config) // 2
        fresh = sum(
            1 for p in self.config if p != self.id and now - self.last_peer_ack.get(p, -1e18) <= window
        )
        return 1 + fresh > len(self.config) // 2

    # ------------------------------------------------------------------ info

    def info(self) -> dict:
        return {
            "id": self.id,
            "role": self.role.value,
            "term": self.persister.current_term,
            "leader": self.leader_id,
            "log_len": self.log.last_index,
            "commit": self.commit_index,
            "applied": self.applied_index,
        }

