"""Randomized fault-injection fuzzing: partitions, crashes, restarts and
proposals interleaved randomly across seeds. Asserts Raft SAFETY properties:

  1. Election safety   - at most one leader per term (sampled continuously)
  2. Log matching      - after healing, every node agrees on committed prefix
  3. State machine     - all converged replicas have identical KV snapshots
"""

import os
import random
import sys

import pytest

sys_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from pylog.sim import SimCluster


def run_fuzz(seed: int, rounds: int = 140):
    rng = random.Random(seed)
    ids = ["n1", "n2", "n3", "n4", "n5"]
    c = SimCluster(ids, seed=seed)
    leader_seen_per_term = {}
    majority_committed = []          # payloads that reached majority commit

    def note_leaders():
        """Sample only LIVE nodes - crashed ones freeze in stale roles."""
        for n in ids:
            if n in c.crashed:
                continue
            nd = c.nodes[n]
            term = nd.persister.current_term
            if nd.role.value == "leader":
                prev = leader_seen_per_term.setdefault(term, nd.id)
                assert prev == nd.id, (
                    f"two leaders in term {term}: {prev} and {nd.id}")

    def try_propose():
        leader = c.leader()
        if leader is None:
            return
        payload = f"k{rng.randint(0, 999)}={rng.randint(0, 9999)}".encode()
        idx = c.nodes[leader].propose(payload)
        if idx is not None:
            try:
                c.run_until(lambda: c.majority_reached(idx), max_ms=3000)
                if c.majority_reached(idx):
                    majority_committed.append(payload)
            except TimeoutError:
                pass  # expected during partitions

    actions = ["partition", "heal_one", "crash", "restart", "propose",
               "propose", "propose", "idle"]

    for _round in range(rounds):
        act = rng.choice(actions)
        if act == "partition" and len(ids) >= 2:
            a, b = rng.sample(ids, 2)
            c.partition(a, b)
        elif act == "heal_one":
            n = rng.choice(ids)
            c.heal_all(n)
        elif act == "crash":
            victim = rng.choice(ids)
            others_alive = sum(1 for x in ids if x not in c.crashed and x != victim)
            if others_alive * 2 > len(ids):
                c.crash(victim)
        elif act == "restart":
            if c.crashed:
                c.restart(rng.choice(sorted(c.crashed)))
        elif act == "propose":
            try_propose()
        note_leaders()
        c.run_for(rng.uniform(20, 80))

    # ---- heal everything, converge fully, verify safety ----
    for n in ids:
        c.heal_all(n)
    for n in sorted(c.crashed):
        c.restart(n)

    deadline = c.clock + 30000
    marker_idx = None
    while c.clock < deadline:
        c.run_for(200)
        leader = c.leader()
        if leader is None:
            continue
        idx = c.nodes[leader].propose(b"final-marker")
        if idx is None:
            continue
        c.run_until(lambda: c.majority_reached(idx), max_ms=5000)
        # wait until EVERY live node has applied this marker
        target_commit = idx
        def all_applied():
            return all(
                c.nodes[n].applied_index >= target_commit
                for n in ids if n not in c.crashed
            )
        c.run_until(all_applied, max_ms=10000)
        marker_idx = idx
        break

    assert marker_idx is not None, "cluster never reconverged after heal"
    # settle: no more in-flight appends
    c.run_for(500)

    snaps = [c.state_machines[n].snapshot() for n in ids]
    for s in snaps[1:]:
        assert s == snaps[0], f"state machines diverged (seed {seed})"
    commits = {tuple(
        (e["index"], e["term"])
        for e in c.logs[n].entries[: c.nodes[n].commit_index]
    ) for n in ids}
    assert len(commits) == 1, f"committed prefixes diverged (seed {seed})"

    # all live nodes must have identical state machines (convergence proof)
    snaps = [c.state_machines[n].snapshot() for n in ids]
    for s in snaps[1:]:
        assert s == snaps[0], f"state machines diverged (seed {seed})"
    commits = {tuple(
        (e["index"], e["term"])
        for e in c.logs[n].entries[: c.nodes[n].commit_index]
    ) for n in ids}
    assert len(commits) == 1, f"committed prefixes diverged (seed {seed})"
    return len(majority_committed)


@pytest.mark.parametrize("seed", [1, 2, 3, 7, 42])
def test_partition_rejoin_safety(seed):
    committed = run_fuzz(seed)
    assert isinstance(committed, int)

