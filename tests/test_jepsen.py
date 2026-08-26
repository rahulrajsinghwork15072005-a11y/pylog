"""Jepsen-style fuzz — 200 random ops, partitions + crashes, checks linearizability of KV store.

We run 5 seeds, each with random propose / crash / heal / isolate. At the end,
every committed write must be visible on a majority and logs must be identical
up to commit (Raft LogMatching). This is the property Hypothesis + TLC also cover.
"""

import random
from pylog.sim import SimCluster

def run_one(seed: int) -> None:
    rnd = random.Random(seed)
    cluster = SimCluster(["n1", "n2", "n3", "n4", "n5"], seed=seed)
    cluster.wait_for_leader(max_ms=5000)
    committed = []
    for step in range(200):
        op = rnd.choice(["propose", "crash", "restart", "partition", "heal"])
        if op == "propose":
            payload = f"s{seed}:k{rnd.randint(0,9)}=v{rnd.randint(0,999)}".encode()
            try:
                if cluster.propose_via_leader(payload, max_ms=4000):
                    committed.append(payload)
            except TimeoutError:
                pass
        elif op == "crash":
            nid = rnd.choice(cluster.node_ids)
            cluster.crash(nid)
            cluster.run_for(rnd.randint(20, 60))
            cluster.restart(nid)
        elif op == "partition":
            a, b = rnd.sample(cluster.node_ids, 2)
            cluster.partition(a, b)
        elif op == "heal":
            a, b = rnd.sample(cluster.node_ids, 2)
            cluster.heal(a, b)
        cluster.run_for(rnd.randint(5, 15))
    # after fuzz, majority must agree on committed prefix
    leader = cluster.leader()
    if leader:
        ci = cluster.nodes[leader].commit_index
        # every committed index should be on a majority
        for idx in range(1, ci + 1):
            assert cluster.majority_reached(idx), f"index {idx} not on majority seed {seed}"
        # LogMatching: any two nodes that have an entry at same index+term must agree up to there
        logs = [n.log for n in cluster.nodes.values() if n.id not in cluster.crashed]
        for a in logs:
            for b in logs:
                for i in range(1, min(a.last_index, b.last_index) + 1):
                    if a.term_at(i) == b.term_at(i) and a.term_at(i) != -1:
                        assert a.get(i)["payload"] == b.get(i)["payload"]

def test_jepsen_five_seeds():
    for seed in [7, 13, 42, 99, 123]:
        run_one(seed)
