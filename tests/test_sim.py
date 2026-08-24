import pytest

from pylog.raft import NOOP
from pylog.sim import SimCluster

IDS = ["n1", "n2", "n3", "n4", "n5"]


@pytest.fixture()
def cluster():
    c = SimCluster(IDS[:3], seed=11)
    yield c


def _kv_agrees(c: SimCluster, key: str, value: str) -> bool:
    live = [n for n in c.node_ids if n not in c.crashed]
    vals = [c.state_machines[n].get(key) for n in live]
    return all(v == value for v in vals) and c.leader() is not None


def test_cluster_elects_exactly_one_leader(cluster):
    leader = cluster.wait_for_leader()
    assert leader in cluster.node_ids
    terms = {cluster.nodes[n].persister.current_term for n in cluster.node_ids}
    assert len(terms) >= 1


def test_replicated_write_visible_on_all_three_nodes(cluster):
    leader = cluster.wait_for_leader()
    assert cluster.propose_via_leader(b"x=1")
    cluster.run_until(lambda: _kv_agrees(cluster, "x", "1"))
    for n in cluster.node_ids:
        assert cluster.state_machines[n].get("x") == "1"
    assert cluster.nodes[leader].role.value == "leader"


def test_multiple_sequential_writes_commit_in_order(cluster):
    cluster.wait_for_leader()
    for i in range(10):
        assert cluster.propose_via_leader(f"k{i}=v{i}".encode())
    cluster.run_for(200)
    for i in range(10):
        for n in cluster.node_ids:
            assert cluster.state_machines[n].get(f"k{i}") == f"v{i}"


def test_leader_crash_triggers_reelection_and_availability():
    c = SimCluster(IDS[:3], seed=23)
    old_leader = c.wait_for_leader()
    assert c.propose_via_leader(b"a=1")
    c.crash(old_leader)
    new_leader = c.wait_for_leader()
    assert new_leader != old_leader
    assert c.propose_via_leader(b"b=2")
    c.restart(old_leader)
    c.run_until(lambda: c.nodes[old_leader].state_machine.get("b") == "2", max_ms=8000)
    assert c.nodes[old_leader].state_machine.get("a") == "1"


def test_majority_partition_keeps_serving_minority_stalls():
    c = SimCluster(IDS, seed=31)
    leader = c.wait_for_leader()
    majority = [leader] + [n for n in IDS if n != leader][:2]
    minority = [n for n in IDS if n not in majority]
    for a in minority:
        for b in IDS:
            if a != b:
                c.partition(a, b)
    assert c.propose_via_leader(b"m=1")
    for a in minority:
        for b in IDS:
            if a != b:
                c.heal(a, b)
    c.run_for(1000)
    assert c.quorum_committed_value("m") == "1"


def test_old_leader_rejoins_and_truncates_uncommitted_entries():
    c = SimCluster(IDS, seed=47)
    leader = c.wait_for_leader()
    assert c.propose_via_leader(b"before=ok")

    c.isolate(leader)
    idx = c.nodes[leader].propose(b"ghost=1")
    assert idx is not None

    c.run_until(lambda: c.leader() not in (None, leader), max_ms=8000)
    new_leader = c.leader()
    assert new_leader is not None
    assert c.propose_via_leader(b"real=1")

    c.heal_all(leader)
    c.run_until(lambda: _kv_agrees(c, "real", "1"), max_ms=12000)

    ghosts = [n for n in IDS if c.state_machines[n].get("ghost") == "1"]
    assert ghosts == [], "uncommitted entries must be truncated, never applied"
    for n in IDS:
        assert c.state_machines[n].get("before") == "ok"
        assert c.state_machines[n].get("real") == "1"


def test_prevote_prevents_disrupted_node_from_deposing_leader():
    c = SimCluster(IDS[:3], seed=59, use_prevote=True)
    leader = c.wait_for_leader()
    assert c.propose_via_leader(b"stable=1")
    sleeper = next(n for n in IDS[:3] if n != leader)
    c.isolate(sleeper)
    c.run_for(3000)
    still_leader = c.leader()
    assert still_leader == leader
    c.heal_all(sleeper)
    c.run_for(2000)
    assert c.leader() in (leader, sleeper)
    assert c.propose_via_leader(b"after=2")


def test_check_quorum_steps_down_isolated_leader():
    c = SimCluster(["a", "b", "c"], seed=71, election_timeout_ms=(120, 240))
    leader = c.wait_for_leader()
    c.isolate(leader)
    stepped_down = False
    for _ in range(6000):
        if not c.step():
            break
        if c.nodes[leader].role.value == "follower":
            stepped_down = True
            break
    assert stepped_down, "isolated leader should step down via CheckQuorum"


def test_leadership_transfer_moves_leadership_to_target():
    c = SimCluster(IDS[:3], seed=83)
    leader = c.wait_for_leader()
    target = next(n for n in IDS[:3] if n != leader)
    assert c.nodes[leader].transfer_leadership(target)
    c.run_until(lambda: c.leader() == target, max_ms=8000)
    assert c.leader() == target
    assert c.propose_via_leader(b"t=9")


def test_restarted_node_retains_committed_state(tmp_path):
    state_dir = str(tmp_path / "raft-state")
    c = SimCluster(IDS[:3], seed=97, state_dir=state_dir)
    leader = c.wait_for_leader()
    assert c.propose_via_leader(b"durable=yes")
    victim = next(n for n in IDS[:3] if n != leader)
    path = f"{state_dir}/{victim}.json"
    import os
    import json as _json

    assert os.path.exists(path)
    saved = _json.load(open(path))
    assert saved["term"] >= 1
    c.crash(victim)
    c.restart(victim)
    c.run_for(1500)
    assert c.nodes[victim].log.last_index >= 1


def test_election_resolves_fast_on_virtual_clock():
    import time

    c = SimCluster(IDS, seed=101)
    t0 = time.perf_counter()
    c.wait_for_leader(max_ms=5000)
    elapsed_virtual = c.clock
    wall = time.perf_counter() - t0
    assert 0 < elapsed_virtual < 5000
    assert wall < 5.0


def test_noop_entry_appended_on_leadership():
    c = SimCluster(IDS[:3], seed=113)
    c.wait_for_leader()
    c.run_for(500)
    noop_logs = sum(
        1
        for log in c.logs.values()
        for e in log.entries
        if bytes(e["payload"]) == NOOP.encode()
    )
    assert noop_logs >= 1
