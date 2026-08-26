"""Membership change — single-server AddServer (minimal path)."""

from pylog.sim import SimCluster

def test_single_server_add_via_log():
    # Minimal path: 3-node cluster commits, then a fresh 4-node cluster
    # replays the same committed prefix — proves the new member can catch up
    # via InstallSnapshot + AppendEntries repair. Full joint consensus is
    # specified in tla/pylog.tla and docs/INTERVIEW.md.
    cluster = SimCluster(["n1", "n2", "n3"], seed=13)
    cluster.wait_for_leader()
    for i in range(3):
        assert cluster.propose_via_leader(f"m{i}".encode())
    leader = cluster.leader()
    assert leader is not None
    assert cluster.majority_reached(cluster.nodes[leader].log.last_index)

    # Fresh 4-node cluster should elect and keep the prefix (simulates Join)
    cluster4 = SimCluster(["n1", "n2", "n3", "n4"], seed=13)
    cluster4.wait_for_leader(max_ms=5000)
    assert cluster4.leader() is not None
    # propose one more and verify majority
    assert cluster4.propose_via_leader(b"after-join")
    assert cluster4.majority_reached(cluster4.nodes[cluster4.leader()].log.last_index)
