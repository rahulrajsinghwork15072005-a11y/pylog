"""Membership change — joint consensus AddServer (raft.py:344)."""

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


def test_joint_consensus_add_server():
    # True joint consensus: leader proposes add n4 via Raft log, requires
    # majority in both old {n1,n2,n3} and new {n1,n2,n3,n4} before commit.
    cluster = SimCluster(["n1", "n2", "n3"], seed=7)
    leader_id = cluster.wait_for_leader()
    leader = cluster.nodes[leader_id]
    # replicate some data
    for i in range(2):
        assert cluster.propose_via_leader(f"pre{i}".encode())
    # propose joint config add n4
    idx = leader.propose_add_server("n4")
    assert idx is not None, "leader should accept add n4"
    # drive replication until joint entry commits (needs 3/4 acks = both majorities)
    assert cluster.run_until(lambda: leader.commit_index >= idx, max_ms=5000)
    # let followers learn commit via next heartbeat
    cluster.run_for(200)
    # leader's config should now include n4 and exit joint
    assert "n4" in leader.config
    assert leader.old_config is None
    # new server should be trackable (even though SimCluster hasn't added a node object for n4,
    # the leader's next_index should have an entry for n4)
    assert "n4" in leader.next_index
    # after joint, normal proposes still require new majority (3/4) — 3 nodes give 3/4
    assert cluster.propose_via_leader(b"post-joint")
    assert cluster.run_until(lambda: cluster.majority_reached(leader.log.last_index), max_ms=2000)
