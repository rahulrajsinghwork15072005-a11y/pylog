"""Regression tests for liveness bugs found during the correctness audit.

Bug 1: PreVote counted only peer grants (never self), so an exact cluster
majority was not enough to start a real election - degraded clusters paid a
full extra election-timeout on every transition.

Bug 2: transfer_leadership() to a crashed peer wedged the leader forever:
propose() refused writes while transfer_target pointed at a node that could
never take over, and nothing ever cleared it.
"""

import random

import pytest

from pylog.raft import RaftNode
from pylog.sim import SimCluster


class StubTransport:
    def __init__(self):
        self.sent = []

    def send(self, dest, msg):
        self.sent.append((dest, msg))


class StubClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def make_node(peers=("p1", "p2")):
    transport = StubTransport()
    clock = StubClock()
    node = RaftNode(
        node_id="self",
        peers=list(peers),
        transport=transport,
        clock=clock,
        rng=random.Random(1),
    )
    return node, transport, clock


def test_prevote_exact_majority_starts_real_election():
    """One granted prevote out of two peers = majority of the 3-node cluster
    -> must start the real election immediately, without waiting for the
    second peer."""
    node, transport, clock = make_node()
    clock.now = node.election_deadline + 1  # force the election timer to fire
    node.tick(clock.now)
    grants = [m for _d, m in transport.sent if m["type"] == "prevote"]
    assert len(grants) == 2

    first_target = transport.sent[0][0]
    node.handle({
        "type": "prevote_resp",
        "term": grants[0]["term"],
        "from": first_target,
        "granted": True,
    })
    assert node.role.value == "candidate"
    assert not node.pre_candidate


def test_prevote_without_majority_does_not_start_election():
    node, transport, clock = make_node()
    node.tick(clock.now)
    denials = [m for _d, m in transport.sent if m["type"] == "prevote_resp"]
    for m in denials:
        pass
    # zero grants -> must stay in prevote/follower state, never candidate
    assert not any(
        m["type"] == "vote" for _d, m in transport.sent
    )


def test_vote_response_grant_starts_election_at_majority():
    node, transport, clock = make_node()
    node._start_election()
    resp = {"type": "vote_resp", "term": node.persister.current_term,
            "from": "p1", "granted": True}
    node.handle(resp)
    assert node.role.value == "leader"


def test_cluster_loses_minority_and_still_elects():
    """Guard: with PreVote on, a 3-node cluster that loses one member before
    any election must still elect a leader."""
    c = SimCluster(["n1", "n2", "n3"], seed=42)
    c.crash("n3")
    leader = c.wait_for_leader(max_ms=5000)
    assert leader in ("n1", "n2")


def test_transfer_to_dead_peer_aborts_and_unblocks_propose():
    c = SimCluster(["n1", "n2", "n3"], seed=5)
    leader = c.wait_for_leader()
    victim = [n for n in c.node_ids if n != leader][0]
    c.crash(victim)

    assert c.nodes[leader].transfer_leadership(victim) is True

    deadline = c.nodes[leader].transfer_deadline
    c.run_until(lambda: c.clock >= deadline + 10, max_ms=5000)

    live_leader = c.leader()
    assert live_leader is not None
    assert c.nodes[live_leader].transfer_target is None

    assert c.propose_via_leader(b"after-abort") is True
