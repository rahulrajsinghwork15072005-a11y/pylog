"""Live Raft demo: election, replication, leader crash, re-election."""

import time

from pylog.server import RaftClusterRunner


def main() -> None:
    runner = RaftClusterRunner(count=3)
    ports = [n.bound_port for n in runner.nodes]
    print(f"booted raft cluster on ports {ports}")

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and runner.leader() is None:
        time.sleep(0.05)
    leader = runner.leader()
    assert leader, "no leader elected"
    print(f"leader elected: {leader.node_id} term={leader.node.persister.current_term}")

    for i in range(5):
        idx = leader.node.propose(f"key{i}=value{i}".encode())
        time.sleep(0.2)
        print(f"proposed key{i}=value{i} at index {idx}")
    time.sleep(0.5)
    for n in runner.nodes:
        role = n.node.role.value
        kv = n.state_machine.data
        print(f"  {n.node_id}: role={role:<8} commit={n.node.commit_index} kv={kv}")

    victim = next(n for n in runner.nodes if n is not leader)
    print(f"\ncrashing the leader's peer {victim.node_id}; cluster keeps serving")
    victim.stop()
    idx = leader.node.propose(b"after-crash=yes")
    time.sleep(1.0)
    for n in runner.nodes:
        if n._running:
            print(f"  {n.node_id}: role={n.node.role.value:<8} kv={n.state_machine.data}")
    print(f"wrote after-crash=yes at index {idx}")

    print("\nstopping")
    try:
        runner.stop()
    except Exception:
        pass


if __name__ == "__main__":
    main()
