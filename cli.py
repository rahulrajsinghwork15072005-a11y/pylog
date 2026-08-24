"""pylog command-line interface: serve, produce, consume, status, replicate."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from pylog.api import HttpGateway
from pylog.broker import Broker
from pylog.net import FrameClient, FrameServer


def _add_common(p, default_dir):
    p.add_argument("--data-dir", default=default_dir)


def cmd_serve(args) -> int:
    broker = Broker(args.data_dir)
    gateway = HttpGateway(broker, port=args.http_port)
    tcp = FrameServer("127.0.0.1", args.tcp_port, handler=lambda req: _tcp_dispatch(broker, req))
    gateway.start()
    tcp.start()
    print(f"pylog serving  http://127.0.0.1:{gateway.bound_port}  (dashboard: /)")
    print(f"pylog serving  tcp://127.0.0.1:{tcp.bound_port}")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        tcp.stop()
        gateway.stop()
        broker.close()
    return 0


def _tcp_dispatch(broker: Broker, req: dict) -> dict:
    op = req.get("op")
    if op == "produce":
        return broker.produce(req["topic"], req["payload"].encode(), key=req.get("key"))
    if op == "fetch":
        msgs = broker.fetch(req["topic"], req.get("partition", 0), req.get("after", -1))
        return {"messages": msgs}
    if op == "stats":
        return broker.stats()
    return {"error": f"unknown op {op!r}"}


def cmd_produce(args) -> int:
    client = FrameClient("127.0.0.1", args.tcp_port)
    req = {
        "op": "produce",
        "topic": args.topic,
        "payload": args.payload,
        "key": args.key,
    }
    res = client.call(req)
    print(json.dumps(res))
    client.close()
    return 0


def cmd_consume(args) -> int:
    client = FrameClient("127.0.0.1", args.tcp_port)
    after = -1
    shown = 0
    try:
        while shown < args.limit:
            req = {
                "op": "fetch",
                "topic": args.topic,
                "partition": args.partition,
                "after": after,
            }
            res = client.call(req)
            msgs = res.get("messages", [])
            if not msgs:
                if shown == 0 and not args.follow:
                    print("(no new messages)")
                if not args.follow:
                    break
                time.sleep(0.5)
                continue
            for m in msgs:
                print(f"[{m['offset']}] {m['payload']}")
            after = msgs[-1]["offset"]
            shown += len(msgs)
    finally:
        client.close()
    return 0


def cmd_status(args) -> int:
    client = FrameClient("127.0.0.1", args.tcp_port)
    stats = client.call({"op": "stats"})
    print(json.dumps(stats, indent=2))
    client.close()
    return 0


def cmd_replicate(args) -> int:
    import shutil

    from pylog.server import RaftClusterRunner

    if args.fresh and args.state_dir and os.path.exists(args.state_dir):
        shutil.rmtree(args.state_dir)
    count = max(3, args.nodes if args.nodes % 2 == 1 else args.nodes + 1)
    resumed = args.state_dir and os.path.exists(args.state_dir) and bool(os.listdir(args.state_dir))
    runner = RaftClusterRunner(count=count, state_dir=args.state_dir or None)
    print(
        f"raft cluster: {count} durable nodes, state in '{args.state_dir}/' "
        f"({'resumed from disk' if resumed else 'fresh start'})"
    )

    def propose_stable(payload: bytes, attempts: int = 60) -> bool:
        for _ in range(attempts):
            leader_node = runner.leader()
            if leader_node is not None and leader_node.node.propose(payload) is not None:
                return True
            time.sleep(0.1)
        return False

    def live_nodes():
        return [n for n in runner.nodes if not getattr(n, "stopped", False)]

    def cluster_stable(timeout: float = 15.0) -> str | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            live = live_nodes()
            live_ids = {n.node_id for n in live}
            claimed = {n.node.leader_id for n in live}
            if len(claimed) == 1:
                leader_id = next(iter(claimed))
                if leader_id in live_ids and any(
                    n.node_id == leader_id and n.node.role.value == "leader" for n in live
                ):
                    return leader_id
            time.sleep(0.05)
        return None

    def show(reason: str) -> None:
        print(f"\n-- {reason} --")
        for n in runner.nodes:
            if getattr(n, "stopped", False):
                print(f"  {n.node_id}: <crashed>")
                continue
            info = n.node.info()
            kv = n.state_machine.data
            try:
                disk = n.node.log.stats()["disk_records"]
                log_part = f"log={info['log_len']}/disk={disk}"
            except AttributeError:
                log_part = f"log={info['log_len']}"
            role = info["role"]
            print(
                f"  {n.node_id}: {role:<8} term={info['term']} "
                f"commit={info['commit']} {log_part} kv={kv}"
            )

    leader_id = cluster_stable()
    if leader_id is None:
        print("no stable leader elected")
        return 1
    show(f"cluster formed (leader: {leader_id})")

    for i in range(5):
        ok = propose_stable(f"key{i}=value{i}".encode())
        if not ok:
            print(f"warning: write key{i} did not commit")
    cluster_stable()
    show("replicated writes")

    old_leader = runner.leader()
    res = old_leader.node.take_snapshot()
    included = res["last_included_index"] if res else "-"
    print(f"\nsnapshot taken on {old_leader.node_id}: last_included_index={included}")

    print(f"\ncrashing the LEADER ({old_leader.node_id}) ...")
    old_leader.stop()
    old_leader.stopped = True
    time.sleep(0.5)
    new_leader_id = cluster_stable(timeout=20.0)
    propose_stable(b"after-crash=yes")
    time.sleep(0.3)
    show(f"re-elected '{new_leader_id}' and kept writing")
    print("\nstopping cluster")
    for n in runner.nodes:
        if not getattr(n, "stopped", False):
            n.stop()
    return 0


def cmd_raft_node(args) -> int:
    """Run ONE raft node (for multi-process / multi-host clusters)."""
    import json as _json

    from pylog.server import RaftServerNode

    peers = {}
    for spec in args.peer:
        nid, addr = spec.split("=", 1)
        host, port = addr.rsplit(":", 1)
        peers[nid] = (host, int(port))
    if args.id not in peers:
        print(f"error: --id {args.id} must appear in --peer list", file=sys.stderr)
        return 2

    node = RaftServerNode(
        args.id,
        peers,
        state_dir=args.state_dir or None,
        election_timeout_ms=(args.election_min, args.election_max),
    )
    node.start()
    peer_ports = {nid: addr[1] for nid, addr in peers.items()}
    print(f"raft node {args.id} listening on {node.bound_port}; peers={_json.dumps(peer_ports)}")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
    return 0


def build_parser(default_dir="pylog-data") -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="pylog", description="Kafka-style log with Raft replication"
    )
    sub = root.add_subparsers(dest="cmd", required=True)

    serve = sub.add_parser("serve", help="run broker (TCP + HTTP + dashboard)")
    _add_common(serve, default_dir)
    serve.add_argument("--http-port", type=int, default=8787)
    serve.add_argument("--tcp-port", type=int, default=8788)
    serve.set_defaults(fn=cmd_serve)

    produce = sub.add_parser("produce", help="produce a message to a topic")
    produce.add_argument("topic")
    produce.add_argument("payload")
    produce.add_argument("--key", default=None)
    produce.add_argument("--tcp-port", type=int, default=8788)
    produce.set_defaults(fn=cmd_produce)

    consume = sub.add_parser("consume", help="consume messages from a topic partition")
    consume.add_argument("topic")
    consume.add_argument("--partition", type=int, default=0)
    consume.add_argument("--limit", type=int, default=100)
    consume.add_argument("--follow", action="store_true")
    consume.add_argument("--tcp-port", type=int, default=8788)
    consume.set_defaults(fn=cmd_consume)

    status = sub.add_parser("status", help="broker stats")
    status.add_argument("--tcp-port", type=int, default=8788)
    status.set_defaults(fn=cmd_status)

    repl = sub.add_parser("replicate", help="demo: live raft cluster election + failover")
    repl.add_argument("--nodes", type=int, default=3)
    repl.add_argument("--state-dir", default="pylog-raft-state")
    repl.add_argument("--no-fresh", dest="fresh", action="store_false", help="resume prior state")
    repl.set_defaults(fn=cmd_replicate, fresh=True)

    node_cmd = sub.add_parser("raft-node", help="run a single raft node process")
    node_cmd.add_argument("--id", required=True)
    node_cmd.add_argument(
        "--peer",
        action="append",
        required=True,
        help="id=host:port for each cluster member (repeat for all nodes)",
    )
    node_cmd.add_argument("--state-dir", default="pylog-raft-state")
    node_cmd.add_argument("--election-min", type=int, default=300)
    node_cmd.add_argument("--election-max", type=int, default=600)
    node_cmd.set_defaults(fn=cmd_raft_node)
    return root


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
