"""Realtime Raft nodes over TCP sockets (threads), for the `replicate` mode."""

from __future__ import annotations

import base64
import os
import threading
import time

from .net import Disconnected, FrameClient, FrameServer
from .raft import DurableState, RaftNode
from .statemachine import KVStore

_B64_KEY = "__b64__"


def _encode_wire(obj):
    if isinstance(obj, bytes):
        return {_B64_KEY: base64.b64encode(obj).decode("ascii")}
    if isinstance(obj, dict):
        return {k: _encode_wire(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_encode_wire(v) for v in obj]
    return obj


def _decode_wire(obj):
    if isinstance(obj, dict):
        if _B64_KEY in obj and len(obj) == 1:
            return base64.b64decode(obj[_B64_KEY])
        return {k: _decode_wire(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decode_wire(v) for v in obj]
    return obj


def _ms() -> float:
    return time.monotonic() * 1000


class TcpTransport:
    """Sends raft messages to peer addresses over reusable frame connections."""

    def __init__(self, addresses: dict[str, tuple[str, int]]) -> None:
        self.addresses = addresses
        self._conns: dict[str, FrameClient] = {}
        self._lock = threading.Lock()

    def _conn(self, peer: str) -> FrameClient:
        conn = self._conns.get(peer)
        if conn is None:
            host, port = self.addresses[peer]
            conn = FrameClient(host, port, timeout=3.0)
            self._conns[peer] = conn
        return conn

    def send(self, peer: str, msg: dict) -> None:
        wire = _encode_wire(msg)
        try:
            self._conn(peer).call(wire)
        except (OSError, Disconnected, ValueError):
            with self._lock:
                old = self._conns.pop(peer, None)
            if old is not None:
                old.close()
                try:
                    self._conn(peer).call(wire)
                except (OSError, Disconnected):
                    pass


class RaftServerNode:
    def __init__(
        self,
        node_id: str,
        addresses: dict[str, tuple[str, int]],
        state_dir: str | None = None,
        election_timeout_ms=(300, 600),
        heartbeat_interval_ms=100,
        use_prevote=True,
        durable_log: bool = True,
    ) -> None:
        self.node_id = node_id
        self.addresses = addresses
        self.transport = TcpTransport(addresses)
        self.state_machine = KVStore()
        persister = DurableState(f"{state_dir}/{node_id}.json") if state_dir else DurableState()
        raft_log = None
        if durable_log and state_dir:
            from .durable_log import DurableRaftLog

            raft_log = DurableRaftLog(os.path.join(state_dir, f"{node_id}-wal"))
        self.node = RaftNode(
            node_id=node_id,
            peers=list(addresses),
            transport=self.transport,
            clock=_ms,
            persister=persister,
            state_machine=self.state_machine,
            election_timeout_ms=election_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            use_prevote=use_prevote,
            log=raft_log,
            snapshot_dir=os.path.join(state_dir, f"{node_id}-snapshots") if state_dir else None,
        )
        host, port = addresses[node_id]
        self.server = FrameServer(host, port, handler=self._on_wire_message)
        self._timer_thread = None
        self._running = False

    def _on_wire_message(self, wire_msg: dict) -> dict:
        msg = _decode_wire(wire_msg)
        kind = msg.get("type")
        if kind == "propose":
            index = self.node.propose(msg["payload"])
            return {"index": index, **self.node.info()}
        if kind == "read":
            key = bytes(msg["payload"]).decode("utf-8")
            if self.node.can_serve_linearizable_read():
                value = self.state_machine.get(key)
                return {"value": value, "linearizable": True, "leader": self.node_id}
            leader = self.node.leader_id
            addr = self.addresses.get(leader)
            return {
                "value": None,
                "linearizable": False,
                "redirect": f"{leader}:{addr[1]}" if addr else None,
            }
        if kind == "snapshot":
            snap = self.node.take_snapshot()
            return {"snapshot": bool(snap), **self.node.info()}
        if kind == "info":
            info = self.node.info()
            log_stats = getattr(self.node.log, "stats", None)
            if log_stats is not None:
                try:
                    info["log"] = log_stats()
                except Exception:
                    pass
            return info
        self.node.handle(msg)
        return {"ok": True, **self.node.info()}

    @property
    def bound_port(self) -> int:
        return self.server.bound_port

    def start(self) -> None:
        self.server.start()
        self.addresses[self.node_id] = ("127.0.0.1", self.server.bound_port)
        self._running = True
        self._timer_thread = threading.Thread(target=self._timer_loop, daemon=True)
        self._timer_thread.start()

    def _timer_loop(self) -> None:
        while self._running:
            now = _ms()
            if now >= self.node.next_event_due():
                self.node.tick(now)
            time.sleep(0.002)

    def stop(self) -> None:
        self._running = False
        if self._timer_thread is not None:
            self._timer_thread.join(timeout=1.0)
        self.server.stop()

    def wait_for_role(self, role: str, timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.node.role.value == role:
                return True
            time.sleep(0.01)
        return False


class RaftClusterRunner:
    """Boots N raft nodes as threaded servers inside one process."""

    def __init__(
        self,
        count: int = 3,
        base_port: int = 0,
        state_dir: str | None = None,
        durable_log: bool = True,
    ) -> None:
        ids = [f"node{i}" for i in range(1, count + 1)]
        placeholder = ("127.0.0.1", 0)
        self.addresses = {n: list(placeholder) for n in ids}
        self.nodes: list[RaftServerNode] = []
        self.ids = ids
        self.state_dir = state_dir
        for n in ids:
            node = RaftServerNode(
                n,
                {k: tuple(v) for k, v in self.addresses.items()},
                state_dir=state_dir,
                durable_log=durable_log,
            )
            node.start()
            self.addresses[n] = ("127.0.0.1", node.bound_port)
            for other in self.nodes:
                other.addresses[n] = self.addresses[n]
            self.nodes.append(node)

    def leader(self) -> RaftServerNode | None:
        for n in self.nodes:
            if n.node.role.value == "leader":
                return n
        return None

    def stop(self) -> None:
        for n in self.nodes:
            n.stop()
