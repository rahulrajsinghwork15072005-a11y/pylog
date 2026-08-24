"""Length-prefixed JSON framing over TCP, plus a small threaded request/response server."""

from __future__ import annotations

import json
import socket
import struct
import threading

_HEADER = struct.Struct("<I")
MAX_FRAME_BYTES = 64 * 1024 * 1024


class Disconnected(Exception):
    pass


def send_frame(sock: socket.socket, obj) -> None:
    data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    sock.sendall(_HEADER.pack(len(data)) + data)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise Disconnected("peer closed")
        buf.extend(chunk)
    return bytes(buf)


def recv_frame(sock: socket.socket):
    header = _recv_exact(sock, _HEADER.size)
    (length,) = _HEADER.unpack(header)
    if length > MAX_FRAME_BYTES:
        raise ValueError(f"frame too large: {length}")
    return json.loads(_recv_exact(sock, length).decode("utf-8"))


class FrameServer:
    """Threaded TCP server: one connection = a sequence of JSON frames; each frame is
    dispatched to ``handler`` and the result is sent back as one frame."""

    def __init__(self, host: str, port: int, handler, backlog: int = 128) -> None:
        self.host = host
        self.port = port
        self.handler = handler
        self.backlog = backlog
        self._sock = None
        self._threads: list[threading.Thread] = []
        self._accept_thread = None
        self._running = False
        self._lock = threading.Lock()

    def start(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(self.backlog)
        self._running = True
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    @property
    def bound_port(self) -> int:
        assert self._sock is not None
        return self._sock.getsockname()[1]

    def _accept_loop(self) -> None:
        while self._running:
            try:
                conn, addr = self._sock.accept()
            except OSError:
                break
            t = threading.Thread(target=self._serve_conn, args=(conn, addr), daemon=True)
            with self._lock:
                self._threads.append(t)
            t.start()

    def _serve_conn(self, conn: socket.socket, addr) -> None:
        try:
            while True:
                try:
                    req = recv_frame(conn)
                except (Disconnected, ValueError):
                    break
                try:
                    resp = self.handler(req)
                except Exception as exc:
                    resp = {"type": "error", "error": f"{type(exc).__name__}: {exc}"}
                try:
                    send_frame(conn, resp)
                except OSError:
                    break
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def stop(self) -> None:
        self._running = False
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass


def frame_call(host: str, port: int, request, timeout: float = 5.0):
    """One-shot client: open a connection, send one frame, read one response."""
    with socket.create_connection((host, port), timeout=timeout) as sock:
        send_frame(sock, request)
        return recv_frame(sock)


class FrameClient:
    """Thread-safe persistent request/response connection."""

    def __init__(self, host: str, port: int, timeout: float = 5.0) -> None:
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self._lock = threading.Lock()

    def call(self, request):
        with self._lock:
            send_frame(self.sock, request)
            return recv_frame(self.sock)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass
