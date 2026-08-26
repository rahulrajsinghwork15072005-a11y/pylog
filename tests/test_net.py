import json
import urllib.error
import urllib.request

import pytest

from pylog.api import HttpGateway
from pylog.broker import Broker
from pylog.net import Disconnected, FrameClient, FrameServer, recv_frame, send_frame


def test_frame_roundtrip():
    import socket as s

    a, b = s.socketpair()
    msg = {"hello": ["world", 1, 2.5, None], "n": 42}
    send_frame(a, msg)
    assert recv_frame(b) == msg
    big = {"blob": "x" * 100000}
    send_frame(a, big)
    assert recv_frame(b) == big
    a.close()
    b.close()


def test_frame_server_request_response():
    server = FrameServer("127.0.0.1", 0, handler=lambda req: {"echo": req, "ok": True})
    server.start()
    try:
        c = FrameClient("127.0.0.1", server.bound_port)
        r1 = c.call({"op": "ping"})
        assert r1["echo"] == {"op": "ping"} and r1["ok"] is True
        r2 = c.call([1, 2, 3])
        assert r2["echo"] == [1, 2, 3]
        c.close()
        c2 = FrameClient("127.0.0.1", server.bound_port)
        assert c2.call("hi")["echo"] == "hi"
        c2.close()
    finally:
        server.stop()


def test_recv_frame_raises_on_disconnect_mid_header():
    import socket as s

    a, b = s.socketpair()
    a.send(b"\x00\x00")  # partial header, then close
    a.close()
    with pytest.raises(Disconnected):
        recv_frame(b)
    b.close()


@pytest.fixture()
def http_broker(tmp_path):
    b = Broker(str(tmp_path / "data"), default_partitions=2)
    gw = HttpGateway(b, port=0)
    gw.start()
    yield b, gw
    gw.stop()
    b.close()


def _get(port, path):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as resp:
        return resp.status, json.loads(resp.read().decode())


def _post(port, path, body):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, json.loads(resp.read().decode())


def test_http_produce_consume_commit_cycle(http_broker):
    b, gw = http_broker
    status, res = _post(gw.bound_port, "/topics/orders/produce", {"payload": "pizza", "key": "u1"})
    assert status == 200 and res["topic"] == "orders"

    _post(gw.bound_port, "/topics/orders/produce", {"payload": "pasta", "key": "u2"})
    _post(gw.bound_port, "/topics/orders/produce", {"payload": "sushi", "key": "u1"})

    status, health = _get(gw.bound_port, "/health")
    assert health == {"status": "ok"}

    status, stats = _get(gw.bound_port, "/stats")
    assert stats["topics"]["orders"]["partitions"] == 2

    total = []
    for p in range(2):
        _, out = _get(gw.bound_port, f"/topics/orders/consume/{p}?group=web&limit=10")
        total.extend(out["messages"])
    payloads = sorted(m["payload"] for m in total)
    assert payloads == ["pasta", "pizza", "sushi"]

    for p in range(2):
        _, out = _get(gw.bound_port, f"/topics/orders/consume/{p}?group=web&limit=10")
        assert out["messages"] == []


def test_http_dashboard_serves_html(http_broker):
    b, gw = http_broker
    b.produce("dash", b"x")
    req = urllib.request.Request(f"http://127.0.0.1:{gw.bound_port}/")
    with urllib.request.urlopen(req, timeout=5) as resp:
        html = resp.read().decode()
    assert "pylog broker" in html
    assert "dash" in html
    assert "/metrics" in html


def test_http_error_paths_return_4xx(http_broker):
    _, gw = http_broker
    try:
        _get(gw.bound_port, "/nope")
        raised = False
    except urllib.error.HTTPError as exc:
        raised = exc.code == 404
    assert raised
    try:
        _post(gw.bound_port, "/topics/ghost/consume/99", {})
        raised = False
    except urllib.error.HTTPError as exc:
        raised = exc.code >= 400
    assert raised
