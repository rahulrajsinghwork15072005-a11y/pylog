"""HTTP REST gateway + live dashboard for the broker (stdlib http.server)."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

DASHBOARD_HTML = """<!doctype html>
<html><head><title>pylog</title>
<meta charset="utf-8"><meta http-equiv="refresh" content="2">
<style>
 body{font-family:Consolas,monospace;background:#0d1117;color:#c9d1d9;margin:2rem}
 h1{color:#58a6ff} table{border-collapse:collapse;margin-top:1rem}
 td,th{border:1px solid #30363d;padding:.4rem .8rem;text-align:left}
 th{background:#161b22}.ok{color:#3fb950}.num{text-align:right}
</style></head><body>
<h1>pylog broker</h1>
<p class="ok">● live</p>
<table><tr><th>topic</th><th>partitions</th><th>high watermarks</th><th>bytes</th></tr>
{rows}
</table>
</body></html>"""


def make_handler(broker):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _json(self, obj, status=200):
            body = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            q = parse_qs(url.query)
            try:
                if url.path == "/":
                    return self._dashboard()
                if url.path == "/health":
                    return self._json({"status": "ok"})
                if url.path == "/stats":
                    return self._json(broker.stats())
                if len(parts) >= 1 and parts[0] == "topics":
                    if len(parts) == 1:
                        return self._json({"topics": sorted(broker.topics)})
                    topic = parts[1]
                    if len(parts) == 2 and topic in broker.topics:
                        st = broker.stats()["topics"].get(topic, {"partitions": 0})
                        return self._json({"topic": topic, **st})
                    if len(parts) == 4 and parts[2] == "consume":
                        partition = int(parts[3])
                        group = (q.get("group") or [""])[0]
                        limit = int((q.get("limit") or ["100"])[0])
                        after = -1
                        if group:
                            after = broker.group(group).committed(topic, partition)
                        msgs = broker.fetch(topic, partition, after_offset=after, limit=limit)
                        if group and msgs:
                            broker.commit(group, topic, partition, msgs[-1]["offset"])
                        return self._json({"messages": msgs})
            except Exception as exc:
                return self._json({"error": str(exc)}, status=400)
            return self._json({"error": "not found"}, status=404)

        def do_POST(self):
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                if len(parts) == 3 and parts[0] == "topics" and parts[2] == "produce":
                    req = json.loads(raw.decode("utf-8") or "{}")
                    payload = req.get("payload", "")
                    if isinstance(payload, str):
                        payload_b = payload.encode("utf-8")
                    else:
                        payload_b = raw if not req else json.dumps(req["payload"]).encode()
                    res = broker.produce(
                        parts[1],
                        payload_b,
                        key=req.get("key"),
                    )
                    return self._json(res)
                if len(parts) == 4 and parts[2] == "commit":
                    req = json.loads(raw.decode("utf-8"))
                    broker.commit(
                        req.get("group", "default"),
                        parts[1],
                        int(parts[3]),
                        int(req["offset"]),
                    )
                    return self._json({"status": "committed"})
            except Exception as exc:
                return self._json({"error": str(exc)}, status=400)
            return self._json({"error": "not found"}, status=404)

        def _dashboard(self):
            st = broker.stats()
            rows = []
            for name in sorted(st["topics"]):
                t = st["topics"][name]
                rows.append(
                    f"<tr><td>{name}</td><td>{t['partitions']}</td>"
                    f"<td>{t['high_watermarks']}</td><td>{t['size_bytes']}</td></tr>"
                )
            html = DASHBOARD_HTML.replace("{rows}", "\n".join(rows))
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


class HttpGateway:
    def __init__(self, broker, host: str = "", port: int = 0) -> None:
        self._server = ThreadingHTTPServer((host, port), make_handler(broker))

    @property
    def bound_port(self) -> int:
        return self._server.server_address[1]

    def start(self) -> None:
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
