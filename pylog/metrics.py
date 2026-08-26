"""Prometheus-style metrics for pylog — stdlib only, no deps."""

from __future__ import annotations

import time
from collections import Counter, defaultdict
import threading

class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.counters: Counter = Counter()
        self.hist: defaultdict[str, list] = defaultdict(list)
        self.gauges: dict[str, float] = {}
        self.start = time.perf_counter()

    def inc(self, name: str, value: float = 1) -> None:
        with self._lock:
            self.counters[name] += value

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            lst = self.hist[name]
            lst.append(value)
            if len(lst) > 2048:
                lst[:] = lst[-1024:]

    def set(self, name: str, value: float) -> None:
        with self._lock:
            self.gauges[name] = value

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "counters": dict(self.counters),
                "gauges": dict(self.gauges),
                "hist": {k: list(v) for k, v in self.hist.items()},
                "uptime_sec": time.perf_counter() - self.start,
            }

    def prometheus(self) -> str:
        snap = self.snapshot()
        lines = []
        for k, v in snap["counters"].items():
            lines.append(f"# TYPE {k} counter")
            lines.append(f"{k} {v}")
        for k, v in snap["gauges"].items():
            lines.append(f"# TYPE {k} gauge")
            lines.append(f"{k} {v}")
        for k, vals in snap["hist"].items():
            if not vals:
                continue
            vals_sorted = sorted(vals)
            n = len(vals_sorted)
            p50 = vals_sorted[n//2]
            p99 = vals_sorted[int(n*0.99)] if n>1 else vals_sorted[0]
            avg = sum(vals_sorted)/n
            lines.append(f"# TYPE {k}_avg gauge")
            lines.append(f"{k}_avg {avg}")
            lines.append(f"# TYPE {k}_p50 gauge")
            lines.append(f"{k}_p50 {p50}")
            lines.append(f"# TYPE {k}_p99 gauge")
            lines.append(f"{k}_p99 {p99}")
        lines.append(f"pylog_uptime_sec {snap['uptime_sec']}")
        return "\n".join(lines) + "\n"

global_metrics = Metrics()
