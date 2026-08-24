"""A tiny deterministic key/value state machine used on top of Raft."""

from __future__ import annotations


class KVStore:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self.ops = 0

    def apply(self, payload) -> dict:
        self.ops += 1
        parts = bytes(payload).decode("utf-8").split("=", 1)
        if len(parts) == 2:
            self.data[parts[0]] = parts[1]
            return {"op": "set", "key": parts[0]}
        return {"op": "get", "key": parts[0], "value": self.data.get(parts[0])}

    def get(self, key: str):
        return self.data.get(key)

    def snapshot(self) -> dict:
        return {"keys": dict(self.data), "ops": self.ops}

    def restore(self, state: dict) -> None:
        self.data = dict(state.get("keys", {}))
        self.ops = state.get("ops", 0)
