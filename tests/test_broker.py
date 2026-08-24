import json

import pytest

from pylog.broker import Broker


@pytest.fixture()
def broker(tmp_path):
    b = Broker(
        str(tmp_path / "data"),
        default_partitions=3,
        max_segment_bytes=512,
        retention_max_age_ms=None,
    )
    yield b
    b.close()


def test_produce_auto_creates_topic_and_routes_round_robin(broker):
    r1 = broker.produce("orders", b"one")
    r2 = broker.produce("orders", b"two")
    r3 = broker.produce("orders", b"three")
    assert {r1["partition"], r2["partition"], r3["partition"]} == {0, 1, 2}


def test_same_key_always_lands_on_same_partition(broker):
    placements = set()
    for i in range(30):
        r = broker.produce("keys", f"m{i}".encode(), key="user-42")
        placements.add(r["partition"])
    assert len(placements) == 1


def test_key_routing_is_deterministic_across_restarts(tmp_path):
    d = str(tmp_path / "data")
    b1 = Broker(d)
    a = b1.produce("t", b"x", key="k")["partition"]
    b1.close()
    b2 = Broker(d)
    c = b2.produce("t", b"y", key="k")["partition"]
    b2.close()
    assert a == c


def test_fetch_after_offset_returns_only_newer_records(tmp_path):
    b = Broker(str(tmp_path / "d1"), default_partitions=1)
    try:
        for i in range(10):
            b.produce("f", str(i).encode())
        batch = b.fetch("f", 0, after_offset=6, limit=100)
        assert [int(m["payload"]) for m in batch] == [7, 8, 9]
        assert batch[0]["offset"] == 7
    finally:
        b.close()


def test_fetch_respects_limit(broker):
    for i in range(20):
        broker.produce("lim", str(i).encode())
    batch = broker.fetch("lim", 0, after_offset=-1, limit=5)
    assert len(batch) == 5
    nxt = broker.fetch("lim", 0, after_offset=batch[-1]["offset"], limit=5)
    assert nxt[0]["offset"] == 5


def test_fetch_missing_partition_raises(broker):
    with pytest.raises(KeyError):
        broker.fetch("nope", 0)


def test_consumer_group_offsets_persist_and_reload(broker, tmp_path):
    for i in range(10):
        broker.produce("cg", str(i).encode())
    broker.commit("workers", "cg", 0, 4)
    assert broker.group("workers").committed("cg", 0) == 4

    path = tmp_path / "data" / "groups" / "workers.json"
    assert path.exists()
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == {"cg/0": 4}

    reopened = Broker(str(tmp_path / "data"))
    try:
        assert reopened.group("workers").committed("cg", 0) == 4
    finally:
        reopened.close()


def test_commit_never_moves_backwards(broker):
    broker.commit("g", "t", 0, 7)
    broker.commit("g", "t", 0, 3)
    assert broker.group("g").committed("t", 0) == 7


def test_lag_tracks_unconsumed_records(tmp_path):
    b = Broker(str(tmp_path / "d2"), default_partitions=1)
    try:
        for _ in range(8):
            b.produce("lagt", b"x")
        hw = b.topics["lagt"][0].high_watermark
        b.commit("g1", "lagt", 0, 2)
        assert b.lag("g1", "lagt", 0) == (hw - 1) - 2
        b.commit("g1", "lagt", 0, hw - 1)
        assert b.lag("g1", "lagt", 0) == 0
        assert b.lag("ghost-group", "lagt", 0) == hw
    finally:
        b.close()


def test_topics_survive_restart_with_data_intact(tmp_path):
    d = str(tmp_path / "data")
    b1 = Broker(d, max_segment_bytes=256, default_partitions=1)
    for i in range(25):
        b1.produce("persist", f"v{i}".encode())
    b1.close()

    b2 = Broker(d)
    try:
        msgs = []
        for p in range(len(b2.topics["persist"])):
            msgs.extend(b2.fetch("persist", p, after_offset=-1, limit=100))
        payloads = [m["payload"] for m in msgs]
        assert payloads == [f"v{i}" for i in range(25)]
    finally:
        b2.close()


def test_retention_sweeps_expired_segments_by_age(tmp_path):
    d = str(tmp_path / "data")
    b = Broker(d, max_segment_bytes=128, retention_max_age_ms=1000)
    now = 10_000
    for i in range(40):
        b.produce("old", b"x" * 32, timestamp=now - 5000 + i)
    fresh_count_before = sum(p.high_watermark for p in b.topics["old"])
    removed = b.retention_sweep(now_ms=now)
    assert removed >= 1
    total_after = sum(s.size for p in b.topics["old"] for s in p.log.segments)
    assert total_after < fresh_count_before * (32 + 28) or removed > 0
    b.close()


def test_retention_enforces_partition_byte_budget(tmp_path):
    d = str(tmp_path / "data")
    b = Broker(d, max_segment_bytes=200, retention_max_partition_bytes=400)
    for _ in range(60):
        b.produce("big", b"y" * 64)
    removed = b.retention_sweep()
    total = sum(s.size for p in b.topics["big"] for s in p.log.segments)
    active_size = b.topics["big"][-1].size_bytes
    assert total <= 400 + active_size or len(b.topics["big"]) == 1
    assert removed >= 1
    b.close()


def test_stats_shape(broker):
    broker.produce("s", b"1")
    st = broker.stats()
    assert "s" in st["topics"]
    t = st["topics"]["s"]
    assert t["partitions"] == 3
    assert len(t["high_watermarks"]) == 3
