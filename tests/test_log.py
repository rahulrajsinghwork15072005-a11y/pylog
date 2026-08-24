import os
import struct

import pytest

from pylog.log import (
    CommitLog,
    CorruptRecord,
    Record,
    Segment,
    encode_record,
    read_record,
)


@pytest.fixture()
def log_dir(tmp_path):
    return str(tmp_path / "data")


def test_encode_read_roundtrip(tmp_path):
    path = tmp_path / "rec.bin"
    blob = encode_record(7, b"hello", timestamp=123)
    with open(path, "wb") as f:
        f.write(blob)
    with open(path, "rb") as f:
        rec = read_record(f)
    assert rec == Record(offset=7, timestamp=123, payload=b"hello")


def test_read_record_clean_eof(tmp_path):
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")
    with open(path, "rb") as f:
        assert read_record(f) is None


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda b: b[: len(b) // 2], id="torn-header"),
        pytest.param(lambda b: b[:-3], id="torn-payload"),
        pytest.param(lambda b: b[:-1] + bytes([b[-1] ^ 0xFF]), id="payload-flip"),
        pytest.param(lambda b: b"garbage" + b[7:], id="bad-magic"),
    ],
)
def test_corruption_detected(mutate, tmp_path):
    path = tmp_path / "rec.bin"
    path.write_bytes(mutate(encode_record(0, b"x" * 64)))
    with pytest.raises(CorruptRecord):
        with open(path, "rb") as f:
            read_record(f)


def test_append_and_read_roundtrip(log_dir):
    log = CommitLog(log_dir, max_segment_bytes=1024)
    try:
        offsets = [log.append(f"msg-{i}".encode()) for i in range(100)]
        assert offsets == list(range(100))
        for i in range(100):
            rec = log.read(i)
            assert rec is not None
            assert rec.payload == f"msg-{i}".encode()
            assert rec.offset == i
    finally:
        log.close()


def test_sparse_index_keeps_reads_fast_across_many_records(log_dir):
    log = CommitLog(log_dir, max_segment_bytes=10 * 1024 * 1024, index_interval_bytes=256)
    try:
        n = 2000
        payload = b"p" * 64
        for _ in range(n):
            log.append(payload)
        stats = log.stats()
        total_index_entries = sum(len(s.index) for s in log.segments)
        total_bytes = stats["size_bytes"]
        expected_upper = sum(s.size // 256 + 2 for s in log.segments)
        assert total_index_entries <= expected_upper
        assert total_index_entries * 256 <= total_bytes * 2
        assert total_index_entries < n
        assert stats["next_offset"] == n
        rec = log.read(n - 1)
        assert rec is not None and rec.payload == payload
        rec = log.read(n // 2)
        assert rec is not None
    finally:
        log.close()


def test_segments_roll_by_size(log_dir):
    log = CommitLog(log_dir, max_segment_bytes=512, index_interval_bytes=128)
    try:
        payload = b"z" * 64
        for _ in range(40):
            log.append(payload)
        assert len(log.segments) > 1
        bases = [s.base_offset for s in log.segments]
        assert bases == sorted(bases)
        for i in range(40):
            rec = log.read(i)
            assert rec is not None and rec.payload == payload
    finally:
        log.close()


def test_recovery_truncates_torn_tail(log_dir):
    log = CommitLog(log_dir, max_segment_bytes=1024 * 1024)
    for i in range(50):
        log.append(f"good-{i}".encode(), timestamp=i)
    log.close()

    seg = log.segments[0]
    clean_size = os.path.getsize(seg.log_path)
    with open(seg.log_path, "ab") as f:
        torn = encode_record(50, b"torn-half-written-record")
        f.write(torn[: len(torn) // 2])
        f.write(b"\x00\x00")

    reopened = CommitLog(log_dir)
    try:
        assert reopened.next_offset == 50
        assert reopened.read(49).payload == b"good-49"
        assert reopened.read(50) is None
        size_after = os.path.getsize(reopened.segments[0].log_path)
        assert clean_size - 1 < size_after <= clean_size
        reopened.append(b"post-recovery")
        assert reopened.read(50).payload == b"post-recovery"
    finally:
        reopened.close()


def test_recovery_partial_final_record(log_dir):
    log = CommitLog(log_dir)
    for i in range(20):
        log.append(f"m{i}".encode())
    seg_path = log.segments[0].log_path
    full_size = os.path.getsize(seg_path)
    log.close()
    with open(seg_path, "r+b") as f:
        f.truncate(full_size - 4)

    reopened = CommitLog(log_dir)
    try:
        assert reopened.next_offset == 19
        assert reopened.read(18).payload == b"m18"
        assert reopened.read(19) is None
        reopened.append(b"resumed")
        assert reopened.read(19).payload == b"resumed"
    finally:
        reopened.close()


def test_recovery_fresh_directory_creates_empty_files(log_dir):
    log = CommitLog(log_dir)
    try:
        assert log.next_offset == 0
        assert os.path.exists(log.segments[0].log_path)
        assert os.path.exists(log.segments[0].index_path)
        assert log.stats()["size_bytes"] == 0
    finally:
        log.close()


def test_recovery_rebuilds_sparse_index(log_dir):
    log = CommitLog(log_dir, index_interval_bytes=64)
    for i in range(300):
        log.append(b"data" * 8, timestamp=i)
    expected_entries = [tuple(e) for e in log.segments[0].index]
    log.close()

    reopened = CommitLog(log_dir, index_interval_bytes=64)
    try:
        rebuilt = [tuple(e) for e in reopened.segments[0].index]
        assert rebuilt == expected_entries
        assert reopened.read(150).payload == b"data" * 8
    finally:
        reopened.close()


def test_offsets_monotonic_after_recovery(log_dir):
    log = CommitLog(log_dir)
    for _ in range(10):
        log.append(b"a")
    log.close()

    reopened = CommitLog(log_dir)
    next_off = reopened.append(b"after-crash")
    try:
        assert next_off == 10
        assert reopened.read(10).payload == b"after-crash"
        assert reopened.read(9).payload == b"a"
    finally:
        reopened.close()


def test_corrupt_middle_record_discards_everything_after(log_dir):
    log = CommitLog(log_dir)
    for i in range(30):
        log.append(f"v{i}".encode())
    seg = log.segments[0]

    positions = {}
    with open(seg.log_path, "rb") as f:
        while True:
            pos = f.tell()
            head = f.read(struct.calcsize("<4sIQqI"))
            if not head:
                break
            _, length, off, ts, crc = struct.unpack("<4sIQqI", head)
            f.read(length)
            positions[off] = pos

    target = positions[15]
    with open(seg.log_path, "r+b") as f:
        f.seek(target + 29)
        raw = f.read(1)
        f.seek(target + 29)
        f.write(bytes([raw[0] ^ 0xFF]))
    log.close()

    reopened = CommitLog(log_dir)
    try:
        assert reopened.read(14).payload == b"v14"
        assert reopened.next_offset == 15
        assert reopened.read(16) is None
    finally:
        reopened.close()


def test_fsync_append_survives_process_level_close(log_dir):
    log = CommitLog(log_dir)
    log.append(b"durable", fsync=True)
    log.close()
    reopened = CommitLog(log_dir)
    try:
        assert reopened.read(0).payload == b"durable"
    finally:
        reopened.close()


def test_read_range(log_dir):
    log = CommitLog(log_dir)
    try:
        for i in range(25):
            log.append(str(i).encode())
        batch = log.read_range(5, limit=10)
        assert [r.offset for r in batch] == list(range(5, 15))
        assert batch[-1].payload == b"14"
        tail = log.read_range(23, limit=10)
        assert [r.offset for r in tail] == [23, 24]
    finally:
        log.close()


def test_stats_shape(log_dir):
    log = CommitLog(log_dir, max_segment_bytes=256)
    try:
        for _ in range(30):
            log.append(b"s" * 32)
        st = log.stats()
        assert set(st) == {"next_offset", "segments", "size_bytes", "first_offset"}
        assert st["next_offset"] == 30
        assert st["segments"] >= 2
        assert st["size_bytes"] > 30 * 32
    finally:
        log.close()


def test_segment_direct_usage_matches_guide_semantics(tmp_path):
    seg = Segment(str(tmp_path), base_offset=0, index_interval_bytes=32)
    assert seg.recover() == 0
    for i in range(50):
        seg.append(i, encode_record(i, f"r{i}".encode()), timestamp=i)
    assert seg.next_offset == 50
    assert len(seg.index) < 50
    rec = seg.read(42)
    assert rec.payload == b"r42"
    seg.close_handles()
