"""Walkthrough demo: the durable append-only log, crash, recovery."""

import shutil

from pylog import CommitLog


def main() -> None:
    shutil.rmtree("demo-data", ignore_errors=True)
    log = CommitLog("demo-data", max_segment_bytes=512, index_interval_bytes=128)

    print("== appending 200 records ==")
    for i in range(200):
        log.append(f"event-{i:04d}".encode(), timestamp=1000 + i)
    st = log.stats()
    print(f"stats: {st}")
    print(f"index entries in active segment: {len(log.segments[0].index)} (sparse!)")

    rec = log.read(137)
    print(f"read(137) -> offset={rec.offset} payload={rec.payload.decode()}")

    print("\n== simulating a torn write (crash mid-append) ==")
    seg = log.segments[0]
    with open(seg.log_path, "ab") as f:
        f.write(b"\x00\x01\x02\x03partial-garbage-without-valid-crc")
    log.close()

    recovered = CommitLog("demo-data", max_segment_bytes=512, index_interval_bytes=128)
    print(f"recovered next_offset = {recovered.next_offset} (corrupt tail truncated)")
    for _ in range(3):
        off = recovered.next_offset
        recovered.append(f"post-crash-{off}".encode())
    print(f"appended 3 records after recovery; final stats: {recovered.stats()}")
    recovered.close()
    print("\ndone — data dir: demo-data/")


if __name__ == "__main__":
    main()
