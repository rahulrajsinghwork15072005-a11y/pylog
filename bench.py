"""Benchmarks: durable log appends, broker produce/fetch, read path."""

from __future__ import annotations

import argparse
import shutil
import time

from pylog.broker import Broker
from pylog.log import CommitLog


def bench_commit_log(n: int, fsync_every: int) -> float:
    shutil.rmtree("bench-data", ignore_errors=True)
    log = CommitLog("bench-data", max_segment_bytes=64 * 1024 * 1024)
    payload = b"x" * 100
    start = time.perf_counter()
    for i in range(n):
        log.append(payload, timestamp=i, fsync=fsync_every > 0 and i % fsync_every == 0)
    elapsed = time.perf_counter() - start
    rate = n / elapsed
    log.close()
    shutil.rmtree("bench-data", ignore_errors=True)
    return rate


def bench_group_commit(n: int, batch: int) -> float:
    shutil.rmtree("bench-gc", ignore_errors=True)
    log = CommitLog("bench-gc", max_segment_bytes=64 * 1024 * 1024, index_interval_bytes=8192)
    payload = b"x" * 100
    start = time.perf_counter()
    done = 0
    while done < n:
        take = min(batch, n - done)
        log.append_many([payload] * take, timestamps=[0] * take)
        done += take
    elapsed = time.perf_counter() - start
    rate = n / elapsed
    log.close()
    shutil.rmtree("bench-gc", ignore_errors=True)
    return rate


def bench_reads(n_written: int) -> tuple[float, int]:
    shutil.rmtree("bench-reads", ignore_errors=True)
    log = CommitLog("bench-reads", index_interval_bytes=4096)
    payload = b"y" * 100
    for i in range(n_written):
        log.append(payload, timestamp=i)
    random_offsets = [i * 7 % n_written for i in range(10_000)]
    start = time.perf_counter()
    for off in random_offsets:
        log.read(off)
    elapsed = time.perf_counter() - start
    rate = len(random_offsets) / elapsed
    log.close()
    shutil.rmtree("bench-reads", ignore_errors=True)
    return rate, n_written


def bench_broker(n: int) -> dict:
    shutil.rmtree("bench-broker", ignore_errors=True)
    broker = Broker("bench-broker", default_partitions=4)
    payload = b"z" * 200
    start = time.perf_counter()
    for i in range(n):
        broker.produce("bench", payload, key=f"k{i % 97}")
    produce_rate = n / (time.perf_counter() - start)

    start = time.perf_counter()
    total_fetched = 0
    for p in range(4):
        msgs = broker.fetch("bench", p, after_offset=-1, limit=n)
        total_fetched += len(msgs)
    fetch_rate = total_fetched / max(time.perf_counter() - start, 1e-9)
    broker.close()
    shutil.rmtree("bench-broker", ignore_errors=True)
    return {"produce_per_sec": produce_rate, "fetch_msgs_per_sec": fetch_rate}


def main() -> None:
    parser = argparse.ArgumentParser(description="pylog benchmarks")
    parser.add_argument("--n", type=int, default=150_000)
    args = parser.parse_args()

    print("benchmark                    | rate")
    print("-----------------------------|-----------")
    rate = bench_commit_log(args.n, fsync_every=0)
    print(f"commit-log appends/sec       | {rate:>11,.0f}")
    gc_rate = bench_group_commit(args.n, batch=256)
    print(f"commit-log group-commit/sec  | {gc_rate:>11,.0f}  (batches of 256)")
    rate = bench_commit_log(min(args.n, 20_000), fsync_every=1)
    print(f"commit-log appends/sec+flush | {rate:>11,.0f}")
    reads_rate, written = bench_reads(min(args.n, 100_000))
    print(f"log random reads/sec         | {reads_rate:>11,.0f}  ({written:,} records)")
    b = bench_broker(max(1000, args.n // 10))
    print(f"broker produce/sec           | {b['produce_per_sec']:>11,.0f}")
    print(f"broker fetch msgs/sec        | {b['fetch_msgs_per_sec']:>11,.0f}")


if __name__ == "__main__":
    main()
