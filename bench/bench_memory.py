"""Peak Python memory: naive copy vs s3xfer, across file sizes.

Runs against moto as a separate local server, so it measures only *this process's*
allocations during the copy. Real-S3 runs belong in the blog too.

    python bench/bench_memory.py            # prints a table and writes bench/results.csv
"""

from __future__ import annotations

import csv
import gc
import os
import sys
import tracemalloc
from pathlib import Path

import boto3
import socket
import subprocess
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from s3xfer import s3_copy  # noqa: E402

MiB = 1024 * 1024
SIZES_MIB = [64, 128, 256, 512, 1024]
BUDGET = "64MiB"


def naive_copy(c, size_key):
    body = c.get_object(Bucket="src", Key=size_key)["Body"].read()
    c.put_object(Bucket="dst", Key=size_key, Body=body)


def measure(fn) -> int:
    gc.collect()
    tracemalloc.start()
    tracemalloc.reset_peak()
    fn()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak


def start_fake_s3(port: int = 5055) -> subprocess.Popen:
    """Run moto as a separate process so its storage isn't counted as ours."""
    proc = subprocess.Popen([sys.executable, "-m", "moto.server", "-p", str(port)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return proc
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("moto server didn't start")


def main():
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    rows = []
    server = start_fake_s3()
    try:
        c = boto3.client("s3", region_name="us-east-1", endpoint_url="http://127.0.0.1:5055")
        c.create_bucket(Bucket="src")
        c.create_bucket(Bucket="dst")
        for mib in SIZES_MIB:
            key = f"f{mib}"
            chunk = os.urandom(MiB)
            c.put_object(Bucket="src", Key=key, Body=chunk * mib)
            del chunk
            naive = measure(lambda: naive_copy(c, key))
            flat = measure(lambda: s3_copy(c, "src", key, "dst", key + ".flat",
                                           memory_budget=BUDGET, concurrency=8))
            rows.append((mib, naive / MiB, flat / MiB))
            print(f"{mib:>5} MiB file | naive peak {naive / MiB:8.1f} MiB | s3xfer peak {flat / MiB:6.1f} MiB")
            c.delete_object(Bucket="src", Key=key)
    finally:
        server.terminate()
    out = Path(__file__).with_name("results.csv")
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["file_mib", "naive_peak_mib", "s3xfer_peak_mib"])
        w.writerows((m, f"{n:.1f}", f"{s:.1f}") for m, n, s in rows)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
