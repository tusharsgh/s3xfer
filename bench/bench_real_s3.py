"""Real-S3 benchmark: speed and peak memory of s3xfer copies.

Three experiments, each repeated --reps times (reps interleaved to even out drift):

  concurrency  1 GiB object, --memory 256MiB, concurrency 1..64
  chunk        1 GiB object, concurrency 16, budgets giving 5 / 8 / 16 MiB chunks
  filesize     1 / 4 / 16 GiB objects, --memory 256MiB, concurrency 16

Every copy runs in a fresh Python process, so its peak RSS is that copy's own
(Python + boto3 + CRT + data in flight). Run it close to the bucket, e.g. on EC2
in the bucket's region; from a laptop you mostly measure your uplink.

    python bench/bench_real_s3.py --bucket my-bench-bucket [--profile p] [--region r]
    python bench/plot_real_s3.py

Test objects live under bench-src/ (4 and 16 GiB ones are assembled server-side
with UploadPartCopy, so nothing big is uploaded). Each copy is deleted right
after it's measured. Writes bench/real_s3_results.csv and bench/real_s3_meta.json.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import resource
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
MiB = 1024 * 1024


def client(args):
    import boto3

    s = boto3.Session(profile_name=args.profile, region_name=args.region)
    return s.client("s3", endpoint_url=args.endpoint_url)


def peak_rss_mib() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / MiB if sys.platform == "darwin" else r / 1024  # bytes on macOS, KiB on Linux


# --- one measured copy (runs in its own process) -----------------------------

def single(args) -> None:
    from s3xfer import s3_copy

    c = client(args)
    dst = f"bench-dst/{os.getpid()}.bin"
    t = time.monotonic()
    res = s3_copy(c, args.bucket, args.key, args.bucket, dst,
                  memory_budget=args.memory, concurrency=args.concurrency)
    seconds = time.monotonic() - t
    rss = peak_rss_mib()
    c.delete_object(Bucket=args.bucket, Key=dst)
    p = res.plan
    print(json.dumps(dict(seconds=seconds, peak_rss_mib=rss, chunk_mib=p.chunk_size / MiB,
                          streams=p.concurrency, parts=p.num_parts, retries=res.retries)))


# --- test objects --------------------------------------------------------------

def ensure_objects(c, bucket: str, unit: int, multiples: list[int]) -> dict[int, str]:
    keys = {m: f"bench-src/{m * unit // MiB}MiB.bin" for m in multiples}
    base = keys.get(1) or f"bench-src/{unit // MiB}MiB.bin"

    def exists(k):
        try:
            return c.head_object(Bucket=bucket, Key=k)["ContentLength"]
        except c.exceptions.ClientError:
            return None

    if exists(base) != unit:
        print(f"uploading {unit // MiB} MiB random base object {base}", file=sys.stderr)
        up = c.create_multipart_upload(Bucket=bucket, Key=base)["UploadId"]
        part, parts = 16 * MiB, []
        for i, off in enumerate(range(0, unit, part), 1):
            body = os.urandom(min(part, unit - off))
            parts.append({"PartNumber": i, "ETag": c.upload_part(
                Bucket=bucket, Key=base, UploadId=up, PartNumber=i, Body=body)["ETag"]})
        c.complete_multipart_upload(Bucket=bucket, Key=base, UploadId=up,
                                    MultipartUpload={"Parts": parts})

    for m, k in keys.items():
        if m == 1 or exists(k) == m * unit:
            continue
        print(f"assembling {k} from {m} server-side copies of {base}", file=sys.stderr)
        up = c.create_multipart_upload(Bucket=bucket, Key=k)["UploadId"]
        parts = [{"PartNumber": i, "ETag": c.upload_part_copy(
            Bucket=bucket, Key=k, UploadId=up, PartNumber=i,
            CopySource={"Bucket": bucket, "Key": base})["CopyPartResult"]["ETag"]}
            for i in range(1, m + 1)]
        c.complete_multipart_upload(Bucket=bucket, Key=k, UploadId=up,
                                    MultipartUpload={"Parts": parts})
    keys[1] = base
    return keys


# --- orchestration ---------------------------------------------------------------

def plan_runs(multiples: list[int]) -> list[tuple[str, int, str, int]]:
    runs = [("concurrency", 1, "256MiB", c) for c in (1, 2, 4, 8, 16, 32, 64)]
    runs += [("chunk", 1, b, 16) for b in ("160MiB", "256MiB", "512MiB")]  # 5 / 8 / 16 MiB chunks
    runs += [("filesize", m, "256MiB", 16) for m in multiples]
    return runs


def instance_info() -> dict:
    try:
        tok = urllib.request.urlopen(urllib.request.Request(
            "http://169.254.169.254/latest/api/token", method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}), timeout=1).read().decode()
        get = lambda p: urllib.request.urlopen(urllib.request.Request(  # noqa: E731
            f"http://169.254.169.254/latest/meta-data/{p}",
            headers={"X-aws-ec2-metadata-token": tok}), timeout=1).read().decode()
        return {"instance_type": get("instance-type"), "az": get("placement/availability-zone")}
    except Exception:
        return {"instance_type": None, "az": None}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--profile")
    ap.add_argument("--region")
    ap.add_argument("--endpoint-url", help="e.g. a local moto server, for a dry run")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--unit-mib", type=int, default=1024, help="base object size (default 1 GiB)")
    ap.add_argument("--multiples", default="1,4,16", help="file sizes for the filesize experiment")
    ap.add_argument("--out", default=str(HERE / "real_s3_results.csv"))
    # internal: one measured copy
    ap.add_argument("--single", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--key", help=argparse.SUPPRESS)
    ap.add_argument("--memory", help=argparse.SUPPRESS)
    ap.add_argument("--concurrency", type=int, help=argparse.SUPPRESS)
    args = ap.parse_args()
    if args.single:
        return single(args)

    import s3xfer

    unit, multiples = args.unit_mib * MiB, [int(m) for m in args.multiples.split(",")]
    keys = ensure_objects(client(args), args.bucket, unit, multiples)
    common = ["--bucket", args.bucket]
    for flag in ("profile", "region", "endpoint_url"):
        if getattr(args, flag):
            common += [f"--{flag.replace('_', '-')}", getattr(args, flag)]

    def run(size_mult, memory, conc) -> dict:
        out = subprocess.run([sys.executable, __file__, "--single", "--key", keys[size_mult],
                              "--memory", memory, "--concurrency", str(conc), *common],
                             check=True, capture_output=True, text=True).stdout
        return json.loads(out.strip().splitlines()[-1])

    run(1, "256MiB", 8)  # warm-up: DNS, TLS, S3 request routing
    runs = plan_runs(multiples)
    rows = []
    for rep in range(1, args.reps + 1):
        for exp, m, memory, conc in runs:
            r = run(m, memory, conc)
            size_mib = m * unit / MiB
            row = dict(experiment=exp, rep=rep, file_mib=int(size_mib), memory=memory,
                       concurrency=conc, chunk_mib=int(r["chunk_mib"]), streams=r["streams"],
                       parts=r["parts"], seconds=round(r["seconds"], 3),
                       mib_per_s=round(size_mib / r["seconds"], 1),
                       peak_rss_mib=round(r["peak_rss_mib"], 1), retries=r["retries"])
            rows.append(row)
            print(" ".join(f"{k}={v}" for k, v in row.items()), flush=True)

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    meta = dict(date=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                s3xfer=s3xfer.__version__, python=platform.python_version(),
                machine=platform.machine(), region=args.region, reps=args.reps, **instance_info())
    Path(args.out).with_name("real_s3_meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"wrote {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
