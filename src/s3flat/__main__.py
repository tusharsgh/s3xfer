"""CLI.

    s3flat cp   s3://src-bucket/key    s3://dst-bucket/key    --memory 512MiB
    s3flat sync s3://src-bucket/prefix s3://dst-bucket/prefix --memory 512MiB
"""

from __future__ import annotations

import argparse
import sys
import warnings
from urllib.parse import urlparse


def _split(uri: str, *, allow_empty_key: bool = False) -> tuple[str, str]:
    p = urlparse(uri)
    key = p.path.lstrip("/")
    if p.scheme != "s3" or not p.netloc or (not key and not allow_empty_key):
        want = "s3://bucket[/prefix]" if allow_empty_key else "s3://bucket/key"
        raise SystemExit(f"Expected {want}, got {uri!r}")
    return p.netloc, key


def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("src")
    p.add_argument("dst")
    p.add_argument("--memory", default="256MiB", help="memory budget, e.g. 512MiB (default 256MiB)")
    p.add_argument("--concurrency", type=int, default=8, help="max parallel parts per file (default 8)")
    p.add_argument("--src-profile", help="AWS profile for reading the source (default: boto3 default chain)")
    p.add_argument("--dst-profile", help="AWS profile for writing the destination (default: same as source)")
    p.add_argument("--max-chunk", default="16MiB",
                   help="largest chunk to use (default 16MiB, the sweet spot). Raising it lets chunks "
                        "grow to fill --memory; prints a warning when that happens")
    p.add_argument("-q", "--quiet", action="store_true", help="no progress output")


def _clients(args):
    import boto3  # imported late so --help works without boto3

    src = boto3.Session(profile_name=args.src_profile).client("s3")
    dst = boto3.Session(profile_name=args.dst_profile).client("s3") if args.dst_profile else None
    return src, dst


def _cmd_cp(args) -> int:
    from .copy import s3_copy
    from .progress import ProgressBar

    sb, sk = _split(args.src)
    db, dk = _split(args.dst)
    src, dst = _clients(args)
    size = src.head_object(Bucket=sb, Key=sk)["ContentLength"]
    bar = ProgressBar(size, enabled=not args.quiet)
    bar.start_file(0, sk, size)
    try:
        res = s3_copy(src, sb, sk, db, dk, dst_client=dst, memory_budget=args.memory,
                      concurrency=args.concurrency, progress=bar.update,
                      max_chunk=args.max_chunk)
    finally:
        bar.close()
    p = res.plan
    if not args.quiet:
        print(f"copied {p.file_size:,} B in {res.parts_uploaded} part(s) "
              f"[chunk {p.chunk_size // 1024**2} MiB x {p.concurrency} streams, "
              f"<= {p.peak_memory // 1024**2} MiB in flight] "
              f"{res.seconds:.1f}s, {res.retries} retries")
    return 0


def _cmd_sync(args) -> int:
    from .progress import ProgressBar, human
    from .sync import s3_sync

    sb, sp = _split(args.src, allow_empty_key=True)
    db, dp = _split(args.dst, allow_empty_key=True)
    src, dst = _clients(args)
    bar: list[ProgressBar] = []

    def on_plan(to_copy, skipped):
        total = sum(i.size for i in to_copy)
        if not args.quiet:
            print(f"{len(to_copy)} to copy ({human(total)}), {len(skipped)} already up to date",
                  file=sys.stderr, flush=True)
        bar.append(ProgressBar(total, len(to_copy), enabled=not args.quiet and bool(to_copy)))

    def on_file(i, item):
        bar[0].start_file(i, item.src_key, item.size)

    try:
        res = s3_sync(src, sb, sp, db, dp, dst_client=dst, memory_budget=args.memory,
                      concurrency=args.concurrency, force=args.force, max_chunk=args.max_chunk,
                      on_plan=on_plan, on_file=on_file,
                      progress=lambda n: bar[0].update(n))
    finally:
        if bar:
            bar[0].close()
    if not args.quiet:
        print(f"synced {len(res.copied)} object(s), {human(res.bytes_copied)}, "
              f"skipped {len(res.skipped)}, {res.seconds:.1f}s, {res.retries} retries")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="s3flat", description="Constant-memory S3 tools")
    sub = ap.add_subparsers(dest="cmd", required=True)

    cp = sub.add_parser("cp", help="copy one object S3 -> S3")
    _add_common(cp)
    cp.set_defaults(func=_cmd_cp)

    sy = sub.add_parser("sync", help="copy every object under a prefix S3 -> S3, skipping up-to-date ones")
    _add_common(sy)
    sy.add_argument("--force", action="store_true", help="copy everything, even if already up to date")
    sy.set_defaults(func=_cmd_sync)

    args = ap.parse_args(argv)
    from .plan import LargeChunkWarning

    seen: set[str] = set()

    def show(message, *_a, **_k):  # print each distinct warning once (sync would repeat it per file)
        if str(message) not in seen:
            seen.add(str(message))
            print(f"warning: {message}", file=sys.stderr, flush=True)

    with warnings.catch_warnings():
        warnings.simplefilter("always", LargeChunkWarning)
        warnings.showwarning = show
        return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
