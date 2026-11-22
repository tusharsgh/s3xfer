"""Command line: builds the objects from the arguments and runs them.

    s3xfer cp   s3://src-bucket/key    s3://dst-bucket/key    --memory 512MiB
    s3xfer sync s3://src-bucket/prefix s3://dst-bucket/prefix --memory 512MiB

This module is the composition root: it's the only place that knows about
argparse, AWS profiles and the terminal. Everything else receives what it
needs as constructor arguments.
"""

from __future__ import annotations

import argparse
import sys

import boto3

from .copy import Copier
from .paths import S3Path
from .plan import MiB, TransferConfig
from .progress import TerminalEvents, human
from .sync import Syncer


def _path(uri: str, *, allow_empty_key: bool = False) -> S3Path:
    try:
        return S3Path.parse(uri, allow_empty_key=allow_empty_key)
    except ValueError as e:
        raise SystemExit(str(e)) from None


def _copier(args, events: TerminalEvents) -> Copier:
    # Always boto3.Session(...) at call time: tests swap it for a fake.
    source = boto3.Session(profile_name=args.src_profile).client("s3")
    dest = boto3.Session(profile_name=args.dst_profile).client("s3") if args.dst_profile else None
    config = TransferConfig(args.memory, args.concurrency, args.max_chunk)
    return Copier(source, dest, config=config, events=events)


def _cmd_cp(args) -> int:
    src, dst = _path(args.src), _path(args.dst)
    res = _copier(args, TerminalEvents(enabled=not args.quiet)).copy(src, dst)
    p = res.plan
    if not args.quiet:
        print(f"copied {p.file_size:,} B in {res.parts_uploaded} part(s) "
              f"[chunk {p.chunk_size // MiB} MiB x {p.concurrency} streams, "
              f"<= {p.peak_memory // MiB} MiB in flight] "
              f"{res.seconds:.1f}s, {res.retries} retries")
    return 0


def _cmd_sync(args) -> int:
    src, dst = _path(args.src, allow_empty_key=True), _path(args.dst, allow_empty_key=True)
    copier = _copier(args, TerminalEvents(enabled=not args.quiet, summary=True))
    res = Syncer(copier, force=args.force).sync(src, dst)
    if not args.quiet:
        print(f"synced {len(res.copied)} object(s), {human(res.bytes_copied)}, "
              f"skipped {len(res.skipped)}, {res.seconds:.1f}s, {res.retries} retries")
    return 0


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


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="s3xfer", description="Constant-memory S3 tools")
    sub = ap.add_subparsers(dest="cmd", required=True)

    cp = sub.add_parser("cp", help="copy one object S3 -> S3")
    _add_common(cp)
    cp.set_defaults(func=_cmd_cp)

    sy = sub.add_parser("sync", help="copy every object under a prefix S3 -> S3, skipping up-to-date ones")
    _add_common(sy)
    sy.add_argument("--force", action="store_true", help="copy everything, even if already up to date")
    sy.set_defaults(func=_cmd_sync)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)
