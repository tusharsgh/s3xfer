"""Copy every object under an S3 prefix to another prefix, in constant memory.

Objects are copied one at a time (each copy is already parallel inside),
so the memory budget applies to the whole sync, not per file.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from .copy import s3_copy


@dataclass
class SyncItem:
    src_key: str
    dst_key: str
    size: int


@dataclass
class SyncResult:
    copied: list[SyncItem] = field(default_factory=list)
    skipped: list[SyncItem] = field(default_factory=list)
    bytes_copied: int = 0
    retries: int = 0
    seconds: float = 0.0


def _as_dir(prefix: str) -> str:
    """'data' -> 'data/' so it can't match 'data2/...'. '' stays '' (whole bucket)."""
    prefix = prefix.lstrip("/")
    return prefix if not prefix or prefix.endswith("/") else prefix + "/"


def _list(client, bucket: str, prefix: str):
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj


def plan_sync(client, src_bucket: str, src_prefix: str, dst_bucket: str, dst_prefix: str,
              *, dst_client=None, force: bool = False) -> tuple[list[SyncItem], list[SyncItem]]:
    """Work out which objects need copying. Returns (to_copy, skipped).

    An object is skipped when the destination already has it with the same
    size and a LastModified that isn't older than the source (the same rule
    `aws s3 sync` uses). `force=True` copies everything.
    """
    dst = dst_client or client
    src_prefix, dst_prefix = _as_dir(src_prefix), _as_dir(dst_prefix)

    existing = {}
    if not force:
        for obj in _list(dst, dst_bucket, dst_prefix):
            existing[obj["Key"]] = obj

    to_copy, skipped = [], []
    for obj in _list(client, src_bucket, src_prefix):
        key = obj["Key"]
        if key.endswith("/"):  # "folder" placeholder objects
            continue
        item = SyncItem(key, dst_prefix + key[len(src_prefix):], obj["Size"])
        have = existing.get(item.dst_key)
        if have and have["Size"] == obj["Size"] and have["LastModified"] >= obj["LastModified"]:
            skipped.append(item)
        else:
            to_copy.append(item)
    return to_copy, skipped


def s3_sync(
    client,
    src_bucket: str,
    src_prefix: str,
    dst_bucket: str,
    dst_prefix: str,
    *,
    dst_client=None,
    memory_budget: int | str = "256MiB",
    concurrency: int = 8,
    force: bool = False,
    on_plan: Callable[[list[SyncItem], list[SyncItem]], None] | None = None,
    on_file: Callable[[int, SyncItem], None] | None = None,
    progress: Callable[[int], None] | None = None,
    **copy_kwargs,
) -> SyncResult:
    """Copy all objects under src_prefix to dst_prefix.

    Callbacks for showing progress:
      on_plan(to_copy, skipped)  once, after listing
      on_file(index, item)       before each object is copied (index from 0)
      progress(nbytes)           as bytes finish uploading (thread-safe needed)
    """
    started = time.monotonic()
    to_copy, skipped = plan_sync(client, src_bucket, src_prefix, dst_bucket, dst_prefix,
                                 dst_client=dst_client, force=force)
    if on_plan:
        on_plan(to_copy, skipped)

    result = SyncResult(skipped=skipped)
    for i, item in enumerate(to_copy):
        if on_file:
            on_file(i, item)
        r = s3_copy(client, src_bucket, item.src_key, dst_bucket, item.dst_key,
                    dst_client=dst_client, memory_budget=memory_budget,
                    concurrency=concurrency, progress=progress, **copy_kwargs)
        result.copied.append(item)
        result.bytes_copied += item.size
        result.retries += r.retries
    result.seconds = time.monotonic() - started
    return result
