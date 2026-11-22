"""Copy every object under an S3 prefix to another prefix, in constant memory.

Objects are copied one at a time (each copy is already parallel inside),
so the memory budget applies to the whole sync, not per file.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from .copy import Copier
from .events import CallbackEvents
from .paths import S3Path, TransferItem
from .plan import PREFERRED_CHUNK, TransferConfig
from .retry import RetryPolicy

SyncItem = TransferItem  # kept for code written against the original API


@dataclass
class SyncResult:
    copied: list[TransferItem] = field(default_factory=list)
    skipped: list[TransferItem] = field(default_factory=list)
    bytes_copied: int = 0
    retries: int = 0
    seconds: float = 0.0


def _as_dir(prefix: str) -> str:
    """'data' -> 'data/' so it can't match 'data2/...'. '' stays '' (whole bucket)."""
    prefix = prefix.lstrip("/")
    return prefix if not prefix or prefix.endswith("/") else prefix + "/"


def _list(client, bucket: str, prefix: str):
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


class Syncer:
    """Copies a prefix to another prefix, skipping objects that are up to date.

    It is composed from a Copier and uses that copier's clients (the source
    client lists the source, the dest client lists the destination), its
    config and retry policy for every object, and its events for reporting.
    """

    def __init__(self, copier: Copier, *, force: bool = False):
        self.copier = copier
        self.force = force

    def plan(self, src: S3Path, dst: S3Path) -> tuple[list[TransferItem], list[TransferItem]]:
        """Work out which objects need copying. Returns (to_copy, skipped).

        An object is skipped when the destination already has it with the same
        size and a LastModified that isn't older than the source (the same rule
        `aws s3 sync` uses). `force=True` copies everything.
        """
        src = S3Path(src.bucket, _as_dir(src.key))
        dst = S3Path(dst.bucket, _as_dir(dst.key))

        existing = {}
        if not self.force:
            existing = {o["Key"]: o for o in _list(self.copier.dest, dst.bucket, dst.key)}

        to_copy, skipped = [], []
        for obj in _list(self.copier.source, src.bucket, src.key):
            key = obj["Key"]
            if key.endswith("/"):  # "folder" placeholder objects
                continue
            item = TransferItem(S3Path(src.bucket, key), dst.child(key[len(src.key):]), obj["Size"])
            have = existing.get(item.dst_key)
            if have and have["Size"] == obj["Size"] and have["LastModified"] >= obj["LastModified"]:
                skipped.append(item)
            else:
                to_copy.append(item)
        return to_copy, skipped

    def sync(self, src: S3Path, dst: S3Path) -> SyncResult:
        started = time.monotonic()
        events = self.copier.events
        to_copy, skipped = self.plan(src, dst)
        events.start(to_copy, skipped)
        result = SyncResult(skipped=skipped)
        try:
            for i, item in enumerate(to_copy):
                events.file_started(i, item)
                r = self.copier.copy_item(item)
                result.copied.append(item)
                result.bytes_copied += item.size
                result.retries += r.retries
        finally:
            events.finished()
        result.seconds = time.monotonic() - started
        return result


# --- function shortcuts (original API) --------------------------------------------

def plan_sync(client, src_bucket: str, src_prefix: str, dst_bucket: str, dst_prefix: str,
              *, dst_client=None, force: bool = False) -> tuple[list[TransferItem], list[TransferItem]]:
    """Shortcut for Syncer(Copier(client, dst_client), force=force).plan(...)."""
    return Syncer(Copier(client, dst_client), force=force).plan(
        S3Path(src_bucket, src_prefix), S3Path(dst_bucket, dst_prefix))


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
    on_plan: Callable[[list[TransferItem], list[TransferItem]], None] | None = None,
    on_file: Callable[[int, TransferItem], None] | None = None,
    progress: Callable[[int], None] | None = None,
    max_chunk: int | str = PREFERRED_CHUNK,
    max_attempts: int = 3,
    backoff: float = 0.2,
) -> SyncResult:
    """Copy all objects under src_prefix to dst_prefix. Shortcut for Syncer(...).sync(...).

    Callbacks: on_plan(to_copy, skipped) once after listing; on_file(index, item)
    before each object; progress(nbytes) as parts finish (from worker threads).
    """
    copier = Copier(client, dst_client,
                    config=TransferConfig(memory_budget, concurrency, max_chunk),
                    retry=RetryPolicy(max_attempts, backoff),
                    events=CallbackEvents(progress=progress, on_plan=on_plan, on_file=on_file))
    return Syncer(copier, force=force).sync(S3Path(src_bucket, src_prefix),
                                            S3Path(dst_bucket, dst_prefix))
