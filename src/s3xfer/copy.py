"""Constant-memory S3 -> S3 copy.

Parallel range GETs on the source, multipart upload on the destination.
Parts may finish in any order; CompleteMultipartUpload stitches them
together by part number, so ordering is never our problem.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from .events import CallbackEvents, NullEvents, TransferEvents
from .paths import S3Path, TransferItem
from .plan import MIN_PART_SIZE, PREFERRED_CHUNK, TransferConfig, TransferPlan
from .retry import RetryCounter, RetryPolicy


@dataclass
class CopyResult:
    plan: TransferPlan
    parts_uploaded: int
    retries: int
    seconds: float


class Copier:
    """Copies objects with bounded memory. Built from its collaborators:

    source   boto3 S3 client used for every read (head/get)
    dest     boto3 S3 client used for every write (defaults to `source`);
             a different client lets the two sides use different credentials
    config   memory budget, concurrency, max chunk  -> TransferConfig
    retry    per-call retry policy                    -> RetryPolicy
    events   where progress and warnings go          -> TransferEvents

    Data streams through this process, so peak memory stays near
    plan.peak_memory however large the object is.
    """

    def __init__(self, source, dest=None, *, config: TransferConfig | None = None,
                 retry: RetryPolicy | None = None, events: TransferEvents | None = None):
        self.source = source
        self.dest = dest or source
        self.config = config or TransferConfig()
        self.retry = retry or RetryPolicy()
        self.events: TransferEvents = events or NullEvents()

    def copy(self, src: S3Path, dst: S3Path) -> CopyResult:
        """Copy one object, reporting a whole run (start .. finished) to `events`."""
        size = self.source.head_object(Bucket=src.bucket, Key=src.key)["ContentLength"]
        item = TransferItem(src, dst, size)
        self.events.start([item], [])
        try:
            self.events.file_started(0, item)
            return self.copy_item(item)
        finally:
            self.events.finished()

    def copy_item(self, item: TransferItem) -> CopyResult:
        """Copy one object as part of a larger run: only reports bytes and warnings.

        The caller (copy() or a Syncer) owns start/file_started/finished.
        """
        started = time.monotonic()
        src, dst, size = item.src, item.dst, item.size
        plan = self.config.plan(size)
        if plan.warning:
            self.events.warning(plan.warning)
        retries = RetryCounter()
        call = lambda fn: self.retry.call(fn, retries)  # noqa: E731

        # Small objects: one GET, one PUT.
        if size <= MIN_PART_SIZE:
            body = call(lambda: self.source.get_object(Bucket=src.bucket, Key=src.key)["Body"].read())
            call(lambda: self.dest.put_object(Bucket=dst.bucket, Key=dst.key, Body=body))
            self.events.advance(size)
            return CopyResult(plan, 1, retries.count, time.monotonic() - started)

        upload_id = self.dest.create_multipart_upload(Bucket=dst.bucket, Key=dst.key)["UploadId"]

        # The semaphore is what makes memory constant: at most `concurrency`
        # chunks exist at once, however many parts the file has.
        slots = threading.BoundedSemaphore(plan.concurrency)
        failed = threading.Event()

        def copy_part(part_number: int) -> dict:
            start = (part_number - 1) * plan.chunk_size
            end = min(start + plan.chunk_size, size) - 1
            try:
                data = call(lambda: self.source.get_object(
                    Bucket=src.bucket, Key=src.key, Range=f"bytes={start}-{end}")["Body"].read())
                etag = call(lambda: self.dest.upload_part(
                    Bucket=dst.bucket, Key=dst.key, UploadId=upload_id,
                    PartNumber=part_number, Body=data)["ETag"])
                self.events.advance(end - start + 1)
                return {"PartNumber": part_number, "ETag": etag}
            except BaseException:
                failed.set()
                raise
            finally:
                slots.release()

        parts: list[dict] = []
        try:
            with ThreadPoolExecutor(max_workers=plan.concurrency) as pool:
                futures = []
                for n in range(1, plan.num_parts + 1):
                    slots.acquire()  # back-pressure: don't schedule more than we can hold
                    if failed.is_set():
                        slots.release()
                        break
                    futures.append(pool.submit(copy_part, n))
                for f in as_completed(futures):
                    parts.append(f.result())
            parts.sort(key=lambda p: p["PartNumber"])
            self.dest.complete_multipart_upload(Bucket=dst.bucket, Key=dst.key, UploadId=upload_id,
                                                MultipartUpload={"Parts": parts})
        except BaseException:
            self.dest.abort_multipart_upload(Bucket=dst.bucket, Key=dst.key, UploadId=upload_id)
            raise
        return CopyResult(plan, len(parts), retries.count, time.monotonic() - started)


def s3_copy(
    client,
    src_bucket: str,
    src_key: str,
    dst_bucket: str,
    dst_key: str,
    *,
    memory_budget: int | str = "256MB",
    concurrency: int = 8,
    dst_client=None,
    max_attempts: int = 3,
    backoff: float = 0.2,
    progress: Callable[[int], None] | None = None,
    max_chunk: int | str = PREFERRED_CHUNK,
) -> CopyResult:
    """Copy one object with bounded memory. Shortcut for Copier(...).copy(...).

    `dst_client` lets source and destination use different credentials.
    `progress(nbytes)` is called as parts finish, from worker threads.
    `max_chunk` raises the 16 MiB chunk ceiling (issues a LargeChunkWarning).
    """
    copier = Copier(client, dst_client,
                    config=TransferConfig(memory_budget, concurrency, max_chunk),
                    retry=RetryPolicy(max_attempts, backoff),
                    events=CallbackEvents(progress=progress))
    return copier.copy(S3Path(src_bucket, src_key), S3Path(dst_bucket, dst_key))
