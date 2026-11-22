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

from .plan import MIN_PART_SIZE, PREFERRED_CHUNK, TransferPlan, plan_transfer


@dataclass
class CopyResult:
    plan: TransferPlan
    parts_uploaded: int
    retries: int
    seconds: float


def _with_retries(fn, attempts: int, backoff: float, counter: list[int], lock: threading.Lock):
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception:
            if attempt == attempts:
                raise
            with lock:
                counter[0] += 1
            time.sleep(backoff * 2 ** (attempt - 1))


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
    """Copy one object with bounded memory.

    `dst_client` lets source and destination use different credentials
    (e.g. different accounts). Data streams through this process, so peak
    memory stays near plan.peak_memory however large the object is.

    `progress`, if given, is called with the number of bytes each time a
    part finishes uploading (like boto3's Callback). It may be called from
    worker threads, so it must be thread-safe.

    `max_chunk` raises the 16 MiB chunk ceiling (see plan_transfer).
    """
    dst = dst_client or client
    started = time.monotonic()
    size = client.head_object(Bucket=src_bucket, Key=src_key)["ContentLength"]
    plan = plan_transfer(size, memory_budget, concurrency, max_chunk)
    retries, lock = [0], threading.Lock()

    # Small objects: one GET, one PUT.
    if size <= MIN_PART_SIZE:
        body = _with_retries(
            lambda: client.get_object(Bucket=src_bucket, Key=src_key)["Body"].read(),
            max_attempts, backoff, retries, lock)
        _with_retries(lambda: dst.put_object(Bucket=dst_bucket, Key=dst_key, Body=body),
                      max_attempts, backoff, retries, lock)
        if progress:
            progress(size)
        return CopyResult(plan, 1, retries[0], time.monotonic() - started)

    upload_id = dst.create_multipart_upload(Bucket=dst_bucket, Key=dst_key)["UploadId"]

    # The semaphore is what makes memory constant: at most `concurrency`
    # chunks exist at once, however many parts the file has.
    slots = threading.BoundedSemaphore(plan.concurrency)
    failed = threading.Event()

    def copy_part(part_number: int) -> dict:
        start = (part_number - 1) * plan.chunk_size
        end = min(start + plan.chunk_size, size) - 1
        try:
            data = _with_retries(
                lambda: client.get_object(Bucket=src_bucket, Key=src_key,
                                          Range=f"bytes={start}-{end}")["Body"].read(),
                max_attempts, backoff, retries, lock)
            etag = _with_retries(
                lambda: dst.upload_part(Bucket=dst_bucket, Key=dst_key, UploadId=upload_id,
                                        PartNumber=part_number, Body=data)["ETag"],
                max_attempts, backoff, retries, lock)
            if progress:
                progress(end - start + 1)
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
        dst.complete_multipart_upload(Bucket=dst_bucket, Key=dst_key, UploadId=upload_id,
                                      MultipartUpload={"Parts": parts})
    except BaseException:
        dst.abort_multipart_upload(Bucket=dst_bucket, Key=dst_key, UploadId=upload_id)
        raise
    return CopyResult(plan, len(parts), retries[0], time.monotonic() - started)
