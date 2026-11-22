"""Chunk planning: turn a file size and a memory budget into a safe transfer plan.

The rule the whole library is built on:

    peak memory ~= concurrency x chunk_size x COPY_FACTOR

File size never decides how much memory we use. It only decides how many
chunks there are.
"""

from __future__ import annotations

import math
import re
import warnings
from dataclasses import dataclass

MiB = 1024 * 1024
GiB = 1024 * MiB

# S3 multipart limits (every part except the last must be >= 5 MiB,
# at most 10,000 parts, at most 5 GiB per part).
MIN_PART_SIZE = 5 * MiB
MAX_PART_SIZE = 5 * GiB
MAX_PARTS = 10_000

# Each in-flight chunk can exist roughly twice in memory
# (response buffer + the bytes object handed to upload_part).
COPY_FACTOR = 2

# AWS guidance: 8-16 MB ranges give good throughput per connection.
PREFERRED_CHUNK = 16 * MiB

_UNITS = {"": 1, "B": 1, "KB": 1000, "MB": 1000**2, "GB": 1000**3,
          "KIB": 1024, "MIB": MiB, "GIB": GiB}


def parse_size(value: int | str) -> int:
    """Parse '512MB', '1.5GiB', '1048576' or an int into bytes."""
    if isinstance(value, int):
        return value
    m = re.fullmatch(r"\s*([\d.]+)\s*([a-zA-Z]*)\s*", value)
    if not m or m.group(2).upper() not in _UNITS:
        raise ValueError(f"Can't parse size: {value!r}")
    return int(float(m.group(1)) * _UNITS[m.group(2).upper()])


@dataclass(frozen=True)
class TransferPlan:
    file_size: int
    chunk_size: int
    concurrency: int
    num_parts: int
    warning: str | None = None  # set when max_chunk let chunks grow past 16 MiB

    @property
    def peak_memory(self) -> int:
        """Upper bound on bytes held in flight."""
        return self.concurrency * self.chunk_size * COPY_FACTOR


class BudgetTooSmall(ValueError):
    """The memory budget can't fit even one minimum-size part."""


class LargeChunkWarning(UserWarning):
    """Chunks above PREFERRED_CHUNK were allowed with max_chunk and are being used."""


def plan_transfer(file_size: int, memory_budget: int | str, concurrency: int = 8,
                  max_chunk: int | str = PREFERRED_CHUNK) -> TransferPlan:
    """Plan a transfer, issuing a LargeChunkWarning if plan.warning is set.

    See `make_plan` for the rule. Library code that reports warnings its own
    way (Copier, via TransferEvents) calls `make_plan` instead.
    """
    plan = make_plan(file_size, memory_budget, concurrency, max_chunk)
    if plan.warning:
        warnings.warn(plan.warning, LargeChunkWarning, stacklevel=2)
    return plan


def make_plan(file_size: int, memory_budget: int | str, concurrency: int = 8,
              max_chunk: int | str = PREFERRED_CHUNK) -> TransferPlan:
    """Pick a chunk size and concurrency that respect the memory budget.

    The budget is shared between the streams: asking for more concurrency
    makes each chunk smaller, so memory stays at the budget instead of
    growing with concurrency.

        chunk = budget / (concurrency x COPY_FACTOR), clamped to
                [smallest part S3 allows for this file, max_chunk (default 16 MiB)]

    By default chunks never grow past PREFERRED_CHUNK (16 MiB) just because
    the budget is big: huge parts don't speed anything up and make every
    retry expensive. Pass a larger `max_chunk` (up to 5 GiB) to let chunks
    grow to fill the budget anyway; plan.warning explains the trade-off when
    that actually produces chunks over 16 MiB. (S3's 10,000-part limit on very
    large files can also force bigger chunks; that never warns.)
    Concurrency is reduced only when even the smallest allowed chunk
    doesn't fit `concurrency` times in the budget.
    """
    budget = parse_size(memory_budget)
    max_chunk = parse_size(max_chunk)
    if not MIN_PART_SIZE <= max_chunk <= MAX_PART_SIZE:
        raise ValueError("max_chunk must be between 5 MiB and 5 GiB")
    if file_size < 0:
        raise ValueError("file_size must be >= 0")
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")

    # Smallest chunk S3 allows for this file (5 MiB minimum, 10,000-part limit),
    # rounded up to a whole MiB.
    min_chunk = _round_up(max(MIN_PART_SIZE, math.ceil(file_size / MAX_PARTS)), MiB)
    if min_chunk > MAX_PART_SIZE:
        raise ValueError("File is larger than S3's 5 TiB multipart maximum")
    ceiling = max(max_chunk // MiB * MiB, min_chunk)
    ceiling = min(ceiling, max(_round_up(file_size, MiB), min_chunk))  # never bigger than the file

    # Don't plan more streams than there could ever be parts.
    streams = min(concurrency, max(1, math.ceil(file_size / min_chunk)))

    # Split the budget between the streams; round down to a whole MiB.
    share = budget // (streams * COPY_FACTOR)
    chunk = min(max(share // MiB * MiB, min_chunk), ceiling)

    # Even the smallest chunk doesn't fit `streams` times: fewer streams.
    streams = min(streams, budget // (chunk * COPY_FACTOR))
    if streams < 1:
        raise BudgetTooSmall(
            f"Budget {budget} B can't hold one {chunk} B part "
            f"(needs >= {chunk * COPY_FACTOR} B)"
        )

    num_parts = max(1, math.ceil(file_size / chunk))
    streams = min(streams, num_parts)
    warning = None
    if chunk > max(PREFERRED_CHUNK, min_chunk) and num_parts > 1:
        warning = (f"using {chunk // MiB} MiB chunks (max_chunk raised above "
                   f"{PREFERRED_CHUNK // MiB} MiB). Bigger chunks rarely add speed, and each "
                   f"failed part retries {chunk // MiB} MiB instead of {PREFERRED_CHUNK // MiB} MiB.")
    return TransferPlan(file_size, chunk, streams, num_parts, warning)


@dataclass(frozen=True)
class TransferConfig:
    """How much memory and parallelism a transfer may use."""

    memory_budget: int | str = "256MiB"
    concurrency: int = 8
    max_chunk: int | str = PREFERRED_CHUNK

    def plan(self, file_size: int) -> TransferPlan:
        return make_plan(file_size, self.memory_budget, self.concurrency, self.max_chunk)


def _round_up(n: int, step: int) -> int:
    return -(-n // step) * step
