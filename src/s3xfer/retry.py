"""Retry policy for single S3 calls. No package imports."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


class RetryCounter:
    """Thread-safe count of retries across all worker threads of one transfer."""

    def __init__(self) -> None:
        self._n = 0
        self._lock = threading.Lock()

    def add(self) -> None:
        with self._lock:
            self._n += 1

    @property
    def count(self) -> int:
        return self._n


@dataclass(frozen=True)
class RetryPolicy:
    """Retry a call up to `max_attempts` times with exponential backoff."""

    max_attempts: int = 3
    backoff: float = 0.2

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def call(self, fn: Callable[[], T], counter: RetryCounter | None = None) -> T:
        for attempt in range(1, self.max_attempts + 1):
            try:
                return fn()
            except Exception:
                if attempt == self.max_attempts:
                    raise
                if counter is not None:
                    counter.add()
                time.sleep(self.backoff * 2 ** (attempt - 1))
        raise AssertionError("unreachable")
