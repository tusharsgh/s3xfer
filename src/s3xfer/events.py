"""What a transfer reports while it runs.

A Copier or Syncer is given one TransferEvents object and tells it what is
happening; it never knows whether that's a terminal progress bar, a log, a
metrics exporter or nothing at all.

Order of calls for one run (one `cp`, or one `sync` of many objects):

    start(items, skipped)            once
    file_started(index, item)        before each object
    advance(nbytes)                  as parts finish  <- from worker threads
    warning(message)                 any time (e.g. large chunks)
    finished()                       once, always (even on failure)
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Sequence
from typing import Protocol

from .paths import TransferItem
from .plan import LargeChunkWarning


class TransferEvents(Protocol):
    def start(self, items: Sequence[TransferItem], skipped: Sequence[TransferItem]) -> None: ...
    def file_started(self, index: int, item: TransferItem) -> None: ...
    def advance(self, nbytes: int) -> None: ...
    def warning(self, message: str) -> None: ...
    def finished(self) -> None: ...


class NullEvents:
    """Reports nothing, except warnings, which go to Python's `warnings` module."""

    def start(self, items: Sequence[TransferItem], skipped: Sequence[TransferItem]) -> None:
        pass

    def file_started(self, index: int, item: TransferItem) -> None:
        pass

    def advance(self, nbytes: int) -> None:
        pass

    def warning(self, message: str) -> None:
        warnings.warn(message, LargeChunkWarning, stacklevel=3)

    def finished(self) -> None:
        pass


class CallbackEvents(NullEvents):
    """Adapts plain callables (the s3_copy / s3_sync keyword arguments) to TransferEvents."""

    def __init__(
        self,
        progress: Callable[[int], None] | None = None,
        on_plan: Callable[[list[TransferItem], list[TransferItem]], None] | None = None,
        on_file: Callable[[int, TransferItem], None] | None = None,
    ) -> None:
        self._progress, self._on_plan, self._on_file = progress, on_plan, on_file

    def start(self, items: Sequence[TransferItem], skipped: Sequence[TransferItem]) -> None:
        if self._on_plan:
            self._on_plan(list(items), list(skipped))

    def file_started(self, index: int, item: TransferItem) -> None:
        if self._on_file:
            self._on_file(index, item)

    def advance(self, nbytes: int) -> None:
        if self._progress:
            self._progress(nbytes)
