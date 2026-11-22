"""Terminal reporting for the CLI: a progress bar and a TransferEvents for it.

On a terminal the bar redraws one line in place. When output is redirected
(CI logs, files) it prints a plain line per file instead, so logs stay
readable.
"""

from __future__ import annotations

import shutil
import sys
import threading
import time
from collections.abc import Sequence

from .paths import TransferItem


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n} B"


class ProgressBar:
    """Thread-safe: `update()` may be called from copy worker threads."""

    def __init__(self, total_bytes: int, total_files: int = 1, *, stream=None,
                 enabled: bool = True, min_interval: float = 0.1):
        self.total = max(total_bytes, 0)
        self.total_files = total_files
        self.done = 0
        self.file_index = 0
        self.label = ""
        self.stream = stream or sys.stderr
        self.enabled = enabled
        self.tty = enabled and hasattr(self.stream, "isatty") and self.stream.isatty()
        self._lock = threading.Lock()
        self._min_interval = min_interval
        self._last_draw = 0.0
        self._started = time.monotonic()

    def start_file(self, index: int, label: str, size: int) -> None:
        with self._lock:
            self.file_index, self.label = index, label
            if self.enabled and not self.tty:
                print(f"[{index + 1}/{self.total_files}] {label} ({human(size)})",
                      file=self.stream, flush=True)
            self._draw(force=True)

    def update(self, nbytes: int) -> None:
        with self._lock:
            self.done += nbytes
            self._draw()

    def close(self) -> None:
        with self._lock:
            self._draw(force=True)
            if self.tty:
                self.stream.write("\n")
                self.stream.flush()

    def _draw(self, force: bool = False) -> None:
        if not self.tty:
            return
        now = time.monotonic()
        if not force and now - self._last_draw < self._min_interval:
            return
        self._last_draw = now
        frac = 1.0 if self.total == 0 else min(self.done / self.total, 1.0)
        elapsed = max(now - self._started, 1e-9)
        rate = self.done / elapsed
        eta = (self.total - self.done) / rate if rate > 0 else 0
        files = f" {self.file_index + 1}/{self.total_files}" if self.total_files > 1 else ""
        stats = (f" {frac:4.0%} {human(self.done)}/{human(self.total)}"
                 f" {human(rate)}/s ETA {int(eta) // 60}:{int(eta) % 60:02d}{files} ")
        width = shutil.get_terminal_size((80, 20)).columns
        bar_w = max(10, min(25, width - 70))  # fixed width so the line doesn't jitter
        filled = int(bar_w * frac)
        line = f"[{'#' * filled}{'.' * (bar_w - filled)}]{stats}"
        room = width - len(line) - 1
        label = self.label if len(self.label) <= room else "…" + self.label[-(room - 1):] if room > 1 else ""
        self.stream.write("\r" + (line + label)[: width - 1].ljust(width - 1))
        self.stream.flush()


class TerminalEvents:
    """TransferEvents that drives a ProgressBar on stderr.

    `summary=True` also prints "N to copy (size), M already up to date" when
    the run starts (used by `sync`). Each distinct warning is printed once,
    even with `enabled=False`, so -q never hides a warning.
    """

    def __init__(self, *, enabled: bool = True, summary: bool = False, stream=None):
        self.enabled = enabled
        self.summary = summary
        self.stream = stream or sys.stderr
        self.bar: ProgressBar | None = None
        self._warned: set[str] = set()
        self._lock = threading.Lock()

    def start(self, items: Sequence[TransferItem], skipped: Sequence[TransferItem]) -> None:
        total = sum(i.size for i in items)
        if self.summary and self.enabled:
            print(f"{len(items)} to copy ({human(total)}), {len(skipped)} already up to date",
                  file=self.stream, flush=True)
        self.bar = ProgressBar(total, len(items), stream=self.stream,
                               enabled=self.enabled and bool(items))

    def file_started(self, index: int, item: TransferItem) -> None:
        if self.bar:
            self.bar.start_file(index, item.src_key, item.size)

    def advance(self, nbytes: int) -> None:
        if self.bar:
            self.bar.update(nbytes)

    def warning(self, message: str) -> None:
        with self._lock:
            if message in self._warned:
                return
            self._warned.add(message)
        # On a terminal the bar line is mid-draw; start the warning on a fresh line.
        prefix = "\n" if self.bar and self.bar.tty else ""
        print(f"{prefix}warning: {message}", file=self.stream, flush=True)

    def finished(self) -> None:
        if self.bar:
            self.bar.close()
