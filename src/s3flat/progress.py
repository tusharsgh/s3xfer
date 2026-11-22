"""A dependency-free progress bar for the CLI.

On a terminal it redraws one line in place. When output is redirected
(CI logs, files) it prints a plain line per file instead, so logs stay
readable.
"""

from __future__ import annotations

import shutil
import sys
import threading
import time


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
