"""Draw bench/memory_vs_filesize.png from bench/results.csv.

    python bench/bench_memory.py && python bench/plot.py
"""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = Path(__file__).resolve().parent
BUDGET_MIB = 64
NAIVE, OURS, INK, MUTED, GRID = "#d73027", "#1a7f3c", "#111111", "#777777", "#e5e5e5"


def label(mib: int) -> str:
    return f"{mib // 1024} GiB" if mib >= 1024 else f"{mib} MiB"


def main() -> None:
    rows = list(csv.DictReader(open(HERE / "results.csv")))
    x = list(range(len(rows)))
    sizes = [int(r["file_mib"]) for r in rows]
    naive = [float(r["naive_peak_mib"]) for r in rows]
    ours = [float(r["s3xfer_peak_mib"]) for r in rows]

    fig, ax = plt.subplots(figsize=(10, 5.6), dpi=144)
    ax.plot(x, naive, color=NAIVE, lw=2.5, marker="o", label="Naive copy (read whole file)")
    ax.plot(x, ours, color=OURS, lw=2.5, marker="o", label=f"s3xfer ({BUDGET_MIB} MiB budget)")
    ax.axhline(BUDGET_MIB, color=MUTED, ls="--", lw=1)
    ax.text(x[len(x) // 2], BUDGET_MIB + 15, f"{BUDGET_MIB} MiB budget", color=MUTED, ha="center", fontsize=10)
    ax.annotate(f"{naive[-1]:.0f} MiB", (x[-1], naive[-1]), xytext=(-8, 10), textcoords="offset points",
                ha="right", color=NAIVE, fontsize=10)
    ax.annotate(f"{ours[-1]:.0f} MiB", (x[-1], ours[-1]), xytext=(0, 12), textcoords="offset points",
                ha="center", color=OURS, fontsize=10)

    ax.set_xticks(x, [label(s) for s in sizes])
    ax.set_xlabel("File size", color=INK)
    ax.set_ylabel("Peak memory of the copying process (MiB)", color=INK)
    ax.set_title("Peak memory vs file size: naive grows, s3xfer stays flat",
                 loc="left", fontweight="bold", color=INK)
    ax.grid(axis="y", color=GRID)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.legend(frameon=False, loc="upper left")
    fig.text(0.01, 0.01, "Measured with tracemalloc against a local moto S3 server.",
             color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(HERE / "memory_vs_filesize.png")
    print(f"wrote {HERE / 'memory_vs_filesize.png'}")


if __name__ == "__main__":
    main()
