"""Draw the real-S3 charts from bench/real_s3_results.csv (see bench_real_s3.py).

    python bench/plot_real_s3.py

Writes bench/real_s3_concurrency.png, bench/real_s3_chunk_size.png and
bench/real_s3_file_size.png. Each point/bar is the median of the reps.
"""

from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = Path(__file__).resolve().parent

# Reference palette (light): one series per panel -> categorical slot 1.
SERIES = "#2a78d6"
SURFACE, INK, INK_2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": AXIS, "axes.labelcolor": INK_2, "xtick.color": MUTED, "ytick.color": MUTED,
    "xtick.labelcolor": INK_2, "ytick.labelcolor": INK_2, "axes.titlecolor": INK,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
    "axes.grid.axis": "y", "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "font.size": 10, "axes.titlesize": 11, "axes.titleweight": "bold", "axes.titlelocation": "left",
})


def load():
    rows = list(csv.DictReader(open(HERE / "real_s3_results.csv")))
    meta = json.loads((HERE / "real_s3_meta.json").read_text())
    groups = defaultdict(list)
    for r in rows:
        groups[(r["experiment"], int(r["file_mib"]), r["memory"], int(r["concurrency"]))].append(r)

    def med(exp):
        out = []
        for (e, size, mem, conc), rs in sorted(groups.items(), key=lambda kv: (kv[0][1], kv[0][3],
                                                                             int(kv[1][0]["chunk_mib"]))):
            if e != exp:
                continue
            out.append(dict(file_mib=size, memory=mem, concurrency=conc,
                            chunk=int(rs[0]["chunk_mib"]), streams=int(rs[0]["streams"]),
                            mibs=statistics.median(float(r["mib_per_s"]) for r in rs),
                            rss=statistics.median(float(r["peak_rss_mib"]) for r in rs),
                            secs=statistics.median(float(r["seconds"]) for r in rs), n=len(rs)))
        return out
    return med, meta


def footer(fig, meta):
    where = f"{meta.get('instance_type') or meta.get('machine')} in {meta.get('az') or meta.get('region')}"
    fig.text(0.01, 0.015, f"Real S3, {where}, s3xfer {meta['s3xfer']}, Python {meta['python']}. "
             f"Median of {meta['reps']} runs.\nPeak memory = max RSS of the copying process: data in flight "
             f"(≤ budget) + ~60 MiB fixed for Python, boto3 and the CRT.",
             color=MUTED, fontsize=8)


def budget_line(ax, mib, side="right"):
    ax.axhline(mib, color=MUTED, ls=(0, (4, 3)), lw=1)
    ax.annotate(f"{mib} MiB budget", (1 if side == "right" else 0, mib), xycoords=("axes fraction", "data"),
                xytext=(0 if side == "right" else 4, 4), textcoords="offset points", ha=side,
                color=INK_2, fontsize=9)


def line(ax, xs, ys, labels=None):
    ax.plot(xs, ys, color=SERIES, lw=2, marker="o", ms=7, markeredgecolor=SURFACE, markeredgewidth=2)
    for x, y, t in zip(xs, ys, labels or []):
        if t:
            ax.annotate(t, (x, y), xytext=(0, 9), textcoords="offset points", ha="center",
                        color=INK, fontsize=9)


def bars(ax, names, ys, fmt):
    b = ax.bar(names, ys, width=0.55, color=SERIES, edgecolor=SURFACE, linewidth=2)
    for rect, y in zip(b, ys):
        ax.annotate(fmt(y), (rect.get_x() + rect.get_width() / 2, y), xytext=(0, 4),
                    textcoords="offset points", ha="center", color=INK, fontsize=9)


def concurrency_chart(med, meta):
    d = med("concurrency")
    xs = list(range(len(d)))
    ticks = [f"{r['concurrency']}\n{r['chunk']}×{r['streams']}" for r in d]
    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4.6), dpi=144)
    fig.suptitle("Fixed 256 MiB budget: memory grows until the budget is full (8 streams), "
                 "then stays flat as chunks shrink",
                 x=0.01, ha="left", fontweight="bold", color=INK, fontsize=12)
    line(a, xs, [r["mibs"] for r in d], [f"{r['mibs']:.0f}" for r in d])
    a.set_title("Throughput (MiB/s)")
    line(b, xs, [r["rss"] for r in d], [f"{r['rss']:.0f}" for r in d])
    budget_line(b, 256, side="left")
    b.set_title("Peak memory (MiB)")
    for ax in (a, b):
        ax.set_xticks(xs, ticks)
        ax.set_xlabel("--concurrency   (chunk MiB × streams used)")
        ax.set_ylim(0, None)
        ax.margins(y=0.15)
    b.set_ylim(0, max(max(r["rss"] for r in d), 256) * 1.25)
    fig.tight_layout(rect=(0, 0.07, 1, 0.95))
    footer(fig, meta)
    return fig, d


def chunk_chart(med, meta):
    d = sorted(med("chunk"), key=lambda r: r["chunk"])
    names = [f"{r['chunk']} MiB\n({r['memory']})" for r in d]
    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4.6), dpi=144)
    speeds = [r["mibs"] for r in d]
    spread = 1 - min(speeds) / max(speeds)
    fig.suptitle(f"Same {d[0]['streams']} streams, different chunk sizes: memory scales with chunk size "
                 f"({d[0]['rss']:.0f} → {d[-1]['rss']:.0f} MiB), throughput within {spread:.0%}",
                 x=0.01, ha="left", fontweight="bold", color=INK, fontsize=12)
    bars(a, names, [r["mibs"] for r in d], lambda v: f"{v:.0f}")
    a.set_title("Throughput (MiB/s)")
    bars(b, names, [r["rss"] for r in d], lambda v: f"{v:.0f}")
    b.set_title("Peak memory (MiB)")
    for ax in (a, b):
        ax.set_xlabel("chunk size   (--memory that produces it)")
        ax.margins(y=0.12)
    fig.tight_layout(rect=(0, 0.07, 1, 0.95))
    footer(fig, meta)
    return fig, d


def filesize_chart(med, meta):
    d = sorted(med("filesize"), key=lambda r: r["file_mib"])
    xs = list(range(len(d)))
    gib = [f"{r['file_mib'] / 1024:g} GiB" for r in d]
    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4.6), dpi=144)
    first, last = d[0], d[-1]
    fig.suptitle(f"{last['file_mib'] // first['file_mib']}× the data, peak memory {first['rss']:.0f} → "
                 f"{last['rss']:.0f} MiB: bounded by the budget + fixed overhead, not by file size",
                 x=0.01, ha="left", fontweight="bold", color=INK, fontsize=12)
    line(a, xs, [r["rss"] for r in d], [f"{r['rss']:.0f}" for r in d])
    budget_line(a, 256)
    a.set_title("Peak memory (MiB)")
    a.set_ylim(0, max(max(r["rss"] for r in d), 256) * 1.3)
    line(b, xs, [r["mibs"] for r in d], [f"{r['mibs']:.0f}" for r in d])
    b.set_title("Throughput (MiB/s)")
    b.set_ylim(0, max(r["mibs"] for r in d) * 1.3)
    for ax in (a, b):
        ax.set_xticks(xs, gib)
        ax.set_xlabel(f"file size   (--memory 256MiB, --concurrency {d[0]['concurrency']})")
        ax.set_xlim(-0.4, len(xs) - 0.6)
    fig.tight_layout(rect=(0, 0.07, 1, 0.95))
    footer(fig, meta)
    return fig, d


def main() -> None:
    med, meta = load()
    for name, make in (("real_s3_concurrency", concurrency_chart), ("real_s3_chunk_size", chunk_chart),
                       ("real_s3_file_size", filesize_chart)):
        fig, data = make(med, meta)
        fig.savefig(HERE / f"{name}.png")
        plt.close(fig)
        print(f"wrote bench/{name}.png")
        for r in data:
            print(f"   {r['file_mib']:>6} MiB  {r['memory']:>7}  c={r['concurrency']:<3} "
                  f"chunk {r['chunk']:>3} MiB x {r['streams']:<3}  {r['secs']:6.2f}s  "
                  f"{r['mibs']:6.0f} MiB/s  rss {r['rss']:5.0f} MiB  (n={r['n']})")


if __name__ == "__main__":
    main()
