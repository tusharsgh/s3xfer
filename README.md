# s3xfer

Stream and copy S3 objects of **any size in constant memory**.

A naive S3 copy reads the whole object into memory, so a 10 GB file needs 10+ GB of RAM and a worker with a fixed memory limit gets OOM-killed. `s3xfer` keeps memory flat: you give it a budget, and file size only changes how many chunks it moves, never how much it holds at once.

![Peak memory vs file size](bench/memory_vs_filesize.png)

## What's inside

| Piece | What it does |
| --- | --- |
| `S3RangeStream` | A seekable, read-only file object over S3. `seek()` moves a cursor, and `read()` issues a Range GET with a small read-ahead buffer. Pass it to `zipfile`, `openpyxl`, `csv` or `pandas` and only the bytes they ask for are downloaded. |
| `s3_copy()` | S3 → S3 copy with parallel range GETs and multipart upload. Parts finish in any order, and `CompleteMultipartUpload` joins them by part number. Retries happen per part, and a failure aborts the upload, so nothing is left half-done. |
| `s3_sync()` | Copies every object under a prefix to another prefix, one object at a time, so the memory budget covers the whole sync. Skips objects the destination already has with the same size and a newer-or-equal `LastModified`, like `aws s3 sync`. |
| `plan_transfer()` | Turns a file size and a memory budget into a chunk size and a concurrency that respect S3's limits (5 MiB minimum part, 10,000 parts). |

## The rule

The memory budget is shared between the parallel streams. More concurrency means smaller chunks, not more memory:

```
chunk_size  = budget / (concurrency × 2)        (2 = response buffer + upload copy)
              clamped to [5 MiB, 16 MiB]         (S3's minimum part / a retry-friendly maximum)
concurrency = what you ask for, reduced only if even 5 MiB chunks don't fit the budget
peak memory ≈ concurrency × chunk_size × 2  ≤  budget
num_chunks  = ceil(file_size / chunk_size)      ← the only thing file size affects
```

| `--memory` | `--concurrency` | chunk | streams | peak |
| --- | --- | --- | --- | --- |
| 256MiB | 8 | 16 MiB | 8 | 256 MiB |
| 256MiB | 16 | 8 MiB | 16 | 256 MiB |
| 256MiB | 32 | 5 MiB | 25 | 250 MiB |
| 2GiB | 8 | 16 MiB | 8 | 256 MiB (chunks don't grow past 16 MiB) |
| 2GiB | 8, `--max-chunk 1GiB` | 128 MiB | 8 | 2 GiB (prints a warning) |

Chunks only go above 16 MiB when S3's 10,000-part limit forces it (files over ~156 GiB), or when you raise the ceiling with `--max-chunk` (`max_chunk=` in Python). Doing that prints a warning, since bigger parts rarely add speed and each failed part retries more data. On top of the budget, expect ~60 MiB of fixed overhead for Python, boto3 and the CRT.

## Usage

```python
import boto3, zipfile
from s3xfer import S3RangeStream, s3_copy

s3 = boto3.client("s3")

# Copy a 50 GB object using at most ~256 MiB of RAM
result = s3_copy(s3, "src-bucket", "big.parquet", "dst-bucket", "big.parquet",
                 memory_budget="256MB", concurrency=8)
print(result.plan)   # TransferPlan(file_size=..., chunk_size=16777216, concurrency=8, num_parts=...)

# Read a zip's index from the end of a huge archive without downloading it
with zipfile.ZipFile(S3RangeStream(s3, "bucket", "archive.zip")) as z:
    print(z.namelist())
```

Different credentials for source and destination (for example, cross-account):

```python
s3_copy(src_client, "a", "k", "b", "k", dst_client=dst_client, memory_budget="512MB")
```

Copy a whole prefix, with progress callbacks:

```python
from s3xfer import s3_sync

res = s3_sync(s3, "src-bucket", "data/2024", "dst-bucket", "backup/2024",
              memory_budget="512MiB",
              on_plan=lambda todo, skipped: print(len(todo), "to copy"),
              progress=lambda nbytes: ...)   # called as each part finishes (from worker threads)
```

`s3_copy()` takes the same `progress` callback.

CLI:

```bash
s3xfer cp   s3://src-bucket/big.bin   s3://dst-bucket/big.bin   --memory 512MiB --concurrency 8
s3xfer sync s3://src-bucket/data/2024 s3://dst-bucket/backup/2024 --memory 512MiB

# Cross-account: read with one AWS profile, write with another
s3xfer cp s3://src-bucket/big.bin s3://dst-bucket/big.bin --src-profile account-a --dst-profile account-b
```

On a terminal, both commands show a live progress bar (percent, bytes, speed, ETA, current file). When output is redirected they print one line per file instead. Use `-q` to silence it, and `sync --force` to copy everything even if it's already up to date.

A prefix is treated as a folder: `s3://b/data` matches `data/...` but not `data2/...`. `s3://b` alone means the whole bucket.

Without `--dst-profile`, the destination uses the same credentials as the source. Without `--src-profile`, the source uses boto3's default credential chain.

### Measured on EC2 (m7i-flex.large, ap-southeast-2, 1 GiB object, median of 3)

| `--memory` | `--concurrency` | chunk × streams | time | peak RSS |
| --- | --- | --- | --- | --- |
| 256MiB | 1 | 16 MiB × 1 | 22.3s | 87 MiB |
| 256MiB | 4 | 16 MiB × 4 | 5.9s | 185 MiB |
| 256MiB | 8 | 16 MiB × 8 | 3.2s | 316 MiB |
| 256MiB | 16 | 8 MiB × 16 | **2.8s** | 323 MiB |
| 256MiB | 32 | 5 MiB × 25 | 3.4s | 263 MiB |
| 512MiB | 16 | 16 MiB × 16 | 2.6s | 579 MiB |
| 160MiB | 16 | 5 MiB × 16 | 3.5s | 211 MiB |

At a fixed budget, memory stays flat as concurrency rises. 8 MiB chunks cost ~8% speed versus 16 MiB; 5 MiB chunks cost ~30%, enough that 25 × 5 MiB is slower than 8 × 16 MiB.

## Development

```bash
pip install -e ".[dev]"
pytest                          # 58 tests against moto (fake S3, no AWS account needed)
pip install -e ".[bench]"
python bench/bench_memory.py    # memory benchmark → bench/results.csv
python bench/plot.py            # redraw bench/memory_vs_filesize.png
```

## Roadmap

- [ ] Async backend (aioboto3)
- [ ] Real-S3 benchmarks: throughput vs concurrency, cost per GB
- [ ] Resume interrupted copies from completed parts
- [ ] Server-side `UploadPartCopy` fast path when one set of credentials can read both buckets

## License

MIT
