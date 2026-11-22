# s3xfer

Stream, copy and sync S3 objects of **any size in constant memory**. Requires Python 3.10+.

A naive S3 copy reads the whole object into memory, so a 10 GB file needs 10+ GB of RAM and a worker with a fixed memory limit gets OOM-killed. s3xfer keeps memory flat: you give it a budget, and file size only changes how many chunks it moves, never how much it holds at once.

```bash
pip install s3xfer
```

## Command line

```bash
s3xfer cp   s3://src-bucket/big.bin   s3://dst-bucket/big.bin   --memory 512MiB --concurrency 8
s3xfer sync s3://src-bucket/data/2024 s3://dst-bucket/backup/2024 --memory 512MiB

# Different credentials for each side (e.g. cross-account): read with one profile, write with another
s3xfer cp s3://src-bucket/big.bin s3://dst-bucket/big.bin --src-profile account-a --dst-profile account-b
```

`sync` skips objects the destination already has with the same size and a newer-or-equal `LastModified`, like `aws s3 sync` (`--force` copies everything). On a terminal both commands show a progress bar; `-q` silences it.

## Python

```python
import boto3
from s3xfer import Copier, RetryPolicy, S3Path, Syncer, TransferConfig

src = boto3.Session(profile_name="account-a").client("s3")
dst = boto3.Session(profile_name="account-b").client("s3")

copier = Copier(src, dst,
                config=TransferConfig(memory_budget="512MiB", concurrency=16),
                retry=RetryPolicy(max_attempts=5))

copier.copy(S3Path("src-bucket", "big.parquet"), S3Path("dst-bucket", "big.parquet"))
Syncer(copier).sync(S3Path("src-bucket", "data/2024"), S3Path("dst-bucket", "backup/2024"))
```

Pass `events=` any object with `start`, `file_started`, `advance(nbytes)`, `warning` and `finished` methods (subclass `NullEvents`) to send progress to your own logs or metrics. `S3RangeStream(client, bucket, key)` is a seekable file object over S3 range GETs, so `zipfile`, `openpyxl`, `csv` or `pandas` download only the bytes they read. `s3_copy()` and `s3_sync()` are one-call shortcuts.

## How memory is bounded

```
chunk_size  = budget / (concurrency × 2), clamped to [5 MiB, 16 MiB]
concurrency = what you ask for, reduced only if even 5 MiB chunks don't fit the budget
peak memory ≈ concurrency × chunk_size × 2  ≤  budget   (+ ~60 MiB fixed for Python, boto3 and the CRT)
```

Chunks only go above 16 MiB when S3's 10,000-part limit forces it (files over ~156 GiB), or when you raise the ceiling with `--max-chunk` / `max_chunk=`, which prints a warning.

Measured on real S3 (EC2 `m7i-flex.large`, `--memory 256MiB --concurrency 16`): 1, 4 and 16 GiB objects peaked at 324, 325 and 330 MiB, at 370–430 MiB/s.

## Credentials and access

s3xfer never reads credentials itself: it takes boto3 clients (or profile names on the CLI), and boto3 resolves credentials from profiles, SSO, `aws login` or instance roles. There are no options for access keys.

Inside s3xfer each client is wrapped as soon as it's passed in: the source side can only head, get and list, and the destination side can only put, run a multipart upload and list. Clients are kept out of reprs and refuse to be pickled or copied. That prevents accidental leaks, but it isn't a boundary against other code in the same process, so give each side a least-privilege IAM policy:

- **Source:** `s3:GetObject` on the source prefix, `s3:ListBucket` on the bucket (limited to that prefix).
- **Destination:** `s3:PutObject` and `s3:AbortMultipartUpload` on the destination prefix, plus `s3:ListBucket` for `sync`.

Prefer short-lived credentials (roles, SSO, `aws login`) over long-lived access keys.

## License

MIT
