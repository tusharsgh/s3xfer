"""A seekable, read-only file-like object backed by S3 range GETs.

Any library that accepts a file object (zipfile, openpyxl, csv, pandas)
can read straight from S3 through this, and only the byte ranges it
actually asks for are downloaded.
"""

from __future__ import annotations

import io

from .plan import MiB


class S3RangeStream(io.RawIOBase):
    """File-like view of one S3 object.

    seek() and tell() only move a cursor. read() fetches bytes with a
    Range GET, pulling at least `readahead` bytes at a time so lots of tiny
    reads don't each become an HTTP request. Memory stays bounded by the
    size of the read-ahead buffer.
    """

    def __init__(self, client, bucket: str, key: str, *, readahead: int = 1 * MiB):
        super().__init__()
        self._client = client
        self.bucket = bucket
        self.key = key
        self.size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        self._pos = 0
        self._readahead = max(1, readahead)
        self._buf = b""
        self._buf_start = 0
        self.requests = 0  # handy for tests and benchmarks

    # --- io.RawIOBase protocol -------------------------------------------
    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new = offset
        elif whence == io.SEEK_CUR:
            new = self._pos + offset
        elif whence == io.SEEK_END:
            new = self.size + offset
        else:
            raise ValueError(f"invalid whence: {whence}")
        if new < 0:
            raise ValueError("negative seek position")
        self._pos = new
        return self._pos

    def readinto(self, b) -> int:
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def read(self, size: int = -1) -> bytes:
        if self._pos >= self.size:
            return b""
        if size is None or size < 0:
            size = self.size - self._pos
        size = min(size, self.size - self._pos)

        # Serve from the read-ahead buffer when the whole range is in it.
        buf_end = self._buf_start + len(self._buf)
        if not (self._buf_start <= self._pos and self._pos + size <= buf_end):
            self._fill(self._pos, max(size, self._readahead))
        offset = self._pos - self._buf_start
        out = self._buf[offset:offset + size]
        self._pos += len(out)
        return out

    def readall(self) -> bytes:
        return self.read(-1)

    # --- internals --------------------------------------------------------
    def _fill(self, start: int, length: int) -> None:
        end = min(start + length, self.size) - 1
        resp = self._client.get_object(Bucket=self.bucket, Key=self.key, Range=f"bytes={start}-{end}")
        self.requests += 1
        self._buf = resp["Body"].read()
        self._buf_start = start
