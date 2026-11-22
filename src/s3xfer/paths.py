"""S3 locations. Pure data, no package imports."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True)
class S3Path:
    """A bucket plus a key (an object) or a prefix ("" = whole bucket)."""

    bucket: str
    key: str = ""

    @classmethod
    def parse(cls, uri: str, *, allow_empty_key: bool = False) -> S3Path:
        """'s3://bucket/key' -> S3Path. Raises ValueError on anything else."""
        p = urlparse(uri)
        key = p.path.lstrip("/")
        if p.scheme != "s3" or not p.netloc or (not key and not allow_empty_key):
            want = "s3://bucket[/prefix]" if allow_empty_key else "s3://bucket/key"
            raise ValueError(f"Expected {want}, got {uri!r}")
        return cls(p.netloc, key)

    def child(self, relative: str) -> S3Path:
        return S3Path(self.bucket, self.key + relative)

    def __str__(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


@dataclass(frozen=True)
class TransferItem:
    """One object to move: where from, where to, how big."""

    src: S3Path
    dst: S3Path
    size: int

    @property
    def src_key(self) -> str:
        return self.src.key

    @property
    def dst_key(self) -> str:
        return self.dst.key
