"""Scoped, private access to S3: the only way the rest of s3xfer touches a client.

A boto3 client carries credentials. s3xfer never reads credentials itself
(boto3 resolves them from profiles, SSO, roles, ...), and it never hands the
client around either. Each side gets a narrow wrapper instead:

    SourceReader  head, ranged get, list            (read-only)
    DestWriter    put, multipart upload, list       (write side)

so code that holds the source can't write, and code that holds the
destination can't read objects. The client sits in a name-mangled slot, is
hidden from repr() (and so from logs and tracebacks that print the object),
and the wrappers refuse to be pickled or copied, so credentials can't leak
into a serialized blob by accident.

This guards against accidental exposure and misuse inside s3xfer. It is not a
security boundary against other code in the same process: limit what the
credentials themselves can do with a least-privilege IAM policy (see README).
"""

from __future__ import annotations

from collections.abc import Iterator

from .paths import S3Path


def _list(client, prefix: S3Path) -> Iterator[dict]:
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=prefix.bucket, Prefix=prefix.key)
    for page in pages:
        yield from page.get("Contents", [])


def _check(client) -> None:
    if isinstance(client, (SourceReader, DestWriter)):
        raise TypeError(f"pass a boto3 S3 client, not a {type(client).__name__}")


class _NoLeak:
    """No repr, pickle or copy of anything that holds a client."""

    __slots__ = ()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(<client hidden>)"

    def __reduce_ex__(self, protocol):
        raise TypeError(f"{type(self).__name__} holds credentials and can't be pickled or copied")


class SourceReader(_NoLeak):
    """Read-only operations on the source side. The client is reachable only from these methods."""

    __slots__ = ("__client",)

    def __init__(self, client) -> None:
        _check(client)
        self.__client = client

    def size(self, path: S3Path) -> int:
        return self.__client.head_object(Bucket=path.bucket, Key=path.key)["ContentLength"]

    def read(self, path: S3Path, start: int | None = None, end: int | None = None) -> bytes:
        """Whole object, or bytes start..end inclusive."""
        kw = {"Range": f"bytes={start}-{end}"} if start is not None else {}
        return self.__client.get_object(Bucket=path.bucket, Key=path.key, **kw)["Body"].read()

    def list(self, prefix: S3Path) -> Iterator[dict]:
        """Objects under a prefix (needs s3:ListBucket on the source)."""
        return _list(self.__client, prefix)


class DestWriter(_NoLeak):
    """Write operations on the destination side, plus listing for sync's skip check."""

    __slots__ = ("__client",)

    def __init__(self, client) -> None:
        _check(client)
        self.__client = client

    def put(self, path: S3Path, body: bytes) -> None:
        self.__client.put_object(Bucket=path.bucket, Key=path.key, Body=body)

    def start_multipart(self, path: S3Path) -> str:
        return self.__client.create_multipart_upload(Bucket=path.bucket, Key=path.key)["UploadId"]

    def upload_part(self, path: S3Path, upload_id: str, number: int, body: bytes) -> str:
        return self.__client.upload_part(Bucket=path.bucket, Key=path.key, UploadId=upload_id,
                                         PartNumber=number, Body=body)["ETag"]

    def complete_multipart(self, path: S3Path, upload_id: str, parts: list[dict]) -> None:
        self.__client.complete_multipart_upload(Bucket=path.bucket, Key=path.key, UploadId=upload_id,
                                                MultipartUpload={"Parts": parts})

    def abort_multipart(self, path: S3Path, upload_id: str) -> None:
        self.__client.abort_multipart_upload(Bucket=path.bucket, Key=path.key, UploadId=upload_id)

    def list(self, prefix: S3Path) -> Iterator[dict]:
        """Objects under a prefix (needs s3:ListBucket on the destination)."""
        return _list(self.__client, prefix)
