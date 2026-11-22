"""s3xfer: stream and copy S3 objects of any size in constant memory."""

from .copy import CopyResult, s3_copy
from .plan import BudgetTooSmall, LargeChunkWarning, TransferPlan, parse_size, plan_transfer
from .stream import S3RangeStream
from .sync import SyncItem, SyncResult, plan_sync, s3_sync

__all__ = [
    "S3RangeStream",
    "s3_copy",
    "CopyResult",
    "s3_sync",
    "plan_sync",
    "SyncResult",
    "SyncItem",
    "plan_transfer",
    "TransferPlan",
    "BudgetTooSmall",
    "LargeChunkWarning",
    "parse_size",
]
__version__ = "0.1.0"
