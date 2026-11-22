"""s3xfer: stream, copy and sync S3 objects of any size in constant memory.

Compose the pieces you need:

    copier = Copier(src_client, dst_client,
                    config=TransferConfig(memory_budget="512MiB", concurrency=16),
                    retry=RetryPolicy(max_attempts=5),
                    events=my_events)              # any TransferEvents
    copier.copy(S3Path("a", "big.bin"), S3Path("b", "big.bin"))
    Syncer(copier).sync(S3Path("a", "data/"), S3Path("b", "backup/"))

or use the function shortcuts s3_copy / s3_sync, which build those for you.
"""

from .copy import Copier, CopyResult, s3_copy
from .events import CallbackEvents, NullEvents, TransferEvents
from .paths import S3Path, TransferItem
from .plan import (BudgetTooSmall, LargeChunkWarning, TransferConfig, TransferPlan, make_plan,
                   parse_size, plan_transfer)
from .retry import RetryPolicy
from .stream import S3RangeStream
from .sync import SyncItem, SyncResult, Syncer, plan_sync, s3_sync

__all__ = [
    # composable pieces
    "Copier",
    "Syncer",
    "TransferConfig",
    "RetryPolicy",
    "TransferEvents",
    "NullEvents",
    "CallbackEvents",
    "S3Path",
    "TransferItem",
    "S3RangeStream",
    # results and plans
    "CopyResult",
    "SyncResult",
    "TransferPlan",
    "make_plan",
    "plan_transfer",
    "parse_size",
    "BudgetTooSmall",
    "LargeChunkWarning",
    # function shortcuts
    "s3_copy",
    "s3_sync",
    "plan_sync",
    "SyncItem",
]
__version__ = "0.1.0"
