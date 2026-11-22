"""`python -m s3xfer ...` -> the CLI in cli.py."""

import sys

from .cli import main

__all__ = ["main"]

if __name__ == "__main__":
    sys.exit(main())
