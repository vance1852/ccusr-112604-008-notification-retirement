#!/usr/bin/env python3
"""Command-line entry point for ``python retirement.py``.

The implementation lives in the :mod:`retirement` package; this file is a
thin script wrapper for operators who expect a single named executable.
"""

from retirement.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
