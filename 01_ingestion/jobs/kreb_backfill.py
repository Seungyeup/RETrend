from __future__ import annotations

"""Entry point for KREB backfill job.

Canonical local run:
  pip install -e "src/kreb[dev]"
  python jobs/kreb_backfill.py

This keeps the library code in `src/kreb/src/kreb_etl_v2/` and provides a stable
top-level job entrypoint.
"""

from kreb_etl_v2.backfill import run_backfill_once


if __name__ == "__main__":
    run_backfill_once()
