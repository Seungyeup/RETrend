from __future__ import annotations

"""Entry point for KREB daily sync job.

This mode is intended to run every day with a quota limit.
It continuously cycles the full range and updates pages when API output changes.
"""

from kreb_etl_v2.daily_sync import run_daily_sync_once


if __name__ == "__main__":
    run_daily_sync_once()
