from __future__ import annotations

import os
import sys


def pytest_configure():
    # Existing tests in this repo still target the historical `docs/history/kreb_etl` package.
    repo_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )
    history_root = os.path.join(repo_root, "docs", "history")
    if (
        os.path.isdir(os.path.join(history_root, "kreb_etl"))
        and history_root not in sys.path
    ):
        sys.path.insert(0, history_root)
