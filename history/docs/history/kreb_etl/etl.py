from __future__ import annotations
import time

from .config import load_kreb_config, load_s3_config
from .domain import load_lawd_codes, months_between
from .client import fetch_page
from .transform import normalize_rows
from .writer import write_partition

def run_etl():
    cfg = load_kreb_config()
    s3 = load_s3_config()
    
    if not cfg.service_key:
        raise RuntimeError("KREB_SERVICE_KEY (or SERVICE_KEY) is required")

    codes = load_lawd_codes(cfg, s3)
    months = months_between(cfg.start_ym, cfg.end_ym)

    if not months:
        raise RuntimeError("Set START_YM and END_YM in YYYYMM")

    if cfg.limit_codes:
        codes = codes[:cfg.limit_codes]
    if cfg.limit_months:
        months = months[:cfg.limit_months]
    print(f"[INFO] Codes={len(codes)} Months={len(months)} RowsPerPage={cfg.num_rows}")

    for ci, code in enumerate(codes, 1):
        print(f"\n[CODE] {code} ({ci}/{len(codes)})")

        for mi, ym in enumerate(months, 1):
            print(f"  [MONTH] {ym} ({mi}/{len(months)})")
            page = 1
            total = None
            while True:
                try:
                    rows, total = fetch_page(cfg, code, ym, page)
                except Exception as e:
                    print(f"    - ERROR page {page}: {e}")
                    break

                rows_n = normalize_rows(rows)
                if rows_n:
                    write_partition(cfg, s3, code, ym, page, rows_n)
                else:
                    print("    - no rows")
                
                if cfg.limit_pages and page >= cfg.limit_pages:
                    print(f"    - page limit reached: {cfg.limit_pages}")
                    break

                if total is not None and page * cfg.num_rows >= total:
                    break

                page += 1
                time.sleep(cfg.sleep_sec)
            time.sleep(cfg.sleep_sec)
        time.sleep(cfg.sleep_sec)