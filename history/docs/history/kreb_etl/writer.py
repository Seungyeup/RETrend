from __future__ import annotations
import pandas as pd
from .config import KREBConfig, S3Config


def write_partition(
    cfg: KREBConfig,
    s3: S3Config,
    lawd_cd: str,
    ym: str,
    page_no: int,
    rows: list[dict],
) -> None:
    year = ym[:4]
    month = ym[4:]
    out_path = (
        f"{cfg.out_prefix}/lawdCd={lawd_cd}/year={year}/month={month}/part-{page_no:04d}.csv"
    )

    if cfg.no_write or cfg.dry_run_sample:
        print(f"[DRY] {len(rows)} rows -> {out_path}")
        if rows:
            print(pd.DataFrame(rows).head(3).to_string(index=False))
        return

    df = pd.DataFrame(rows)
    df.to_csv(
        out_path, 
        index=False, 
        encoding="utf-8-sig", 
        storage_options=s3.storage_options
    )

    print(f"[SAVE] {len(rows)} rows -> {out_path}")