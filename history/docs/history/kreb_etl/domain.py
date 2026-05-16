from __future__ import annotations
import pandas as pd
from .config import KREBConfig, S3Config


def months_between(start_ym: str, end_ym: str) -> list[str]:
    """
    Calculate months between start_ym and end_ym
    """
    if not (start_ym and end_ym) or len(start_ym) != 6 or len(end_ym) != 6:
        return []
    sy, sm = int(start_ym[:4]), int(start_ym[4:])
    ey, em = int(end_ym[:4]), int(end_ym[4:])
    cur_y, cur_m = sy, sm
    
    out: list[str] = []
    while (cur_y < ey) or (cur_y == ey and cur_m <= em):
        out.append(f"{cur_y}{cur_m:02d}")
        cur_m += 1
        if cur_m > 12:
            cur_m = 1
            cur_y += 1
    
    return out


def load_lawd_codes(cfg: KREBConfig, s3: S3Config) -> list[str]:
    """
    Load lawd codes from environment variables or S3
    """
    if cfg.lawd_codes_env:
        return cfg.lawd_codes_env

    storage_options = s3.storage_options

    if cfg.lawd_codes_s3:
        df = pd.read_csv(cfg.lawd_codes_s3, storage_options=storage_options)
        col = None
        for c in df.columns:
            if c.lower() in ("lawd_cd", "lawdcode", "code"):
                col = c
                break
        if not col:
            raise RuntimeError("LAWD_CODES_S3 missing expected lawd code column")
        codes = (
            df[col]
            .astype(str)
            .str.strip()
            .str.slice(0, 5)
            .dropna()
            .unique()
            .tolist()
        )
        return codes
    
    df = pd.read_csv(cfg.shigungu_s3, storage_options=storage_options)
    if 'cortarNo' not in df.columns:
        raise RuntimeError("shigungu CSV missing 'cortarNo' column")
    codes = (
        df['cortarNo']
        .astype(str)
        .str.strip()
        .str.slice(0, 5)
        .dropna()
        .unique()
        .tolist()
    )
    return codes