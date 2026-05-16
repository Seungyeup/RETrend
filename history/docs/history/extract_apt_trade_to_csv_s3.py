import os
import time
import math
import requests
import pandas as pd
import xmltodict
from urllib.parse import quote, unquote


# MinIO / S3 configuration from environment
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")

s3_storage_options = {
    "client_kwargs": {
        "endpoint_url": MINIO_ENDPOINT,
    },
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
}

# KREB(국토부 실거래가) API settings
DEFAULT_API_BASES = [
    "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev"
]

API_BASES = [b.strip() for b in os.environ.get("KREB_API_BASES", "").split(",") if b.strip()] or DEFAULT_API_BASES
API_PATH = os.environ.get("KREB_API_PATH", "getRTMSDataSvcAptTradeDev")
SERVICE_KEY = os.environ.get("KREB_SERVICE_KEY") or os.environ.get("SERVICE_KEY")

# Input region codes
LAWD_CODES_S3 = os.environ.get("KREB_LAWD_CODES_S3", "")  # CSV with column 'lawd_cd'
LAWD_CODES = [c.strip() for c in os.environ.get("KREB_LAWD_CODES", "").split(",") if c.strip()]
SHIGUNGU_S3 = os.environ.get("KREB_SHIGUNGU_S3", "s3://retrend-raw-data/shigungu_list.csv")

# Period (inclusive)
START_YM = os.environ.get("START_YM", "")  # e.g., 202401
END_YM = os.environ.get("END_YM", "")      # e.g., 202406

# Throttle and limits
SLEEP_SEC = float(os.environ.get("KREB_SLEEP", "0.5"))
RETRIES = int(os.environ.get("KREB_RETRIES", "3") or 3)
BACKOFF = float(os.environ.get("KREB_BACKOFF", "1.0"))
LIMIT_CODES = int(os.environ.get("KREB_LIMIT_CODES", "0") or 0)
LIMIT_MONTHS = int(os.environ.get("KREB_LIMIT_MONTHS", "0") or 0)
LIMIT_PAGES = int(os.environ.get("KREB_LIMIT_PAGES", "0") or 0)
NUM_ROWS = int(os.environ.get("KREB_NUM_ROWS", "100") or 100)
DRY_RUN_SAMPLE = os.environ.get("DRY_RUN_SAMPLE", "0") == "1"
NO_WRITE = os.environ.get("NO_WRITE", "0") == "1"

# Output prefix (bronze layer, partitioned by lawdCd/year/month)
OUT_PREFIX = os.environ.get(
    "KREB_BRONZE_S3", "s3://retrend-raw-data/bronze/kreb/apt_trade/"
).rstrip("/")

# Optional headers (일부 환경에서 UA 요구)
REQUEST_HEADERS = {
    "User-Agent": os.environ.get(
        "KREB_UA",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    ),
    "Accept": "application/xml, text/xml;q=0.9, */*;q=0.8",
}


def months_between(start_ym: str, end_ym: str):
    if not (start_ym and end_ym) or len(start_ym) != 6 or len(end_ym) != 6:
        return []
    sy, sm = int(start_ym[:4]), int(start_ym[4:])
    ey, em = int(end_ym[:4]), int(end_ym[4:])
    cur_y, cur_m = sy, sm
    out = []
    while (cur_y < ey) or (cur_y == ey and cur_m <= em):
        out.append(f"{cur_y}{cur_m:02d}")
        cur_m += 1
        if cur_m > 12:
            cur_m = 1
            cur_y += 1
    return out


def load_lawd_codes():
    # 1) Direct list via env
    if LAWD_CODES:
        return LAWD_CODES
    # 2) CSV that already contains LAWD codes
    if LAWD_CODES_S3:
        df = pd.read_csv(LAWD_CODES_S3, storage_options=s3_storage_options)
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
    # 3) Derive from shigungu list (first 5 digits of cortarNo)
    df = pd.read_csv(SHIGUNGU_S3, storage_options=s3_storage_options)
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


def fetch_page(lawd_cd: str, deal_ym: str, page_no: int):
    # Use the key as provided. Avoid double-encoding which can cause 401.
    key_variants = [SERVICE_KEY] if SERVICE_KEY else []
    last_exc = None
    for base in API_BASES:
        print(f"    - TRY base={base} page={page_no}")
        url = f"{base}/{API_PATH}"
        for key_idx, key_val in enumerate(key_variants, 1):
            params = {
                "serviceKey": key_val,
                "LAWD_CD": lawd_cd,
                "DEAL_YMD": deal_ym,
                "pageNo": page_no,
                "numOfRows": NUM_ROWS,
            }
            masked = key_val[:6] + "..." if key_val else ""
            for attempt in range(1, RETRIES + 1):
                try:
                    resp = requests.get(url, params=params, headers=REQUEST_HEADERS, timeout=30)
                    resp.raise_for_status()
                    data = xmltodict.parse(resp.text)
                    header = (((data or {}).get("response", {}) or {}).get("header", {}) or {})
                    result_code = str(header.get("resultCode") or "").strip()
                    result_msg = str(header.get("resultMsg") or "").strip()
                    # Treat common success codes/messages as success
                    success_codes = {"00", "000"}
                    success_msgs = {"OK", "NORMAL SERVICE.", "NORMAL_SERVICE"}
                    if result_code and result_code not in success_codes and result_msg not in success_msgs:
                        raise RuntimeError(f"API error {result_code}: {result_msg}")
                    body = (((data or {}).get("response", {}) or {}).get("body", {}) or {})
                    total_count = int((body.get("totalCount") or 0)) if body.get("totalCount") is not None else None
                    items = body.get("items", {})
                    item = items.get("item") if isinstance(items, dict) else items
                    rows = []
                    if item is None:
                        return rows, total_count
                    if isinstance(item, list):
                        rows = item
                    else:
                        rows = [item]
                    return rows, total_count
                except Exception as e:
                    last_exc = e
                    if attempt < RETRIES:
                        print(f"      -> retry {attempt}/{RETRIES} (key-variant {key_idx}/{len(key_variants)} {masked}) after {BACKOFF * attempt:.1f}s: {type(e).__name__}: {e}")
                        time.sleep(BACKOFF * attempt)
                    else:
                        # 다음 key variant 시도
                        break
        # 다음 base로 넘어가기 전, 짧은 지연
        time.sleep(BACKOFF)
    # 모든 base/재시도 실패
    raise last_exc if last_exc else RuntimeError("Unknown request error")


def normalize_rows(rows):
    # Convert numbers and strip spaces where reasonable
    def to_int(v):
        try:
            return int(str(v).strip().replace(",", ""))
        except Exception:
            return None

    def to_float(v):
        try:
            return float(str(v).strip().replace(",", ""))
        except Exception:
            return None

    out = []
    for r in rows:
        rr = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
        # Common fields in API (names may vary slightly)
        rr["거래금액(만원)"] = to_int(rr.get("거래금액") or rr.get("dealAmount"))
        rr["건축년도"] = to_int(rr.get("건축년도") or rr.get("buildYear"))
        rr["년"] = to_int(rr.get("년") or rr.get("dealYear"))
        rr["월"] = to_int(rr.get("월") or rr.get("dealMonth"))
        rr["일"] = to_int(rr.get("일") or rr.get("dealDay"))
        rr["전용면적"] = to_float(rr.get("전용면적") or rr.get("exclusiveArea"))
        rr["층"] = to_int(rr.get("층") or rr.get("floor"))
        out.append(rr)
    return out


def write_partition(lawd_cd: str, ym: str, page_no: int, rows):
    year = ym[:4]
    month = ym[4:]
    out_path = f"{OUT_PREFIX}/lawdCd={lawd_cd}/year={year}/month={month}/part-{page_no:04d}.csv"
    if NO_WRITE or DRY_RUN_SAMPLE:
        print(f"[DRY] {len(rows)} rows -> {out_path}")
        if rows:
            print(pd.DataFrame(rows).head(3).to_string(index=False))
        return
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False, encoding="utf-8-sig", storage_options=s3_storage_options)
    print(f"[SAVE] {len(rows)} rows -> {out_path}")


def main():
    if not SERVICE_KEY:
        raise RuntimeError("KREB_SERVICE_KEY (or SERVICE_KEY) is required")
    codes = load_lawd_codes()
    months = months_between(START_YM, END_YM)
    if not months:
        raise RuntimeError("Set START_YM and END_YM in YYYYMM")

    if LIMIT_CODES:
        codes = codes[:LIMIT_CODES]
    if LIMIT_MONTHS:
        months = months[:LIMIT_MONTHS]

    print(f"[INFO] Codes={len(codes)} Months={len(months)} RowsPerPage={NUM_ROWS}")

    for ci, code in enumerate(codes, 1):
        print(f"\n[CODE] {code} ({ci}/{len(codes)})")
        for mi, ym in enumerate(months, 1):
            print(f"  [MONTH] {ym} ({mi}/{len(months)})")
            page = 1
            total = None
            while True:
                try:
                    rows, total = fetch_page(code, ym, page)
                except Exception as e:
                    print(f"    - ERROR page {page}: {e}")
                    break
                rows_n = normalize_rows(rows)
                if rows_n:
                    write_partition(code, ym, page, rows_n)
                else:
                    print("    - no rows")
                # stop conditions
                if LIMIT_PAGES and page >= LIMIT_PAGES:
                    print(f"    - page limit reached: {LIMIT_PAGES}")
                    break
                # API totalCount -> stop when covered
                if total is not None and page * NUM_ROWS >= total:
                    break
                page += 1
                time.sleep(SLEEP_SEC)
            time.sleep(SLEEP_SEC)
        time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()
