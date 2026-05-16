import os
import time
import requests
import pandas as pd

# MinIO configuration from environment
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

COMPLEX_LIST_S3 = os.environ.get("COMPLEX_LIST_S3", "s3://retrend-raw-data/complex_list.csv")
# Bronze 계층: 단일 대형 CSV 대신 단지별 분리 저장
PYEONGINFO_S3 = os.environ.get("PYEONGINFO_S3", "s3://retrend-raw-data/bronze/pyeonginfo/")

HEADERS = {
    'accept': '*/*',
    'accept-language': 'ko,en;q=0.9,ko-KR;q=0.8,en-US;q=0.7',
    'authorization': os.environ.get('NAVER_LAND_AUTH', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE3NTA1MDczOTMsImV4cCI6MTc1MDUxODE5M30.KIcXGSOQ0iXjffmvxg5STweJdllk3awsmOi54mB_itw'),
    'priority': 'u=1, i',
    'referer': 'https://new.land.naver.com/complexes',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0'
}

# Optional throttle and limit for quick verification
LIMIT_COMPLEXES = int(os.environ.get("PYEONGINFO_LIMIT_COMPLEXES", "0") or 0)
# Default 0.5s to reduce rate limiting; adjustable via env
SLEEP_SEC = float(os.environ.get("PYEONGINFO_SLEEP", "0.5"))
DRY_RUN_SAMPLE = os.environ.get("DRY_RUN_SAMPLE", "0") == "1"
NO_WRITE = os.environ.get("NO_WRITE", "0") == "1"


def fetch_pyeongs(complex_no: int):
    if DRY_RUN_SAMPLE:
        import json
        sample_path = os.path.join(os.path.dirname(__file__), "..", "sample", "depth5_pyeonginfo_response.json")
        sample_path = os.path.abspath(sample_path)
        with open(sample_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get('pyeongs', [])
    url = f'https://new.land.naver.com/api/complexes/overview/{complex_no}?complexNo={complex_no}&tradeTypes='
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    return data.get('pyeongs', [])


def main():
    if DRY_RUN_SAMPLE:
        print("[DRY] Using sample complex list (119219)")
        complexes = pd.DataFrame([
            {"complexNo": 119219, "complexName": "SAMPLE_COMPLEX", "eupmeandongCortarName": "SAMPLE_EMD"}
        ])
    else:
        print(f"[INFO] Read complexes from {COMPLEX_LIST_S3}")
        complexes = pd.read_csv(COMPLEX_LIST_S3, storage_options=s3_storage_options)
    processed = 0

    for _, c in complexes.iterrows():
        complex_no = int(c.get('complexNo'))
        complex_name = c.get('complexName')
        eup_name = c.get('eupmeandongCortarName') or ''
        print(f"[PYEONG] {eup_name} > {complex_name} ({complex_no})")
        try:
            pyeongs = fetch_pyeongs(complex_no)
        except Exception as e:
            print(f"  - ERROR fetching pyeongs: {e}")
            continue
        for p in pyeongs:
            p['complexNo'] = complex_no
            p['complexName'] = complex_name
            p['eupmeandongCortarName'] = eup_name
        print(f"  - collected {len(pyeongs)} pyeongs")

        # 단지별 분리 저장 (bronze)
        out_prefix = PYEONGINFO_S3.rstrip('/')
        out_path = f"{out_prefix}/complexNo={complex_no}/pyeonginfo_{complex_no}.csv"
        if NO_WRITE or DRY_RUN_SAMPLE:
            preview_df = pd.DataFrame(pyeongs)
            print(f"  - [DRY] would write {len(preview_df)} rows -> {out_path}")
            print(preview_df.head(3).to_string(index=False))
        else:
            df_one = pd.DataFrame(pyeongs)
            df_one.to_csv(out_path, index=False, encoding='utf-8-sig', storage_options=s3_storage_options)
            print(f"  - wrote {len(df_one)} rows -> {out_path}")
        processed += 1
        if LIMIT_COMPLEXES and processed >= LIMIT_COMPLEXES:
            print(f"[INFO] Limit reached: {LIMIT_COMPLEXES}")
            break
        time.sleep(SLEEP_SEC)

    # 단지별 즉시 저장 방식으로 변경되었으므로 합본 저장 없음
    if DRY_RUN_SAMPLE or NO_WRITE:
        print("[DRY] Completed without writes (per-complex previews above)")


if __name__ == "__main__":
    main()
