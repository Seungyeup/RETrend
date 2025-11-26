import os
import time
import requests
import pandas as pd
import fsspec

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

PYEONGINFO_S3 = os.environ.get("PYEONGINFO_S3", "s3://retrend-raw-data/bronze/pyeonginfo/")
# Bronze 계층: 단지/평형/연/월 단위로 분리 저장
TRADE_DIR_S3 = os.environ.get("TRADE_DIR_S3", "s3://retrend-raw-data/bronze/trade_history")

HEADERS_TPL = {
    'accept': '*/*',
    'accept-language': 'ko,en;q=0.9,ko-KR;q=0.8,en-US;q=0.7',
    'authorization': os.environ.get('NAVER_LAND_AUTH', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE3NTA1MDczOTMsImV4cCI6MTc1MDUxODE5M30.KIcXGSOQ0iXjffmvxg5STweJdllk3awsmOi54mB_itw'),
    'priority': 'u=1, i',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0'
}

# Optional throttling/limits for quick verification
LIMIT_COMPLEXES = int(os.environ.get("TRADE_LIMIT_COMPLEXES", "0") or 0)
LIMIT_AREAS = int(os.environ.get("TRADE_LIMIT_AREAS", "0") or 0)
LIMIT_PAGES = int(os.environ.get("TRADE_LIMIT_PAGES", "0") or 0)
# Default 0.5s to reduce rate limiting; adjustable via env
SLEEP_SEC = float(os.environ.get("TRADE_SLEEP", "0.5"))
DRY_RUN_SAMPLE = os.environ.get("DRY_RUN_SAMPLE", "0") == "1"
NO_WRITE = os.environ.get("NO_WRITE", "0") == "1"


def headers_for(complex_no: int):
    h = dict(HEADERS_TPL)
    h['referer'] = f'https://new.land.naver.com/complexes/{complex_no}'
    return h


def fetch_trade_history(complex_no: int, area_no: int):
    if DRY_RUN_SAMPLE:
        import json
        sample_path = os.path.join(os.path.dirname(__file__), "..", "sample", "depth6_prices_page1_response.json")
        sample_path = os.path.abspath(sample_path)
        with open(sample_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        all_trades = []
        for month in data.get('realPriceOnMonthList', []):
            for trade in month.get('realPriceList', []):
                trade['areaNo'] = area_no
                trade['complexNo'] = complex_no
                all_trades.append(trade)
        return all_trades
    all_trades = []
    added_row_count = 0
    total_row_count = None
    page = 1
    while True:
        price_chart_change = 'true' if page == 1 else 'false'
        url = (
            f"https://new.land.naver.com/api/complexes/{complex_no}/prices/real"
            f"?complexNo={complex_no}&tradeType=A1&year=5&priceChartChange={price_chart_change}&areaNo={area_no}&type=table"
        )
        if page > 1:
            url += f"&addedRowCount={added_row_count}"
        try:
            resp = requests.get(url, headers=headers_for(complex_no))
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"    - ERROR fetching trades page {page}: {e}")
            break

        count_this_page = 0
        for month in data.get('realPriceOnMonthList', []):
            for trade in month.get('realPriceList', []):
                trade['areaNo'] = area_no
                trade['complexNo'] = complex_no
                all_trades.append(trade)
                count_this_page += 1

        print(f"    - page {page}: {count_this_page} rows")
        added_row_count = data.get('addedRowCount', 0)
        total_row_count = data.get('totalRowCount', 0)
        if LIMIT_PAGES and page >= LIMIT_PAGES:
            print(f"    - page limit reached: {LIMIT_PAGES}")
            break
        if added_row_count >= total_row_count:
            break
        page += 1
        time.sleep(SLEEP_SEC)
    return all_trades


def main():
    if DRY_RUN_SAMPLE:
        # fabricate minimal pyeonginfo using known sample areaNo=11 for complex 119219
        print("[DRY] Using sample pyeonginfo (complex 119219, area 11)")
        pyeong_df = pd.DataFrame([
            {"complexNo": 119219, "complexName": "SAMPLE_COMPLEX", "pyeongNo": 11}
        ])
    else:
        print(f"[INFO] Read pyeonginfo from {PYEONGINFO_S3}")
        # 디렉토리 프리픽스면 하위 모든 CSV를 병합하여 로드
        if PYEONGINFO_S3.lower().endswith('.csv'):
            pyeong_df = pd.read_csv(PYEONGINFO_S3, storage_options=s3_storage_options)
        else:
            prefix = PYEONGINFO_S3.rstrip('/')
            fs, path = fsspec.core.url_to_fs(prefix, **s3_storage_options)
            paths = fs.glob(path + '/**/*.csv')
            if not paths:
                # 레거시 단일 파일 폴백
                legacy = 's3://retrend-raw-data/pyeonginfo_list.csv'
                print(f"[WARN] No CSV under prefix. Falling back to legacy file: {legacy}")
                pyeong_df = pd.read_csv(legacy, storage_options=s3_storage_options)
            else:
                frames = []
                for p in paths:
                    with fs.open(p, 'rb') as f:
                        frames.append(pd.read_csv(f))
                pyeong_df = pd.concat(frames, ignore_index=True)

    # Ensure expected columns
    if 'complexNo' not in pyeong_df.columns:
        raise RuntimeError("pyeonginfo missing 'complexNo' column")
    area_col = 'pyeongNo' if 'pyeongNo' in pyeong_df.columns else ('areaNo' if 'areaNo' in pyeong_df.columns else None)
    if not area_col:
        raise RuntimeError("pyeonginfo missing 'pyeongNo'/'areaNo' column")

    complexes = (
        pyeong_df[['complexNo', 'complexName']]
        .drop_duplicates()
        .sort_values('complexNo')
        .reset_index(drop=True)
    )

    processed_complexes = 0
    for _, row in complexes.iterrows():
        complex_no = int(row['complexNo'])
        complex_name = row.get('complexName', '')
        trade_path = f"{TRADE_DIR_S3}/trade_history_{complex_no}.csv"
        print(f"[TRADE] {complex_name} ({complex_no}) -> {trade_path}")

        # Area list for this complex
        area_list = (
            pyeong_df[pyeong_df['complexNo'] == complex_no][area_col]
            .dropna().astype(int).unique().tolist()
        )
        if LIMIT_AREAS:
            area_list = area_list[:LIMIT_AREAS]

        all_trades = []
        for area_no in area_list:
            print(f"  - areaNo={area_no}")
            trades = fetch_trade_history(complex_no, area_no)
            all_trades.extend(trades)
            time.sleep(SLEEP_SEC)

        if not all_trades:
            print("  - WARN: no trades collected")
        else:
            df = pd.DataFrame(all_trades)
            # 파티션 컬럼 생성
            # tradeYear는 문자열일 수 있으므로 정수 변환 시도
            if 'tradeYear' in df.columns:
                df['tradeYear'] = df['tradeYear'].astype(str).str.extract(r'(\d+)').astype(int)
            else:
                # formattedTradeYearMonth 예: '2025.03.22' -> 2025
                df['tradeYear'] = df['formattedTradeYearMonth'].str.slice(0,4).astype(int)
            if 'tradeMonth' in df.columns:
                df['tradeMonth'] = df['tradeMonth'].astype(int)
            else:
                df['tradeMonth'] = df['formattedTradeYearMonth'].str.slice(5,7).astype(int)

            # 그룹 단위 저장: complexNo/areaNo/year/month
            out_prefix = TRADE_DIR_S3.rstrip('/')
            grouped = df.groupby(['areaNo', 'tradeYear', 'tradeMonth'], as_index=False)
            saved_files = 0
            for (area_no, year, month), g in grouped:
                out_path = f"{out_prefix}/complexNo={complex_no}/areaNo={int(area_no)}/year={int(year)}/month={int(month):02d}/trades.csv"
                if NO_WRITE or DRY_RUN_SAMPLE:
                    print(f"  - [DRY] {len(g)} rows -> {out_path}")
                else:
                    g.to_csv(out_path, index=False, encoding='utf-8-sig', storage_options=s3_storage_options)
                    saved_files += 1
            if not (NO_WRITE or DRY_RUN_SAMPLE):
                print(f"  - saved partitions: {saved_files}")

        processed_complexes += 1
        if LIMIT_COMPLEXES and processed_complexes >= LIMIT_COMPLEXES:
            print(f"[INFO] Complex limit reached: {LIMIT_COMPLEXES}")
            break


if __name__ == "__main__":
    main()
