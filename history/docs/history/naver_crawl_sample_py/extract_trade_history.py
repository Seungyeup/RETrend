import requests
import pandas as pd
import os
import time

if os.uname().nodename == "daves-MacBook-Pro.local" or os.path.expanduser("~") == "/Users/dave":
    # 맥북 환경
    output_dir = os.path.expanduser('~/nfs_mount')
    complex_list_path = os.path.expanduser('~/nfs_mount/complex_list.csv')
else:
    # 서버 등 다른 환경
    output_dir = '/nfs/data'
    complex_list_path = '/nfs/data/complex_list.csv'

# 단지코드 리스트 불러오기
def load_complex_no_list(complex_list_path):
    with open(complex_list_path, 'r') as f:
        return [line.strip().split(',')[0] for line in f if line.strip().split(',')[0].isdigit()]

# 네이버 부동산 API 헤더/쿠키 (기존과 동일)
def get_headers(complex_no):
    return {
        'accept': '*/*',
        'accept-language': 'ko,en;q=0.9,ko-KR;q=0.8,en-US;q=0.7',
        'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE3NTA1MDczOTMsImV4cCI6MTc1MDUxODE5M30.KIcXGSOQ0iXjffmvxg5STweJdllk3awsmOi54mB_itw',
        'priority': 'u=1, i',
        'referer': f'https://new.land.naver.com/complexes/{complex_no}',
        'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36'
    }

cookies = {
    'NNB': 'ND2FJPVFPZLGQ',
    'NAC': 'RfCJDQhnZsYSB',
    'nhn.realestate.article.rlet_type_cd': 'A01',
    'nhn.realestate.article.trade_type_cd': '',
    'nhn.realestate.article.ipaddress_city': '1100000000',
    'NACT': '1',
    '_fwb': '177YkT8IsvaPA4GebLaZdLQ.1750507352101',
    'landHomeFlashUseYn': 'Y',
    'SRT30': '1750507352',
    'SRT5': '1750507352',
    'nid_inf': '1627393939',
    'NID_AUT': '9LvTeDOwRCOi6uaQN4XPn0igwEBHIryFzkPW39ku9ulqTA3Z/1zXw2xs+ob9D52x',
    'NID_SES': 'AAABkuMWW68JC/MSBF1CUdaucJbKxkXmZJhXN7mPPVz07XWMuK5I1O5P5etJOcReMWUfn8DS+9ZOYJlJPbkThYHJvomOnXfqkrbaL7IQRj4DSJQKQujGZLl0J7x5+EWe0RbhErQI6WwFxTKPptTJfBrltCtJGyUOWSU9PMt9ax115ELAK6ZDC6ku3sAj49E8CpGhamqsbHvYC80QLBC0KyqwoI//Vpfz8/GkJze4f/UaUqzu/yU4cJ+3kc195KIDCw5BoJgCQjui1yv8yi2QW+8wgEQzuyF7iy+Kov6K8b8noLTAQ0ibqIWDyIi3a1DjXzjjsjjpA6Qq7Od17XcTtbNwW0cdX58e9LatUliW22zAXsyOT9WWllYuOKn8vIjSXYnSBuMZ8zG9hsQpiui3kWjT0ovPMcNqPDI3z5gvkWVTARNv1sm8NibzzcN8MxsKxMsk2LV3ejAVSsoWTFR/oiMoRmcONZH7hmszqyQkv1NTOuimt/XGsp2GzFeGHPBGY2DFAmo72kg7wTcff1GiEs/0kGHO0EDI4iSTMZV7NAygyjX9',
    'REALESTATE': 'Sat%20Jun%2021%202025%2021%3A03%3A13%20GMT%2B0900%20(Korean%20Standard%20Time)',
    'PROP_TEST_KEY': '1750507393367.7807ccee80c4c5069c9f8a11c6b631f11a7d9c73523a7e0ef70f91c6c319e95e',
    'PROP_TEST_ID': '7f4251920cb5f26406469cbb90e7749198b3131374c4beadc2b09969b1583197',
    'BUC': 'szce0VgFzh7XFq2LLn7t2JvyMvByiXQQrRS_A3LjFNs='
}

def get_areaNos_from_pyeonginfo(pyeonginfo_csv, complex_no):
    if not os.path.exists(pyeonginfo_csv):
        print(f"[오류] 평형정보 파일이 없습니다: {pyeonginfo_csv}")
        return []
    df = pd.read_csv(pyeonginfo_csv)
    print("컬럼명:", df.columns)  # 컬럼명 확인용
    df = df[df['complexNo'] == int(complex_no)]
    if 'pyeongNo' not in df.columns:
        print("[오류] 'pyeongNo' 컬럼이 없습니다.")
        return []
    areaNos = pd.Series(df['pyeongNo']).unique().tolist()
    print(f"[INFO] {len(areaNos)}개의 areaNo(pyeongNo) 추출: {areaNos}")
    return areaNos

def fetch_trade_history(complex_no, areaNo):
    """특정 단지/평형(areaNo)의 거래내역을 페이지네이션 따라 모두 수집"""
    all_trades = []
    addedRowCount = 0
    totalRowCount = None
    page = 1
    while True:
        priceChartChange = 'true' if page == 1 else 'false'
        url = f"https://new.land.naver.com/api/complexes/{complex_no}/prices/real?complexNo={complex_no}&tradeType=A1&year=5&priceChartChange={priceChartChange}&areaNo={areaNo}"
        if page > 1:
            url += f"&addedRowCount={addedRowCount}"
        url += "&type=table"
        print(f"[요청] {url}")
        try:
            resp = requests.get(url, headers=get_headers(complex_no), cookies=cookies)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[오류] 거래내역 요청 실패: {e}")
            break
        for month in data.get('realPriceOnMonthList', []):
            for trade in month.get('realPriceList', []):
                trade['areaNo'] = areaNo
                all_trades.append(trade)
                print(f"  > {trade['formattedTradeYearMonth']} {trade['formattedPrice']}층:{trade['floor']}")
        addedRowCount = data.get('addedRowCount', 0)
        totalRowCount = data.get('totalRowCount', 0)
        if addedRowCount >= totalRowCount:
            break
        page += 1
        time.sleep(0.5)
    return all_trades

if __name__ == "__main__":
    # os.makedirs(output_dir, exist_ok=True)
    # print(complex_list_path)
    complex_no_list = load_complex_no_list(complex_list_path)
    # print("단지코드 리스트:", complex_no_list)  # ← 이 줄 추가
    for complex_no in complex_no_list:
        print(f"단지코드 {complex_no}의 평형별 거래내역을 수집합니다.")
        pyeonginfo_csv = os.path.join(output_dir, f'pyeonginfo_list.csv')
        trade_csv = os.path.join(output_dir, f'trade_history/trade_history_{complex_no}.csv')
        areaNos = get_areaNos_from_pyeonginfo(pyeonginfo_csv, complex_no)
        if not areaNos:
            print("[오류] areaNo가 없습니다. 평형정보를 먼저 수집하세요.")
            continue
        all_trades = []
        for areaNo in areaNos:
            print(f"\n[수집] areaNo={areaNo} 거래내역 수집 시작...")
            trades = fetch_trade_history(complex_no, areaNo)
            all_trades.extend(trades)
        if not all_trades:
            print("[오류] 거래내역이 없습니다.")
            continue
        df = pd.DataFrame(all_trades)
        df.to_csv(trade_csv, index=False, encoding='utf-8-sig')
        print(f"\nCSV 저장 완료: {trade_csv}") 