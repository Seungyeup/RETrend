import requests
import pandas as pd
import os
import time
import re

# --- 설정 ---
# Complex CSV 파일 (입력)
complex_csv_path = 'tmp/raw/complex_list.csv'
# 거래 내역 저장 디렉토리
output_dir = 'tmp/raw/trade_history'

# 네이버 부동산 API 헤더/쿠키 (기존과 동일)
headers = {
    'accept': '*/*',
    'accept-language': 'ko,en;q=0.9,ko-KR;q=0.8,en-US;q=0.7',
    'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE3NTA1MDczOTMsImV4cCI6MTc1MDUxODE5M30.KIcXGSOQ0iXjffmvxg5STweJdllk3awsmOi54mB_itw',
    'priority': 'u=1, i',
    'referer': 'https://new.land.naver.com/complexes?ms=37.3595704,127.105399,16&a=APT:ABYG:JGC:PRE&e=RETAIL',
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

def get_goyang_complexes(complex_csv_path):
    """고양시 아파트 단지 목록을 필터링하여 반환합니다."""
    if not os.path.exists(complex_csv_path):
        print(f"[오류] 단지 목록 파일이 없습니다: {complex_csv_path}")
        return pd.DataFrame()
    
    # 읍면동 목록에서 고양시 구들의 하위 읍면동 찾기
    eupmeandong_csv_path = 'output/eupmeandong_list.csv'
    if not os.path.exists(eupmeandong_csv_path):
        print(f"[오류] 읍면동 목록 파일이 없습니다: {eupmeandong_csv_path}")
        return pd.DataFrame()
    
    eupmeandong_df = pd.read_csv(eupmeandong_csv_path)
    
    # 고양시 3개 구의 cortarNo
    goyang_cortar_nos = [4128100000, 4128500000, 4128700000]  # 고양시 덕양구, 일산동구, 일산서구
    
    # 고양시 구들의 하위 읍면동 cortarNo 찾기
    goyang_eupmeandong_nos = eupmeandong_df[eupmeandong_df['parentCortarNo'].isin(goyang_cortar_nos)]['cortarNo'].tolist()
    
    print(f"[INFO] 고양시 구별 읍면동 수:")
    for cortar_no in goyang_cortar_nos:
        eupmeandong_count = len(eupmeandong_df[eupmeandong_df['parentCortarNo'] == cortar_no])
        print(f"  - {cortar_no}: {eupmeandong_count}개 읍면동")
    
    print(f"[INFO] 고양시 총 {len(goyang_eupmeandong_nos)}개 읍면동 발견")
    
    # 단지 목록에서 고양시 읍면동의 단지들 필터링
    df = pd.read_csv(complex_csv_path)
    goyang_complexes = df[df['cortarNo'].isin(goyang_eupmeandong_nos)]
    
    print(f"[INFO] 총 {len(df)}개 단지 중 고양시 단지 {len(goyang_complexes)}개 발견")
    
    return goyang_complexes

def get_pyeonginfo(complex_no):
    """특정 단지의 평형 정보를 가져옵니다."""
    url = f'https://new.land.naver.com/api/complexes/overview/{complex_no}?complexNo={complex_no}&tradeTypes='
    
    try:
        response = requests.get(url, headers=headers, cookies=cookies)
        response.raise_for_status()
        data = response.json()
        return data.get('pyeongs', [])
    except Exception as e:
        print(f"[오류] 평형정보 요청 실패 (단지코드: {complex_no}): {e}")
        return []

def fetch_trade_history(complex_no, areaNo):
    """특정 단지/평형(areaNo)의 거래내역을 페이지네이션 따라 모두 수집"""
    all_trades = []
    addedRowCount = 0
    totalRowCount = None
    page = 1
    
    while True:
        # page1: priceChartChange=true, page2 이후: false
        priceChartChange = 'true' if page == 1 else 'false'
        url = f"https://new.land.naver.com/api/complexes/{complex_no}/prices/real?complexNo={complex_no}&tradeType=A1&year=5&priceChartChange={priceChartChange}&areaNo={areaNo}"
        if page > 1:
            url += f"&addedRowCount={addedRowCount}"
        url += "&type=table"
        
        try:
            resp = requests.get(url, headers=headers, cookies=cookies)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[오류] 거래내역 요청 실패 (단지코드: {complex_no}, areaNo: {areaNo}): {e}")
            break
        
        # 거래내역 누적
        trade_count = 0
        for month in data.get('realPriceOnMonthList', []):
            for trade in month.get('realPriceList', []):
                trade['areaNo'] = areaNo
                trade['complexNo'] = complex_no
                all_trades.append(trade)
                trade_count += 1
        
        if trade_count > 0:
            print(f"  > {trade_count}개 거래내역 수집")
        
        # 페이지네이션 처리
        addedRowCount = data.get('addedRowCount', 0)
        totalRowCount = data.get('totalRowCount', 0)
        
        if addedRowCount >= totalRowCount:
            break
            
        page += 1
        time.sleep(0.5)  # 서버 부하 방지
    
    return all_trades

def process_complex_trade_history(complex_no, complex_name, address):
    """특정 단지의 모든 평형에 대한 거래내역을 수집합니다."""
    print(f"\n[수집] {complex_name} (단지코드: {complex_no})")
    print(f"  주소: {address}")
    
    # 이미 수집된 파일이 있는지 확인
    output_file = os.path.join(output_dir, f'trade_history_{complex_no}.csv')
    if os.path.exists(output_file):
        print(f"  > 이미 수집된 파일이 존재합니다: {output_file}")
        return True
    
    # 1. 평형 정보 가져오기
    pyeongs = get_pyeonginfo(complex_no)
    if not pyeongs:
        print(f"  > 평형 정보가 없습니다.")
        return False
    
    print(f"  > {len(pyeongs)}개 평형 발견")
    
    # 2. 각 평형별 거래내역 수집
    all_trades = []
    areaNos = [pyeong['pyeongNo'] for pyeong in pyeongs]
    
    for areaNo in areaNos:
        print(f"  [평형 {areaNo}] 거래내역 수집 중...")
        trades = fetch_trade_history(complex_no, areaNo)
        all_trades.extend(trades)
        time.sleep(0.5)  # 서버 부하 방지
    
    if not all_trades:
        print(f"  > 거래내역이 없습니다.")
        return False
    
    # 3. CSV 저장
    df = pd.DataFrame(all_trades)
    
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"  > {len(all_trades)}개 거래내역 저장 완료: {output_file}")
    return True

if __name__ == "__main__":
    print("고양시 아파트 단지 거래내역 수집을 시작합니다.")
    
    # 1. 고양시 단지 목록 가져오기
    goyang_complexes = get_goyang_complexes(complex_csv_path)
    if goyang_complexes.empty:
        print("[오류] 고양시 단지가 없습니다.")
        exit(1)
    
    # 2. 각 단지별 거래내역 수집
    success_count = 0
    total_count = len(goyang_complexes)
    
    for idx, (index, complex_info) in enumerate(goyang_complexes.iterrows(), 1):
        complex_no = complex_info['complexNo']
        complex_name = complex_info['complexName']
        address = complex_info['cortarAddress']
        
        print(f"\n[{idx}/{total_count}] 처리 중...")
        
        try:
            if process_complex_trade_history(complex_no, complex_name, address):
                success_count += 1
        except Exception as e:
            print(f"[오류] 단지 처리 실패 (단지코드: {complex_no}): {e}")
        
        # 서버 부하 방지를 위한 지연
        time.sleep(1)
    
    print(f"\n=== 수집 완료 ===")
    print(f"총 {total_count}개 단지 중 {success_count}개 성공")
    print(f"저장 위치: {output_dir}") 