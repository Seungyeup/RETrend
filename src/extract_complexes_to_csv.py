import requests
import pandas as pd
import os
import time

# --- 설정 ---
# Eupmeandong CSV 파일 (입력)
eupmeandong_csv_path = '/nfs/data/eupmeandong_list.csv'
# Complex CSV 파일 (출력)
output_dir = '/nfs/data'
complex_csv_path = os.path.join(output_dir, 'complex_list.csv')

# API URL 형식
base_url = 'https://new.land.naver.com/api/regions/complexes?cortarNo={cortarNo}&realEstateType=APT%3AABYG%3AJGC%3APRE&order='

# 이전 스크립트에서 사용한 헤더와 쿠키
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

# --- 로직 ---
def get_complex_list(cortar_no):
    """주어진 cortarNo에 대해 아파트 단지 목록을 가져옵니다."""
    url = base_url.format(cortarNo=cortar_no)
    try:
        response = requests.get(url, headers=headers, cookies=cookies)
        response.raise_for_status()
        data = response.json()
        return data.get('complexList', [])
    except requests.exceptions.RequestException as e:
        print(f"오류 발생 ({cortar_no}): {e}")
        return None

if __name__ == "__main__":
    print("아파트 단지 정보 수집을 시작합니다.")

    # Eupmeandong 목록 파일 확인
    if not os.path.exists(eupmeandong_csv_path):
        print(f"오류: 읍면동 목록 파일이 없습니다. '{eupmeandong_csv_path}'")
        print("읍면동 정보 수집을 먼저 실행해주세요.")
        exit()

    # Eupmeandong 목록 읽기
    eupmeandong_df = pd.read_csv(eupmeandong_csv_path)
    print(f"'{eupmeandong_csv_path}' 파일에서 {len(eupmeandong_df)}개의 읍면동 정보를 읽었습니다.")

    all_complex_list = []
    
    # 각 읍면동에 대해 단지 정보 수집
    for index, eupmeandong in eupmeandong_df.iterrows():
        eupmeandong_name = eupmeandong['cortarName']
        cortar_no = eupmeandong['cortarNo']
        
        print(f"\n[{eupmeandong['parentCortarName']} > {eupmeandong_name}({cortar_no})]의 단지 정보를 수집합니다...")
        
        complex_list = get_complex_list(cortar_no)
        
        if complex_list:
            # 부모 지역 정보 추가
            for complex_item in complex_list:
                complex_item['eupmeandongCortarNo'] = cortar_no
                complex_item['eupmeandongCortarName'] = eupmeandong_name
            all_complex_list.extend(complex_list)
            print(f"  > {len(complex_list)}개의 단지 정보를 수집했습니다.")
        else:
            print(f"  > 단지 정보 수집에 실패했거나 데이터가 없습니다.")
            
        # 서버 부하를 줄이기 위한 지연
        time.sleep(0.5)

    if not all_complex_list:
        print("\n수집된 단지 정보가 없습니다. 작업을 종료합니다.")
        exit()

    # 데이터프레임 생성 및 CSV 저장
    print(f"\n총 {len(all_complex_list)}개의 단지 정보를 CSV 파일로 저장합니다.")
    complex_df = pd.DataFrame(all_complex_list)
    
    os.makedirs(output_dir, exist_ok=True)
    
    complex_df.to_csv(complex_csv_path, index=False, encoding='utf-8-sig')
    print(f"CSV 파일 저장 완료: {complex_csv_path}") 