import requests
import pandas as pd
import os

# API URL 및 헤더/쿠키 정보
url = 'https://new.land.naver.com/api/regions/list?cortarNo=0000000000'
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

# 1. output 폴더 생성
output_dir = 'output'
os.makedirs(output_dir, exist_ok=True)

# 2. 저장 경로 변경
csv_path = os.path.join(output_dir, 'shido_list.csv')

print("Sido 정보 수집을 시작합니다...")
print(f"API 요청: {url}")
response = requests.get(url, headers=headers, cookies=cookies)
response.raise_for_status()
print("API 요청 성공. 데이터 처리 중...")
data = response.json()

region_list = data.get('regionList', [])
print(f"총 {len(region_list)}개의 Sido 정보를 찾았습니다.")

rows = [
    {
        'cortarNo': region.get('cortarNo'),
        'cortarName': region.get('cortarName'),
        'centerLat': region.get('centerLat'),
        'centerLon': region.get('centerLon'),
        'cortarType': region.get('cortarType')
    }
    for region in region_list
]
df = pd.DataFrame(rows)
print("데이터프레임 생성 완료.")

df.to_csv(csv_path, index=False, encoding='utf-8-sig')
print(f"CSV 파일로 저장 완료: {csv_path}") 