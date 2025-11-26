curl 'https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev?LAWD_CD=11110&DEAL_YMD=201801&serviceKey=taVSJNwCXif5C1TK86D389Yu1NO4irUN5v7l5TOuezgTqZqAU4O0qn9dmu9t1Rs5hULYKVLvWfvnWekpuChnJA==&pageNo=1&numOfRows=3' \
  -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7' \
  -H 'Accept-Language: ko,en;q=0.9,ko-KR;q=0.8,en-US;q=0.7' \
  -H 'Cache-Control: max-age=0' \
  -H 'Connection: keep-alive' \
  -H 'Sec-Fetch-Dest: document' \
  -H 'Sec-Fetch-Mode: navigate' \
  -H 'Sec-Fetch-Site: cross-site' \
  -H 'Sec-Fetch-User: ?1' \
  -H 'Upgrade-Insecure-Requests: 1' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36' \
  -H 'sec-ch-ua: "Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"'



export KREB_SERVICE_KEY='taVSJNwCXif5C1TK86D389Yu1NO4irUN5v7l5TOuezgTqZqAU4O0qn9dmu9t1Rs5hULYKVLvWfvnWekpuChnJA=='
export KREB_LAWD_CODES='11110'
export START_YM='202401'
export END_YM='202401'
export KREB_LIMIT_PAGES=1
export KREB_RETRIES=3
export KREB_BACKOFF=1.0
export NO_WRITE=1
python src/kreb/extract_apt_trade_to_csv_s3.py