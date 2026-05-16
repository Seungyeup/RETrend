# 01_ingestion

이 레이어는 RETrend의 **원천 데이터 수집 계층**입니다.

현재 기준으로 이 레이어의 책임은 아래와 같습니다.

- KREB API 호출
- 수집 상태(state) 관리
- Bronze CSV 생성
- 백필(backfill) / 일일 동기화(daily sync) 실행 엔트리포인트 제공

## 포함 범위

- `src/kreb/`: 수집 파이썬 패키지와 테스트
- `jobs/`: 사람이 직접 실행하는 수집 엔트리포인트
- `docker/kreb-backfill/`: 수집 실행용 이미지 정의

현재 이 레이어는 **`kreb_etl_v2` 기준**으로 재구성합니다.
과거 `kreb_etl` 계열 실험성 테스트/모듈은 `history/`에 남겨두고, 새 기준 위치에는 포함하지 않습니다.

## 입력 / 출력

- 입력
  - `KREB_SERVICE_KEY`
  - `KREB_LAWD_CSV`
  - `KREB_STATE_URI`
  - `KREB_OUTPUT_URI`
- 출력
  - Bronze CSV 파일
  - state JSON
  - daily sync manifest

## 레이어 경계

- `02_orchestration`는 이 레이어의 코드를 Airflow DAG로 실행합니다.
- `03_lakehouse`는 이 레이어가 만든 Bronze 데이터를 Iceberg 테이블로 적재합니다.

## 재구성 원칙

`history/`는 참고용 아카이브로 유지합니다.
이 폴더는 앞으로 수집 계층의 **새 기준 위치**로 사용합니다.

## 로컬 실행 기준

```bash
pip install -e "01_ingestion/src/kreb[dev]"
python 01_ingestion/jobs/kreb_backfill.py
python 01_ingestion/jobs/kreb_daily_sync.py
```
