# KREB 실거래가 수집 파이프라인 Runbook

이 문서는 RETrend의 “KREB(공공데이터) 아파트 매매 실거래가” 수집(backfill) 파이프라인을 운영/검증하기 위한 최소한의 가이드입니다.

## 구성요소(활성 경로)

- Airflow DAG(오케스트레이션)
  - `dags/retrend_crawler_with_quota_dag.py`
  - `KubernetesPodOperator`로 컨테이너 실행 → `python /app/src/kreb/src/kreb_etl_v2/backfill.py`
- 수집 엔진(백필)
  - `src/kreb/src/kreb_etl_v2/backfill.py`
  - 상태(state) 저장: `KREB_STATE_URI` (예: `s3://retrend-raw-data/kreb_state.json`)
  - 브론즈 저장: `KREB_OUTPUT_URI` (예: `s3://retrend-raw-data/bronze/kreb_etl_v2/apt_trade`)
- 분석 레이어(후처리)
  - Spark → Iceberg: `jobs/spark/kreb_csv_to_iceberg.py`, `infra/spark/k8s/kreb_csv_to_iceberg.yaml`
  - Trino: `infra/trino/k8s/values-trino.yaml`
  - Superset: `infra/superset/docker-compose.yaml`

## 데이터 흐름(요약)

1. Airflow가 매일 1회 Pod를 띄워 `backfill.py` 실행
2. `backfill.py`가 (LAWD_CD × DEAL_YM × page) 단위로 KREB API 호출
3. 페이지 결과를 CSV로 브론즈(prefix 아래 파티션 디렉토리)에 저장
4. 페이지마다 state를 갱신/저장하여, 쿼터 소진(429) 또는 장애 시 다음 실행에서 이어받음
5. Spark가 브론즈 CSV를 Iceberg 테이블로 적재, Trino/Superset에서 조회

## 출력(브론즈) 레이아웃

`KREB_OUTPUT_URI` 아래에 다음 형태로 저장됩니다.

```
.../LAWD_CD=11110/DEAL_YM=202401/page=1.csv
.../LAWD_CD=11110/DEAL_YM=202401/page=2.csv
.../LAWD_CD=11110/DEAL_YM=202401/_SUCCESS.json
```

- `page=N.csv`: API 페이지 단위 결과
- `_SUCCESS.json`: 해당 (LAWD_CD, DEAL_YM) 파티션이 “마지막 페이지까지 완료”되었음을 의미하는 마커

## State 파일(kreb_state.json) 의미

`KREB_STATE_URI`는 JSON이며, 핵심 필드는 아래와 같습니다.

- `cursor`: 다음에 가져올 위치
  - `{ "lawd_cd": "11110", "deal_ym": "202401", "page_no": 3 }`
- `last_success`: 마지막으로 성공적으로 처리한 페이지(페이지 단위로 갱신)
- `last_completed`: 마지막으로 완료된 파티션(월 단위 완료)
- `last_error`: 마지막 오류(쿼터 소진 포함)
- `done`: 전체 범위 완료 여부(마지막 파티션의 마지막 페이지까지 완료했을 때만 true)

## 신뢰성(재개/누락 방지) 동작

`src/kreb/src/kreb_etl_v2/backfill.py`는 다음 원칙으로 동작합니다.

- 페이지 성공 단위로 state를 즉시 저장
  - Pod가 중간에 죽어도, 다음 실행에서 `cursor`로 이어서 진행
- 완료(done) 조건을 엄격하게
  - “마지막 (LAWD_CD, DEAL_YM)에 도달”이 아니라 “마지막 페이지까지 완료”여야 `done=true`
- 429(쿼터/레이트리밋)은 ‘정상 중단’으로 기록
  - state를 저장하고 종료 → 다음 스케줄에서 이어받음
- 5xx/timeout/빈 응답(중간 페이지) 등은 bounded retry

## 환경변수(중요)

필수:
- `KREB_SERVICE_KEY`
- `KREB_LAWD_CSV` (예: `s3://retrend-raw-data/shigungu_list.csv`)
- `KREB_OUTPUT_URI`

권장/옵션:
- `KREB_STATE_URI` (기본값 있음)
- `KREB_DAILY_LIMIT` (기본 10000)
- `LOG_LEVEL` (기본 INFO)

HTTP 재시도 튜닝:
- `KREB_HTTP_RETRIES` (기본 3)
- `KREB_HTTP_BACKOFF_BASE` (기본 1.0)
- `KREB_HTTP_BACKOFF_CAP` (기본 30.0)
- `KREB_HTTP_TIMEOUT_SEC` (기본 10.0)

운영용:
- `KREB_IGNORE_DONE=1` (state가 done=true라도 강제로 다시 실행)

MinIO/S3 연동(필요 시):
- `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`

## 로컬 실행(스모크 테스트)

`src/kreb/src/kreb_etl_v2/commands.md`에 예시가 있습니다.

예: state/output을 로컬로 두고 빠르게 확인

```
export KREB_SERVICE_KEY=...
export KREB_DAILY_LIMIT=20
export KREB_LAWD_CSV=/tmp/lawd_test.csv
export KREB_STATE_URI=file:///tmp/kreb_state.json
export KREB_OUTPUT_URI=file:///tmp/kreb_output
export LOG_LEVEL=INFO

PYTHONPATH=./src python -m kreb_etl_v2.backfill
```

## 완결성(누락 여부) 검증 방법

운영자가 “이번 run이 빠짐없이 됐는지”를 보는 최소 체크리스트:

1. state 확인
   - `done=true`인지
   - `last_error`가 있는지(특히 QuotaExceeded)
2. 파티션 완료 마커 확인
   - 각 파티션(`LAWD_CD=.../DEAL_YM=...`)에 `_SUCCESS.json`이 있어야 “완료”로 간주
3. 이상 징후
   - `_SUCCESS.json`은 있는데 `page=1.csv`조차 없다 → 0건 파티션일 수도 있으나, 표본으로 API/로그 확인 권장
   - state의 `last_success.total_pages` 대비 page 파일이 부족 → 누락 가능

## 자주 발생하는 장애/원인

- `QuotaExceeded` / HTTP 429
  - 정상 중단(다음날/다음 스케줄에서 재개)
- `MINIO_* 환경변수 필요` 오류
  - `KREB_STATE_URI` 또는 `KREB_OUTPUT_URI`가 s3://인데 MinIO 설정이 빠진 경우
- 테스트 실패(예전 kreb_etl 모듈)
  - `src/kreb/tests/conftest.py`가 `docs/history`를 PYTHONPATH로 추가하여 해결

## 보안(중요)

현재 repo에는 DAG/도커파일에 접근키/서비스키가 하드코딩된 흔적이 있습니다.
운영 환경에서는 반드시 다음으로 이동하세요.

- Airflow Connection/Variable/Secret 또는 Kubernetes Secret
- `KubernetesPodOperator`의 `secrets` 기능 사용(공식 문서에서도 env로 secret 전달을 금지)

## Airflow DAG 소스 주의

`helm/airflow/airflow-onprem.yaml`에서 `gitSync.repo`를 사용하고 있어,
실제로 배포되는 DAG가 이 repo의 `dags/`가 아닐 수 있습니다.

- 배포된 Airflow에서 “실제 실행되는 DAG 코드”가 어디서 왔는지(Repo/Branch/SubPath) 먼저 확인하세요.
