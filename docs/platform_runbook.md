# RETrend Platform Runbook (Ingestion -> Lakehouse -> BI)

이 문서는 RETrend의 “실행 단계 + 실행 커맨드”를 레이어별로 한 곳에 모은 운영 문서입니다.

## 0) Repo 구조 (권장 골격 / Canonical)

이 repo는 과거 실험(`docs/history/`)과 활성 코드가 섞여 있습니다. 운영 관점에서 아래를 기준(캐노니컬)으로 봅니다.

- `dags/`: Airflow DAG(오케스트레이션)
- `jobs/`: 실행 엔트리포인트(배치 잡) — 사람이 실행하는 단일 진입점
- `src/kreb/`: Python 패키지(수집 로직)
- `jobs/spark/`: Spark 잡(PySpark 엔트리포인트)
- `infra/spark/`: SparkApplication/YAML
- `infra/trino/`: Trino 배포/설정
- `infra/superset/`: Superset 배포/설정
- `helm/`: Airflow/Ingress 등 배포
- `docs/`: 운영/아키텍처 문서 (`docs/history/`는 참고용)

## 1) Orchestration: Airflow

핵심 DAG:
- `dags/retrend_crawler_with_quota_dag.py`

Airflow 설치/업데이트(Helm):
```
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f helm/airflow/airflow-onprem.yaml
```

Airflow ingress 적용:
```
kubectl apply -f helm/nginx/airflow-ingress-manual.yaml
```

주의(매우 중요):
- 실제 배포되는 DAG는 `helm/airflow/airflow-onprem.yaml`의 `dags.gitSync.repo/branch/subPath`가 가리키는 경로입니다.

## 2) Ingestion (Bronze): KREB Backfill

엔진 코드:
- `src/kreb/src/kreb_etl_v2/backfill.py`

Airflow 실행 방식:
- 컨테이너 이미지: `dave126/kreb-backfill:0.1.1`
- 커맨드: `python /app/src/kreb/src/kreb_etl_v2/backfill.py`

### 필수 환경변수

- `KREB_SERVICE_KEY`
- `KREB_LAWD_CSV` (예: `s3://retrend-raw-data/shigungu_list.csv`)
- `KREB_OUTPUT_URI` (예: `s3://retrend-raw-data/bronze/kreb_etl_v2/apt_trade`)

권장:
- `KREB_STATE_URI` (예: `s3://retrend-raw-data/kreb_state.json`)
- `KREB_DAILY_LIMIT` (기본 10000)
- `LOG_LEVEL`

HTTP 재시도 튜닝:
- `KREB_HTTP_RETRIES` (기본 3)
- `KREB_HTTP_BACKOFF_BASE` (기본 1.0)
- `KREB_HTTP_BACKOFF_CAP` (기본 30.0)
- `KREB_HTTP_TIMEOUT_SEC` (기본 10.0)

운영용:
- `KREB_IGNORE_DONE=1` (state가 done=true여도 강제로 재실행)

### 로컬 실행(캐노니컬)

1) 패키지 설치
```
python -m pip install -e "src/kreb[dev]"
```

2) env 설정 후 실행
```
export KREB_SERVICE_KEY=...
export KREB_DAILY_LIMIT=20
export KREB_LAWD_CSV=/tmp/lawd_test.csv
export KREB_STATE_URI=file:///tmp/kreb_state.json
export KREB_OUTPUT_URI=file:///tmp/kreb_output
export LOG_LEVEL=INFO

python jobs/kreb_backfill.py
```

레거시(기존 문서/컨테이너 경로 유지):
- `src/kreb/src/kreb_etl_v2/commands.md` 참고

### 출력 레이아웃/완결성 확인

브론즈 출력은 파티션 디렉토리로 저장됩니다.

```
.../LAWD_CD=11110/DEAL_YM=202401/page=1.csv
.../LAWD_CD=11110/DEAL_YM=202401/page=2.csv
.../LAWD_CD=11110/DEAL_YM=202401/_SUCCESS.json
```

운영 체크리스트:
- state(`KREB_STATE_URI`)에서 `last_error` 확인(QuotaExceeded 포함)
- 파티션 완료는 `_SUCCESS.json` 존재로 판단

## 3) Processing (Bronze -> Iceberg): Spark

PySpark 잡:
- `jobs/spark/kreb_csv_to_iceberg.py`

SparkApplication:
- `infra/spark/k8s/kreb_csv_to_iceberg.yaml`

ConfigMap 생성(예: Kubeflow namespace):
```
kubectl -n kubeflow create configmap kreb-csv-to-iceberg-app \
  --from-file=kreb_csv_to_iceberg.py=jobs/spark/kreb_csv_to_iceberg.py \
  --dry-run=client -o yaml | kubectl apply -f -
```

SparkApplication 제출:
```
kubectl apply -f infra/spark/k8s/kreb_csv_to_iceberg.yaml
```

## 4) Query: Trino

배포:
```
kubectl create namespace trino
helm upgrade --install trino trino/trino -n trino -f infra/trino/k8s/values-trino.yaml
kubectl apply -f infra/trino/k8s/trino-ingress.yaml
```

## 5) BI: Superset

로컬/VM에서 docker compose:
```
docker compose -f infra/superset/docker-compose.yaml up -d
```

초기화:
```
docker exec -it superset superset db upgrade
docker exec -it superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@example.com --password admin
docker exec -it superset superset init
```

Trino connection 예:
- `trino://superset@trino.home.lab:80/iceberg/default`

## 6) 테스트/검증

KREB 패키지 테스트:
```
python -m pytest -q src/kreb/tests
```

## 7) 보안(필수 개선)

현재 DAG/도커파일에 secret이 하드코딩된 흔적이 있습니다.
운영에서는 반드시 Kubernetes Secret / Airflow Connection/Variable/Secret로 옮기고,
`KubernetesPodOperator`의 `secrets` 기능을 사용하세요.
