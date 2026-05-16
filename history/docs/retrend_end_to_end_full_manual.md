# RETrend 완전 상세 설명서 (수집 → Lakehouse → Superset)

본 문서는 RETrend 프로젝트의 **실제 코드/인프라 기준**으로, 최초 데이터 수집부터 최종 Superset 시각화까지의 전 과정을 코드 단위로 상세히 설명한다. 목적은 이 문서만 읽고도 운영/개발/장애대응/개선 포인트를 파악할 수 있게 하는 것이다.

---

## 0. 문서 범위와 기준

### 0.1 범위
- KREB API 수집 파이프라인 (백필 + 데일리 동기화)
- Bronze 저장 구조 및 상태(state) 설계
- Spark 기반 Bronze → Iceberg 적재(풀/증분)
- Airflow 오케스트레이션(DAG)
- Kubernetes/Helm/Ingress 인프라
- Trino 조회 계층, Superset 시각화 계층
- OpenLineage(Marquez) 리니지 계층

### 0.2 활성 코드/이력 코드 구분
- **활성 핵심 코드**
  - `src/kreb/src/kreb_etl_v2/`
  - `jobs/`, `jobs/spark/`
  - `dags/`
  - `docker/`, `helm/`, `infra/`
- **이력/참고 코드**
  - `docs/history/**`
  - `dags/sample/**`

---

## 1. 시스템 전체 구조 요약

RETrend의 현재 핵심 흐름은 다음과 같다.

1) Airflow DAG가 KubernetesPodOperator로 수집 컨테이너 실행  
2) KREB API를 `LAWD_CD × DEAL_YM × page` 키스페이스로 호출  
3) 페이지 CSV를 Bronze 경로에 저장 + 상태(cursor/state) 갱신  
4) Spark 작업이 Bronze CSV를 Iceberg 테이블로 변환/반영  
5) Trino가 Iceberg 테이블을 조회  
6) Superset이 Trino를 통해 대시보드 시각화  
7) OpenLineage(Marquez)로 실행 계보(lineage) 추적

핵심 데이터 경로(표준):

```text
KREB API
  -> s3://.../bronze/kreb_etl_v2/apt_trade/LAWD_CD=xxxxx/DEAL_YM=yyyymm/page=n.csv
  -> Iceberg table (iceberg.default.apt_trade_raw 또는 apt_trade)
  -> Trino catalog: iceberg
  -> Superset dashboard
```

---

## 2. 최초 수집 계층: KREB 백필 엔진 (`backfill.py`)

핵심 파일:
- `src/kreb/src/kreb_etl_v2/backfill.py`
- `jobs/kreb_backfill.py` (엔트리포인트)

## 2.1 파일 역할
- `backfill.py`는 수집 파이프라인의 메인 엔진이다.
- 기능: API 호출, 쿼터 제어, 페이지 순회, CSV 저장, 상태 저장, 완료 마커 기록.
- `jobs/kreb_backfill.py`는 `run_backfill_once()`를 실행하는 얇은 런처다.

## 2.2 핵심 클래스/함수 설명

### (A) 쿼터 제어
- `QuotaManager`
  - `limit`, `used`, `remaining`으로 일일 호출량 제어.
  - `consume()`에서 초과 시 `QuotaExceeded` 발생.
- 의의: API 쿼터 환경에서 안전한 중단/재개.

### (B) API 클라이언트
- `KrebClient.fetch_page(lawd_cd, deal_ym, page_no)`
  - 요청 파라미터: `serviceKey`, `LAWD_CD`, `DEAL_YMD`, `pageNo`, `numOfRows`.
  - 응답 파싱: `xmltodict`.
  - 반환: `(total_count, rows, total_pages, page_no_resp, items)`.
  - 예외 정책:
    - 429 -> `QuotaExceeded`
    - 5xx/네트워크 오류/중간 페이지 비정상 빈 응답 -> 재시도 대상

### (C) 페이지 순회
- `iter_month_pages(client, lawd_cd, deal_ym, start_page=1)`
  - 한 `(지역, 월)` 조합의 페이지를 마지막까지 순회.

### (D) 상태 저장/복원
- `load_state(fs, path)`, `save_state(fs, path, state)`
- state 구조 핵심 필드:
  - `cursor`: 다음 재개 위치 (`lawd_cd`, `deal_ym`, `page_no`)
  - `last_success`, `last_completed`, `last_error`
  - `done`, `done_at`
- S3 계열은 단일 overwrite, 로컬은 tmp+move 방식.

### (E) Bronze 저장
- `write_page_csv(...)`
  - 출력: `.../LAWD_CD=xxxxx/DEAL_YM=yyyymm/page=n.csv`
  - 기존 정상 파일(size>0)이 있으면 skip (idempotent).
- `write_partition_success(...)`
  - 파티션 완료 마커 `_SUCCESS.json` 생성.

### (F) 키스페이스 생성
- `load_lawd_codes_from_csv(...)`
  - CSV 컬럼: `LAWD_CD`/`lawd_cd`/`cortarNo` 대응.
- `generate_last_10y_months()`
  - 최근 10년 `YYYYMM` 생성.

### (G) 메인 실행
- `run_backfill_once()`
  - 환경변수 로드
  - client/quota/fs/state 초기화
  - `lawd_list × ym_list` 루프
  - 페이지 fetch→CSV write→state update
  - 파티션 완료 시 `_SUCCESS` + 다음 cursor 이동

## 2.3 엔트리포인트
- `jobs/kreb_backfill.py`
  - `from kreb_etl_v2.backfill import run_backfill_once`
  - 스케줄러/로컬 실행 양쪽에서 공통 사용.

---

## 3. 데일리 동기화 계층: 변경 반영 엔진 (`daily_sync.py`)

핵심 파일:
- `src/kreb/src/kreb_etl_v2/daily_sync.py`
- DAG 연계: `dags/kreb_daily_sync_daily.py`

## 3.1 파일 역할
- 일일 배치에서 쿼터 한도 내로 전체 키스페이스를 순회하면서 **기존 CSV 변경 여부를 반영(업데이트)**.
- 백필과 달리 "최근 데이터 변경 반영"을 목적에 둠.

## 3.2 핵심 설계

### (A) 상태 네임스페이스 분리
- `KREB_DAILY_SYNC_STATE_URI` 사용 권장.
- state 내 `daily_sync` 하위에 별도 필드 유지:
  - `cycles_completed`, `cursor`, `last_success`, `last_completed`, `last_run`, `last_error`.

### (B) 파일 변경 감지 업데이트
- `write_page_csv_update(...)`
  - 새 CSV 바이트의 MD5 계산
  - 기존 파일 ETag/파일바이트와 비교
  - 결과 분기: `created`, `updated`, `unchanged`, `skipped_empty`

### (C) 진행률/운영 메트릭
- `_compute_slot_index`, `_compute_percent_position`
  - 전체 슬롯 기준 대략적 진행률 계산.

### (D) 파티션 완료 + manifest 생성
- 파티션 완료 시 `_SUCCESS.json` 기록.
- 실행 종료 시:
  - `.../_manifests/daily_sync/latest.json` 생성
  - `completed_partitions` 목록 저장
  - Spark 증분 잡의 입력 계약 데이터로 사용.

---

## 4. Bronze 저장 레이어 데이터 계약

기본 출력 prefix:
- `KREB_OUTPUT_URI`

경로 구조:

```text
<KREB_OUTPUT_URI>/LAWD_CD=11110/DEAL_YM=202401/page=1.csv
<KREB_OUTPUT_URI>/LAWD_CD=11110/DEAL_YM=202401/page=2.csv
<KREB_OUTPUT_URI>/LAWD_CD=11110/DEAL_YM=202401/_SUCCESS.json
<KREB_OUTPUT_URI>/_manifests/daily_sync/latest.json
```

파일 의미:
- `page=n.csv`: 원천 API page 단위 데이터
- `_SUCCESS.json`: 해당 파티션(지역+월) 완료 표시
- `latest.json`: 데일리 동기화에서 이번 런 완료 파티션 목록

---

## 5. Spark 변환 계층: Bronze → Iceberg

핵심 파일:
- `jobs/spark/kreb_csv_to_iceberg.py` (풀/초기 적재)
- `jobs/spark/kreb_csv_to_iceberg_incremental.py` (증분)

## 5.1 공통 스키마 설계
- `CSV_COLS`: KREB 원본 컬럼 집합
- `DERIVED_COLS`: `lawdCd`, `deal_ym`, `year`, `month`, `_file`
- `ALL_COLS = CSV_COLS + DERIVED_COLS`
- `ensure_columns(df)`로 누락 컬럼 방어

## 5.2 풀 적재 잡 (`kreb_csv_to_iceberg.py`)
- Bronze glob 읽기:
  - `.../LAWD_CD=*/DEAL_YM=*/page=*.csv`
- 경로 기반 파생:
  - `lawdCd` <- `LAWD_CD=...`
  - `deal_ym` <- `DEAL_YM=...`
  - `year`, `month` 파생
- Iceberg 테이블 생성/유지:
  - 기본 table: `iceberg.default.apt_trade_raw`
  - LOCATION: `{warehouse_base}/default/apt_trade_raw`
  - PARTITIONED BY `(year, month, lawdCd)`
- 쓰기 방식: `append()`

## 5.3 증분 잡 (`kreb_csv_to_iceberg_incremental.py`)
- manifest 읽기:
  - `KREB_DAILY_SYNC_MANIFEST_PATH`
  - `completed_partitions`만 대상.
- 대상 파티션만 glob 후 로드.
- 쓰기 방식: `overwritePartitions()`

의의:
- 데일리 동기화 변경분만 선택 반영해 불필요한 전체 재처리 감소.

---

## 6. Airflow 오케스트레이션 (DAG)

핵심 DAG 파일:
- `dags/retrend_crawler_with_quota_dag.py`
- `dags/kreb_daily_sync_daily.py`
- `dags/kreb_daily_sync_to_iceberg_incremental_daily.py`
- `dags/kreb_bronze_to_iceberg_backfill.py`

## 6.1 `retrend_crawler_with_quota_dag.py`
- DAG ID: `kreb_backfill_daily`
- 매일 02:00 KST
- `KubernetesPodOperator`로 `dave126/kreb-backfill:0.1.1` 실행
- 실행 스크립트: `/app/src/kreb/src/kreb_etl_v2/backfill.py`

## 6.2 `kreb_daily_sync_daily.py`
- DAG ID: `kreb_daily_sync_daily`
- 매일 02:00 KST
- `daily_sync.py` 단일 실행
- 서비스키는 Airflow Variable (`{{ var.value.KREB_SERVICE_KEY }}`)로 주입

## 6.3 `kreb_daily_sync_to_iceberg_incremental_daily.py`
- DAG ID: `kreb_daily_sync_to_iceberg_incremental_daily`
- 02:30 KST 실행 (daily_sync 후속)
- 작업 체인:
  - `kreb_daily_sync_once`
  - `spark_kreb_csv_to_iceberg_incremental`
- 특징:
  - ConfigMap으로 PySpark 앱 동적 주입
  - SparkApplication을 kubectl로 제출/상태 폴링

## 6.4 `kreb_bronze_to_iceberg_backfill.py`
- 수동 실행(backfill 배치용)
- Bronze 전체를 Iceberg로 백필 반영
- 구성 방식:
  - ConfigMap으로 백필 PySpark 코드 주입
  - SparkApplication 제출 + 완료/실패 모니터링

---

## 7. 컨테이너/이미지 계층

핵심 파일:
- `docker/kreb-backfill/Dockerfile`
- `docker/crawler/Dockerfile`

## 7.1 `docker/kreb-backfill/Dockerfile`
- Python 3.9 slim 기반
- 필수 라이브러리 설치: `requests`, `xmltodict`, `fsspec`, `s3fs`, `pandas`, `boto3`, `psycopg2-binary`, `asyncpg`
- `/app/src`에 코드 복사
- KREB/MinIO 관련 ENV 기본값 포함

## 7.2 `docker/crawler/Dockerfile`
- Airflow worker/crawler 공용 성격의 Python 실행 이미지
- `src`, `sample` 복사
- MinIO/KREB ENV 선언

---

## 8. Airflow/Kubernetes 배포 계층

핵심 파일:
- `helm/airflow/airflow-onprem.yaml`
- `helm/nginx/airflow-ingress-manual.yaml`
- `commands.md`

## 8.1 `airflow-onprem.yaml`
- Airflow 2.8.4
- Executor: `KubernetesExecutor`
- worker image: `dave126/retrend-crawler:2.8.5`
- gitSync로 DAG repo 동기화
- `OPENLINEAGE_URL`, `OPENLINEAGE_NAMESPACE` 설정 포함

## 8.2 `airflow-ingress-manual.yaml`
- host: `airflow.home.lab`
- nginx ingress로 airflow-webserver 노출

## 8.3 `commands.md`의 운영 역할
- buildx 빌드/푸시 표준 명령
- Helm 설치/업데이트 절차
- Spark Operator 설치 절차
- 백필/테스트용 환경변수 및 로컬/MinIO 실행 예시

---

## 9. Spark on Kubernetes 인프라 매니페스트

핵심 파일:
- `infra/spark/k8s/kreb_csv_to_iceberg.yaml`
- `infra/spark/k8s/kreb_csv_to_iceberg_incremental.yaml`
- `infra/spark/k8s/drop-apt-trade.yaml`

핵심 포인트:
- SparkApplication 스펙으로 드라이버/익스큐터 리소스 제어
- Iceberg + Hive Metastore + S3A(MinIO) 연결 정보 명시
- 증분 잡은 manifest path를 받아 부분 파티션 반영

---

## 10. Query 계층: Trino

핵심 파일:
- `infra/trino/k8s/values-trino.yaml`
- `infra/trino/k8s/trino-ingress.yaml`
- `infra/trino/commands.md`

## 10.1 역할
- Iceberg catalog를 통해 Lakehouse 쿼리 제공
- Superset이 연결하는 SQL 엔진 역할

## 10.2 핵심 설정
- `additionalCatalogs.iceberg`에 Hive metastore URI + S3 endpoint/credential 지정
- `trino.home.lab` ingress 노출

---

## 11. BI 계층: Superset

핵심 파일:
- `infra/superset/docker-compose.yaml`
- `infra/superset/superset_home/superset_config.py`
- `infra/superset/commands.md`

## 11.1 역할
- 최종 시각화/대시보드 레이어
- Trino(`iceberg/default`)에 연결해 분석 쿼리 수행

## 11.2 실행 구조
- Superset + Postgres(meta DB) + Redis를 docker-compose로 운영
- `superset_config.py`에서 timeout 관련 운영 파라미터 관리

---

## 12. 데이터 리니지 계층: OpenLineage / Marquez

핵심 파일:
- `infra/openlineage/k8s/00-namespace.yaml`
- `infra/openlineage/k8s/10-postgres.yaml`
- `infra/openlineage/k8s/20-marquez.yaml`
- `infra/openlineage/k8s/25-marquez-web.yaml`
- `infra/openlineage/k8s/30-ingress.yaml`
- `helm/airflow/airflow-onprem.yaml` (`OPENLINEAGE_URL`, `OPENLINEAGE_NAMESPACE`)

역할:
- Airflow 런 메타데이터를 OpenLineage endpoint로 발행
- Marquez에서 파이프라인 실행 계보 조회

---

## 13. 데이터 테이블/컬럼 의미

## 13.1 핵심 테이블
- 기본 운영 테이블 후보:
  - `iceberg.default.apt_trade_raw`
  - 일부 DAG/설정에는 `iceberg.default.apt_trade`도 사용

운영 정리 포인트:
- 테이블명(`apt_trade_raw` vs `apt_trade`) 표준화를 권장.

## 13.2 대표 컬럼 의미
- 가격: `dealAmount`
- 거래일: `dealYear`, `dealMonth`, `dealDay`, 파생 `deal_ym`
- 위치: `lawdCd`, `sggCd`, `umdCd`, `jibun`, `roadNm*`
- 물건: `aptNm`, `aptDong`, `excluUseAr`, `floor`, `buildYear`
- 거래 속성: `buyerGbn`, `slerGbn`, `dealingGbn`, `cdealType`, `cdealDay`
- 운영/추적: `_file`, `year`, `month`

---

## 14. 환경변수 계약 (요약)

### 수집 공통
- `KREB_SERVICE_KEY`
- `KREB_BASE_URL`
- `KREB_DAILY_LIMIT`
- `KREB_LAWD_CSV`
- `KREB_OUTPUT_URI`
- `LOG_LEVEL`
- `KREB_HTTP_RETRIES`, `KREB_HTTP_BACKOFF_BASE`, `KREB_HTTP_BACKOFF_CAP`, `KREB_HTTP_TIMEOUT_SEC`

### 상태
- 백필: `KREB_STATE_URI`
- 데일리: `KREB_DAILY_SYNC_STATE_URI`

### MinIO/S3
- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`

### Spark/Iceberg
- `HIVE_METASTORE_URI`
- `BRONZE_PREFIX`
- `WAREHOUSE_BASE`
- `ICEBERG_TABLE`
- `KREB_DAILY_SYNC_MANIFEST_PATH`
- `RECREATE_TABLE`

---

## 15. 테스트/검증 포인트

테스트 디렉터리:
- `src/kreb/tests/`
  - `test_backfill_v2_state.py`
  - `test_backfill_v2_iter_month_pages.py`
  - `test_backfill_v2_client_retry.py`
  - `test_daily_sync_helpers.py`
  - 기타 도메인/클라이언트 테스트

검증 관점:
- state 저장/복원 정확성
- pagination 순회/재시도 정책
- quota 소진 시 정상 중단/재개
- daily sync 헬퍼 로직 안정성

---

## 16. 운영자가 알아야 할 핵심 포인트

1) **재개 가능성의 핵심은 state 파일**  
   - state를 잃으면 재처리 범위 계산이 어려워짐.

2) **데일리와 백필 state 분리 필수**  
   - 동일 state 공유 시 cursor 충돌 위험.

3) **manifest 기반 증분 반영이 비용 절감 포인트**  
   - daily_sync 완료 파티션만 Spark incremental 대상.

4) **테이블명 표준화 필요**  
   - `apt_trade_raw`/`apt_trade` 혼재는 운영 혼란 유발.

5) **리니지 계층 활용**  
   - OpenLineage + Marquez로 DAG 런 추적과 장애 분석 개선.

---

## 17. 파일별 핵심 역할 인덱스 (빠른 참조)

- 수집 엔진
  - `src/kreb/src/kreb_etl_v2/backfill.py`
  - `src/kreb/src/kreb_etl_v2/daily_sync.py`
  - `jobs/kreb_backfill.py`
- 변환 엔진
  - `jobs/spark/kreb_csv_to_iceberg.py`
  - `jobs/spark/kreb_csv_to_iceberg_incremental.py`
- DAG
  - `dags/retrend_crawler_with_quota_dag.py`
  - `dags/kreb_daily_sync_daily.py`
  - `dags/kreb_daily_sync_to_iceberg_incremental_daily.py`
  - `dags/kreb_bronze_to_iceberg_backfill.py`
- 컨테이너
  - `docker/kreb-backfill/Dockerfile`
  - `docker/crawler/Dockerfile`
- 오케스트레이션/인프라
  - `helm/airflow/airflow-onprem.yaml`
  - `helm/nginx/airflow-ingress-manual.yaml`
  - `infra/spark/k8s/*.yaml`
  - `infra/trino/k8s/*.yaml`
  - `infra/superset/docker-compose.yaml`
  - `infra/openlineage/k8s/*.yaml`
- 운영 가이드
  - `commands.md`

---

## 18. 결론

RETrend는 단순 수집 스크립트 단계를 넘어, **쿼터 제약 API 수집 + 재개 가능한 state 관리 + Bronze 표준화 + Spark/Iceberg 적재 + Trino/Superset 분석 + OpenLineage 리니지**가 결합된 운영형 데이터 플랫폼으로 진화했다.

현재 기준에서 운영 성숙도를 더 끌어올리는 가장 중요한 후속 과제는 다음 3가지다.
- 테이블명 표준화 (`apt_trade_raw` vs `apt_trade`)
- 시크릿 하드코딩 제거(Secret Manager/K8s Secret로 이전)
- Superset 대시보드 정의/메타데이터의 코드 자산화
