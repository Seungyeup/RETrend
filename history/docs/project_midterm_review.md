# RETrend 프로젝트 중간 점검 문서 (통합 이해본)

이 문서는 **현재 RETrend 레포 기준으로 실제 동작 중인 파이프라인**을 한 번에 이해하기 위한 통합 문서다. 
읽는 대상은 "코드를 직접 열어보지 않고도 프로젝트의 진행상황/데이터 흐름/최종 테이블/시각화/인프라"를 파악하려는 운영자·개발자다.

---

## 1) 프로젝트 한 줄 요약

RETrend는 한국부동산원(KREB) 아파트 실거래가 Open API를 **일일 쿼터 제약 하에서 안정적으로 수집**하고, 브론즈 CSV를 Spark로 Iceberg 테이블로 적재해 Trino/Superset에서 조회·시각화하는 배치형 데이터 플랫폼이다.

핵심 활성 경로:

- 수집 엔진: `src/kreb/src/kreb_etl_v2/backfill.py`, `src/kreb/src/kreb_etl_v2/daily_sync.py`
- 오케스트레이션: `dags/*.py` (Airflow `KubernetesPodOperator`)
- 변환(브론즈→Iceberg): `jobs/spark/kreb_csv_to_iceberg.py`, `jobs/spark/kreb_csv_to_iceberg_incremental.py`
- 쿼리/BI: `infra/trino/k8s/values-trino.yaml`, `infra/superset/docker-compose.yaml`

---

## 2) 지금까지 진행사항 (중간 점검)

### 2-1. 코드/운영 관점 진행 단계

1. **초기 구축 및 데이터 수집 기반 마련**
   - KREB 수집 로직과 Airflow 기반 실행 경로를 확보.
2. **레이크하우스 연결 단계 진입**
   - 브론즈 CSV를 Spark로 Iceberg에 적재하는 작업(풀 백필/증분 반영) 구성.
3. **조회/시각화 단계 연결**
   - Trino 카탈로그(iceberg) 구성 및 Superset 연결 경로 정리.
4. **운영 문서화·표준화 진행 중**
   - `docs/platform_runbook.md`, `docs/runbook_kreb_backfill.md`, `docs/architecture_visuals.md` 등 운영 문서가 정리됨.

### 2-2. Git 히스토리 기반 주요 이정표

`git log` 기준 확인되는 주요 커밋:

- `03dceb2` (2025-06-01) Initial commit
- `749d354` (2025-06-23) NFS 초기화 관련 재작성
- `205027f` (2025-06-29) Iceberg/Trino/Superset 통합(phase2 성격)
- `7969746` (2025-11-02) KubernetesPodOperator 이미지 경로 업데이트
- 이후 일부 `backup` 커밋 다수(운영/실험 이력 성격)

> 참고: 최근 커밋 메시지는 `backup`처럼 요약형이 많아, 기능 진행상태는 코드/런북 파일 근거로 함께 해석해야 정확하다.

---

## 3) 전체 플로우 (수집 → 저장 → 변환 → 조회/시각화)

## 3-1. 수집 계층 (KREB API)

### 입력 데이터

- **API 원천**: KREB 아파트 매매 실거래가 API
  - 기본 URL: 
    `https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev`
  - 사용 파라미터: `LAWD_CD`, `DEAL_YMD(YYYYMM)`, `pageNo`, `numOfRows`
- **지역 코드 마스터**: `KREB_LAWD_CSV`
  - `LAWD_CD`/`lawd_cd`/`cortarNo` 컬럼 중 하나를 읽어 시군구 코드 목록 생성

### 처리 방식

- 키스페이스: `(LAWD_CD × DEAL_YM × page)`
- 쿼터 제약: `KREB_DAILY_LIMIT` + in-memory `QuotaManager`
- 재시도 정책:
  - 429 → `QuotaExceeded`로 정상 중단/재개
  - timeout/connection/5xx/중간 페이지 빈응답 → bounded retry

### 상태 관리(중요)

`KREB_STATE_URI`(백필) 또는 `KREB_DAILY_SYNC_STATE_URI`(데일리 동기화) JSON에 진행상태 저장:

- `cursor`: 다음 호출 위치 (`lawd_cd`, `deal_ym`, `page_no`)
- `last_success`: 마지막 성공 페이지 메타
- `last_completed`: 마지막 완료 파티션 메타
- `last_error`: 마지막 오류 메타
- `done`, `done_at`: 전체 백필 완결 여부
- `daily_sync` 하위 필드: `cycles_completed`, `last_run`, `completed_partitions` 등

의미:
- **쿼터 소진/장애가 발생해도 다음 실행에서 이어받기**가 가능하도록 설계됨.

---

## 3-2. 브론즈 저장 계층

출력 경로(`KREB_OUTPUT_URI`) 아래 파티션 레이아웃:

```text
.../LAWD_CD=11110/DEAL_YM=202401/page=1.csv
.../LAWD_CD=11110/DEAL_YM=202401/page=2.csv
.../LAWD_CD=11110/DEAL_YM=202401/_SUCCESS.json
```

파일 의미:

- `page=N.csv`: API 응답의 페이지 단위 원본 레코드
- `_SUCCESS.json`: 해당 `(LAWD_CD, DEAL_YM)`이 마지막 페이지까지 완료됐다는 마커

Idempotency 포인트:

- 기존 `page=N.csv`가 정상 파일이면 재쓰기 스킵(백필)
- `daily_sync`는 내용 변경 시 overwrite하여 최신화

---

## 3-3. 변환 계층 (Spark → Iceberg)

### 풀 적재/백필

- 코드: `jobs/spark/kreb_csv_to_iceberg.py`
- 읽기 패턴: `.../LAWD_CD=*/DEAL_YM=*/page=*.csv`
- 파생 컬럼:
  - `lawdCd` (경로 파싱)
  - `deal_ym` (경로 파싱)
  - `year`, `month` (deal_ym 분해)
  - `_file` (원본 파일 경로)

### 증분 반영

- 코드: `jobs/spark/kreb_csv_to_iceberg_incremental.py`
- `daily_sync`가 남긴 manifest(`_manifests/daily_sync/latest.json`)의 `completed_partitions`만 읽어 overwritePartitions 수행

---

## 3-4. 최종 테이블(현재 확인 기준)

Iceberg 테이블 생성 구문(공통 패턴):

- 위치: `{warehouse_base}/default/apt_trade_raw`
- 파티션: `(year, month, lawdCd)`
- 컬럼: KREB 원본 CSV 컬럼 32개 + 파생 5개

핵심 컬럼 묶음:

- 원천 거래 정보: `dealAmount`, `dealYear`, `dealMonth`, `dealDay`, `excluUseAr`, `floor`, `aptNm`, `buildYear` 등
- 위치/행정코드: `sggCd`, `umdCd`, `landCd`, `roadNm*`, `jibun`
- 거래 특성: `buyerGbn`, `slerGbn`, `dealingGbn`, `cdealType`, `cdealDay`
- 파생 운영 컬럼: `lawdCd`, `deal_ym`, `year`, `month`, `_file`

### 테이블명 상태 주의점

- 작업 스크립트 기본값은 `iceberg.default.apt_trade_raw`인 경우가 많음
  - 예: `jobs/spark/kreb_csv_to_iceberg.py`, `jobs/spark/kreb_csv_to_iceberg_incremental.py`
- 일부 DAG 내 인라인 Spark 코드/환경변수는 `iceberg.default.apt_trade`를 사용
  - 예: `dags/kreb_bronze_to_iceberg_backfill.py`, `dags/kreb_daily_sync_to_iceberg_incremental_daily.py`

즉, **운영 시점에 `apt_trade`/`apt_trade_raw` 중 어떤 이름을 표준으로 쓸지 명확화가 필요한 상태**다.

---

## 4) 각 데이터가 의미하는 것

## 4-1. 원천(KREB API) 데이터 의미

실거래 단위 레코드로, "어떤 지역의 어떤 아파트가 언제 얼마에 거래되었는지"를 표현한다.

- 가격 축: `dealAmount`
- 시간 축: `dealYear`, `dealMonth`, `dealDay` (+ 파생 `deal_ym`)
- 공간 축: `LAWD_CD`(경로/파생 `lawdCd`) + `sggCd`, `umdCd`, `jibun`, `roadNm`
- 물건 축: `aptNm`, `aptDong`, `excluUseAr`, `floor`, `buildYear`
- 거래속성 축: `buyerGbn`, `slerGbn`, `dealingGbn`, 해제 관련(`cdealType`, `cdealDay`)

## 4-2. 상태(state) 데이터 의미

state는 비즈니스 데이터가 아니라 **수집 엔진의 재개 가능성/완결성 보장 메타데이터**다.

- `cursor`는 "다음 재시작 지점"
- `last_success`/`last_completed`는 "어디까지 안전하게 처리됐는지"
- `last_error`는 "왜 멈췄는지"
- `done`은 "백필 전체 완료 여부"

## 4-3. Manifest 데이터 의미

`daily_sync` manifest는 "이번 런에서 완료된 파티션 목록"을 Spark 증분 적재로 전달하기 위한 계약 데이터다.

---

## 5) 시각화(Visualization) 현황

## 5-1. 현재 활성 BI 경로

- Superset 실행: `infra/superset/docker-compose.yaml`
- Trino 연결 문자열 예시: `trino://superset@trino.home.lab:80/iceberg/default`
- README에 Streamlit/Superset 스크린샷 존재: `README.md`, `docs/image/*`

즉, 현재 문서/구성 기준으로는 **Iceberg(Trino) → Superset**이 주 시각화 경로다.

## 5-2. 레포 내 시각화 자산 상태

- 활성 경로에서 대시보드 정의(JSON/SQL assets)는 별도 관리 파일이 뚜렷하지 않음
- Streamlit 앱 코드는 `docs/history/phase1`, `docs/history/phase2` 하위에 존재(역사/참고용)

해석:
- **시각화 엔진 자체(Superset)는 활성**, 하지만 "완성된 대시보드 정의를 코드로 버전관리"하는 단계는 아직 제한적이다.

---

## 6) 인프라 구성 (현재 운영 구조)

## 6-1. 오케스트레이션

- Airflow Helm 값: `helm/airflow/airflow-onprem.yaml`
  - `KubernetesExecutor` 사용
  - worker 이미지: `dave126/retrend-crawler:2.8.5`
  - DAG gitSync 활성 (`dags.gitSync.repo` 설정)

## 6-2. DAG 실행 방식

- DAG는 `KubernetesPodOperator`로 수집 컨테이너/스파크 제출 컨테이너 실행
- 예:
  - `dags/retrend_crawler_with_quota_dag.py` (백필)
  - `dags/kreb_daily_sync_daily.py` (일일 동기화)
  - `dags/kreb_daily_sync_to_iceberg_incremental_daily.py` (동기화 + 증분 적재)
  - `dags/kreb_bronze_to_iceberg_backfill.py` (브론즈→Iceberg 백필)

## 6-3. 데이터 레이크/쿼리/BI

- Object Storage: MinIO(S3 API)
- Spark on K8s: `infra/spark/k8s/*.yaml`
- Catalog/Metastore: Hive Metastore URI 사용 (`thrift://172.30.1.30:9083`)
- Query: Trino (`infra/trino/k8s/values-trino.yaml`)
- BI: Superset (`infra/superset/docker-compose.yaml`)

## 6-4. 외부 노출

- Airflow ingress: `helm/nginx/airflow-ingress-manual.yaml` (`airflow.home.lab`)
- Trino ingress: `infra/trino/k8s/trino-ingress.yaml` (`trino.home.lab`)

---

## 7) 운영 플로우(실행 순서)

1. 도커 이미지 빌드/푸시
   - `docker/crawler/Dockerfile`, `docker/kreb-backfill/Dockerfile`
   - 명령 참고: `commands.md`
2. Airflow(Helm) 배포/업데이트
   - `helm/airflow/airflow-onprem.yaml`
3. DAG 스케줄 실행으로 KREB 수집
   - 브론즈 CSV + state + `_SUCCESS` 생성
4. Spark 작업으로 Iceberg 테이블 반영
   - 풀 백필 또는 증분 overwritePartitions
5. Trino/Superset에서 분석/시각화

---

## 8) 활성 코드 vs 이력 코드 구분

### 활성(운영 기준)

- `dags/`
- `src/kreb/src/kreb_etl_v2/`
- `jobs/`
- `infra/`, `helm/`
- `docs/platform_runbook.md`, `docs/runbook_kreb_backfill.md`, `docs/architecture_visuals.md`

### 이력/참고

- `docs/history/**`
- `dags/sample/**`

주의:
- 현재 Airflow Helm 값에서 `gitSync.repo`가 별도 repo를 가리킬 수 있으므로,
  "레포에 있는 DAG"와 "실제 배포되어 실행 중인 DAG"가 다를 가능성을 항상 점검해야 한다.

---

## 9) 현재 상태 한눈에 결론

이 프로젝트는 이미 **수집-저장-변환-조회/시각화까지 E2E 구조가 연결된 상태**이며,
특히 KREB 쿼터 제약 환경에서 재개 가능한 수집(state/cursor)과 증분 반영(manifest 기반)까지 구현되어 운영 안정성 기반을 갖췄다.

동시에, 중간 점검 시점의 주요 정리 포인트는 아래 3가지다.

1. `apt_trade` vs `apt_trade_raw` 테이블명 표준화
2. 하드코딩된 secret/env의 비밀관리 체계 전환(Airflow Secret/K8s Secret)
3. Superset 대시보드 정의의 코드 자산화(SQL/semantic layer/versioning)

이 3가지를 정리하면, 현재 파이프라인은 운영 성숙도 측면에서 다음 단계(안정 운영 + 비용/성능 최적화)로 넘어가기 좋은 상태다.
