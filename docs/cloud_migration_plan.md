# RETrend 클라우드 마이그레이션 기획안 (온프레미스 -> 클라우드, 비용 효율 중심)

## 1) 목적과 전제

이 문서는 현재 온프레미스에서 운영 중인 RETrend(배치 수집 + Lakehouse + BI)를 클라우드로 이전할 때, 운영 부담을 최소화하면서 비용 효율을 극대화하는 목표로 권장 기술스택과 구성을 제안한다.

전제:

- 워크로드는 배치성(일/월 단위)이며 초단위 실시간 처리가 핵심 요구사항이 아니다.
- 부동산 원천/가공 데이터(브론즈/실버/마트)를 전량 적재하고, Superset으로 분석/모니터링을 수행한다.
- 테이블 포맷은 Iceberg를 유지(이식성/쿼리엔진 선택의 자유)하는 것을 기본 방향으로 한다.

비목표(이번 문서 범위 밖):

- 특정 벤더의 “정확한” 월 비용 산정(워크로드/동시성/스캔량이 고정되지 않아 수치가 오해를 만든다)
- 실시간 스트리밍/CDC 기반 아키텍처 설계

## 2) 현행(온프레미스) 구성 요약

현재 운영 골격(레포 기준):

- Orchestration: Airflow(Helm) + DAG(`dags/`)
- Ingestion(Bronze): KREB backfill/daily sync 컨테이너(`src/kreb/src/kreb_etl_v2/`)
- Processing(Bronze->Iceberg): Spark(SparkApplication)로 CSV/Parquet 변환 및 Iceberg 적재
- Storage: MinIO(S3 API) + Iceberg
- Catalog/Metastore: Hive Metastore
- Query: Trino
- BI: Superset(docker compose)

핵심 리스크(최근 이슈에서 확인됨):

- BI가 원본 테이블을 크게 스캔하거나 중위수/분위수 같은 비싼 집계를 반복하면 Trino 메모리/OOM이 발생 가능
- K8s 노드 상태/리소스에 따라 쿼리 서비스가 불안정해질 수 있음

## 3) 클라우드 이전의 “완료” 정의(성공 기준)

1) 일/월 배치 수집이 클라우드에서 자동 실행되고, 누락 없이 상태/커서가 지속(재시작 가능)된다.

2) 브론즈/실버/마트 레이어가 객체 스토리지에 저장되고, Iceberg 카탈로그에 등록되어 쿼리 가능하다.

3) Superset에서 주요 대시보드가 타임아웃 없이 조회되며(마트 기반), 장애 시 관측/알림이 가능하다.

4) 운영 비용이 “항상 켜져있는 클러스터” 중심이 아니라 “사용량 기반”으로 설계되어 피크/비피크에 탄력적이다.

## 4) 권장 목표 아키텍처(최소 운영, 비용 효율)

핵심 원칙은 “객체 스토리지 + Iceberg + 관리형 카탈로그 + 서버리스 실행 + 서버리스 SQL(가능하면)”이다.

### 4.1 1순위 권장안: AWS 중심(대부분 서버리스)

권장 이유: Iceberg를 S3에 두고, 카탈로그/스파크/SQL을 서버리스로 구성하는 선택지가 가장 성숙해 운영 부담과 상시 비용을 줄이기 유리하다.

구성(권장):

- Object Storage: Amazon S3 (브론즈/실버/마트)
- Catalog/Metastore: AWS Glue Data Catalog (Iceberg 카탈로그)
- Orchestration: Amazon MWAA(Managed Airflow) 또는 경량 워크플로(향후)로 전환
- Ingestion 실행: 컨테이너 기반 작업(ECS Fargate)으로 배치 실행(기존 백필/데일리 컨테이너 재사용)
- Transform(스파크): EMR Serverless(Spark)로 Iceberg 적재/정리/마트 생성
- Query(서빙): Athena(Iceberg) 우선 검토
  - Athena로 커버되지 않는 커넥터/세만틱이 필요하면 Trino를 별도로 운영(단, “항상 켜짐” 비용/운영 증가)
- BI: Superset(ECS Fargate + RDS Postgres)
- Observability: CloudWatch(Log/Metrics/Alarm) + 필요 시 Managed Grafana/Prometheus
- Secrets/IAM: Secrets Manager/SSM + IAM Role 기반 접근

현재 컴포넌트 매핑(현행 -> AWS):

| 현행 | 권장 매핑 | 변경 포인트 |
|---|---|---|
| MinIO | S3 | 경로/권한(IAM)만 교체 |
| Hive Metastore | Glue Catalog | 카탈로그 운영 제거 |
| Spark-on-K8s | EMR Serverless | Spark 엔트리 유지, 제출/권한 방식 변경 |
| Airflow on K8s | MWAA | DAG는 유지, Operator/실행 매체(ECS/EMR) 변경 |
| Trino on K8s | Athena(우선) 또는 Trino(보완) | “항상 켜짐”을 피하는 방향 |
| Superset docker-compose | ECS Fargate + RDS | 메타DB 관리형으로 전환 |

### 4.2 대안 A: GCP(관리형 오케스트레이션/스파크 + Trino 유지)

- Storage: GCS
- Orchestration: Cloud Composer(Airflow)
- Spark: Dataproc(서버리스/클러스터)
- Catalog: Dataproc Metastore(HMS) 또는 REST Catalog(Nessie 등)
- Query: Trino on GKE(상시 비용 존재) 또는 BigQuery 외부 테이블(요구사항 충족 여부 검증 필요)
- BI: Superset(Cloud Run 또는 GKE) + Cloud SQL(Postgres)

### 4.3 대안 B: “변경 최소” Managed Kubernetes(EKS/GKE/AKS)

현행 형태를 거의 유지하되, 객체 스토리지만 클라우드로 교체하고 K8s 운영을 관리형으로 옮긴다.

- 장점: 코드/운영 방식 변화 최소
- 단점: 클러스터 상시 비용 + 운영 복잡도 + 쿼리 안정성(리소스 튜닝) 부담 유지

## 5) 데이터 레이어 설계(브론즈/실버/마트)

### 5.1 저장 레이아웃(권장)

- `bronze/`: 원본 보존(재처리/감사 대비), 수집일 기준 파티션
- `silver/`: 타입 정규화/결측/취소거래 등 정제 + 중복 제거
- `marts/`: Superset에서 바로 쓰는 사전 집계/피처 테이블

### 5.2 Iceberg 파티셔닝/정렬 기본값(배치/분석 최적화)

원칙: 파티션은 “자주 필터되는 저/중 카디널리티” 위주로 최소화하고, 나머지는 정렬로 스캔 효율을 확보한다.

- 시간: `months(deal_date)` 같은 time transform 파티션 우선
- 지역: 지역 필터가 매우 잦으면 `identity(lawd_cd 또는 sggcd)` 고려(카디널리티/쿼리패턴을 보고 결정)
- 정렬(권장): `(deal_date, region_code)` 또는 `(region_code, deal_date)` 중 하나로 통일
- 운영 필수: 작은 파일 방지(컴팩션), 스냅샷/메타데이터 정리(만료)

### 5.3 마트(사전 집계) 운영 원칙

Superset에서 분위수/중위수/최근 12개월 같은 지표를 “원본에서 실시간 계산”하지 않도록 한다.

- 원칙: 대시보드에서 쓰는 핵심 지표는 마트로 미리 계산하여 행 수를 줄인다.
- 갱신: 일 배치로 refresh(또는 증분 업데이트)
- 검증: 마트가 원본 대비 유효한지(카운트/기간/필터) 자동 체크

## 6) 운영/모니터링(관측 가능성) 계획

필수 관측(최소 세트):

- 배치 성공/실패(작업별, 파티션별) + 마지막 커서/상태
- 처리량(건수/파일 수) + 쿼리 스캔량(서버리스 SQL의 비용 드라이버)
- 마트 갱신 시각/지연 + 대시보드 타임아웃/에러율

알림 기준 예시(정책):

- N회 연속 실패 또는 특정 파티션 누락
- 전일 대비 스캔 바이트 급증(대시보드/쿼리 비용 폭증 신호)
- 파일 수 급증(작은 파일 누적) 또는 메타데이터 증가(성능 저하 신호)

## 7) 보안/권한/비밀정보(필수)

- 비밀정보는 이미지/DAG에 하드코딩 금지(Secrets Manager/SSM 등 관리형 저장소로 이동)
- 원칙적으로 Writer(수집/스파크)와 Reader(BI/분석)의 권한을 분리
- 객체 스토리지는 private + 암호화 at-rest(KMS) + in-transit(TLS)

## 8) 마이그레이션 실행 계획(단계별)

### 8.1 0단계: PoC(최소 경로)로 “끝까지” 한 번 돌리기

- 대상: 특정 1~2개 데이터셋(예: 거래)만
- 흐름: 수집(브론즈) -> 정제(실버) -> 마트 1~2개 -> Superset 차트 1~2개
- 성공 기준: 재실행/재시작 가능 + 대시보드 타임아웃 없음

### 8.2 1단계: 전체 적재(브론즈/실버) + 마트 확장

- 전체 지역/기간 backfill 전략 확정(파티션 단위 재처리, 상태 저장)
- 마트 목록을 대시보드 기준으로 확정하고 우선순위로 구현

### 8.3 2단계: 성능/비용 최적화(운영 루프)

- 컴팩션/스냅샷 만료 스케줄링
- 대시보드 쿼리 스캔량 상한 정책(필터 강제, 기간 기본값)
- 필요 시 “서버리스 SQL vs Trino 상시 운영” 재평가

## 9) 비용 효율 레버(수치 대신 ‘드라이버’로 관리)

- Spark 실행 시간(변환/컴팩션/마트 갱신): 가장 큰 변동비 후보
- SQL 스캔 바이트(대시보드/분석): 서버리스 SQL의 핵심 비용 항목
- 24/7 서비스(상시 Trino/K8s): 고정비 증가 요인

비용 절감 핵심 전략:

- 대시보드는 마트 기반으로만 구성(원본 테이블 대용량 스캔 금지)
- 기간/필터 기본값을 보수적으로 설정(최근 12개월 등)
- 컴팩션으로 작은 파일을 줄여 쿼리 비용과 지연을 동시에 낮춤

## 10) 리스크와 대응

- (리스크) BI 쿼리 폭주로 쿼리 엔진 장애/비용 급증
  - (대응) 마트 우선 + 캐시/동시성 제한 + 기간 필터 기본값 강제

- (리스크) 백필 기간이 길어 비용/시간이 예상보다 큼
  - (대응) 파티션 단위로 나눠서 실행, 우선순위(최근/핵심지역)부터 완성

- (리스크) 벤더 종속
  - (대응) Iceberg를 핵심 표준으로 유지하고, 카탈로그/쿼리엔진을 교체 가능한 형태로 설계

## 11) 결론(추천)

운영 최소화/비용 효율을 최우선으로 두면, 1순위는 “S3 + Iceberg + Glue Catalog + Serverless Spark(EMR Serverless) + Serverless SQL(Athena) + Superset(Managed Container)” 조합이다. 쿼리 특성상 Athena로 부족한 구간이 확인될 때만 Trino를 보완적으로 유지하는 전략이 전체 비용과 운영 부담을 가장 안정적으로 낮춘다.
