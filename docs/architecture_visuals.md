# RETrend Architecture Visuals

이 문서는 RETrend의 현재 구조(수집 → 저장 → 처리 → 쿼리/BI)를 빠르게 이해할 수 있도록 Mermaid 기반 시각자료를 제공합니다.

> GitHub/Markdown 뷰어에서 Mermaid 렌더링이 지원되면 다이어그램이 그대로 보입니다.

## 1) End-to-End 아키텍처

```mermaid
flowchart LR
  subgraph Orchestration[Orchestration]
    AF[Airflow DAG\nKubernetesPodOperator]
  end

  subgraph Ingestion[Ingestion (KREB)]
    JOB[Backfill Job\nPython backfill.py]
    API[KREB Open API\n(Quota limited)]
    STATE[(State JSON\nKREB_STATE_URI)]
  end

  subgraph Storage[Storage]
    BRONZE[(Bronze CSV on MinIO/S3\nKREB_OUTPUT_URI)]
  end

  subgraph Processing[Processing]
    SP[SparkApplication\n(csv -> iceberg)]
    ICE[(Iceberg Table\nwarehouse)]
    HMS[(Hive Metastore)]
  end

  subgraph Serving[Serving]
    TR[Trino]
    BI[Superset]
  end

  AF -->|runs container job| JOB
  JOB -->|HTTP requests| API
  JOB -->|page CSV writes| BRONZE
  JOB -->|cursor/progress writes| STATE

  SP -->|reads| BRONZE
  SP -->|writes| ICE
  SP -->|uses| HMS

  TR -->|queries| ICE
  BI -->|connects| TR
```

## 2) KREB Backfill 상태(State) 흐름

```mermaid
stateDiagram-v2
  [*] --> LoadState
  LoadState --> Done: done=true
  LoadState --> ChooseSlot: done=false

  ChooseSlot --> FetchPage
  FetchPage --> WritePageCSV: success
  WritePageCSV --> SaveState: per-page commit
  SaveState --> NextPage

  NextPage --> FetchPage: more pages
  NextPage --> MarkPartitionComplete: last page

  MarkPartitionComplete --> WriteSuccessMarker: _SUCCESS.json
  WriteSuccessMarker --> AdvanceCursor
  AdvanceCursor --> ChooseSlot: next (LAWD_CD, DEAL_YM)

  FetchPage --> QuotaStop: HTTP 429 / QuotaExceeded
  QuotaStop --> SaveState
  SaveState --> [*]

  FetchPage --> RetryableError: timeout/5xx/transient
  RetryableError --> FetchPage: bounded retry
  RetryableError --> Fail: retries exhausted
  Fail --> SaveState
  Done --> [*]
```

## 3) 브론즈 데이터 레이아웃(파티션/파일)

```mermaid
flowchart TB
  B[KREB_OUTPUT_URI]
  B --> L1[LAWD_CD=11110]
  L1 --> M1[DEAL_YM=202401]
  M1 --> P1[page=1.csv]
  M1 --> P2[page=2.csv]
  M1 --> S[_SUCCESS.json]
  L1 --> M2[DEAL_YM=202402]
  M2 --> P3[page=1.csv]
```

## 4) 코드/폴더 레이어 맵(현재 기준)

```mermaid
flowchart TB
  R[Repo Root]
  R --> DAGS[dags/\nAirflow DAGs]
  R --> JOBS[jobs/\nRunnable entrypoints]
  R --> INFRA[infra/\nK8s/Helm configs]
  R --> DOCKER[docker/\nDockerfiles]
  R --> PKG[src/kreb/\nPython package]
  R --> DOCS[docs/\nRunbooks]

  DAGS --> DAG1[retrend_crawler_with_quota_dag.py]
  JOBS --> J1[kreb_backfill.py]
  JOBS --> J2[spark/kreb_csv_to_iceberg.py]

  INFRA --> I1[spark/k8s/kreb_csv_to_iceberg.yaml]
  INFRA --> I2[trino/k8s/*]
  INFRA --> I3[superset/docker-compose.yaml]

  DOCKER --> DK1[crawler/Dockerfile]
  DOCKER --> DK2[kreb-backfill/Dockerfile]
  DOCKER --> DK3[spark/spark-py-s3a.Dockerfile]
```
