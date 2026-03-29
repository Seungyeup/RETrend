from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# 로컬 타임존 (한국)
local_tz = pendulum.timezone("Asia/Seoul")

DAG_ID = "kreb_daily_sync_daily"

DATASET_LAWD_CSV = Dataset("s3://retrend-raw-data/shigungu_list.csv")
DATASET_BRONZE_APT_TRADE = Dataset("s3://retrend-raw-data/bronze/kreb_etl_v2/apt_trade")
DATASET_DAILY_SYNC_STATE = Dataset("s3://retrend-raw-data/kreb_state_daily_sync.json")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=local_tz.datetime(2025, 1, 1, 0, 0),
    schedule_interval="0 2 * * *",  # 매일 새벽 2시 (KST 기준)
    catchup=False,
    max_active_runs=1,
    tags=["retrend", "kreb", "daily-sync"],
) as dag:
    kreb_daily_sync = KubernetesPodOperator(
        task_id="kreb_daily_sync_once",
        name="kreb-daily-sync-once",
        namespace="airflow",
        # daily_sync.py가 포함된 이미지 태그로 맞춰야 함
        image="dave126/kreb-backfill:0.1.2",
        cmds=["python"],
        arguments=["/app/src/kreb/src/kreb_etl_v2/daily_sync.py"],
        env_vars={
            # MinIO 설정
            "MINIO_ENDPOINT": "http://172.30.1.28:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            # KREB API
            # Airflow Variable `KREB_SERVICE_KEY`로 주입
            "KREB_SERVICE_KEY": "{{ var.value.KREB_SERVICE_KEY }}",
            # Quota
            "KREB_DAILY_LIMIT": "10000",
            # LAWD 코드 CSV
            "KREB_LAWD_CSV": "s3://retrend-raw-data/shigungu_list.csv",
            # daily sync 전용 state (backfill과 반드시 분리 권장)
            "KREB_DAILY_SYNC_STATE_URI": "s3://retrend-raw-data/kreb_state_daily_sync.json",
            # 브론즈 출력
            # daily sync는 "API 값 변동 시 overwrite"가 목적이므로 기존 prefix를 그대로 써도 됨
            "KREB_OUTPUT_URI": "s3://retrend-raw-data/bronze/kreb_etl_v2/apt_trade",
            # 로그
            "LOG_LEVEL": "INFO",
            # (옵션) HTTP 재시도 튜닝
            # "KREB_HTTP_RETRIES": "3",
            # "KREB_HTTP_BACKOFF_BASE": "1.0",
            # "KREB_HTTP_BACKOFF_CAP": "30.0",
            # "KREB_HTTP_TIMEOUT_SEC": "10.0",
        },
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
        # service_account_name="airflow",
        inlets=[DATASET_LAWD_CSV],
        outlets=[DATASET_BRONZE_APT_TRADE, DATASET_DAILY_SYNC_STATE],
    )

    kreb_daily_sync
