from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# 로컬 타임존 (한국)
local_tz = pendulum.timezone("Asia/Seoul")

DAG_ID = "kreb_backfill_daily"

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
    tags=["retrend", "kreb", "backfill"],
) as dag:

    kreb_backfill = KubernetesPodOperator(
        task_id="kreb_backfill_once",
        name="kreb-backfill-once",
        namespace="airflow",
        image="dave126/kreb-backfill:0.1.1",
        # 컨테이너 안에서 실행할 명령
        cmds=["python"],
        arguments=["/app/src/kreb/src/kreb_etl_v2/backfill.py"],
        # Airflow 쪽에서 override 하고 싶은 ENV들
        env_vars={
            # MinIO 설정
            "MINIO_ENDPOINT": "http://172.30.1.28:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",

            # KREB API / 백필 설정
            # 서비스키는 나중엔 Airflow Secret/Variable로 빼는 게 안전
            "KREB_SERVICE_KEY": "taVSJNwCXif5C1TK86D389Yu1NO4irUN5v7l5TOuezgTqZqAU4O0qn9dmu9t1Rs5hULYKVLvWfvnWekpuChnJA==",
            "KREB_DAILY_LIMIT": "10000",  # 테스트용으로 살짝 낮게, 나중에 10000 등으로 올리면 됨

            # LAWD 코드 CSV / state / output 경로
            "KREB_LAWD_CSV": "s3://retrend-raw-data/shigungu_list.csv",
            "KREB_STATE_URI": "s3://retrend-raw-data/kreb_state.json",
            "KREB_OUTPUT_URI": "s3://retrend-raw-data/bronze/kreb_etl_v2/apt_trade",

            # 로그 레벨
            "LOG_LEVEL": "INFO",
        },
        get_logs=True,
        in_cluster=True,             # Airflow가 K8s 안에서 돌고 있을 때
        is_delete_operator_pod=True, # 작업 끝난 뒤 Pod 자동 삭제
        # service_account_name="airflow",  # 필요하면 네 SA 이름으로 지정
    )

    kreb_backfill
