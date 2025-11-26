import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# KST 타임존
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retrend_real_estate_crawler",
    default_args=default_args,
    start_date=local_tz.datetime(2023, 1, 1, 0, 0),  # 반드시 과거 시점
    schedule_interval="0 0 * * *",                   # Airflow 2에서는 schedule_interval 사용
    catchup=False,
    params={
        # 수집 윈도우: '1m' (최근 1개월) 또는 '10y' (최근 10년)
        "window": "1m",
    },
    tags=["retrend", "crawler", "minio"],
) as dag:

    start_task = KubernetesPodOperator(
        task_id="start_crawler_pipeline",
        name="start-crawler-pipeline",
        namespace="airflow",  # Airflow 설치 네임스페이스
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["bash", "-cx"],
        arguments=["echo 'Starting real estate crawling pipeline'"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_shido = KubernetesPodOperator(
        task_id="extract_shido_data",
        name="extract-shido-data",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/extract_shido_to_excel.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_shigungu = KubernetesPodOperator(
        task_id="extract_shigungu_data",
        name="extract-shigungu-data",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/extract_shigungu_to_csv.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_eupmeandong = KubernetesPodOperator(
        task_id="extract_eupmeandong_data",
        name="extract-eupmeandong-data",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/extract_eupmeandong_to_csv.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_complexes = KubernetesPodOperator(
        task_id="extract_complexes_data",
        name="extract-complexes-data",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/extract_complexes_to_csv.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    # extract_pyeonginfo = KubernetesPodOperator(
    #     task_id="extract_pyeonginfo_data",
    #     name="extract-pyeonginfo-data",
    #     namespace="airflow",
    #     image="dave126/retrend-crawler:2.8.5",
    #     image_pull_policy="Always",
    #     cmds=["python"],
    #     arguments=["/app/src/extract_pyeonginfo_to_csv_s3.py"],
    #     env_vars={
    #         "DRY_RUN_SAMPLE": "{{ '1' if params.smoke else '0' }}",
    #         "NO_WRITE": "{{ '1' if params.smoke else '0' }}",
    #         "PYEONGINFO_LIMIT_COMPLEXES": "{{ params.pyeonginfo_limit_complexes }}",
    #         "PYEONGINFO_SLEEP": "{{ params.pyeonginfo_sleep }}"
    #     },
    #     do_xcom_push=False,
    #     is_delete_operator_pod=True,
    #     get_logs=True,
    # )

    # extract_trade_history = KubernetesPodOperator(
    #     task_id="extract_trade_history_data",
    #     name="extract-trade-history-data",
    #     namespace="airflow",
    #     image="dave126/retrend-crawler:2.8.5",
    #     image_pull_policy="Always",
    #     cmds=["python"],
    #     arguments=["/app/src/extract_trade_history_s3.py"],
    #     env_vars={
    #         "DRY_RUN_SAMPLE": "{{ '1' if params.smoke else '0' }}",
    #         "NO_WRITE": "{{ '1' if params.smoke else '0' }}",
    #         "TRADE_LIMIT_COMPLEXES": "{{ params.trade_limit_complexes }}",
    #         "TRADE_LIMIT_AREAS": "{{ params.trade_limit_areas }}",
    #         "TRADE_LIMIT_PAGES": "{{ params.trade_limit_pages }}",
    #         "TRADE_SLEEP": "{{ params.trade_sleep }}"
    #     },
    #     do_xcom_push=False,
    #     is_delete_operator_pod=True,
    #     get_logs=True,
    # )

    # 한국 부동산원(국토부 실거래가) – 수집 태스크 (두 옵션: 최근 1개월 / 최근 10년)

    kreb_apt_trade = KubernetesPodOperator(
        task_id="kreb_apt_trade_bronze",
        name="kreb-apt-trade-bronze",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["python"],
        arguments=["/app/src/kreb/extract_apt_trade_to_csv_s3.py"],
        env_vars={
            "KREB_SERVICE_KEY": "{{ var.value.KREB_SERVICE_KEY }}",
            # 시군구 목록에서 앞 5자리로 LAWD 코드 유도
            "KREB_SHIGUNGU_S3": "s3://retrend-raw-data/shigungu_list.csv",
            # 기간: window 파라미터로 결정
            "START_YM": "{{ (macros.datetime.utcnow() - macros.timedelta(days=365*10)).strftime('%Y%m') if params.window == '10y' else execution_date.strftime('%Y%m') }}",
            "END_YM": "{{ macros.datetime.utcnow().strftime('%Y%m') if params.window == '10y' else execution_date.strftime('%Y%m') }}",
        },
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    end_task = KubernetesPodOperator(
        task_id="end_crawler_pipeline",
        name="end-crawler-pipeline",
        namespace="airflow",
        image="dave126/retrend-crawler:2.8.5",
        image_pull_policy="Always",
        cmds=["bash", "-cx"],
        arguments=["echo 'Real estate crawling pipeline finished'"],
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )


    # KREB 수집만 간단 체인으로 실행
    start_task >> kreb_apt_trade >> end_task

    # start_task \
    #     >> extract_shido \
    #     >> extract_shigungu \
    #     >> extract_eupmeandong \
    #     >> extract_complexes \
    #     # >> extract_pyeonginfo \
    #     # >> extract_trade_history \
    #     >> kreb_apt_trade_bronze \
    #     >> end_task
