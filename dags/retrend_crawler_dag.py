from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='retrend_real_estate_crawler',
    default_args=default_args,
            schedule='0 0 * * *', # Run daily at midnight (changed from schedule_interval)    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['retrend', 'crawler', 'minio'],
) as dag:
    start_task = KubernetesPodOperator(
        task_id='start_crawler_pipeline',
        name='start-crawler-pipeline',
        namespace='airflow', # Assuming Airflow is in 'airflow' namespace
        image='dave126/retrend-crawler:latest',
        cmds=["bash", "-cx"],
        arguments=["echo 'Starting real estate crawling pipeline'"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_shido = KubernetesPodOperator(
        task_id='extract_shido_data',
        name='extract-shido-data',
        namespace='airflow',
        image='dave126/retrend-crawler:latest',
        cmds=["python"],
        arguments=["/app/src/extract_shido_to_excel.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_shigungu = KubernetesPodOperator(
        task_id='extract_shigungu_data',
        name='extract-shigungu-data',
        namespace='airflow',
        image='dave126/retrend-crawler:latest',
        cmds=["python"],
        arguments=["/app/src/extract_shigungu_to_csv.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_eupmeandong = KubernetesPodOperator(
        task_id='extract_eupmeandong_data',
        name='extract-eupmeandong-data',
        namespace='airflow',
        image='dave126/retrend-crawler:latest',
        cmds=["python"],
        arguments=["/app/src/extract_eupmeandong_to_csv.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    extract_complexes = KubernetesPodOperator(
        task_id='extract_complexes_data',
        name='extract-complexes-data',
        namespace='airflow',
        image='dave126/retrend-crawler:latest',
        cmds=["python"],
        arguments=["/app/src/extract_complexes_to_csv.py"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    end_task = KubernetesPodOperator(
        task_id='end_crawler_pipeline',
        name='end-crawler-pipeline',
        namespace='airflow',
        image='dave126/retrend-crawler:latest',
        cmds=["bash", "-cx"],
        arguments=["echo 'Real estate crawling pipeline finished'"],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    start_task >> extract_shido >> extract_shigungu >> extract_eupmeandong >> extract_complexes >> end_task