# Airflow 공식 이미지 기반
FROM apache/airflow:2.8.4-python3.9

# ❗ pip은 root가 아니라 airflow 유저로 실행해야 함
USER airflow

# Airflow 코어에서 필요한 패키지만 설치
RUN pip install --no-cache-dir \
    "apache-airflow-providers-cncf-kubernetes==10.5.0" \
    psycopg2-binary \
    asyncpg

# 여기서부터는 공식 이미지 기본 설정(AIRFLOW_HOME, entrypoint 등) 그대로 사용