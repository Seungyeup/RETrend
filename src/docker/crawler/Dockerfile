FROM python:3.9-slim-buster

WORKDIR /app

RUN pip install --no-cache-dir \
    pandas \
    requests \
    s3fs \
    boto3 \
    xmltodict \
    psycopg2-binary \
    asyncpg

# (KubernetesPodOperator 등에서 Airflow 호환성이 필요하면
#  여기에도 apache-airflow, provider를 넣을 수 있지만
#  보통은 Task에서 직접 airflow를 import하지 않으면 굳이 필요 없음)

# Copy application code. COPY paths are relative to build context, not Dockerfile path.
# Build with context at repo root: `-f src/docker/crawler/Dockerfile .`
COPY src /app/src
COPY sample /app/sample

ENV MINIO_ENDPOINT="http://172.30.1.28:9000"
ENV MINIO_ACCESS_KEY="minioadmin"
ENV MINIO_SECRET_KEY="minioadmin"
ENV NAVER_LAND_AUTH="Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE3NTA1MDczOTMsImV4cCI6MTc1MDUxODE5M30.KIcXGSOQ0iXjffmvxg5STweJdllk3awsmOi54mB_itw"

ENV KREB_SERVICE_KEY="taVSJNwCXif5C1TK86D389Yu1NO4irUN5v7l5TOuezgTqZqAU4O0qn9dmu9t1Rs5hULYKVLvWfvnWekpuChnJA"
# 필요하면 전용 유저 만들고 USER 설정해도 되고, 그냥 root로 놔둬도 됨 (Task용이라)