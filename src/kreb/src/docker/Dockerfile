FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Seoul

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 파이썬 의존성 설치
RUN pip install --no-cache-dir \
      requests \
      xmltodict \
      fsspec \
      s3fs \
      pandas \
      boto3 \
      psycopg2-binary \
      asyncpg

# 코드 복사
COPY src /app/src

ENV MINIO_ENDPOINT="http://172.30.1.28:9000"
ENV MINIO_ACCESS_KEY="minioadmin"
ENV MINIO_SECRET_KEY="minioadmin"
# ENV NAVER_LAND_AUTH="Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE3NTA1MDczOTMsImV4cCI6MTc1MDUxODE5M30.KIcXGSOQ0iXjffmvxg5STweJdllk3awsmOi54mB_itw"

ENV KREB_SERVICE_KEY="taVSJNwCXif5C1TK86D389Yu1NO4irUN5v7l5TOuezgTqZqAU4O0qn9dmu9t1Rs5hULYKVLvWfvnWekpuChnJA=="
ENV KREB_DAILY_LIMIT="10000"                    
ENV KREB_LAWD_CSV="s3://retrend-raw-data/shigungu_list.csv"
ENV KREB_STATE_URI="s3://retrend-raw-data/kreb_state.json" 
ENV KREB_OUTPUT_URI="s3://retrend-raw-data/bronze/kreb_etl_v2/apt_trade"
ENV LOG_LEVEL="INFO"

# 기본 엔트리포인트
# CMD ["python", "-m", "src/kreb/src/kreb_etl_v2"]