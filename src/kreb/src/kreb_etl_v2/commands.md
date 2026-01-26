# 로컬파일 테스트 시
export KREB_SERVICE_KEY="taVSJNwCXif5C1TK86D389Yu1NO4irUN5v7l5TOuezgTqZqAU4O0qn9dmu9t1Rs5hULYKVLvWfvnWekpuChnJA=="
export KREB_DAILY_LIMIT="2"                    # 로컬 테스트니까 10 정도로 작게
export KREB_LAWD_CSV="/tmp/lawd_test.csv"
export KREB_STATE_URI="file:///tmp/kreb_state.json"
export KREB_OUTPUT_URI="file:///tmp/kreb_output"
export LOG_LEVEL="INFO"

# MINIO 연계 테스트 시
export MINIO_ENDPOINT="http://172.30.1.28:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export KREB_SERVICE_KEY="taVSJNwCXif5C1TK86D389Yu1NO4irUN5v7l5TOuezgTqZqAU4O0qn9dmu9t1Rs5hULYKVLvWfvnWekpuChnJA=="
export KREB_DAILY_LIMIT="2"                    
export KREB_LAWD_CSV="s3://retrend-raw-data/shigungu_list.csv"
export KREB_STATE_URI="s3://retrend-raw-data/kreb_state.json" 
export KREB_OUTPUT_URI="s3://retrend-raw-data/bronze/kreb_etl_v2/apt_trade"
export LOG_LEVEL="INFO"

# 실행 커맨드
PYTHONPATH=./src python -m kreb_etl_v2.backfill

# 도커 빌드 커맨드
docker buildx build \
  --platform linux/amd64 \
  -f src/kreb/src/docker/Dockerfile \
  -t dave126/kreb-backfill:0.1.1 \
  --load .

docker push dave126/kreb-backfill:0.1.1

docker run --rm -it dave126/kreb-backfill:0.1.1 /bin/bash

python /app/src/kreb/src/kreb_etl_v2/backfill.py