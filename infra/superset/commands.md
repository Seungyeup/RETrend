# DB migration
docker exec -it superset superset db upgrade

# Admin 계정 생성
docker exec -it superset superset fab create-admin \
  --username admin \
  --firstname Superset \
  --lastname Admin \
  --email admin@example.com \
  --password admin

# 기본 설정 + 예제 대시보드 초기화
docker exec -it superset superset init

# superset 내 trino connection
trino://superset@trino.home.lab:80/iceberg/default
