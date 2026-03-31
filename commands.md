# 새로운 buildx 빌더 생성 + 사용
docker buildx create --name retrend-builder --use

# 빌더 초기화
docker buildx inspect --bootstrap


# airflow core
cd ~/dev/RETrend 

docker buildx build \
  --platform linux/amd64 \
  -t dave126/airflow-core:2.8.4 \
  -f helm/airflow/core/Dockerfile \
  . \
  --push


# Worker / Crawler 이미지 (linux/amd64) 빌드 + 푸시
docker buildx build \
  --platform linux/amd64 \
  -t dave126/retrend-crawler:2.8.5 \
  -f docker/crawler/Dockerfile \
  . \
  --push


# helm
# (처음만) Airflow Helm repo 추가
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# 설치 또는 업데이트
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f helm/airflow/airflow-onprem.yaml

Airflow DAG source (gitSync)
https://github.com/Seungyeup/RETrend.git (branch: main, subPath: dags)

Create/refresh aws_default connection for remote task logs
kubectl -n airflow exec deploy/airflow-scheduler -- airflow connections delete aws_default || true
kubectl -n airflow exec deploy/airflow-scheduler -- airflow connections add aws_default \
  --conn-json '{"conn_type":"aws","extra":{"aws_access_key_id":"<MINIO_ACCESS_KEY>","aws_secret_access_key":"<MINIO_SECRET_KEY>","endpoint_url":"http://172.30.1.28:9000","region_name":"us-east-1"}}'


# metallb
kubectl create namespace metallb-system

helm repo add metallb https://metallb.github.io/metallb
helm repo update

helm install metallb metallb/metallb \
  -n metallb-system

kubectl apply -f metallb-config.yaml


# nginx Ingress
kubectl apply -f ./helm/nginx/airflow-ingress-manual.yaml

metrics-server
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
helm repo update
helm upgrade --install metrics-server metrics-server/metrics-server \
  -n default \
  -f helm/metrics-server/values-onprem.yaml

# --- Smoke test (pre-DAG quick check) ---
# Use environment limits to fetch a tiny sample on S3

# 1) Pyeonginfo: limit to 1 complex
export PYEONGINFO_LIMIT_COMPLEXES=1
python src/extract_pyeonginfo_to_csv_s3.py

# 2) Trade history: limit to 1 complex, 1 area, 1 page
export TRADE_LIMIT_COMPLEXES=1
export TRADE_LIMIT_AREAS=1
export TRADE_LIMIT_PAGES=1
python src/extract_trade_history_s3.py

# Unset limits before full DAG run
unset PYEONGINFO_LIMIT_COMPLEXES TRADE_LIMIT_COMPLEXES TRADE_LIMIT_AREAS TRADE_LIMIT_PAGES



# --- Spark Operator (Kubeflow, Option A: Helm) ---
# 1) Install Spark Operator into 'kubeflow' and watch the same namespace
helm repo add --force-update spark-operator https://kubeflow.github.io/spark-operator
helm repo update

# *실제 watch 대상 네임스페이스는 spark.jobNamespaces로 설정해야 함.
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace kubeflow \
  --create-namespace \
  --set webhook.enable=true \
  --set "spark.jobNamespaces[0]=kubeflow"

# 2) Verify CRDs
kubectl get crd | grep sparkapplications

# 3) Create ServiceAccount and grant permissions in kubeflow namespace
kubectl -n kubeflow create serviceaccount spark || true
kubectl -n kubeflow create rolebinding spark-edit \
  --clusterrole=edit \
  --serviceaccount=kubeflow:spark || true

# Verify operator args include watched namespace
kubectl -n kubeflow get deploy spark-operator-controller -o jsonpath='{.spec.template.spec.containers[0].args}'
# Expect to see: --namespaces=kubeflow

# If it still shows --namespaces=default, force a clean install:
# helm -n kubeflow uninstall spark-operator
# helm upgrade --install spark-operator spark-operator/spark-operator \
#   --namespace kubeflow --create-namespace \
#   --set controller.enableWebhook=true \
#   --set controller.namespaces[0]=kubeflow
kubectl -n kubeflow logs deploy/spark-operator-controller

# --- Spark (bronze CSV -> Parquet -> Iceberg) ---
# 4) Create ConfigMap with PySpark app (Kubeflow namespace)
kubectl -n kubeflow create configmap kreb-csv-to-iceberg-app \
  --from-file=kreb_csv_to_iceberg.py=jobs/spark/kreb_csv_to_iceberg.py \
  --dry-run=client -o yaml | kubectl apply -f -

# 5) Submit SparkApplication (adjust MinIO endpoint/creds and Hive URI inside YAML if needed)
kubectl -n kubeflow delete sparkapplication kreb-csv-to-iceberg || true

# S3A 지원 붙인 커스텀 이미지 빌드
docker buildx build --platform linux/amd64 \
-t dave126/spark-py-s3a:3.3.1 \
-f docker/spark/spark-py-s3a.Dockerfile \
 . \
 --push

kubectl apply -f infra/spark/k8s/kreb_csv_to_iceberg.yaml
# kubectl -n kubeflow delete sparkapplication kreb-csv-to-iceberg

# 6) Inspect SparkApplication
kubectl -n kubeflow get sparkapplications
kubectl -n kubeflow describe sparkapplication kreb-csv-to-iceberg

# Inspect driver pod logs
kubectl -n kubeflow get pods -l spark-role=driver
kubectl -n kubeflow logs -f $(kubectl -n kubeflow get pods -l spark-role=driver -o name)

# Notes (air-gapped)
# - If egress is blocked, deps.packages JAR download will fail.
#   Build a custom Spark image with Iceberg/Hadoop-AWS jars, or mount jars and set 'spark.jars'.

# kreb-csv-to-iceberg 변경 시 configmap 재생성
kubectl -n kubeflow delete configmap kreb-csv-to-iceberg-app
kubectl -n kubeflow create configmap kreb-csv-to-iceberg-app \
  --from-file=kreb_csv_to_iceberg.py=jobs/spark/kreb_csv_to_iceberg.py




# 테스트용 LAWDCODE 1개짜리 CSV
cat >/tmp/lawd_test.csv <<'EOF'
LAWD_CD
11110
EOF

# S3/MinIO 환경변수로 인한 혼선 방지(로컬 테스트면 일단 해제 권장)
unset MINIO_ENDPOINT MINIO_ACCESS_KEY MINIO_SECRET_KEY

# 반드시 테스트용 state/output로 지정
export KREB_SERVICE_KEY='taVSJNwCXif5C1TK86D389Yu1NO4irUN5v7l5TOuezgTqZqAU4O0qn9dmu9t1Rs5hULYKVLvWfvnWekpuChnJA=='
export KREB_DAILY_LIMIT=5
export KREB_LAWD_CSV=/tmp/lawd_test.csv
export KREB_STATE_URI=file:///tmp/kreb_state_test.json
export KREB_OUTPUT_URI=file:///tmp/kreb_output_test
export LOG_LEVEL=INFO

python jobs/kreb_backfill.py

# 결과 확인
ls -la /tmp/kreb_output_test
cat /tmp/kreb_state_test.json

# MinIO(S3)에 “테스트 prefix”로 적재 테스트를 하려면(운영 state/output 건드리지 않게):
export MINIO_ENDPOINT='http://172.30.1.28:9000'
export MINIO_ACCESS_KEY='minioadmin'
export MINIO_SECRET_KEY='minioadmin'
export KREB_SERVICE_KEY='taVSJNwCXif5C1TK86D389Yu1NO4irUN5v7l5TOuezgTqZqAU4O0qn9dmu9t1Rs5hULYKVLvWfvnWekpuChnJA=='
export KREB_DAILY_LIMIT=20
export KREB_LAWD_CSV='s3://retrend-raw-data/shigungu_list.csv'
export KREB_STATE_URI='s3://retrend-raw-data/kreb_state_test.json'
export KREB_OUTPUT_URI='s3://retrend-raw-data/bronze/kreb_etl_v2/apt_trade_test'
export LOG_LEVEL=INFO
python jobs/kreb_backfill.py

# 추가로, “기존 state가 done=true여도 무시하고 강제로 돌리기” 옵션도 있어:
# - export KREB_IGNORE_DONE=1

# docker build 
# - 빌드+푸시(운영/K8s용, repo 루트에서 실행):
docker buildx build --platform linux/amd64 \
  -t dave126/kreb-backfill:0.1.2 \
  -f docker/kreb-backfill/Dockerfile \
  . \
  --push

# - 로컬에서만 테스트(푸시 없이):
docker buildx build --platform linux/amd64 \
  -t dave126/kreb-backfill:0.1.2 \
  -f docker/kreb-backfill/Dockerfile \
  . \
  --load

  # dag push
