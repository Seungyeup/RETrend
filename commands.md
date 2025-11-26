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
  -f src/docker/crawler/Dockerfile \
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


# metallb
kubectl create namespace metallb-system

helm repo add metallb https://metallb.github.io/metallb
helm repo update

helm install metallb metallb/metallb \
  -n metallb-system

kubectl apply -f metallb-config.yaml


# nginx Ingress
kubectl apply -f ./helm/nginx/airflow-ingress-manual.yaml

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
  --from-file=kreb_csv_to_iceberg.py=src/spark/kreb_csv_to_iceberg.py \
  --dry-run=client -o yaml | kubectl apply -f -

# 5) Submit SparkApplication (adjust MinIO endpoint/creds and Hive URI inside YAML if needed)
kubectl -n kubeflow delete sparkapplication kreb-csv-to-iceberg || true

# S3A 지원 붙인 커스텀 이미지 빌드
docker buildx build --platform linux/amd64 \
-t dave126/spark-py-s3a:3.3.1 \
-f src/spark/k8s/spark-py-s3a.Dockerfile \
. \
--push

kubectl apply -f src/spark/k8s/kreb_csv_to_iceberg.yaml
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
