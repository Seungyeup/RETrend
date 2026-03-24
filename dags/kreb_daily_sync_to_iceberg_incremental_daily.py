from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

local_tz = pendulum.timezone("Asia/Seoul")

DAG_ID = "kreb_daily_sync_to_iceberg_incremental_daily"

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
    schedule_interval="30 2 * * *",  # daily_sync 이후(02:00) 조금 뒤
    catchup=False,
    max_active_runs=1,
    tags=["retrend", "kreb", "daily-sync", "iceberg"],
) as dag:
    daily_sync = KubernetesPodOperator(
        task_id="kreb_daily_sync_once",
        name="kreb-daily-sync-once",
        namespace="airflow",
        image="dave126/kreb-backfill:0.1.2",
        cmds=["python"],
        arguments=["/app/src/kreb/src/kreb_etl_v2/daily_sync.py"],
        env_vars={
            "MINIO_ENDPOINT": "http://172.30.1.28:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            # Airflow Variable `KREB_SERVICE_KEY`로 주입
            "KREB_SERVICE_KEY": "{{ var.value.KREB_SERVICE_KEY }}",
            "KREB_DAILY_LIMIT": "10000",
            "KREB_LAWD_CSV": "s3://retrend-raw-data/shigungu_list.csv",
            "KREB_DAILY_SYNC_STATE_URI": "s3://retrend-raw-data/kreb_state_daily_sync.json",
            "KREB_OUTPUT_URI": "s3://retrend-raw-data/bronze/kreb_etl_v2/apt_trade",
            "LOG_LEVEL": "INFO",
        },
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
    )

    # NOTE: This task requires RBAC that allows creating SparkApplication in kubeflow namespace.
    submit_spark_incremental = KubernetesPodOperator(
        task_id="spark_kreb_csv_to_iceberg_incremental",
        name="spark-kreb-csv-to-iceberg-incremental",
        namespace="airflow",
        service_account_name="airflow-worker",
        image="dtzar/helm-kubectl@sha256:709f76a4b44bdbf7433f0e357e25d170a60f1230958c8da988bff1c045a06fba",
        startup_timeout_seconds=180,
        log_events_on_failure=True,
        cmds=["/bin/sh", "-c"],
        arguments=[
            '''
set -euo pipefail

# Provide PySpark app via ConfigMap to avoid failing on missing partitions.
cat <<'YAML' | kubectl -n kubeflow apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: kreb-csv-to-iceberg-incremental-app
  namespace: kubeflow
data:
  kreb_csv_to_iceberg_incremental.py: |
    import json
    import os

    from pyspark.sql import SparkSession, functions as F


    CSV_COLS = [
        "aptDong",
        "aptNm",
        "aptSeq",
        "bonbun",
        "bubun",
        "buildYear",
        "buyerGbn",
        "cdealDay",
        "cdealType",
        "dealAmount",
        "dealDay",
        "dealMonth",
        "dealYear",
        "dealingGbn",
        "estateAgentSggNm",
        "excluUseAr",
        "floor",
        "jibun",
        "landCd",
        "landLeaseholdGbn",
        "rgstDate",
        "roadNm",
        "roadNmBonbun",
        "roadNmBubun",
        "roadNmCd",
        "roadNmSeq",
        "roadNmSggCd",
        "roadNmbCd",
        "sggCd",
        "slerGbn",
        "umdCd",
        "umdNm",
    ]

    DERIVED_COLS = ["lawdCd", "deal_ym", "year", "month", "_file"]
    ALL_COLS = CSV_COLS + DERIVED_COLS


    def build_spark():
        return (
            SparkSession.builder.appName("kreb-csv-to-iceberg-incremental")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config(
                "spark.sql.catalog.iceberg.uri",
                os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"),
            )
            .getOrCreate()
        )


    def ensure_columns(df):
        for c in CSV_COLS:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))

        if "lawdCd" not in df.columns:
            df = df.withColumn("lawdCd", F.lit(None).cast("string"))
        if "deal_ym" not in df.columns:
            df = df.withColumn("deal_ym", F.lit(None).cast("string"))
        if "year" not in df.columns:
            df = df.withColumn("year", F.lit(None).cast("int"))
        if "month" not in df.columns:
            df = df.withColumn("month", F.lit(None).cast("int"))
        if "_file" not in df.columns:
            df = df.withColumn("_file", F.lit(None).cast("string"))

        return df


    def create_or_recreate_table(spark, table, warehouse_base):
        recreate = os.environ.get("RECREATE_TABLE", "false").lower() == "true"
        if recreate:
            spark.sql(f"DROP TABLE IF EXISTS {table}")

        col_defs = [f"{c} string" for c in CSV_COLS]
        col_defs.append("lawdCd string")
        col_defs.append("deal_ym string")
        col_defs.append("year int")
        col_defs.append("month int")
        col_defs.append("_file string")

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
              {", ".join(col_defs)}
            )
            USING ICEBERG
            LOCATION '{warehouse_base}/default/apt_trade_raw'
            PARTITIONED BY (year, month, lawdCd)
            """
        )


    def read_manifest_as_json(spark, manifest_path: str):
        rows = spark.read.text(manifest_path).collect()
        if not rows:
            return None
        return json.loads(rows[0][0])


    def glob_has_matches(spark, path_glob: str) -> bool:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        Path = jvm.org.apache.hadoop.fs.Path

        p = Path(path_glob)
        fs = p.getFileSystem(conf)
        statuses = fs.globStatus(p)
        return statuses is not None and len(statuses) > 0


    def main():
        bronze = os.environ.get(
            "BRONZE_PREFIX",
            "s3a://retrend-raw-data/bronze/kreb_etl_v2/apt_trade",
        ).rstrip("/")
        iceberg_table = os.environ.get("ICEBERG_TABLE", "iceberg.default.apt_trade_raw")
        warehouse_base = os.environ.get(
            "WAREHOUSE_BASE", "s3a://retrend-raw-data/warehouse/iceberg"
        ).rstrip("/")

        manifest_path = os.environ.get(
            "KREB_DAILY_SYNC_MANIFEST_PATH", f"{bronze}/_manifests/daily_sync/latest.json"
        )

        spark = build_spark()

        manifest = read_manifest_as_json(spark, manifest_path)
        if not manifest:
            print(f"No manifest found: {manifest_path}")
            spark.stop()
            return

        completed = manifest.get("completed_partitions") or []
        if not completed:
            print("No completed partitions in manifest. Nothing to do.")
            spark.stop()
            return

        paths = [
            f"{bronze}/LAWD_CD={p['lawd_cd']}/DEAL_YM={p['deal_ym']}/page=*.csv"
            for p in completed
            if isinstance(p, dict) and p.get("lawd_cd") and p.get("deal_ym")
        ]
        if not paths:
            print("Manifest had no usable partition entries. Nothing to do.")
            spark.stop()
            return

        existing_paths = []
        for p in paths:
            if glob_has_matches(spark, p):
                existing_paths.append(p)
            else:
                print(f"Skip missing path: {p}")

        if not existing_paths:
            print("No existing input paths. Nothing to do.")
            spark.stop()
            return

        df = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(existing_paths)
            .withColumn("_file", F.input_file_name())
        )

        df = (
            df.withColumn("lawdCd", F.regexp_extract("_file", r"LAWD_CD=([0-9]{5})", 1))
            .withColumn("deal_ym", F.regexp_extract("_file", r"DEAL_YM=([0-9]{6})", 1))
            .withColumn("year", F.substring("deal_ym", 1, 4).cast("int"))
            .withColumn("month", F.substring("deal_ym", 5, 2).cast("int"))
        )

        df = ensure_columns(df)
        create_or_recreate_table(spark, iceberg_table, warehouse_base)

        out = df.select(*ALL_COLS)
        out.writeTo(iceberg_table).overwritePartitions()

        spark.stop()


    if __name__ == "__main__":
        main()
YAML

# Recreate SparkApplication each day.
kubectl -n kubeflow delete sparkapplication kreb-csv-to-iceberg-incremental --ignore-not-found

cat <<'YAML' | kubectl apply -f -
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: kreb-csv-to-iceberg-incremental
  namespace: kubeflow
spec:
  type: Python
  mode: cluster
  image: dave126/spark-py-s3a-iceberg:3.3.2
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///opt/spark/app/kreb_csv_to_iceberg_incremental.py
  sparkVersion: 3.3.1
  restartPolicy:
    type: Never
  deps:
    packages:
      - org.apache.spark:spark-hadoop-cloud_2.12:3.3.1
      - org.apache.hadoop:hadoop-aws:3.3.4
      - com.amazonaws:aws-java-sdk-bundle:1.12.262
  sparkConf:
    spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    spark.sql.catalog.iceberg: org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.iceberg.type: hive
    spark.sql.catalog.iceberg.uri: thrift://172.30.1.30:9083
    spark.sql.catalog.iceberg.warehouse: s3a://retrend-raw-data/warehouse/iceberg
    spark.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
    spark.hadoop.fs.s3a.endpoint: http://172.30.1.28:9000
    spark.hadoop.fs.s3a.access.key: minioadmin
    spark.hadoop.fs.s3a.secret.key: minioadmin
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
    spark.jars.ivy: /tmp/.ivy
  driver:
    # NOTE: Without explicit coreRequest/coreLimit, SparkOperator defaults can
    # request full cores (cpu=1), which may leave executors stuck Pending under
    # cluster CPU pressure.
    coreRequest: "500m"
    coreLimit: "1000m"
    cores: 1
    memory: 2g
    serviceAccount: spark
    env:
      - name: HIVE_METASTORE_URI
        value: thrift://172.30.1.30:9083
      - name: BRONZE_PREFIX
        value: s3a://retrend-raw-data/bronze/kreb_etl_v2/apt_trade
      - name: ICEBERG_TABLE
        value: iceberg.default.apt_trade
      - name: WAREHOUSE_BASE
        value: s3a://retrend-raw-data/warehouse/iceberg
      - name: KREB_DAILY_SYNC_MANIFEST_PATH
        value: s3a://retrend-raw-data/bronze/kreb_etl_v2/apt_trade/_manifests/daily_sync/latest.json
      - name: AWS_ACCESS_KEY_ID
        value: minioadmin
      - name: AWS_SECRET_ACCESS_KEY
        value: minioadmin
      - name: RECREATE_TABLE
        value: "false"
    volumeMounts:
      - name: app
        mountPath: /opt/spark/app
  executor:
    # This job is I/O heavy (S3A + Iceberg). Keep CPU requests small to avoid
    # long Pending when the cluster is busy.
    instances: 1
    coreRequest: "500m"
    coreLimit: "1000m"
    cores: 1
    memory: 2g
    env:
      - name: AWS_ACCESS_KEY_ID
        value: minioadmin
      - name: AWS_SECRET_ACCESS_KEY
        value: minioadmin
  volumes:
    - name: app
      configMap:
        name: kreb-csv-to-iceberg-incremental-app
YAML

APP_NAME="kreb-csv-to-iceberg-incremental"

dump_debug() {
  echo "--- SparkApplication describe ---" || true
  kubectl -n kubeflow describe sparkapplication "$APP_NAME" || true
  echo "--- Spark pods ---" || true
  kubectl -n kubeflow get pods -l "sparkoperator.k8s.io/app-name=$APP_NAME" -o wide || true
  echo "--- Recent kubeflow events ---" || true
  kubectl -n kubeflow get events --sort-by=.lastTimestamp | tail -n 50 || true
}

# Wait for completion (COMPLETED/FAILED)
for i in $(seq 1 360); do
  state=$(kubectl -n kubeflow get sparkapplication "$APP_NAME" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
  if [ "$state" = "COMPLETED" ]; then
    echo "SparkApplication completed"
    exit 0
  fi
  if [ "$state" = "FAILED" ]; then
    echo "SparkApplication failed"
    dump_debug
    exit 1
  fi
  sleep 10
done

echo "Timed out waiting for SparkApplication"
dump_debug
exit 1
'''
        ],
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
    )

    daily_sync >> submit_spark_incremental
