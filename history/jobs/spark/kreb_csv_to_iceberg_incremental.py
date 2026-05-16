import json
import os

from pyspark.sql import SparkSession, functions as F


# Keep schema consistent with the full-load job
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
    # Expect a single JSON object.
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

    # Incremental reflect: overwrite only partitions present in this DataFrame.
    out.writeTo(iceberg_table).overwritePartitions()

    spark.stop()


if __name__ == "__main__":
    main()
