import os
from pyspark.sql import SparkSession, functions as F, types as T


def build_spark():
    spark = (
        SparkSession.builder.appName("kreb-csv-to-iceberg")
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
    return spark


def main():
    bronze = os.environ.get(
        "BRONZE_PREFIX",
        "s3a://retrend-raw-data/bronze/kreb/apt_trade",
    ).rstrip("/")
    silver = os.environ.get(
        "SILVER_PREFIX",
        "s3a://retrend-raw-data/silver/kreb/apt_trade",
    ).rstrip("/")
    iceberg_table = os.environ.get(
        "ICEBERG_TABLE", "iceberg.default.apt_trade"
    )

    spark = build_spark()

    df = (
        spark.read.option("header", True)
        .csv(f"{bronze}/lawdCd=*/year=*/month=*/part-*.csv")
        .withColumn("_file", F.input_file_name())
    )

    # path → partition 컬럼 파생
    df = (
        df.withColumn(
            "lawdCd", F.regexp_extract("_file", r"lawdCd=([0-9]{5})", 1)
        )
        .withColumn("year", F.regexp_extract("_file", r"year=([0-9]{4})", 1).cast("int"))
        .withColumn("month", F.regexp_extract("_file", r"month=([0-9]{2})", 1).cast("int"))
    )

    # ===== 여기부터 정규화 부분 수정 =====
    def to_int(col):
        return F.regexp_replace(F.col(col), ",", "").cast("int")

    # 1) 거래금액: [거래금액(만원), dealAmount]
    deal_amount = F.coalesce(
        to_int("거래금액(만원)"),
        to_int("dealAmount"),
    ).alias("deal_amount_man")

    # 2) 건축년도: [건축년도, buildYear]
    build_year = F.coalesce(
        F.col("건축년도").cast("int"),
        F.col("buildYear").cast("int"),
    ).alias("build_year")

    # 3) 거래일: [년/월/일, dealYear/dealMonth/dealDay]
    deal_year = F.coalesce(
        F.col("년").cast("int"),
        F.col("dealYear").cast("int"),
    ).alias("deal_year")

    deal_month = F.coalesce(
        F.col("월").cast("int"),
        F.col("dealMonth").cast("int"),
    ).alias("deal_month")

    deal_day = F.coalesce(
        F.col("일").cast("int"),
        F.col("dealDay").cast("int"),
    ).alias("deal_day")

    # 4) 전용면적: [전용면적, excluUseAr]
    exclusive_area = F.coalesce(
        F.col("전용면적").cast("double"),
        F.col("excluUseAr").cast("double"),
    ).alias("exclusive_area")

    # 5) 층: [층, floor]
    floor = F.coalesce(
        F.col("층").cast("int"),
        F.col("floor").cast("int"),
    ).alias("floor")

    # 6) 아파트명: [aptNm, aptDong]
    apt_name = F.coalesce(
        F.col("aptNm"),
        F.col("aptDong"),
    ).alias("apt_name")

    # 7) 법정동(동 이름): [umdNm] (필요시 후보를 더 추가)
    dong = F.coalesce(
        F.col("umdNm"),
    ).alias("legal_dong")

    out = df.select(
        "lawdCd",
        "year",
        "month",
        deal_amount,
        build_year,
        deal_year,
        deal_month,
        deal_day,
        exclusive_area,
        floor,
        apt_name,
        dong,
    )

    # 이하 parquet + Iceberg 쓰기는 그대로
    (
        out.write.mode("overwrite")
        .partitionBy("lawdCd", "year", "month")
        .parquet(silver)
    )

    warehouse_base = "s3a://retrend-raw-data/warehouse/iceberg"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {iceberg_table} (
        lawdCd string,
        year int,
        month int,
        deal_amount_man int,
        build_year int,
        deal_year int,
        deal_month int,
        deal_day int,
        exclusive_area double,
        floor int,
        apt_name string,
        legal_dong string
        )
        USING ICEBERG
        LOCATION '{warehouse_base}/default/apt_trade'
        PARTITIONED BY (year, month, lawdCd)
        """
    )

    df_silver = spark.read.parquet(silver)
    (
        df_silver.select(
            "lawdCd",
            "year",
            "month",
            "deal_amount_man",
            "build_year",
            "deal_year",
            "deal_month",
            "deal_day",
            "exclusive_area",
            "floor",
            "apt_name",
            "legal_dong",
        )
        .writeTo(iceberg_table)
        .append()
    )

    spark.stop()


if __name__ == "__main__":
    main()
