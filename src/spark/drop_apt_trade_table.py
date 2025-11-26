import os
from pyspark.sql import SparkSession


def build_spark():
    return (
        SparkSession.builder.appName("drop-iceberg-apt-trade")
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
        # 필요하면 warehouse / s3a 설정도 추가 가능하지만 DROP만 할 거라 필수는 아님
        .getOrCreate()
    )


def main():
    spark = build_spark()

    # 여기서 Iceberg 테이블 드랍
    spark.sql("DROP TABLE IF EXISTS iceberg.default.apt_trade")

    spark.stop()


if __name__ == "__main__":
    main()
