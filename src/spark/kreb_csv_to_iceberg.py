import os
from pyspark.sql import SparkSession, functions as F

# =========================
# CSV 실제 헤더
# =========================
CSV_COLS = [
    "aptDong", "aptNm", "aptSeq", "bonbun", "bubun", "buildYear", "buyerGbn",
    "cdealDay", "cdealType", "dealAmount", "dealDay", "dealMonth", "dealYear",
    "dealingGbn", "estateAgentSggNm", "excluUseAr", "floor", "jibun", "landCd",
    "landLeaseholdGbn", "rgstDate", "roadNm", "roadNmBonbun", "roadNmBubun",
    "roadNmCd", "roadNmSeq", "roadNmSggCd", "roadNmbCd", "sggCd", "slerGbn",
    "umdCd", "umdNm",
]

# 영문 -> 한글 설명(댓글)
KREB_COL_DESC_KO = {
    "sggCd": "법정동시군구코드",
    "umdCd": "법정동읍면동코드",
    "landCd": "법정동지번코드",
    "bonbun": "법정동본번코드",
    "bubun": "법정동부번코드",
    "roadNm": "도로명",
    "roadNmSggCd": "도로명시군구코드",
    "roadNmCd": "도로명코드",
    "roadNmSeq": "도로명일련번호코드",
    "roadNmbCd": "도로명지상지하코드",
    "roadNmBonbun": "도로명건물본번호코드",
    "roadNmBubun": "도로명건물부번호코드",
    "umdNm": "법정동",
    "aptNm": "단지명",
    "jibun": "지번",
    "excluUseAr": "전용면적",
    "dealYear": "계약년도",
    "dealMonth": "계약월",
    "dealDay": "계약일",
    "dealAmount": "거래금액",
    "floor": "층",
    "buildYear": "건축년도",
    "aptSeq": "단지 일련번호",
    "cdealType": "해제여부",
    "cdealDay": "해제사유발생일",
    "dealingGbn": "거래유형",
    "estateAgentSggNm": "중개사소재지",
    "rgstDate": "등기일자",
    "aptDong": "아파트 동명",
    "slerGbn": "매도자",
    "buyerGbn": "매수자",
    "landLeaseholdGbn": "토지임대부 아파트 여부",
}

DERIVED_COLS = ["lawdCd", "deal_ym", "year", "month", "_file"]
ALL_COLS = CSV_COLS + DERIVED_COLS


def build_spark():
    return (
        SparkSession.builder.appName("kreb-csv-to-iceberg-raw")
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
    # CSV 컬럼 누락 방어(모두 string으로 보강)
    for c in CSV_COLS:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))

    # 파생 컬럼들은 타입 유지
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

    # 타입은 우선 안전하게 “대부분 string”, 파생 year/month만 int 권장
    col_defs = []
    for c in CSV_COLS:
        ko = KREB_COL_DESC_KO.get(c, "")
        comment = f" COMMENT '{ko}'" if ko else ""
        col_defs.append(f"{c} string{comment}")

    col_defs.append("lawdCd string COMMENT '법정동코드(경로에서 파생, LAWD_CD)'")
    col_defs.append("deal_ym string COMMENT '거래년월(경로에서 파생, DEAL_YM=YYYYMM)'")
    col_defs.append("year int COMMENT '거래년도(파생, deal_ym 앞 4자리)'")
    col_defs.append("month int COMMENT '거래월(파생, deal_ym 뒤 2자리)'")
    col_defs.append("_file string COMMENT '원본 파일 경로'")

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


def main():
    bronze = os.environ.get(
        "BRONZE_PREFIX",
        "s3a://retrend-raw-data/bronze/kreb_etl_v2/apt_trade",
    ).rstrip("/")
    iceberg_table = os.environ.get("ICEBERG_TABLE", "iceberg.default.apt_trade_raw")
    warehouse_base = os.environ.get(
        "WAREHOUSE_BASE", "s3a://retrend-raw-data/warehouse/iceberg"
    ).rstrip("/")

    spark = build_spark()

    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)  # 원본 손상 방지: 전부 string로 읽기
        .csv(f"{bronze}/LAWD_CD=*/DEAL_YM=*/page=*.csv")
        .withColumn("_file", F.input_file_name())
    )

    # 파생 컬럼
    df = (
        df.withColumn("lawdCd", F.regexp_extract("_file", r"LAWD_CD=([0-9]{5})", 1))
        .withColumn("deal_ym", F.regexp_extract("_file", r"DEAL_YM=([0-9]{6})", 1))
        .withColumn("year", F.substring("deal_ym", 1, 4).cast("int"))
        .withColumn("month", F.substring("deal_ym", 5, 2).cast("int"))
    )

    df = ensure_columns(df)

    # 중요: 테이블이 이미 “12컬럼”으로 존재하면 IF NOT EXISTS는 안 바뀜
    # -> RECREATE_TABLE=true로 드랍 후 재생성하거나, 별도 테이블명 쓰기(apt_trade_raw 추천)
    create_or_recreate_table(spark, iceberg_table, warehouse_base)

    out = df.select(*ALL_COLS)
    out.writeTo(iceberg_table).append()

    spark.stop()


if __name__ == "__main__":
    main()