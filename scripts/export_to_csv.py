#!/usr/bin/env python3
"""레이크하우스 테이블을 CSV 로 내보낸다 → /workspace/data/export (호스트의 ./data/export).

Bronze(Paimon) + L1(Paimon silver) + L2(Iceberg gold) 를 각각 단일 CSV 로 저장.
raw_json(원본 JSON, 매우 김)은 가독성 위해 제외.
"""
from pyspark.sql import SparkSession

OUT = "/workspace/data/export"
TABLES = {
    "bronze_position_log": "paimon.bronze.subway_position_log",
    "bronze_position_current": "paimon.bronze.subway_position_current",
    "silver_arrival_events": "paimon.silver.subway_arrival_events",
    "gold_headway_by_station_tod": "iceberg.gold.subway_headway_by_station_tod",
    "gold_service_freshness": "iceberg.gold.subway_service_freshness",
}

spark = (
    SparkSession.builder.appName("export-csv")
    .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
    .config("spark.sql.catalog.paimon.warehouse", "s3://paimon/warehouse")
    .config("spark.sql.catalog.paimon.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.paimon.s3.access-key", "minioadmin")
    .config("spark.sql.catalog.paimon.s3.secret-key", "minioadmin")
    .config("spark.sql.catalog.paimon.s3.path.style.access", "true")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
    )
    .getOrCreate()
)

for name, tbl in TABLES.items():
    try:
        df = spark.table(tbl)
        if "raw_json" in df.columns:
            df = df.drop("raw_json")
        n = df.count()
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/{name}")
        print(f"[ok]   {tbl:45s} -> data/export/{name}/  ({n} rows)")
    except Exception as e:  # noqa: BLE001
        print(f"[skip] {tbl}: {e}")

spark.stop()
print("\n완료 → ./data/export/<테이블명>/part-*.csv")
