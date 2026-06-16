#!/usr/bin/env python3
"""Spark 환경 smoke test: Iceberg(REST+MinIO) 쓰기/읽기가 되는지만 빠르게 확인.

position_log 데이터가 없어도 실행 가능. jar 버전·iceberg-rest·S3FileIO 가
제대로 물리는지(지연 잡의 가장 위험한 부분)를 먼저 검증한다.
"""
from pyspark.sql import SparkSession, Row

spark = (
    SparkSession.builder.appName("smoke-iceberg")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")
df = spark.createDataFrame([Row(id=1, nm="개화"), Row(id=2, nm="샛강")])
df.writeTo("iceberg.demo.smoke").createOrReplace()

print("=== iceberg.demo.smoke 읽기 ===")
spark.table("iceberg.demo.smoke").show(truncate=False)
print("✅ Iceberg write/read OK")

spark.stop()
