#!/usr/bin/env python3
"""L2 보조 마트: 서비스 freshness (파이프라인 건강).

subway_position_log 에서 노선×분 단위 수신 heartbeat 와 끊김을 측정한다.
"데이터가 끊김 없이 들어오고 있는가?" = 파이프라인 생존 증명.

산출: iceberg.gold.subway_service_freshness  (grain = line × minute)
  - records           : 그 분에 받은 위치 레코드 수 (heartbeat)
  - distinct_trains   : 그 분에 관측된 고유 열차 수
  - ingest_lag_avg_sec: 수신시각→Flink 처리시각 지연 평균
                        (Flink 를 상시 가동했을 때만 의미. drain 모드면 큼 — 해석 주의)

끊김(gap) 해석:
  - 윈도우 간 간격(>30분)은 '예상된 것'(러시아워 윈도우 수집이라 사이가 비어 있음).
  - 윈도우 내 끊김(1.5~30분)이 있으면 파이프라인 hiccup → 0 이면 건강.

참고: 1호선 코레일 구간 이탈로 개별 trainNo 가 소멸해도 노선 heartbeat 는 영향 없음
      (다른 1호선 열차들이 계속 보고). 개별 열차 누락 감지는 도착(L1) 쪽 별도 규칙.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("subway-freshness-mart")
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


def run(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW base AS
        SELECT
          line,
          date_trunc('minute', CAST(recptn_dt AS TIMESTAMP)) AS minute_ts,
          train_no,
          CAST(recptn_dt AS TIMESTAMP) AS recptn_ts,
          ingested_at
        FROM paimon.bronze.subway_position_log
        WHERE recptn_dt IS NOT NULL AND recptn_dt <> ''
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW freshness AS
        SELECT
          line, minute_ts,
          COUNT(*)                                                              AS records,
          COUNT(DISTINCT train_no)                                              AS distinct_trains,
          ROUND(AVG(unix_timestamp(ingested_at) - unix_timestamp(recptn_ts)), 1) AS ingest_lag_avg_sec
        FROM base
        GROUP BY line, minute_ts
        """
    )
    spark.table("freshness").writeTo("iceberg.gold.subway_service_freshness").createOrReplace()
    print("[gold] iceberg.gold.subway_service_freshness 적재 완료")

    # 노선별 커버리지 요약
    print("\n=== 노선별 수신 커버리지 ===")
    spark.sql(
        """
        SELECT line,
          MIN(minute_ts) AS first_min, MAX(minute_ts) AS last_min,
          COUNT(*) AS minutes_with_data,
          SUM(records) AS total_records
        FROM freshness GROUP BY line ORDER BY line
        """
    ).show(truncate=False)

    # 끊김(gap) 분석: 윈도우 경계(예상) vs 윈도우 내 hiccup(의심)
    print("\n=== 끊김 분석 (윈도우 경계 vs 내부 hiccup) ===")
    spark.sql(
        """
        SELECT line,
          SUM(CASE WHEN gap_min > 30 THEN 1 ELSE 0 END)                  AS window_boundaries,
          SUM(CASE WHEN gap_min > 1.5 AND gap_min <= 30 THEN 1 ELSE 0 END) AS within_window_hiccups,
          ROUND(MAX(CASE WHEN gap_min <= 30 THEN gap_min END), 1)        AS max_hiccup_min
        FROM (
          SELECT line,
            (unix_timestamp(minute_ts)
              - unix_timestamp(LAG(minute_ts) OVER (PARTITION BY line ORDER BY minute_ts))) / 60.0 AS gap_min
          FROM freshness
        )
        WHERE gap_min IS NOT NULL
        GROUP BY line ORDER BY line
        """
    ).show(truncate=False)


def main() -> None:
    spark = build_spark()
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
