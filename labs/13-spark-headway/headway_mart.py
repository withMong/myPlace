#!/usr/bin/env python3
"""L2 결정 마트: headway(배차 간격) 안정성.

silver.subway_arrival_events(도착 이벤트)에서 같은 역·방향의 연속 도착 간격(headway)을
구해, 역×방향×시간대버킷×요일유형별로 P50/P90/CV·초과비율을 집계한다.

핵심 질문: "출퇴근 시간대에 어느 역·방향 배차가 불안정한가?"
  → CV(변동계수)가 높을수록 배차가 들쭉날쭉(불안정).

산출: iceberg.gold.subway_headway_by_station_tod
  grain = line × statn_id × direction × time_band × day_type

코드값: updn_line 0=상행(개화/하행기점 방면), 1=하행
시간대: 새벽5-6 / 출근7-9 / 점심10-16 / 퇴근17-19 / 밤
주의:
  - 30분(1800s) 초과 headway 는 수집 윈도우 경계로 보고 제외(연속 수집 아님).
  - 'over_1p5x_ratio' 는 (현 단계) 시간표 계획배차가 아니라 **관측 중앙값 대비** 1.5배 초과 비율.
    시간표 조인(스트레치) 시 '계획 대비'로 교체 예정.
"""
from __future__ import annotations

import os
from pyspark.sql import SparkSession

MAX_HEADWAY_SEC = int(os.getenv("MAX_HEADWAY_SEC", "900"))  # 15분 초과면 윈도우경계/분기 빈틈으로 제외(정밀화값)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("subway-headway-mart")
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

    # 1) 도착 이벤트에 방향·시간대·요일유형 라벨
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW arrivals AS
        SELECT
          line, statn_id, statn_nm, updn_line, arrival_ts,
          CASE updn_line WHEN '0' THEN '상행' WHEN '1' THEN '하행' ELSE '미상' END AS direction,
          CASE
            WHEN hour(arrival_ts) BETWEEN 5 AND 6  THEN '새벽'
            WHEN hour(arrival_ts) BETWEEN 7 AND 9  THEN '출근'
            WHEN hour(arrival_ts) BETWEEN 10 AND 16 THEN '점심'
            WHEN hour(arrival_ts) BETWEEN 17 AND 19 THEN '퇴근'
            ELSE '밤'
          END AS time_band,
          CASE dayofweek(arrival_ts) WHEN 1 THEN '휴일' WHEN 7 THEN '토요일' ELSE '평일' END AS day_type
        FROM paimon.silver.subway_arrival_events
        WHERE updn_line IN ('0','1')
        """
    )

    # 2) 같은 역·방향의 연속 도착 간격 = headway(초)
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW headways AS
        SELECT * FROM (
          SELECT
            line, statn_id, statn_nm, direction, time_band, day_type, arrival_ts,
            unix_timestamp(arrival_ts) - unix_timestamp(
              LAG(arrival_ts) OVER (PARTITION BY line, statn_id, direction ORDER BY arrival_ts)
            ) AS headway_sec
          FROM arrivals
        )
        WHERE headway_sec IS NOT NULL AND headway_sec > 0 AND headway_sec <= {MAX_HEADWAY_SEC}
        """
    )

    # 3) 그룹 중앙값 (초과비율 기준)
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW grp_med AS
        SELECT line, statn_id, direction, time_band, day_type,
               percentile_approx(headway_sec, 0.5) AS grp_p50
        FROM headways
        GROUP BY line, statn_id, direction, time_band, day_type
        """
    )

    # 4) 결정 마트: P50/P90/CV·초과비율
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW gold AS
        SELECT
          h.line, h.statn_id, MAX(h.statn_nm) AS statn_nm,
          h.direction, h.time_band, h.day_type,
          COUNT(*)                                         AS headway_samples,
          ROUND(percentile_approx(h.headway_sec, 0.5), 0)  AS p50_sec,
          ROUND(percentile_approx(h.headway_sec, 0.9), 0)  AS p90_sec,
          ROUND(AVG(h.headway_sec), 0)                     AS mean_sec,
          ROUND(STDDEV(h.headway_sec), 1)                  AS std_sec,
          ROUND(STDDEV(h.headway_sec) / NULLIF(AVG(h.headway_sec), 0), 3) AS cv,
          ROUND(AVG(CASE WHEN h.headway_sec > 1.5 * g.grp_p50 THEN 1 ELSE 0 END), 3) AS over_1p5x_ratio
        FROM headways h
        JOIN grp_med g
          ON h.line=g.line AND h.statn_id=g.statn_id AND h.direction=g.direction
         AND h.time_band=g.time_band AND h.day_type=g.day_type
        GROUP BY h.line, h.statn_id, h.direction, h.time_band, h.day_type
        """
    )

    spark.table("gold").writeTo("iceberg.gold.subway_headway_by_station_tod").createOrReplace()
    print("[gold] iceberg.gold.subway_headway_by_station_tod 적재 완료")

    # 미리보기: 출퇴근에 배차 가장 불안정한(CV 높은) 역 Top 15 (표본 5개 이상)
    print("\n=== 출퇴근 배차 불안정 역 Top 15 (CV 기준, 표본>=5) ===")
    spark.sql(
        """
        SELECT line, statn_nm, direction, time_band,
               headway_samples AS n, p50_sec, p90_sec, cv, over_1p5x_ratio
        FROM gold
        WHERE time_band IN ('출근','퇴근') AND headway_samples >= 5
        ORDER BY cv DESC
        LIMIT 15
        """
    ).show(50, truncate=False)


def main() -> None:
    spark = build_spark()
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
