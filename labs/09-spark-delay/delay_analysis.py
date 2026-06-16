#!/usr/bin/env python3
"""배치 지연 분석: Paimon position_log ⨝ 시간표 → Iceberg Silver/Gold.

흐름:
  1) dim_timetable: data/timetable/line9_timetable.csv → Iceberg iceberg.dim.timetable
  2) Paimon bronze.subway_position_log 에서 '도착' 이벤트의 실제 도착시각 추출
  3) 역명 + 방향 + 요일유형으로 시간표와 조인, '가장 가까운 예정시각' 매칭
       지연(초) = 실제 도착 − 예정 도착
  4) Silver(이벤트별 지연) + Gold(시간대×요일유형 지연율) 적재

코드값:
  실시간 train_sttus 1=도착 / updn_line 0=상행,1=하행
  시간표 inout_tag 1=상행,2=하행 / week_tag 1=평일,2=토,3=휴일

매칭 주의: 시간표 train_no(C9008)와 실시간 train_no(9585)는 포맷이 달라 직접 매칭 불가 →
역+방향+요일유형 안에서 '실제 도착에 가장 가까운 예정 도착시각'으로 매칭한다.
"""
from __future__ import annotations

import os
from pyspark.sql import SparkSession

CSV_PATH = os.getenv("TIMETABLE_CSV", "/workspace/data/timetable/line9_timetable.csv")
DELAY_THRESHOLD_SEC = int(os.getenv("DELAY_THRESHOLD_SEC", "60"))  # 이 초과면 '지연'으로 카운트


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("subway-delay-analysis")
        # ── Paimon 카탈로그 (Bronze 읽기) ──
        .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon.warehouse", "s3://paimon/warehouse")
        .config("spark.sql.catalog.paimon.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.paimon.s3.access-key", "minioadmin")
        .config("spark.sql.catalog.paimon.s3.secret-key", "minioadmin")
        .config("spark.sql.catalog.paimon.s3.path.style.access", "true")
        # ── Iceberg REST 카탈로그 (Silver/Gold 쓰기) ──
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


def load_timetable_dim(spark: SparkSession) -> None:
    """시간표 CSV → Iceberg iceberg.dim.timetable (idempotent)."""
    df = spark.read.option("header", True).csv(CSV_PATH)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.dim")
    df.writeTo("iceberg.dim.timetable").createOrReplace()
    print(f"[dim] timetable rows = {df.count()}")


def run_delay(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    # 1) 실제 도착 이벤트: 같은 event_id(열차·역·상태·날짜) 의 '첫 관측' = 실제 도착시각
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW arrivals AS
        SELECT
          event_id,
          statn_nm AS station_nm,
          train_no,
          CASE updn_line WHEN '0' THEN '1' WHEN '1' THEN '2' END AS inout_tag,  -- 상행0→1,하행1→2
          MIN(recptn_dt) AS actual_dt
        FROM paimon.bronze.subway_position_log
        WHERE line = '9호선' AND train_sttus = '1'        -- 1 = 도착
        GROUP BY event_id, statn_nm, train_no, updn_line
        """
    )

    # 2) 파생: 타임스탬프, 요일유형(week_tag), 초-of-서비스데이(새벽은 +24h), 시간대 버킷
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW arrivals2 AS
        SELECT
          a.*,
          to_timestamp(actual_dt) AS actual_ts,
          CASE dayofweek(to_timestamp(actual_dt))
            WHEN 1 THEN '3'   -- 일요일 → 휴일
            WHEN 7 THEN '2'   -- 토요일
            ELSE '1'          -- 평일 (공휴일은 근사로 평일 처리)
          END AS week_tag,
          (hour(to_timestamp(actual_dt))*3600 + minute(to_timestamp(actual_dt))*60
             + second(to_timestamp(actual_dt)))
            + CASE WHEN hour(to_timestamp(actual_dt)) < 4 THEN 86400 ELSE 0 END AS actual_sod,
          CASE
            WHEN hour(to_timestamp(actual_dt)) BETWEEN 5 AND 6  THEN '새벽'
            WHEN hour(to_timestamp(actual_dt)) BETWEEN 7 AND 9  THEN '출근'
            WHEN hour(to_timestamp(actual_dt)) BETWEEN 10 AND 16 THEN '점심'
            WHEN hour(to_timestamp(actual_dt)) BETWEEN 17 AND 19 THEN '퇴근'
            ELSE '밤'
          END AS time_band
        FROM arrivals a
        WHERE inout_tag IS NOT NULL
        """
    )

    # 3) 시간표를 초-of-서비스데이로 변환 (HH 가 24/25 일 수 있어 split 으로 직접 계산)
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW tt AS
        SELECT
          station_nm, inout_tag, week_tag, express_yn, arrive_time,
          CAST(split(arrive_time, ':')[0] AS INT)*3600
            + CAST(split(arrive_time, ':')[1] AS INT)*60
            + CAST(split(arrive_time, ':')[2] AS INT) AS sched_sod
        FROM iceberg.dim.timetable
        WHERE arrive_time IS NOT NULL AND arrive_time <> ''
        """
    )

    # 4) 역+방향+요일유형으로 조인 후 '가장 가까운 예정시각' 1건만 선택
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW silver AS
        WITH joined AS (
          SELECT
            a.event_id, a.station_nm, a.train_no, a.inout_tag, a.week_tag,
            a.actual_ts, a.time_band, a.actual_sod,
            t.arrive_time AS sched_arrive, t.express_yn,
            (a.actual_sod - t.sched_sod) AS delay_sec,
            ROW_NUMBER() OVER (
              PARTITION BY a.event_id
              ORDER BY abs(a.actual_sod - t.sched_sod)
            ) AS rn
          FROM arrivals2 a
          JOIN tt t
            ON a.station_nm = t.station_nm
           AND a.inout_tag = t.inout_tag
           AND a.week_tag  = t.week_tag
        )
        SELECT
          event_id, station_nm, train_no,
          CASE inout_tag WHEN '1' THEN '상행' ELSE '하행' END AS direction,
          CASE week_tag WHEN '1' THEN '평일' WHEN '2' THEN '토요일' ELSE '휴일' END AS day_type,
          time_band,
          actual_ts,
          sched_arrive,
          delay_sec,
          CASE WHEN delay_sec > {DELAY_THRESHOLD_SEC} THEN 1 ELSE 0 END AS is_delayed
        FROM joined
        WHERE rn = 1
        """
    )
    spark.table("silver").writeTo("iceberg.silver.arrival_delay").createOrReplace()
    print("[silver] iceberg.silver.arrival_delay 적재 완료")

    # 5) Gold: 시간대 × 요일유형 지연율
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW gold AS
        SELECT
          day_type,
          time_band,
          COUNT(*)                              AS arrivals,
          SUM(is_delayed)                       AS delayed,
          ROUND(SUM(is_delayed) / COUNT(*), 4)  AS delay_rate,
          ROUND(AVG(delay_sec), 1)              AS avg_delay_sec,
          ROUND(percentile_approx(delay_sec, 0.9), 1) AS p90_delay_sec
        FROM silver
        GROUP BY day_type, time_band
        """
    )
    spark.table("gold").writeTo("iceberg.gold.delay_by_timeband").createOrReplace()
    print("[gold] iceberg.gold.delay_by_timeband 적재 완료")

    # 6) Gold: 역별 연착 (가장 연착이 빈번한 역 Top5 용)
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW gold_station AS
        SELECT
          station_nm,
          COUNT(*)                              AS arrivals,
          SUM(is_delayed)                       AS delayed,
          ROUND(SUM(is_delayed) / COUNT(*), 4)  AS delay_rate,
          ROUND(AVG(delay_sec), 1)              AS avg_delay_sec
        FROM silver
        GROUP BY station_nm
        """
    )
    spark.table("gold_station").writeTo("iceberg.gold.delay_by_station").createOrReplace()
    print("[gold] iceberg.gold.delay_by_station 적재 완료")

    print("\n=== 연착 빈번 역 Top5 (지연 건수 기준) ===")
    spark.sql(
        """
        SELECT station_nm, arrivals, delayed, delay_rate, avg_delay_sec
        FROM gold_station
        ORDER BY delayed DESC, delay_rate DESC
        LIMIT 5
        """
    ).show(truncate=False)

    print("\n=== 시간대×요일유형 지연율 (미리보기) ===")
    spark.sql(
        """
        SELECT day_type, time_band, arrivals, delayed, delay_rate, avg_delay_sec
        FROM gold
        ORDER BY day_type,
          CASE time_band WHEN '새벽' THEN 1 WHEN '출근' THEN 2 WHEN '점심' THEN 3
                         WHEN '퇴근' THEN 4 ELSE 5 END
        """
    ).show(50, truncate=False)


def main() -> None:
    spark = build_spark()
    try:
        load_timetable_dim(spark)
        run_delay(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
