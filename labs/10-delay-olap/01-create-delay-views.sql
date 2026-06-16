-- =====================================================================
-- 지연 분석 BI용 View (StarRocks 내부 subway DB)
-- Iceberg Gold 를 BI(Grafana)가 바로 쓰기 좋은 형태로 노출.
-- =====================================================================
SET CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS subway;
USE subway;

-- 1) 연착 빈번 역 (Top5 는 BI 에서 LIMIT)
CREATE OR REPLACE VIEW v_delay_top_station AS
SELECT
  station_nm,
  arrivals,
  delayed,
  delay_rate,
  avg_delay_sec
FROM iceberg_catalog.gold.delay_by_station;

-- 2) 시간대 × 요일유형 지연율
CREATE OR REPLACE VIEW v_delay_by_timeband AS
SELECT
  day_type,
  time_band,
  arrivals,
  delayed,
  delay_rate,
  avg_delay_sec,
  p90_delay_sec
FROM iceberg_catalog.gold.delay_by_timeband;

-- 확인
SELECT * FROM v_delay_top_station ORDER BY delayed DESC LIMIT 5;
SELECT * FROM v_delay_by_timeband ORDER BY day_type, time_band;
