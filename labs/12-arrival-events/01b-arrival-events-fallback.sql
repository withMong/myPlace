-- =====================================================================
-- L1 도착 이벤트 — 대체(fallback) 버전: LAG 윈도우 함수
-- =====================================================================
-- 01-arrival-events.sql 의 MATCH_RECOGNIZE 가 배치 ORDER BY 등으로 막히면 이걸 쓴다.
-- 결과는 동일: train_no 별 시간순에서 statn_id 가 직전 행과 달라지는 행 = 새 역 도착.
-- (MATCH_RECOGNIZE 만큼 'CEP' 스럽진 않지만 PARTITION BY train_no 로 keyed 처리 동일)
-- =====================================================================
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 's3://paimon/warehouse',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = 'minioadmin',
  's3.secret-key' = 'minioadmin',
  's3.path.style.access' = 'true'
);

CREATE DATABASE IF NOT EXISTS paimon_lake.silver;

CREATE TABLE IF NOT EXISTS paimon_lake.silver.subway_arrival_events (
  line STRING,
  train_no STRING,
  statn_id STRING,
  statn_nm STRING,
  statn_tnm STRING,
  updn_line STRING,
  arrival_ts TIMESTAMP(3),
  dt STRING,
  PRIMARY KEY (line, train_no, statn_id, arrival_ts) NOT ENFORCED
) WITH (
  'bucket' = '4'
);

INSERT INTO paimon_lake.silver.subway_arrival_events
SELECT
  line, train_no, statn_id, statn_nm, statn_tnm, updn_line,
  recptn_ts AS arrival_ts,
  DATE_FORMAT(recptn_ts, 'yyyy-MM-dd') AS dt
FROM (
  SELECT
    line, train_no, statn_id, statn_nm, statn_tnm, updn_line, recptn_ts,
    LAG(statn_id) OVER (PARTITION BY train_no ORDER BY recptn_ts) AS prev_statn
  FROM (
    SELECT
      line, train_no, statn_id, statn_nm, statn_tnm, updn_line,
      CAST(recptn_dt AS TIMESTAMP(3)) AS recptn_ts
    FROM paimon_lake.bronze.subway_position_log
    WHERE statn_id IS NOT NULL AND recptn_dt IS NOT NULL AND recptn_dt <> ''
  ) t
) w
WHERE prev_statn IS NULL OR statn_id <> prev_statn;
