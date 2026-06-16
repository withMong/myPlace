-- =====================================================================
-- 지하철 Bronze 두 테이블 생성 (DDL only, batch)
--   subway_position_log     : append (원본 로그, dt 파티션)
--   subway_position_current : upsert (현재 상태, PK=event_id)
-- 보통은 01-insert-...sql 이 IF NOT EXISTS 로 같이 만들지만,
-- 테이블만 따로 만들고 싶을 때 사용.
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

USE CATALOG paimon_lake;
CREATE DATABASE IF NOT EXISTS bronze;
USE bronze;

-- ① append: 원본 폴링 로그
CREATE TABLE IF NOT EXISTS subway_position_log (
  event_id STRING,
  line STRING,
  subway_id STRING,
  train_no STRING,
  statn_id STRING,
  statn_nm STRING,
  statn_tnm STRING,
  updn_line STRING,
  train_sttus STRING,
  direct_at STRING,
  lstcar_at STRING,
  recptn_dt STRING,
  raw_json STRING,
  ingested_at TIMESTAMP_LTZ(3),
  dt STRING
) PARTITIONED BY (dt) WITH (
  'bucket' = '-1'
);

-- ② upsert: 디덥된 현재 상태
CREATE TABLE IF NOT EXISTS subway_position_current (
  event_id STRING,
  line STRING,
  subway_id STRING,
  train_no STRING,
  statn_id STRING,
  statn_nm STRING,
  statn_tnm STRING,
  updn_line STRING,
  train_sttus STRING,
  direct_at STRING,
  lstcar_at STRING,
  recptn_dt STRING,
  raw_json STRING,
  ingested_at TIMESTAMP_LTZ(3),
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'bucket' = '4'
);
