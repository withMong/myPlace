-- 지하철 실시간 위치 Bronze 테이블 생성
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse/paimon'
);

USE CATALOG paimon_lake;

CREATE DATABASE IF NOT EXISTS bronze;

USE bronze;

CREATE TABLE IF NOT EXISTS subway_events_bronze (
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
  'bucket' = '3'
);
