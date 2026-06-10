-- 지하철 실시간 위치: Kafka → Paimon Bronze (무한 스트리밍)
-- 실시간 파이프라인의 핵심. producer가 보내는 이벤트를 계속 적재한다.
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.checkpointing.interval' = '30s';

CREATE TEMPORARY TABLE subway_events_kafka_raw (
  raw_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'subway-events',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-paimon-subway-bronze',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse/paimon'
);

CREATE DATABASE IF NOT EXISTS paimon_lake.bronze;

CREATE TABLE IF NOT EXISTS paimon_lake.bronze.subway_events_bronze (
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

INSERT INTO paimon_lake.bronze.subway_events_bronze
SELECT
  JSON_VALUE(raw_json, '$.event_id')    AS event_id,
  JSON_VALUE(raw_json, '$.line')        AS line,
  JSON_VALUE(raw_json, '$.subway_id')   AS subway_id,
  JSON_VALUE(raw_json, '$.train_no')    AS train_no,
  JSON_VALUE(raw_json, '$.statn_id')    AS statn_id,
  JSON_VALUE(raw_json, '$.statn_nm')    AS statn_nm,
  JSON_VALUE(raw_json, '$.statn_tnm')   AS statn_tnm,
  JSON_VALUE(raw_json, '$.updn_line')   AS updn_line,
  JSON_VALUE(raw_json, '$.train_sttus') AS train_sttus,
  JSON_VALUE(raw_json, '$.direct_at')   AS direct_at,
  JSON_VALUE(raw_json, '$.lstcar_at')   AS lstcar_at,
  JSON_VALUE(raw_json, '$.recptn_dt')   AS recptn_dt,
  raw_json,
  CURRENT_TIMESTAMP AS ingested_at
FROM subway_events_kafka_raw
WHERE JSON_VALUE(raw_json, '$.event_id') IS NOT NULL;
