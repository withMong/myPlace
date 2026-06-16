-- =====================================================================
-- Kafka → Paimon Bronze 두 갈래 (append log + upsert current)
-- 같은 Kafka 스트림을 STATEMENT SET 으로 "한 번만" 읽어 두 테이블에 fan-out.
--   ① subway_position_log     : 폴링마다 들어온 모든 레코드 (append, dt 파티션)
--   ② subway_position_current : event_id 로 디덥된 현재 상태 (upsert)
-- =====================================================================
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.checkpointing.interval' = '30s';
SET 'pipeline.name' = 'subway-bronze-fanout';

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

-- Paimon warehouse 를 MinIO(S3)에 둔다 (StarRocks 가 같은 경로를 읽기 위함).
CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 's3://paimon/warehouse',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = 'minioadmin',
  's3.secret-key' = 'minioadmin',
  's3.path.style.access' = 'true'
);

CREATE DATABASE IF NOT EXISTS paimon_lake.bronze;

-- ① append: 원본 폴링 로그. PK 없음 → 모든 폴링 보존. 날짜(dt) 파티션, 무인지(-1) 버킷.
CREATE TABLE IF NOT EXISTS paimon_lake.bronze.subway_position_log (
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

-- ② upsert: 디덥된 현재 상태. PRIMARY KEY(event_id) 가 중복 제거 담당.
CREATE TABLE IF NOT EXISTS paimon_lake.bronze.subway_position_current (
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

-- Kafka 를 한 번만 읽어 두 테이블에 동시 적재 (소스 재사용)
EXECUTE STATEMENT SET
BEGIN

INSERT INTO paimon_lake.bronze.subway_position_log
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
  CURRENT_TIMESTAMP                     AS ingested_at,
  SUBSTRING(JSON_VALUE(raw_json, '$.recptn_dt') FROM 1 FOR 10) AS dt
FROM subway_events_kafka_raw
WHERE JSON_VALUE(raw_json, '$.event_id') IS NOT NULL;

INSERT INTO paimon_lake.bronze.subway_position_current
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
  CURRENT_TIMESTAMP                     AS ingested_at
FROM subway_events_kafka_raw
WHERE JSON_VALUE(raw_json, '$.event_id') IS NOT NULL;

END;
