-- =====================================================================
-- L1: 도착 이벤트 추출 (Flink MATCH_RECOGNIZE, keyed CEP)
-- =====================================================================
-- subway_position_log(원본 폴링) 에서 train_no 별로 '역 전이'를 감지해
-- 도착 이벤트를 만든다. 30초 폴링이라 같은 역이 여러 번 찍히는데,
-- "도착 = 그 열차가 새 역에 처음 나타난 순간" 으로 정의한다.
--
--   PARTITION BY train_no, ORDER BY recptn_ts
--   PATTERN (A+) : 같은 statn_id 가 연속되는 구간을 한 묶음으로
--   FIRST(A.recptn_ts) : 그 역 첫 관측 시각 = 도착시각
--   다음 역으로 바뀌면 새 매치 → 다음 도착 이벤트
--
-- → 단순 행 카운트가 아니라, train_no 별 상태(역) 전이를 추적하는 CEP.
--
-- 단순화/한계 (포트폴리오에 명시):
--   - 시차보정(recptnDt vs 실제 위치) 미적용 → recptn_dt 를 도착시각 근사로 사용
--   - 폴링 30~40초라 매우 짧은 정차는 1폴에만 잡힐 수 있음(그래도 A+ 는 1행도 매치)
--   - 1호선 코레일 구간(서동탄·인천 등) 이탈로 trainNo 가 사라지면 매치가 자연 종료될 뿐,
--     거짓 도착을 만들지 않음 (누락 감지는 L2 freshness 마트에서 별도 처리)
--
-- 실행: 배치 모드로 한 번 돌리면 누적 log 전체에서 도착 이벤트 생성.
--   docker compose --profile tools run --rm flink-sql-client \
--     /opt/flink/bin/sql-client.sh -f /workspace/labs/12-arrival-events/01-arrival-events.sql
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

-- 도착 이벤트: trainNo × 역 × 도착시각
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
  line, train_no, statn_id, statn_nm, statn_tnm, updn_line, arrival_ts,
  DATE_FORMAT(arrival_ts, 'yyyy-MM-dd') AS dt
FROM (
  SELECT
    train_no, line, statn_id, statn_nm, statn_tnm, updn_line,
    CAST(recptn_dt AS TIMESTAMP(3)) AS recptn_ts
  FROM paimon_lake.bronze.subway_position_log
  WHERE statn_id IS NOT NULL AND recptn_dt IS NOT NULL AND recptn_dt <> ''
)
MATCH_RECOGNIZE (
  PARTITION BY train_no
  ORDER BY recptn_ts
  MEASURES
    FIRST(A.line)      AS line,
    FIRST(A.statn_id)  AS statn_id,
    FIRST(A.statn_nm)  AS statn_nm,
    FIRST(A.statn_tnm) AS statn_tnm,
    FIRST(A.updn_line) AS updn_line,
    FIRST(A.recptn_ts) AS arrival_ts
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A+)
  DEFINE A AS A.statn_id = FIRST(A.statn_id)
) AS ar;
