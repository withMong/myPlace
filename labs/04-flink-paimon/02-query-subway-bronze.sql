-- 지하철 Bronze 테이블 조회 (적재 확인용)
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse/paimon'
);

USE CATALOG paimon_lake;
USE bronze;

-- 총 적재 건수
SELECT COUNT(*) AS total_events FROM subway_events_bronze;

-- 호선별 이벤트 수
SELECT line, COUNT(*) AS cnt
FROM subway_events_bronze
GROUP BY line
ORDER BY line;

-- 최근 이벤트 10건
SELECT line, train_no, statn_nm, statn_tnm, updn_line, train_sttus, recptn_dt
FROM subway_events_bronze
ORDER BY recptn_dt DESC
LIMIT 10;
