-- =====================================================================
-- Bronze 두 테이블 적재 확인 (batch)
-- 핵심 검증: log(원본) 행수 ≥ current(디덥) 행수 여야 정상.
--           폴링 반복분이 log 에는 쌓이고 current 에선 합쳐지기 때문.
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
USE bronze;

-- 원본 로그 총 행수 (폴링마다 누적)
SELECT COUNT(*) AS log_rows FROM subway_position_log;

-- 날짜 파티션별 로그 건수
SELECT dt, COUNT(*) AS cnt
FROM subway_position_log
GROUP BY dt
ORDER BY dt;

-- 디덥된 현재 상태 행수 (log_rows 보다 작거나 같아야 정상)
SELECT COUNT(*) AS current_rows FROM subway_position_current;

-- 현재 상태 최근 10건
SELECT train_no, statn_nm, statn_tnm, updn_line, train_sttus, recptn_dt
FROM subway_position_current
ORDER BY recptn_dt DESC
LIMIT 10;
