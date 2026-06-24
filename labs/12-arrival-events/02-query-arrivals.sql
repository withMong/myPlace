-- L1 도착 이벤트 적재 확인 (batch)
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
USE silver;

-- 총 도착 이벤트 수 (log 6만 → 도착 이벤트는 그보다 훨씬 적어야 정상)
SELECT COUNT(*) AS arrival_events FROM subway_arrival_events;

-- 노선별
SELECT line, COUNT(*) AS cnt FROM subway_arrival_events GROUP BY line ORDER BY line;

-- 한 열차의 도착 시퀀스 (역이 순서대로 바뀌면 정상)
SELECT statn_nm, statn_tnm, updn_line, arrival_ts
FROM subway_arrival_events
WHERE train_no = '9177'
ORDER BY arrival_ts
LIMIT 40;
