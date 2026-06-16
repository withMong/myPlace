-- =====================================================================
-- 9호선 실시간 분석 쿼리 모음 (BI 패널 / 발표 시연용)
-- =====================================================================
-- 먼저 00-create-paimon-catalog.sql, 01-create-views.sql 를 실행해 둘 것.
USE default_catalog.subway;

-- 1) 지금 운행 중인 9호선 열차 한눈에 보기 -----------------------------
SELECT train_no, direction, statn_nm, dest_nm, status_nm,
       is_express, is_lastcar, recptn_ts
FROM v_train_latest
ORDER BY direction, statn_nm;

-- 2) 상단 KPI 카드 (운행 열차 / 상하행 / 급행 / 막차) ------------------
SELECT * FROM v_live_summary;

-- 3) 급행 vs 일반 비율 -------------------------------------------------
SELECT
  CASE WHEN is_express = 1 THEN '급행' ELSE '일반' END AS train_type,
  COUNT(*) AS trains
FROM v_train_latest
GROUP BY is_express
ORDER BY trains DESC;

-- 4) 이벤트가 많은 상위 10개 역 ---------------------------------------
SELECT statn_nm, event_cnt, train_cnt, express_cnt
FROM v_station_traffic
ORDER BY event_cnt DESC
LIMIT 10;

-- 5) 상태 분포 (진입/도착/출발/전역출발) ------------------------------
SELECT status_nm, COUNT(*) AS cnt
FROM v_line9_events
GROUP BY status_nm
ORDER BY cnt DESC;

-- 6) 시간대별 운행 추이 -----------------------------------------------
SELECT hour_bucket, event_cnt, train_cnt
FROM v_hourly_trend
ORDER BY hour_bucket;

-- 7) 종착역(행선지)별 열차 분포 ---------------------------------------
SELECT dest_nm, direction, COUNT(*) AS trains
FROM v_train_latest
GROUP BY dest_nm, direction
ORDER BY trains DESC;

-- 8) 막차 추적 (막차 플래그가 켜진 열차의 현재 위치) ------------------
SELECT train_no, direction, statn_nm, dest_nm, status_nm, recptn_ts
FROM v_train_latest
WHERE is_lastcar = 1
ORDER BY recptn_ts DESC;
