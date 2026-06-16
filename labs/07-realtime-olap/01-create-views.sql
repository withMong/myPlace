-- =====================================================================
-- 9호선 실시간 분석용 View (StarRocks 내부 카탈로그)
-- =====================================================================
-- Paimon Bronze 원본을 BI 가 바로 쓰기 좋은 형태로 가공한 논리 뷰.
-- 뷰는 데이터를 복사하지 않으므로 항상 Paimon 의 최신 상태를 반영한다.
--
-- 코드값 의미:
--   updn_line   0 = 상행/내선, 1 = 하행/외선
--   train_sttus 0 = 진입, 1 = 도착, 2 = 출발, 3 = 전역출발
--   direct_at   1 = 급행
--   lstcar_at   1 = 막차
-- =====================================================================

SET CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS subway;
USE subway;

-- ---------------------------------------------------------------------
-- 0) 9호선 이벤트 베이스 뷰 (코드값을 사람이 읽을 수 있는 라벨로 변환)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_line9_events AS
SELECT
  event_id,
  train_no,
  statn_id,
  statn_nm,
  statn_tnm                                   AS dest_nm,
  CAST(recptn_dt AS DATETIME)                 AS recptn_ts,
  updn_line,
  CASE updn_line WHEN '0' THEN '상행' WHEN '1' THEN '하행' ELSE '미상' END AS direction,
  train_sttus,
  CASE train_sttus
    WHEN '0' THEN '진입' WHEN '1' THEN '도착'
    WHEN '2' THEN '출발' WHEN '3' THEN '전역출발' ELSE '미상' END           AS status_nm,
  CASE WHEN direct_at = '1' THEN 1 ELSE 0 END AS is_express,
  CASE WHEN lstcar_at = '1' THEN 1 ELSE 0 END AS is_lastcar,
  ingested_at
FROM paimon_catalog.bronze.subway_position_current
WHERE line = '9호선';

-- ---------------------------------------------------------------------
-- 1) 열차별 최신 위치 = "현재 운행 중"인 열차 (실시간 노선도/KPI 의 핵심)
--    열차번호별 가장 최근 1건만 남기되, current 테이블은 '오늘 운행한 모든 열차'가
--    누적되므로 시간 창으로 막지 않으면 끝난 열차까지 세어 운행 대수가 부풀려진다.
--    → 데이터의 최신 수신시각 기준 최근 3분 안에 관측된 열차만 '현재 운행'으로 본다.
--      (producer 가 ~30초마다 폴링하므로 운행 중 열차는 매 폴링마다 잡힌다.)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_train_latest AS
SELECT *
FROM (
  SELECT
    e.*,
    ROW_NUMBER() OVER (PARTITION BY train_no ORDER BY recptn_ts DESC) AS rn,
    MAX(recptn_ts) OVER () AS data_latest
  FROM v_line9_events e
) t
WHERE rn = 1
  AND recptn_ts >= DATE_SUB(data_latest, INTERVAL 3 MINUTE);

-- ---------------------------------------------------------------------
-- 2) 실시간 운행 요약 (대시보드 상단 KPI 카드)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_live_summary AS
SELECT
  COUNT(*)                                   AS active_trains,
  SUM(CASE WHEN direction = '상행' THEN 1 ELSE 0 END) AS up_trains,
  SUM(CASE WHEN direction = '하행' THEN 1 ELSE 0 END) AS down_trains,
  SUM(is_express)                            AS express_trains,
  SUM(is_lastcar)                            AS lastcar_trains,
  MAX(recptn_ts)                             AS latest_recptn
FROM v_train_latest;

-- ---------------------------------------------------------------------
-- 3) 역별 트래픽 (어느 역에서 이벤트가 많은가 = 혼잡/요충 역)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_station_traffic AS
SELECT
  statn_nm,
  COUNT(*)                 AS event_cnt,
  COUNT(DISTINCT train_no) AS train_cnt,
  SUM(is_express)          AS express_cnt
FROM v_line9_events
GROUP BY statn_nm;

-- ---------------------------------------------------------------------
-- 4) 시간대별 이벤트 추이 (운행 패턴 / 러시아워)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_hourly_trend AS
SELECT
  DATE_FORMAT(recptn_ts, '%Y-%m-%d %H:00') AS hour_bucket,
  COUNT(*)                                 AS event_cnt,
  COUNT(DISTINCT train_no)                 AS train_cnt
FROM v_line9_events
GROUP BY DATE_FORMAT(recptn_ts, '%Y-%m-%d %H:00');

-- 확인
SHOW TABLES;
SELECT * FROM v_live_summary;
