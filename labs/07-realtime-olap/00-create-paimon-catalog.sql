-- =====================================================================
-- StarRocks → Paimon 외부 카탈로그 (실시간 경로의 OLAP 진입점)
-- =====================================================================
-- 실시간 경로: producer → Kafka → Flink → Paimon(MinIO) → [여기] StarRocks → BI
--
-- Flink 가 s3://paimon/warehouse 에 적재한 Paimon Bronze 테이블을
-- StarRocks 가 "복사 없이" 그대로 조회한다(External Catalog = zero-copy).
-- 같은 MinIO 버킷을 가리키므로 Flink 가 막 쓴 데이터가 거의 즉시 보인다.
--
-- 실행:
--   docker compose exec starrocks-fe \
--     mysql -uroot -h starrocks-fe -P9030 < /workspace/labs/07-realtime-olap/00-create-paimon-catalog.sql
-- =====================================================================

DROP CATALOG IF EXISTS paimon_catalog;

CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES (
  "type" = "paimon",
  "paimon.catalog.type" = "filesystem",
  "paimon.catalog.warehouse" = "s3://paimon/warehouse",
  "aws.s3.endpoint" = "http://minio:9000",
  "aws.s3.access_key" = "minioadmin",
  "aws.s3.secret_key" = "minioadmin",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.enable_ssl" = "false"
);

-- 확인: 카탈로그 / DB / 테이블이 보이면 연결 성공
SHOW CATALOGS;
SHOW DATABASES FROM paimon_catalog;
-- SHOW TABLES FROM paimon_catalog.bronze;

-- 적재 건수 빠른 확인 (현재 상태 테이블)
SELECT COUNT(*) AS current_rows
FROM paimon_catalog.bronze.subway_position_current;
