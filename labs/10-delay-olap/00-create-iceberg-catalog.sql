-- =====================================================================
-- StarRocks → Iceberg 외부 카탈로그 (배치 지연 분석 결과 조회)
-- =====================================================================
-- Spark 가 적재한 Iceberg Silver/Gold 를 StarRocks 가 복사 없이 읽는다.
-- iceberg-rest 카탈로그(REST)에 연결하고, 데이터 파일은 MinIO(S3)에서 읽는다.
--
-- 실행:
--   docker compose exec -T starrocks-fe \
--     mysql -uroot -h starrocks-fe -P9030 < labs/10-delay-olap/00-create-iceberg-catalog.sql
-- =====================================================================

DROP CATALOG IF EXISTS iceberg_catalog;

CREATE EXTERNAL CATALOG iceberg_catalog
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "http://iceberg-rest:8181",
  "aws.s3.endpoint" = "http://minio:9000",
  "aws.s3.access_key" = "minioadmin",
  "aws.s3.secret_key" = "minioadmin",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.enable_ssl" = "false"
);

-- 확인
SHOW DATABASES FROM iceberg_catalog;        -- dim / silver / gold 가 보여야 함
SELECT COUNT(*) AS gold_station_rows FROM iceberg_catalog.gold.delay_by_station;
