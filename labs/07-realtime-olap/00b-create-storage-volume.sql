-- =====================================================================
-- StarRocks 기본 스토리지 볼륨 (shared_data 모드 1회 부트스트랩)
-- =====================================================================
-- StarRocks 가 shared_data 모드라, 내부 테이블/뷰를 만들려면 자체 데이터를
-- 저장할 "기본 스토리지 볼륨"이 있어야 한다. MinIO 의 starrocks 버킷을 쓴다.
-- (외부 Paimon 카탈로그 읽기에는 필요 없지만, default_catalog.subway 의
--  View 를 만들려면 필수.)
--
-- 1회만 실행하면 된다. 01-create-views.sql 보다 먼저.
-- =====================================================================

CREATE STORAGE VOLUME IF NOT EXISTS def_volume
TYPE = S3
LOCATIONS = ("s3://starrocks/")
PROPERTIES (
  "enabled" = "true",
  "aws.s3.endpoint" = "http://minio:9000",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.enable_ssl" = "false",
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "minioadmin",
  "aws.s3.secret_key" = "minioadmin"
);

SET def_volume AS DEFAULT STORAGE VOLUME;

-- 확인: IsDefault 가 true 여야 한다
DESC STORAGE VOLUME def_volume;
