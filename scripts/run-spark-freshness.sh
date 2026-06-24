#!/usr/bin/env bash
# =====================================================================
# L2 freshness 마트: subway_position_log → iceberg.gold.subway_service_freshness
# 사용법: bash scripts/run-spark-freshness.sh
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
org.apache.iceberg:iceberg-aws-bundle:1.6.1,\
org.apache.paimon:paimon-spark-3.5_2.12:1.4.1,\
org.apache.paimon:paimon-s3:1.4.1"
NETTY="-Dorg.apache.iceberg.shaded.io.netty.noUnsafe=true -Dio.netty.noUnsafe=true"

echo "== 의존 서비스 기동 =="
docker compose up -d minio minio-init iceberg-postgres iceberg-rest spark-client

echo "== spark-submit: freshness 마트 =="
docker compose exec -T spark-client /opt/spark/bin/spark-submit \
  --packages "$PKGS" \
  --conf spark.driver.memory=2g \
  --conf spark.sql.iceberg.vectorization.enabled=false \
  --conf "spark.driver.extraJavaOptions=$NETTY" \
  --conf "spark.executor.extraJavaOptions=$NETTY" \
  /workspace/labs/14-spark-freshness/freshness_mart.py

echo ""
echo "✅ 완료 → iceberg.gold.subway_service_freshness"
