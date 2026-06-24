#!/usr/bin/env bash
# =====================================================================
# L2 headway 결정 마트 실행: silver.subway_arrival_events → Iceberg gold
#   사전: labs/12-arrival-events 로 silver.subway_arrival_events 가 적재돼 있어야 함
# 사용법: bash scripts/run-spark-headway.sh
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

echo "== spark-submit: headway 마트 =="
docker compose exec -T spark-client /opt/spark/bin/spark-submit \
  --packages "$PKGS" \
  --conf spark.driver.memory=2g \
  --conf spark.sql.iceberg.vectorization.enabled=false \
  --conf "spark.driver.extraJavaOptions=$NETTY" \
  --conf "spark.executor.extraJavaOptions=$NETTY" \
  /workspace/labs/13-spark-headway/headway_mart.py

echo ""
echo "✅ 완료 → iceberg.gold.subway_headway_by_station_tod"
