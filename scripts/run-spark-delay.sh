#!/usr/bin/env bash
# =====================================================================
# 배치 지연 분석 실행: Paimon log ⨝ 시간표 → Iceberg Silver/Gold
#
# 사전: ① 실시간 경로가 한동안 돌아 position_log 에 도착 이벤트가 쌓여 있어야 함
#       ② labs/08-timetable 로 data/timetable/line9_timetable.csv 생성돼 있어야 함
#
# 사용법: bash scripts/run-spark-delay.sh
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

# 커넥터 jar (첫 실행 시 ivy 캐시로 다운로드)
PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
org.apache.iceberg:iceberg-aws-bundle:1.6.1,\
org.apache.paimon:paimon-spark-3.5_2.12:1.4.1,\
org.apache.paimon:paimon-s3:1.4.1"

echo "== 의존 서비스 기동 (minio, iceberg-rest, spark-client) =="
docker compose up -d minio minio-init iceberg-postgres iceberg-rest spark-client

# netty(Arrow) Unsafe 경로에서 나는 JVM SIGSEGV 방지 + 드라이버 메모리 상향
NETTY_OPTS="-Dorg.apache.iceberg.shaded.io.netty.noUnsafe=true -Dio.netty.noUnsafe=true"

echo "== spark-submit: 지연 분석 =="
docker compose exec -T spark-client /opt/spark/bin/spark-submit \
  --packages "$PKGS" \
  --conf spark.driver.memory=2g \
  --conf spark.sql.iceberg.vectorization.enabled=false \
  --conf "spark.driver.extraJavaOptions=$NETTY_OPTS" \
  --conf "spark.executor.extraJavaOptions=$NETTY_OPTS" \
  /workspace/labs/09-spark-delay/delay_analysis.py

echo ""
echo "✅ 완료. 결과 테이블:"
echo "   iceberg.silver.arrival_delay      (이벤트별 지연)"
echo "   iceberg.gold.delay_by_timeband    (시간대×요일유형 지연율)"
echo "   iceberg.gold.delay_by_station     (연착 빈번 역 Top5)"
