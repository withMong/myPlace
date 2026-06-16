#!/usr/bin/env bash
# =====================================================================
# 지연 BI 셋업: StarRocks Iceberg 외부 카탈로그 + 지연 분석 View
#   사전: Spark 지연 잡(run-spark-delay.sh)이 한 번 돌아 Iceberg Gold 가 있어야 함
#
# 사용법: bash scripts/setup-delay-bi.sh
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

run() {
  echo "▶ $1"
  docker compose exec -T starrocks-fe mysql -uroot -h starrocks-fe -P9030 < "$1"
}

run labs/10-delay-olap/00-create-iceberg-catalog.sql
run labs/10-delay-olap/01-create-delay-views.sql

# Grafana 가 떠 있지 않으면 띄움 (지연 대시보드는 provisioning 으로 자동 등록)
docker compose --profile bi up -d grafana

echo ""
echo "✅ 지연 BI 셋업 완료"
echo "   Grafana → http://localhost:3000 → Seoul Metro → '서울 9호선 지연 분석 (배치)'"
