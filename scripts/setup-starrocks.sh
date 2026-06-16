#!/usr/bin/env bash
# =====================================================================
# StarRocks 설정만 실행 (인프라·producer·Flink 가 이미 떠 있을 때)
#   Paimon 외부 카탈로그 → 기본 스토리지 볼륨 → 9호선 분석 뷰
#
# 사용법: bash scripts/setup-starrocks.sh
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."   # 리포 루트로 이동

run() {
  echo "▶ $1"
  docker compose exec -T starrocks-fe mysql -uroot -h starrocks-fe -P9030 < "$1"
}

run labs/07-realtime-olap/00-create-paimon-catalog.sql
run labs/07-realtime-olap/00b-create-storage-volume.sql
run labs/07-realtime-olap/01-create-views.sql

echo "✅ StarRocks 설정 완료 — 검증: bash scripts/setup-starrocks.sh 이후"
echo "   docker compose exec -T starrocks-fe mysql -uroot -h starrocks-fe -P9030 -e 'SELECT * FROM subway.v_live_summary;'"
