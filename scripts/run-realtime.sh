#!/usr/bin/env bash
# =====================================================================
# 실시간 경로 end-to-end 기동 + 검증 (원클릭)
#   producer → Kafka → Flink → Paimon(MinIO) → StarRocks  (+옵션: Grafana)
#
# 사용법:
#   bash scripts/run-realtime.sh         # 실시간 경로 기동 + 검증
#   bash scripts/run-realtime.sh --bi    # Grafana 대시보드까지 기동
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."   # 리포 루트로 이동

WITH_BI="no"
[ "${1:-}" = "--bi" ] && WITH_BI="yes"

# StarRocks FE 에 SQL 파일 실행
sql_fe() { docker compose exec -T starrocks-fe mysql -uroot -h starrocks-fe -P9030 < "$1"; }

# StarRocks FE 에 단일 쿼리 실행 (헤더 없이 값만)
q_fe() { docker compose exec -T starrocks-fe mysql -uroot -h starrocks-fe -P9030 -N -B -e "$1" 2>/dev/null; }

# 컨테이너가 healthy 될 때까지 대기
wait_healthy() {
  local name="$1" tries="${2:-60}"
  printf '  %-22s' "$name"
  for _ in $(seq 1 "$tries"); do
    case "$(docker inspect -f '{{.State.Health.Status}}' "$name" 2>/dev/null || echo missing)" in
      healthy) echo " ✓"; return 0 ;;
    esac
    printf '.'; sleep 3
  done
  echo " ✗ (healthy 안 됨)"; return 1
}

echo "== 1) 인프라 기동 =="
docker compose up -d \
  kafka kafka-init kafka-ui minio minio-init \
  flink-jobmanager flink-taskmanager \
  starrocks-fe starrocks-cn

echo "== 2) healthy 대기 =="
wait_healthy subway-kafka
wait_healthy subway-minio
wait_healthy subway-starrocks-fe
wait_healthy subway-starrocks-cn

echo "== 3) producer 기동 (9호선 수집) =="
docker compose --profile tools up -d subway-producer
echo "  첫 폴링이 Kafka 로 들어갈 시간 대기(35s)..."
sleep 35

echo "== 4) Flink 스트리밍 적재 잡 제출 =="
docker compose --profile tools run --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/04-flink-paimon/01-insert-subway-bronze-streaming.sql

echo "== 5) StarRocks 설정 (카탈로그 → 스토리지볼륨 → 뷰) =="
sql_fe labs/07-realtime-olap/00-create-paimon-catalog.sql
sql_fe labs/07-realtime-olap/00b-create-storage-volume.sql
sql_fe labs/07-realtime-olap/01-create-views.sql

echo "== 6) 적재 검증 (Bronze 가 채워질 때까지 대기) =="
for _ in $(seq 1 20); do
  rows="$(q_fe 'SELECT COUNT(*) FROM paimon_catalog.bronze.subway_position_current;' | tr -d '[:space:]')"
  echo "  bronze_rows=${rows:-0}"
  [ "${rows:-0}" -gt 0 ] && break
  sleep 6
done

echo "== 7) 9호선 실시간 요약 =="
docker compose exec -T starrocks-fe mysql -uroot -h starrocks-fe -P9030 \
  -e 'SELECT * FROM subway.v_live_summary;'

if [ "$WITH_BI" = "yes" ]; then
  echo "== 8) Grafana 기동 =="
  docker compose --profile bi up -d grafana
fi

echo ""
echo "✅ 실시간 경로 기동 완료"
echo "   Flink UI : http://localhost:8081"
echo "   Kafka UI : http://localhost:8088"
[ "$WITH_BI" = "yes" ] && echo "   Grafana  : http://localhost:3000  (admin/admin)"
echo "   정리     : docker compose --profile tools --profile bi down -v"
