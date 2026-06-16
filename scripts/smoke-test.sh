#!/usr/bin/env bash
# 지하철 파이프라인 1단계(kafka + minio) 전용 smoke test
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
TOPIC="${KAFKA_TOPIC:-subway-events}"
KAFKA_UI_PORT="${KAFKA_UI_HOST_PORT:-8088}"

echo "== Compose services =="
docker compose -f "${COMPOSE_FILE}" ps

echo
echo "== Kafka topic (${TOPIC}) =="
docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --describe \
  --topic "${TOPIC}" || {
    echo "토픽이 아직 없습니다. kafka-init 이 끝났는지 확인하세요:"
    echo "  docker compose -f ${COMPOSE_FILE} up kafka-init"
  }

echo
echo "== Kafka UI =="
if command -v curl >/dev/null 2>&1; then
  curl -fsSI "http://localhost:${KAFKA_UI_PORT}" >/dev/null && \
    echo "Kafka UI 접속 가능: http://localhost:${KAFKA_UI_PORT}" || \
    echo "Kafka UI 아직 준비 안 됨. 잠시 후 재시도."
else
  echo "curl 없음; Kafka UI 체크 생략"
fi

echo
echo "== MinIO buckets =="
docker compose -f "${COMPOSE_FILE}" run --rm minio-init || \
  echo "MinIO init 실패 또는 이미 완료됨."

echo
echo "Smoke test (kafka + minio) 완료."