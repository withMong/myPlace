#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" = "0" ]; then
  mkdir -p /opt/flink/checkpoints /opt/flink/log
  chown -R flink:flink /opt/flink/checkpoints /opt/flink/log
fi

exec /docker-entrypoint.sh "$@"
