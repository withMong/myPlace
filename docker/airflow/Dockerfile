# Airflow + docker CLI.
# DAG 가 host 의 docker.sock 을 통해 기존 컨테이너(spark-client 등)에
# `docker exec` 로 배치 작업을 실행한다 → CLI 만 있으면 된다.
ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.10.5
FROM ${AIRFLOW_BASE_IMAGE}

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
