ARG FLINK_BASE_IMAGE=flink:1.20.4-scala_2.12-java17
FROM ${FLINK_BASE_IMAGE}

ARG FLINK_KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG PAIMON_VERSION=1.4.1
ARG ICEBERG_FLINK_RUNTIME_VERSION=1.11.0
ARG FLINK_SHADED_HADOOP_VERSION=2.8.3-10.0

RUN set -eux; \
    wget -nv -P /opt/flink/lib \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar"; \
    wget -nv -P /opt/flink/lib \
      "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/${PAIMON_VERSION}/paimon-flink-1.20-${PAIMON_VERSION}.jar"; \
    wget -nv -P /opt/flink/lib \
      "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar"; \
    wget -nv -P /opt/flink/lib \
      "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-iceberg/${PAIMON_VERSION}/paimon-iceberg-${PAIMON_VERSION}.jar"; \
    wget -nv -P /opt/flink/lib \
      "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_FLINK_RUNTIME_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_FLINK_RUNTIME_VERSION}.jar"; \
    wget -nv -P /opt/flink/lib \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${FLINK_SHADED_HADOOP_VERSION}/flink-shaded-hadoop-2-uber-${FLINK_SHADED_HADOOP_VERSION}.jar"; \
    mkdir -p /workspace /opt/flink/checkpoints

COPY docker/flink/entrypoint.sh /usr/local/bin/de5-flink-entrypoint.sh

RUN chmod +x /usr/local/bin/de5-flink-entrypoint.sh

WORKDIR /workspace

ENTRYPOINT ["/usr/local/bin/de5-flink-entrypoint.sh"]
