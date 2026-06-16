# Spark 3.5 + Iceberg/Paimon 잡 실행용 이미지.
# 커넥터 jar 는 spark-submit --packages 로 ivy 캐시(/home/spark/.ivy2 볼륨)에 받는다.
ARG SPARK_BASE_IMAGE=apache/spark:3.5.6
FROM ${SPARK_BASE_IMAGE}

USER root
RUN set -eux; \
    mkdir -p /workspace /home/spark/.ivy2; \
    chown -R spark:spark /workspace /home/spark/.ivy2

USER spark
WORKDIR /workspace
