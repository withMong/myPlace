# Jupyter(PySpark) — Iceberg/Paimon 탐색 노트북
# Spark 3.5.6 (지연 잡과 동일) 위에 JupyterLab 을 얹는다.
# 커넥터 jar 는 노트북에서 spark.jars.packages 로 받아 ivy 캐시에 둔다.
ARG SPARK_BASE_IMAGE=apache/spark:3.5.6
FROM ${SPARK_BASE_IMAGE}

USER root
# pyspark(=3.5.6, /opt/spark 와 동일 버전) + 노트북 + toPandas 용 pandas/pyarrow
RUN set -eux; \
    pip install --no-cache-dir jupyterlab pyspark==3.5.6 pandas pyarrow; \
    mkdir -p /home/spark/notebooks /home/spark/.ivy2; \
    chown -R spark:spark /home/spark

USER spark
WORKDIR /home/spark/notebooks
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--ServerApp.token=", "--ServerApp.password=", \
     "--ServerApp.root_dir=/home/spark/notebooks"]
