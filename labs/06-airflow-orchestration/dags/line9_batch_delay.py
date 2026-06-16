"""9호선 배치 지연 분석 오케스트레이션.

매일: 시간표 수집 → Spark 지연 분석(Iceberg Silver/Gold).
StarRocks 의 Iceberg 외부 카탈로그는 최신 스냅샷을 자동으로 읽으므로 별도 갱신 태스크는 없다.

실행 방식:
  Airflow(스케줄러) 컨테이너는 host 의 docker.sock 을 마운트하고 docker CLI 를 갖고 있다.
  각 태스크는 `docker exec` 로 이미 떠 있는 spark-client 컨테이너 안에서 작업을 실행한다.

전제:
  lakehouse 스택(minio, iceberg-rest, spark-client)이 떠 있어야 한다.
  spark-client 는 default 프로파일이라 `docker compose up` 시 함께 뜬다.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "subway-spark-client"

PKGS = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
    "org.apache.paimon:paimon-spark-3.5_2.12:1.4.1",
    "org.apache.paimon:paimon-s3:1.4.1",
])
NETTY = "-Dorg.apache.iceberg.shaded.io.netty.noUnsafe=true -Dio.netty.noUnsafe=true"

# spark-client 가 떠 있는지 보장(이미 떠 있으면 no-op)
ENSURE = f"docker start {SPARK_CONTAINER} >/dev/null 2>&1 || true"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="line9_batch_delay",
    description="9호선 배치 지연 분석: 시간표 수집 → Spark(Iceberg Silver/Gold)",
    schedule="0 5 * * *",          # 매일 05:00
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["seoul-metro", "line9", "batch", "delay"],
) as dag:

    # 1) 시간표 차원 갱신 (순수 stdlib → spark-client 의 python3 로 실행)
    fetch_timetable = BashOperator(
        task_id="fetch_timetable",
        bash_command=(
            f"{ENSURE} && "
            f"docker exec -e OUT_PATH=/workspace/data/timetable/line9_timetable.csv "
            f"{SPARK_CONTAINER} python3 /workspace/labs/08-timetable/fetch_timetable.py"
        ),
    )

    # 2) Spark 지연 분석 → Iceberg Silver/Gold
    spark_delay = BashOperator(
        task_id="spark_delay_analysis",
        bash_command=(
            f"{ENSURE} && "
            f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
            f"--packages '{PKGS}' "
            f"--conf spark.driver.memory=2g "
            f"--conf spark.sql.iceberg.vectorization.enabled=false "
            f"--conf \"spark.driver.extraJavaOptions={NETTY}\" "
            f"--conf \"spark.executor.extraJavaOptions={NETTY}\" "
            f"/workspace/labs/09-spark-delay/delay_analysis.py"
        ),
    )

    fetch_timetable >> spark_delay
