# Airflow 오케스트레이션 — 배치 지연 분석

배치 경로(시간표 수집 → Spark 지연 분석)를 매일 자동 실행한다.

## DAG: `line9_batch_delay`

```
fetch_timetable  ──►  spark_delay_analysis
 (OA-101 → CSV)        (Paimon log ⨝ 시간표 → Iceberg Silver/Gold)
```

- 스케줄: 매일 05:00 (`0 5 * * *`)
- 실행 방식: Airflow 스케줄러가 host 의 `docker.sock` 으로 **`docker exec subway-spark-client`** 를
  호출해, 이미 떠 있는 spark-client 안에서 작업을 돌린다. (그래서 Airflow 이미지에 docker CLI 포함)
- StarRocks Iceberg 외부 카탈로그는 최신 스냅샷을 자동으로 읽으므로 BI 갱신 태스크는 없다.

## 실행

```bash
# 1) lakehouse 스택이 떠 있어야 함 (spark-client 포함, default 프로파일)
docker compose up -d minio minio-init iceberg-postgres iceberg-rest spark-client

# 2) Airflow 기동 (orchestration 프로파일) — 최초엔 이미지 빌드
docker compose --profile orchestration up -d --build

# 3) 웹 UI
#   http://localhost:8080  (기본 계정은 airflow-init 로그 참고)
#   DAG 'line9_batch_delay' 를 켜고(Unpause) 수동 트리거로 한 번 돌려본다.
```

## 확인 포인트

- `fetch_timetable` 성공 → `data/timetable/line9_timetable.csv` 갱신
- `spark_delay_analysis` 성공 → Iceberg `gold.delay_by_station` / `gold.delay_by_timeband` 갱신
- Grafana '서울 9호선 지연 분석' 대시보드에 반영(StarRocks 가 live 로 읽음)

## 메모

- 시간표는 자주 안 바뀌므로 `fetch_timetable` 만 주 1회로 빼고 싶으면 별도 DAG 로 분리 가능.
- spark-client 가 꺼져 있어도 각 태스크가 `docker start` 로 먼저 깨운다.
- 정리: `docker compose --profile orchestration down`
