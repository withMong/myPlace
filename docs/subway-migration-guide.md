# 서울 지하철용 전환 가이드

기존 commerce(Olist) Lakehouse를 **서울 지하철 실시간 위치** 데이터로 전환한 변경 사항 정리.
인프라(Kafka/Flink/MinIO/Iceberg/Spark/StarRocks/Airflow)는 그대로 두고,
데이터가 들어오는 입구(producer)와 Bronze 스키마만 교체했다.

## 추가/변경된 파일

| 파일 | 역할 |
|---|---|
| `labs/03-kafka-producer/subway_producer.py` | **신규** 지하철 API → Kafka producer (파일 읽기 대신 실시간 API 폴링) |
| `docker/producer/Dockerfile.subway` | **신규** producer 이미지에 `requests` 추가 |
| `labs/04-flink-paimon/00-create-subway-bronze.sql` | **신규** 지하철 Bronze 테이블 정의 |
| `labs/04-flink-paimon/01-insert-subway-bronze-streaming.sql` | **신규** Kafka → Paimon Bronze 스트리밍 적재 |
| `labs/04-flink-paimon/02-query-subway-bronze.sql` | **신규** 적재 확인 쿼리 |
| `docker-compose-lite.yml` | **수정** `subway-producer` 서비스 추가, 기본 토픽 `subway-events` 로 변경 |
| `.env.subway.example` | **신규** 환경변수 템플릿 |

기존 commerce 파일들은 그대로 보존했으므로 둘 다 사용 가능.

## 데이터 흐름

```
서울 지하철 API
   │  (subway_producer.py: 30초마다 폴링)
   ▼
Kafka  topic: subway-events   key: train_no
   │  (Flink SQL: 01-insert-subway-bronze-streaming.sql)
   ▼
Paimon Bronze  bronze.subway_events_bronze
   │  (이후 Spark → Iceberg Silver/Gold → StarRocks → BI)
   ▼
   ...
```

## 실행 순서

### 1. 환경변수 준비
```bash
cp .env.subway.example .env
# .env 열어서 SEOUL_API_KEY 입력
```

### 2. 핵심 인프라 기동
```bash
docker compose -f docker-compose-lite.yml up -d
# kafka, minio, iceberg, flink, spark, starrocks 가 뜸
```

### 3. 지하철 producer 실행 (tools 프로파일)
```bash
docker compose -f docker-compose-lite.yml --profile tools up -d subway-producer
docker logs -f de5-subway-producer    # fetched=NN sent=NN 확인
```

### 4. Flink 로 Bronze 적재
```bash
# SQL 클라이언트 접속
docker compose -f docker-compose-lite.yml --profile tools run --rm flink-sql-client

# 클라이언트 안에서:
#   SOURCE '/workspace/labs/04-flink-paimon/01-insert-subway-bronze-streaming.sql';
# (스트리밍 잡이 제출되고 계속 적재됨)
```

### 5. 적재 확인
```bash
docker compose -f docker-compose-lite.yml --profile tools run --rm flink-sql-client
#   SOURCE '/workspace/labs/04-flink-paimon/02-query-subway-bronze.sql';
```

### 6. Kafka UI 로 메시지 눈으로 확인
브라우저에서 http://localhost:8088 → topic `subway-events`

## 테스트 (Kafka 없이 producer 단독 점검)
```bash
# 로컬에서 API만 호출해 콘솔 출력 (Kafka 전송 안 함)
SEOUL_API_KEY=본인키 python labs/03-kafka-producer/subway_producer.py --once --dry-run
```

## 다음 단계 (commerce 버전 참고해 전환)
- `labs/05-spark-iceberg/transform_to_iceberg.py` → 지하철 Silver/Gold 집계로 수정
- `labs/07-realtime-olap/` → StarRocks 지하철 테이블/쿼리로 수정
- `labs/06-airflow-orchestration/dags` → 지하철 파이프라인 DAG 로 수정
