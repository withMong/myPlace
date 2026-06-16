# 실시간 경로 실행 가이드 (9호선)

```
서울 9호선 API
   │  subway_producer.py (POLL_INTERVAL 초마다 폴링)
   ▼
Kafka  topic: subway-events  (key: train_no)
   │  Flink SQL: 01-insert-subway-bronze-streaming.sql (STATEMENT SET, 무한 스트리밍)
   ▼
Paimon Bronze  s3://paimon/warehouse  (MinIO)
   ├─ bronze.subway_position_log     (append · 원본 로그 · 배치/지연분석용)
   └─ bronze.subway_position_current (upsert · 현재 상태 · 실시간용)
   │  StarRocks Paimon External Catalog (zero-copy, current 를 읽음)
   ▼
StarRocks  default_catalog.subway 의 분석 View
   │  MySQL 프로토콜(9030)
   ▼
Grafana  "서울 9호선 실시간 운행 현황" 대시보드
```

핵심 설계: Paimon warehouse 를 **MinIO(S3)** 에 둔다. Flink 가 쓰고 StarRocks 가 같은
버킷을 읽어, 데이터를 복사하지 않고도(zero-copy) 실시간 OLAP 이 가능하다. warehouse 를
로컬 `file:` 경로에 두면 StarRocks 컨테이너가 접근할 수 없다.

---

## 0. 사전 준비

`.env` 에 다음이 채워져 있어야 한다 (이미 설정됨):

```
SEOUL_API_KEY=<서울 열린데이터광장 인증키>
KAFKA_TOPIC=subway-events
SUBWAY_LINES=9호선
POLL_INTERVAL=30
```

## 1. 인프라 기동

```bash
docker compose up -d \
  kafka kafka-init kafka-ui minio minio-init \
  flink-jobmanager flink-taskmanager \
  starrocks-fe starrocks-cn
docker compose ps     # 모두 healthy / Up 확인
bash scripts/smoke-test.sh
```

## 2. 실시간 수집 시작 (producer)

```bash
docker compose --profile tools up -d subway-producer
docker logs -f subway-producer    # fetched=NN sent=NN 가 반복되면 정상
```

## 3. Flink: Kafka → Paimon Bronze (스트리밍 적재)

```bash
docker compose --profile tools run --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/04-flink-paimon/01-insert-subway-bronze-streaming.sql
# → 스트리밍 잡이 제출되고(Job ID 출력) 계속 적재된다. http://localhost:8081 에서 RUNNING 확인.
```

> 참고: SQL 클라이언트의 `SOURCE` 명령은 파일 실행에 쓰지 않는다(파싱 에러).
> 파일은 `sql-client.sh -f <파일>` 로 실행한다. 잡 상태는 다음으로도 볼 수 있다:
> `docker compose exec flink-jobmanager /opt/flink/bin/flink list -a`

적재 확인:

```bash
docker compose --profile tools run --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/04-flink-paimon/02-query-subway-bronze.sql
```

## 4. StarRocks: Paimon 카탈로그 + 분석 View

```bash
# 외부 카탈로그 생성 (Paimon warehouse 연결)
docker compose exec -T starrocks-fe \
  mysql -uroot -h starrocks-fe -P9030 \
  < labs/07-realtime-olap/00-create-paimon-catalog.sql

# (1회) 기본 스토리지 볼륨 등록 — shared_data 모드에서 내부 DB/View 생성에 필수
docker compose exec -T starrocks-fe \
  mysql -uroot -h starrocks-fe -P9030 \
  < labs/07-realtime-olap/00b-create-storage-volume.sql

# 9호선 분석 View 생성
docker compose exec -T starrocks-fe \
  mysql -uroot -h starrocks-fe -P9030 \
  < labs/07-realtime-olap/01-create-views.sql

# 샘플 쿼리 실행
docker compose exec -T starrocks-fe \
  mysql -uroot -h starrocks-fe -P9030 \
  < labs/07-realtime-olap/02-realtime-queries.sql
```

## 5. BI: Grafana 대시보드

```bash
docker compose --profile bi up -d grafana
```

- 접속: http://localhost:3000  (admin / admin, 익명 보기 허용)
- 대시보드: **Seoul Metro → 서울 9호선 실시간 운행 현황** (30초 자동 새로고침)
- 데이터소스(StarRocks, MySQL 프로토콜)와 대시보드는 provisioning 으로 자동 등록된다.

## 정리

```bash
docker compose --profile bi --profile tools down       # 컨테이너 중지
docker compose --profile bi --profile tools down -v     # 볼륨까지 삭제
```

---

## 트러블슈팅

- **StarRocks 에서 Paimon 테이블이 안 보임** → Flink 스트리밍 잡이 한 번이라도 체크포인트를
  찍어 커밋했는지 확인. `02-query-subway-bronze.sql` 로 Bronze 건수가 0 보다 큰지 먼저 본다.
- **`SHOW DATABASES FROM paimon_catalog` 에러** → `aws.s3.endpoint` 가 `http://minio:9000`
  (컨테이너 네트워크 이름)인지, path-style 접근이 켜져 있는지 확인.
- **`The default storage volume does not exist`** → shared_data 모드 StarRocks 는 내부 DB/View
  생성 전에 기본 스토리지 볼륨이 필요하다. `00b-create-storage-volume.sql` 을 먼저 실행한다.
- **Grafana 패널에 No data** → StarRocks 데이터소스의 database 가 `subway` 인지,
  View 가 생성됐는지(`SHOW TABLES FROM subway`) 확인.
- **producer no events** → 운행 종료 시간대에는 API 가 빈 결과를 줄 수 있다. 낮 시간대에 재시도.
