# 서울 지하철 실시간 데이터 Lakehouse

서울시 지하철 실시간 열차 위치 데이터를 수집·처리·저장·분석하는 스트리밍 데이터 파이프라인.
공공 API에서 흘러나오는 실시간 이벤트를 Kafka로 받아 Flink로 처리하고, Lakehouse(Paimon/Iceberg)에
적재한 뒤 OLAP 엔진(StarRocks)으로 조회하는 구조입니다.

> **이 프로젝트를 만든 이유.** 서울 지하철 실시간 위치 API는 매 분 값이 바뀌고 **과거 데이터를 제공하지 않습니다.**
> 즉 지금 수집하지 않으면 영영 사라지는 데이터입니다. 정적 CSV를 내려받아 분석하는 흔한 프로젝트와 달리,
> "직접 수집해서 쌓아야만 분석할 데이터가 생긴다"는 점이 실시간 파이프라인을 만들 명확한 명분이 됩니다.

---

## 아키텍처

```
서울 지하철 API
   │  (subway_producer.py — 60초마다 폴링)
   ▼
Kafka  (topic: subway-events, 3 partitions)
   │
   ├─ 실시간 경로 ─────────────────┐
   │                               │
   ▼                               ▼
Flink (스트림 처리)            StarRocks (OLAP)
   │                               │
   ▼                               ▼
Paimon (Bronze)                  BI / 대시보드
   │
   └─ 배치 경로 ──► Airflow ──► Spark ──► Iceberg (Silver/Gold) ──► StarRocks ──► BI
```

메달리온 아키텍처(Bronze → Silver → Gold)에 스트리밍·배치 경로를 함께 태운 구성입니다.

---

## Architecture
<img width="662" height="653" alt="image" src="https://github.com/user-attachments/assets/a2e69ae2-c1e0-498c-8862-8a6204b058c7" />


## 설계 의사결정 (왜 이렇게 만들었나)

이 프로젝트에서 한 주요 판단들과 그 이유를 정리합니다. 도구 선택보다 **왜 그 선택을 했는지**가
이 프로젝트의 핵심입니다.

### 1. 왜 DB 직접 적재가 아니라 Kafka를 거치는가

API에서 받은 데이터를 곧장 DB에 넣을 수도 있었습니다. 하지만 그러면:

- 수집기가 죽으면 그 사이 데이터는 유실됩니다.
- 데이터를 쓰려는 소비자가 늘어날 때(실시간 대시보드 + 배치 집계 등) 서로 간섭합니다.

Kafka를 **완충 지대**로 두면 들어온 이벤트를 일단 안전하게 보관하고, 여러 소비자가 각자 속도로
가져갈 수 있습니다. **수집과 처리를 분리**하는 것이 실시간 시스템의 기본이라 Kafka를 택했습니다.

### 2. 왜 Bronze / Silver / Gold로 나누는가

- **Bronze**: API 원본을 가공 없이 그대로 적재. 절대 수정하지 않습니다.
- **Silver/Gold**: Bronze를 기반으로 정제·집계한 결과.

원본을 보존하면 집계 로직이 틀려도 Bronze에서 언제든 다시 만들 수 있습니다. 가공본만 들고 있다가
로직 오류를 발견하면 복구가 불가능합니다.

### 3. event_id를 (열차번호 + 역 + 상태 + 날짜)로 설계한 이유

가장 고민한 부분입니다. API의 `recptnDt`(수신시각)는 폴링할 때마다 새 값으로 갱신됩니다.
그래서 처음 설계한 `event_id = 열차번호 + 수신시각`은, 같은 열차가 같은 역에 멈춰 있어도
폴링마다 다른 키가 생겨 **같은 상태가 중복 적재**되는 문제가 있었습니다.

이를 `event_id = 열차번호 + 역ID + 상태 + 날짜`로 바꿔서:

- 같은 열차가 같은 역에 같은 상태로 머무는 동안에는 **1건으로 묶이고**(중복 제거),
- 다음 역으로 이동하거나 상태가 바뀌면(도착→출발) **새 이벤트로 기록**됩니다.

날짜를 키에 포함해, 다른 날 같은 역을 다시 지날 때 이전 기록을 덮어쓰지 않게 했습니다.
중복 제거 자체는 Bronze 테이블의 PRIMARY KEY(upsert)가 담당하며, Kafka에는 원본을 그대로 보냅니다.

### 4. 왜 Docker Compose로 묶는가

Kafka·Flink·MinIO·StarRocks 등 다수의 도구를 개별 설치하면 환경 구축만으로 큰 비용이 듭니다.
컨테이너로 묶으면 `docker compose up` 한 번으로 전체가 뜨고, 누구나 동일하게 재현할 수 있습니다.
**재현 가능성**을 위해 전 구성을 Compose로 정의했습니다.

---

## 기술 스택

| 영역 | 도구 |
|---|---|
| 수집 | Python (requests, confluent-kafka) |
| 메시지 큐 | Apache Kafka (KRaft 모드) |
| 스트림 처리 | Apache Flink |
| 레이크 (Bronze) | Apache Paimon |
| 오브젝트 스토리지 | MinIO (S3 호환) |
| 테이블 포맷 (Silver/Gold) | Apache Iceberg |
| 배치 처리 | Apache Spark |
| OLAP | StarRocks |
| 오케스트레이션 | Apache Airflow |
| 컨테이너 | Docker Compose |

---

## 진행 현황

| 단계 | 상태 |
|---|---|
| 서울 API 실시간 수집 (producer) | ✅ 완료 |
| producer → Kafka 전송 (3 partition 분산) | ✅ 완료 |
| Docker 인프라(Kafka, MinIO) 기동 + smoke test | ✅ 완료 |
| Kafka → Flink → Paimon Bronze 적재 | 🔜 진행 예정 |
| Spark → Iceberg (Silver/Gold) 집계 | 🔜 예정 |
| StarRocks → BI 대시보드 | 🔜 예정 |
| Airflow 오케스트레이션 | 🔜 예정 |

---

## 실행 방법

### 사전 준비

- Docker / Docker Compose
- 서울 열린데이터광장 인증키 ([발급](https://data.seoul.go.kr))

### 1. 환경변수 설정

```bash
cp .env.subway.example .env
# .env 를 열어 SEOUL_API_KEY 에 본인 인증키 입력
```

### 2. 인프라 기동

```bash
docker compose up -d kafka kafka-init kafka-ui minio minio-init
docker compose ps          # kafka, minio 가 healthy 인지 확인
```

### 3. 동작 점검 (smoke test)

```bash
bash scripts/smoke-test.sh
```

컨테이너 상태 / `subway-events` 토픽 / Kafka UI / MinIO 버킷을 확인합니다.

### 4. 실시간 수집 시작

```bash
docker compose --profile tools up -d subway-producer
docker logs -f subway-producer    # fetched=.. sent=.. 가 60초마다 반복되면 정상
```

### 5. 데이터 확인

- **Kafka UI**: http://localhost:8088 → Topics → `subway-events` → Messages
- **CLI**:
  ```bash
  docker compose exec kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:19092 \
    --topic subway-events --from-beginning --max-messages 5
  ```

### 정리

```bash
docker compose --profile tools down       # 컨테이너 중지
docker compose --profile tools down -v     # 볼륨(데이터)까지 삭제
```

---

## 이벤트 스키마

producer가 Kafka로 보내는 이벤트 형식:

| 필드 | 설명 |
|---|---|
| `event_id` | 중복 제거용 키 (열차번호-역ID-상태-날짜) |
| `line` | 호선명 (예: 2호선) |
| `train_no` | 열차번호 (Kafka 메시지 키) |
| `statn_nm` | 현재 역명 |
| `statn_tnm` | 종착역명 |
| `updn_line` | 0=상행/내선, 1=하행/외선 |
| `train_sttus` | 0=진입, 1=도착, 2=출발, 3=전역출발 |
| `direct_at` | 1=급행 |
| `lstcar_at` | 1=막차 |
| `recptn_dt` | API 수신 시각 |
| `ingested_at` | 수집 시각 (UTC) |

---

## 프로젝트 구조

```
.
├── docker-compose.yml              # 전체 스택 정의
├── .env.subway.example             # 환경변수 템플릿
├── docker/producer/                # producer 이미지 빌드
├── labs/
│   ├── 03-kafka-producer/          # 지하철 → Kafka producer
│   └── 04-flink-paimon/            # Flink Bronze 적재 SQL
├── scripts/                        # 운영/점검 스크립트
└── docs/                           # 문서
```

---

## 참고

이 프로젝트는 이커머스 데이터 기반의 Lakehouse 실습 템플릿을 서울 지하철 실시간 데이터로
전환하며 시작했습니다. 인프라 구조는 활용하되, 데이터 소스(파일 → 실시간 API)와 스키마,
중복 제거 로직 등 데이터가 흐르는 부분을 새로 설계했습니다. 전환 내역은
[docs/subway-migration-guide.md](docs/subway-migration-guide.md) 참고.