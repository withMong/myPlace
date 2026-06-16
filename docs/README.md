# 서울 지하철 9호선 실시간·배치 데이터 Lakehouse

서울 지하철 9호선 실시간 열차 위치 데이터를 수집·처리·저장·분석하는 데이터 파이프라인.
공공 API에서 흘러나오는 실시간 이벤트를 Kafka로 받아 Flink로 처리해 Lakehouse(Paimon·Iceberg)에
적재하고, OLAP 엔진(StarRocks)으로 조회합니다. **실시간**과 **배치** 두 경로를 함께 둡니다.

- **실시간 경로 — "지금"을 본다.** 열차의 현재 위치와 상태(진입/도착/출발), 운행 중 열차 수,
  급행/완행 구분, 상·하행 분포 등 *현재 운행 현황*을 StarRocks → 대시보드로 보여줍니다.
- **배치 경로 — "쌓인 이력"을 분석한다.** 직접 누적해온 과거 위치 데이터와 별도로 내려받은
  운행 시간표(예정 시각)를 견줘, 시간대(새벽·출근·점심·퇴근·밤)별 **지연 발생과 지연율**을 집계합니다.

> **이 프로젝트를 만든 이유.** 서울 지하철 실시간 위치 API는 매 분 값이 바뀌고 **과거 데이터를 제공하지 않습니다.**
> 즉 지금 수집하지 않으면 영영 사라지는 데이터입니다. 그래서 실시간으로 직접 수집해 쌓고(실시간 경로),
> 그렇게 **누적된 이력을 시간표 기준과 견줘 지연을 분석**합니다(배치 경로). 정적 CSV를 내려받아 분석하는
> 흔한 프로젝트와 달리, "직접 수집해서 쌓아야만 분석할 데이터가 생긴다"는 점이 이 파이프라인의 명분입니다.

---

## 아키텍처

```
[실시간 위치 API] ──producer(주기 폴링)──► Kafka (topic: subway-events, key: train_no)
                                              │
                                              ▼  Flink STATEMENT SET
                                  (Kafka 를 1회 읽어 두 Bronze 로 fan-out)
                       ┌──────────────────────┴──────────────────────┐
                       ▼                                             ▼
            Paimon: subway_position_log              Paimon: subway_position_current
            (append · 원본 이력 · dt 파티션)            (upsert · PK=event_id · 현재 상태)
                       │                                             │
                       │                                             └─ 실시간 경로 ─►
                       │                                                StarRocks(External Catalog) ─► Grafana
                       │                                                "현재 위치·상태·운행 수·급행/완행"
                       │
                       └─ 배치 경로 ─► Spark  ( subway_position_log ⨝ dim_timetable )
                             ▲                  → 시간대(새벽·출근·점심·퇴근·밤)별 지연 발생·지연율
                             │                  → Iceberg (Silver/Gold) ─► StarRocks ─► BI
                  [운행 시간표 API · OA-101]        (Airflow 오케스트레이션)
                  1회/주 1회 수집 → dim_timetable
```

핵심 포인트 셋:

1. **Paimon warehouse 를 MinIO(S3)** 에 둔다 → Flink 가 적재한 테이블을 StarRocks 가 같은 버킷에서
   바로 읽어(External Catalog) 데이터 복사 없이 실시간 OLAP.
2. **Bronze 를 둘로** — 같은 Kafka 스트림을 Flink `STATEMENT SET` 으로 한 번만 읽어, 원본 로그
   (`subway_position_log`, append)와 현재 상태(`subway_position_current`, upsert)에 동시에 적재.
3. **지연 분석은 배치** — 누적된 `subway_position_log` 를 시간표 차원(`dim_timetable`)과 조인해
   *실제 − 예정* 으로 지연을 계산하고, 시간대 버킷별로 지연율을 집계.

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

### 3. 왜 Bronze를 log(append)와 current(upsert) 둘로 나누는가

같은 Kafka 스트림에서 성격이 다른 두 가지를 동시에 원합니다.

- **`subway_position_log` (append)**: 폴링마다 들어온 **모든** 레코드를 그대로 쌓는 불변 원본 로그.
  PK 없음, 날짜(`dt`) 파티션. `recptnDt`가 폴링마다 갱신돼 같은 열차·역이 여러 번 쌓이는데, 이건
  노이즈가 아니라 **정보**입니다 — 열차가 그 역에 얼마나 머물렀는지(체류·지연)를 복원할 수 있습니다.
- **`subway_position_current` (upsert)**: `PRIMARY KEY(event_id)`로 디덥된 현재 상태. 같은 열차·역·
  상태·날짜는 1건으로 수렴해, 실시간 대시보드가 가볍게 "지금 현황"을 조회합니다.

Flink `STATEMENT SET`으로 Kafka를 **한 번만 읽어** 두 테이블에 fan-out 합니다. 디덥을 Bronze에
박아 원본을 잃는 대신, **원본(log)과 정제본(current)을 둘 다 손에 쥐는** 구조입니다.

### 4. event_id를 (열차번호 + 역 + 상태 + 날짜)로 설계한 이유

`subway_position_current`의 디덥 키입니다. API의 `recptnDt`(수신시각)는 폴링마다 갱신돼서,
처음 설계한 `event_id = 열차번호 + 수신시각`은 같은 열차가 같은 역에 멈춰 있어도 폴링마다 다른
키가 생겨 **같은 상태가 중복 적재**됐습니다.

이를 `event_id = 열차번호 + 역ID + 상태 + 날짜`로 바꿔서:

- 같은 열차가 같은 역에 같은 상태로 머무는 동안에는 **1건으로 묶이고**(중복 제거),
- 다음 역으로 이동하거나 상태가 바뀌면(도착→출발) **새 이벤트로 기록**됩니다.

날짜를 키에 포함해, 다른 날 같은 역을 다시 지날 때 이전 기록을 덮어쓰지 않게 했습니다. 디덥은
`current`의 PRIMARY KEY(upsert)가 담당하고, `log`에는 모든 폴링이 그대로 남습니다.

### 5. 지연을 어떻게 계산하는가 (시간표 차원과의 조인)

실시간 위치 API에는 **지연 필드가 없습니다** — 위치·상태만 줍니다. 그래서 "예정 시각" 기준이
따로 필요합니다.

- **시간표 차원 `dim_timetable`**: 서울교통공사 운행 시간표 API(OA-101)로 9호선 역별 예정 시각을
  수집합니다. 실시간 스트림이 아니라 거의 고정된 **참조 데이터**라, 1회(또는 주 1회) 받아 차원
  테이블로 둡니다. Kafka를 거치지 않습니다.
- **지연 = 실제 − 예정**: 배치(Spark)에서 누적된 `subway_position_log`의 도착 이벤트를
  `dim_timetable`과 **역·방향·요일유형(평일/주말)** 으로 조인해 지연을 계산하고, 시간대
  버킷(새벽·출근·점심·퇴근·밤)별로 **지연 발생 빈도·지연율**을 집계합니다.

지연 분석은 누적 이력이 있어야 의미가 있으므로 **배치 경로**에서 수행합니다.

### 6. 왜 Docker Compose로 묶는가

Kafka·Flink·MinIO·StarRocks 등 다수의 도구를 개별 설치하면 환경 구축만으로 큰 비용이 듭니다.
컨테이너로 묶으면 `docker compose up` 한 번으로 전체가 뜨고, 누구나 동일하게 재현할 수 있습니다.
**재현 가능성**을 위해 전 구성을 Compose로 정의했습니다.

---

## 기술 스택

| 영역 | 도구 |
|---|---|
| 수집 | Python (requests, confluent-kafka) |
| 참조데이터 | 운행 시간표 API(OA-101) → `dim_timetable` (배치) |
| 메시지 큐 | Apache Kafka (KRaft 모드) |
| 스트림 처리 | Apache Flink (STATEMENT SET fan-out) |
| 레이크 (Bronze) | Apache Paimon — `position_log`(append) + `position_current`(upsert) |
| 오브젝트 스토리지 | MinIO (S3 호환) |
| 테이블 포맷 (Silver/Gold) | Apache Iceberg |
| 배치 처리 | Apache Spark (지연 분석) |
| OLAP | StarRocks (Paimon External Catalog) |
| BI / 대시보드 | Grafana (실시간) |
| 오케스트레이션 | Apache Airflow |
| 컨테이너 | Docker Compose |

---

## 진행 현황

| 단계 | 상태 |
|---|---|
| 서울 API 실시간 수집 (producer, 9호선) | ✅ 완료 |
| producer → Kafka 전송 (3 partition 분산) | ✅ 완료 |
| Docker 인프라(Kafka, MinIO) 기동 + smoke test | ✅ 완료 |
| Kafka → Flink → Paimon Bronze 적재 (MinIO/S3) | ✅ 완료 (검증) |
| StarRocks Paimon 카탈로그 + 9호선 분석 View | ✅ 완료 (검증) |
| Grafana 실시간 대시보드 (StarRocks → BI) | ✅ 완료 (검증) |
| Bronze 분리 재설계 — `position_log`(append) + `position_current`(upsert) | 🔜 진행 |
| 시간표 차원 `dim_timetable` 수집 (OA-101) | 🔜 예정 |
| Spark 지연 분석 — log ⨝ timetable, 시간대별 지연율 → Iceberg(Silver/Gold) | 🔜 예정 |
| Jupyter(PySpark)로 Iceberg Silver/Gold 직접 탐색·변환 | 🔜 예정 |
| Airflow 오케스트레이션 | 🔜 예정 |

> 실시간 경로(producer → Kafka → Flink → Paimon → StarRocks → Grafana)가 end-to-end 로 연결되었습니다.
> 실행 순서는 [docs/realtime-pipeline-guide.md](realtime-pipeline-guide.md) 참고.

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