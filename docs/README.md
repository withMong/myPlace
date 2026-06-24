# 서울 지하철 배차 안정성(headway) 분석 — 1·2·9호선

실시간 열차 위치 데이터를 직접 수집해, **출퇴근 시간대에 어느 노선·역·방향에서
배차 간격(headway)이 불안정해지는가**를 회고적으로 집계하는 하이브리드 데이터 파이프라인.

> **중점 질문.** "출퇴근 시간대에 어느 구간·시간대 배차가 불안정한가?"
> **이 답이 바꾸는 결정.** 어느 구간·시간대에 배차 조정/시간표 패딩이 필요한가(운영자),
> 또는 승객용 '대기 신뢰도'를 어떻게 표출할 것인가.

---

## 핵심 성격

- **운행 품질형 BI** — 승객·매출이 아니라 **열차 운행 상태 이벤트**(노선·열차번호·현재역·상태·방향·
  급행/막차·수신시각)를 다룬다. "공급(배차)이 규칙적인가"를 묻지, 수요(승하차)는 다루지 않는다.
- **백필 불가 → streaming의 정당성.** 실시간 위치 API는 "지금" 값만 주고 과거를 제공하지 않는다.
  지금 폴링해 쌓지 않으면 분석 데이터가 0이다. 그래서 **수집은 실시간(필연), 분석은 배치**다.
- **수집=실시간 / 분석=배치 하이브리드** — 실시간 수집의 운영 안정성과, 배치 집계의 의사결정
  산출물을 함께 보여준다.

---

## 아키텍처

```
[실시간 위치 API]  1·2·9호선, 러시아워 윈도우 폴링(30~40s)
       │ producer
       ▼
     Kafka  (subway-events, 30일 보관)
       │ Flink (STATEMENT SET fan-out)
       ▼
  Paimon L0 (MinIO)
   ├ subway_position_log     (append · 원본 폴링 · dt 파티션)
   └ subway_position_current (upsert · event_id · 현재 상태)
       │ Flink (keyed CEP: 역 전이 → 도착 추출)
       ▼
  Paimon L1  silver.subway_arrival_events  (열차×역×도착시각)
       │ Spark (배치 집계)
       ▼
  Iceberg L2 (gold)
   ├ subway_headway_by_station_tod  (역×방향×시간대 · P50/P90/CV ★)
   └ subway_service_freshness        (노선×분 · 수신 heartbeat)
       │ StarRocks (Iceberg External Catalog)
       ▼
     Streamlit BI  (불안정 역 Top N + freshness 패널)
```

메달리온(L0 원본 → L1 정제 → L2 집계 결정 마트) 구조다.

---

## 마트 설계 (계층 · grain)

| 마트 | 계층 | grain | 내용 |
|---|---|---|---|
| `subway_position_log` | L0 | poll 스냅샷 | 원본 위치(가공 전), 모든 폴링 보존 |
| `subway_position_current` | L0 | event_id | 디덥된 현재 상태 |
| `subway_arrival_events` | L1 | 열차×역×도착시각 | 역 전이 감지로 추출한 도착 이벤트 |
| `subway_headway_by_station_tod` ★ | L2 | 역×방향×시간대×요일유형 | headway P50/P90/CV·초과비율 |
| `subway_service_freshness` | L2 | 노선×분 | 수신 heartbeat/끊김(파이프라인 건강) |

---

## 핵심 지표

- **주지표** — 역·방향·시간대별 headway의 **변동계수(CV)**, P50/P90, 관측 중앙값 대비 1.5배 초과 비율.
  CV가 높을수록 배차가 들쭉날쭉(불안정).
- **보조지표** — 데이터 freshness(분당 수신 heartbeat / 윈도우 내 끊김).
- *headway는 지점(역)에서 측정한다.*

---

## 주요 결과 (1일 PoC)

**노선 구조가 배차 안정성을 좌우한다** — 평균 CV 기준:

| 노선 | 평균 CV | 해석 |
|---|---|---|
| **2호선** | ~0.25–0.31 (가장 안정) | 순환선 — 분기·급행 없음, 고빈도 |
| **9호선** | ~0.42–0.47 (중간) | 급행/완행 혼용 → 같은 역 불규칙 도착 |
| **1호선** | ~0.50–0.56 (가장 불안정) | 분기 노선(인천/서동탄 등) → 행선지 혼재 |

불안정 역 상위는 **9호선 급행 정차역**(가양·여의도·당산·고속터미널)과
**1호선 분기·환승역**(구로·신도림=경인/경부 분기점, 영등포·부평)에 집중 — 구조적 원인과 일치.

---

## 수집 전략 (쿼터·윈도우)

- 일일 API 한도 **1,000건/키**. 전노선 전일 폴링(≈수만 건)은 불가능 → **러시아워 윈도우** 수집.
- 출퇴근이 중점 질문이라, 윈도우 집중 수집이 quota와 분석 목적에 모두 맞다.
- 키 2개로 시간대 분담: **키1**(러시 — 출근 08–09·퇴근 18–19, 30초), **키2**(off-peak —
  새벽 05:30·점심 12–13·밤 23, 40초). Windows 작업 스케줄러가 producer를 윈도우마다 자동 기동.

---

## 데이터 품질 규칙

- **멱등** — `(열차번호, 역, 상태, 날짜)` 를 중복 수신 dedup 키로.
- **정상 vs 이상 구분** — 1호선 코레일 구간(서동탄·인천 등) 이탈로 열차번호가 소멸하는 것은
  정상 종료지 누락이 아니다. 도착 추출은 매치가 자연 종료될 뿐 거짓 도착을 만들지 않는다.
- **freshness/heartbeat** — 노선×분 수신을 추적해, 윈도우 내 끊김(파이프라인 hiccup)을 감지.

---

## 설계 의사결정

**왜 Bronze를 log/current 둘로?** 같은 Kafka 스트림을 Flink `STATEMENT SET`으로 한 번만 읽어
원본 로그(append)와 현재 상태(upsert)에 동시에 적재한다. 폴링 반복은 `current`에선 노이즈지만
`log`에선 **체류·도착 시각을 복원할 정보**다 — 디덥본과 원본을 둘 다 쥔다.

**왜 keyed CEP로 도착을 추출?** 30초 폴링이라 같은 역이 여러 번 찍힌다. 열차번호별로 상태(역)
전이를 추적해 "새 역에 처음 나타난 순간 = 도착"으로 잡는다(단순 행 카운트가 아님). 폴링
스냅샷에서 **실제 운행 궤적을 복원**한다.

**왜 분석은 배치?** 회고·집계형 질문이라 신선도보다 누적·재현성이 중요하다.

---

## 실행 방법

```bash
# 1) 인프라 + 토픽/버킷
docker compose up -d kafka kafka-init kafka-ui minio minio-init flink-jobmanager flink-taskmanager

# 2) Flink: Kafka → Paimon Bronze(log/current) 적재 (상시)
docker compose --profile tools run --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/04-flink-paimon/01-insert-subway-bronze-streaming.sql

# 3) 수집 (러시아워 윈도우) — scripts/win/*.bat + Windows 작업 스케줄러
#    또는 수동: docker compose --profile tools up -d subway-producer

# 4) L1 도착 이벤트 (배치)
docker compose --profile tools run --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/12-arrival-events/01b-arrival-events-fallback.sql

# 5) L2 마트 (Spark → Iceberg)
bash scripts/run-spark-headway.sh
bash scripts/run-spark-freshness.sh

# 6) StarRocks Iceberg 카탈로그 + Streamlit BI
docker compose exec -T starrocks-fe mysql -uroot -h starrocks-fe -P9030 \
  < labs/10-delay-olap/00-create-iceberg-catalog.sql
docker compose --profile bi up -d --build streamlit       # → http://localhost:8501
```

탐색·정밀화는 Jupyter(`labs/11-jupyter/headway_analysis.ipynb`)에서.

---

## 기술 스택

| 영역 | 도구 |
|---|---|
| 수집 | Python (requests, confluent-kafka) |
| 메시지 큐 | Apache Kafka (KRaft) |
| 스트림/CEP | Apache Flink (STATEMENT SET, MATCH_RECOGNIZE) |
| 레이크 L0/L1 | Apache Paimon (MinIO/S3) |
| 배치 집계 | Apache Spark |
| 테이블 포맷 L2 | Apache Iceberg (REST 카탈로그) |
| OLAP | StarRocks (Iceberg External Catalog) |
| BI | Streamlit |
| 탐색 | Jupyter (PySpark) |
| 컨테이너 | Docker Compose |

---

## 한계 (정직하게)

- **1일 PoC** — 평일 하루치라 요일유형 비교는 단순화(평일). 다일 수집 시 CV가 안정화된다.
- **폴링 30~40초** — 아주 짧은 정차·도착 전이를 일부 놓칠 수 있다(10~20초 수집 시 정밀도↑).
- **시차보정 미적용** — 수신시각을 도착시각 근사로 사용(서울시 권고의 위치 보정은 향후 개선).
- **계획 대비 초과비율** — 현재는 관측 중앙값 기준. 시간표 API 조인(스트레치) 시 '계획 대비'로 교체.
- **1호선 분기** — 행선지 혼재로 역 단위 headway 해석에 주의(향후 행선지/급행 분리 권장).
