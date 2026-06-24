# L1 — 도착 이벤트 (subway_arrival_events)

원본 폴링 로그(`bronze.subway_position_log`)에서 **열차별 역 전이**를 감지해
"도착 이벤트"를 추출한다. headway·구간소요·지연 분석의 공통 입력(L1)이 된다.

## 핵심 로직

한 열차를 시간순으로 보면 같은 역이 30초마다 여러 번 찍힌다(폴링 반복).
**"도착 = 그 열차가 새 역에 처음 나타난 순간"** 으로 정의하고, `statn_id` 가 바뀌는
전이를 감지해 그 역 첫 관측 시각을 도착시각으로 삼는다.

- `01-arrival-events.sql` — **Flink MATCH_RECOGNIZE**(train_no별 keyed CEP). 계획서의
  "keyed-state 상태전이 추출" 그대로. `PATTERN (A+) DEFINE A AS statn_id=FIRST(statn_id)`.
- `01b-arrival-events-fallback.sql` — MATCH_RECOGNIZE 가 막히면 쓰는 **LAG 윈도우** 버전(결과 동일).

## 실행

```bash
# (A) MATCH_RECOGNIZE 버전
docker compose --profile tools run --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/12-arrival-events/01-arrival-events.sql

# 에러나면 (B) 대체 버전
docker compose --profile tools run --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/12-arrival-events/01b-arrival-events-fallback.sql

# 확인
docker compose --profile tools run --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/12-arrival-events/02-query-arrivals.sql
```

`silver.subway_arrival_events` 생성: grain = `line × train_no × statn_id × arrival_ts`.

## 단순화/한계 (포트폴리오에 명시)

- **시차보정 미적용** — `recptn_dt`(수신시각)를 도착시각 근사로 사용. 서울시 권고의
  위치 보정은 향후 개선 항목.
- **폴링 30~40초** — 아주 짧은 정차는 1폴에만 잡힐 수 있음(그래도 `A+`는 1행도 도착으로 인정).
  10~20초 수집 시 정밀도 향상.
- **1호선 코레일 구간** — 서동탄·인천 등으로 이탈해 trainNo 가 사라지면 매치가 자연 종료될 뿐
  거짓 도착을 만들지 않음. "정상 소멸 vs 진짜 누락" 감지는 L2 freshness 마트에서 처리.

## 다음 단계

- 연속 도착으로 **구간 소요시간**(`subway_section_traversal`) 산출
- 같은 역·방향의 **연속 도착 간격 = headway** → L2 결정 마트(P50/P90/CV·계획대비 초과)
