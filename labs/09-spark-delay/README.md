# 배치 지연 분석 (Spark → Iceberg Silver/Gold)

`subway_position_log`(Paimon, 누적 원본)와 `dim_timetable`(시간표)을 조인해
시간대(새벽·출근·점심·퇴근·밤) × 요일유형(평일·토·휴일)별 **지연 발생·지연율**을 낸다.

## 사전 조건

1. 실시간 경로가 한동안 돌아 `position_log` 에 **도착(train_sttus=1) 이벤트**가 쌓여 있을 것.
   (지연 분석은 누적 이력이 있어야 의미가 있으므로 배치로 수행)
2. `data/timetable/line9_timetable.csv` 생성됨 (labs/08-timetable).

## 실행

```bash
bash scripts/run-spark-delay.sh
```

## 지연 계산 로직

- **실제 도착시각** = `position_log` 의 도착 이벤트(event_id) **첫 관측** `MIN(recptn_dt)`.
- **매칭** = 역명 + 방향(updn_line↔inout_tag) + 요일유형(요일→week_tag) 안에서
  **실제 도착에 가장 가까운 예정 `arrive_time`**. (train_no 포맷이 실시간/시간표 간 달라 직접 매칭 불가)
- **지연(초)** = 실제 − 예정. `DELAY_THRESHOLD_SEC`(기본 60초) 초과면 지연으로 카운트.
- 새벽 0~3시 도착은 서비스데이 연속성을 위해 +24h 로 맞춰 시간표 `24:xx` 와 정렬.

## 산출 테이블

| 테이블 | 내용 |
|---|---|
| `iceberg.dim.timetable` | 시간표 차원 (CSV 적재) |
| `iceberg.silver.arrival_delay` | 도착 이벤트별 지연(초)·지연여부·시간대·요일유형 |
| `iceberg.gold.delay_by_timeband` | 시간대×요일유형 집계: 도착수·지연수·지연율·평균/ p90 지연 |
| `iceberg.gold.delay_by_station` | 역별 집계: 도착수·지연수·지연율·평균지연 (연착 빈번 역 Top5 용) |

## 알려진 한계

- 공휴일은 별도 캘린더가 없어 **일요일=휴일**로 근사 (평일 공휴일은 평일로 잡힘).
- 시간표는 "예정"이라 실제와 차이가 있을 수 있음(API 명시).
- 첫 실행 시 jar 버전은 환경에 따라 미세 조정이 필요할 수 있음.
