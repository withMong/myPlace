# 시간표 차원 (dim_timetable) — 지연 분석의 기준

서울교통공사 운행 시간표 API(OA-101, `SearchSTNTimeTableByIDService`)로 9호선 **예정 시각**을
수집한다. 실시간이 아니라 거의 고정된 참조 데이터라 1회(또는 주 1회) 받아 차원으로 둔다.

## 실행

```bash
# 호스트에서 (인터넷 + 인증키 필요). sandbox 는 이 API 를 막으므로 로컬에서 실행.
SEOUL_API_KEY=본인키 python labs/08-timetable/fetch_timetable.py
# → data/timetable/line9_timetable.csv 생성
```

수집 흐름: `SearchSTNBySubwayLineInfo`(전체 역 → 09호선 필터)로 역코드 확보 →
각 역 × 요일유형(평일/토/휴일) × 방향(상/하행) 으로 시간표 호출 → CSV 저장.

## 수집 필드 (CSV)

| 컬럼 | 의미 |
|---|---|
| `station_cd` / `fr_code` / `station_nm` | 전철역코드 / 외부코드 / 역명 |
| `train_no` | 시간표상 열차번호 (예 `C9008`) |
| `arrive_time` / `left_time` | **예정 도착 / 출발 시각** (HH:MM:SS) |
| `origin_station` / `dest_station` | 출발역 / 종착역 코드 |
| `origin_nm` / `dest_nm` | 출발역명 / 종착역명 |
| `week_tag` | 1=평일, 2=토요일, 3=휴일/일요일 |
| `inout_tag` | 1=상행/내선, 2=하행/외선 |
| `express_yn` | 급행 여부 (값 분포로 확인) |
| `fl_flag` | 첫차/막차 플래그 |

## 지연 계산 — 실시간 ⨝ 시간표 조인 (배치/Spark)

실시간 위치(`subway_position_log`)와 시간표(`dim_timetable`)의 **공통 키 설계**:

- **역**: `station_nm` (역명) 으로 연결. 실시간 `statn_id` 와 시간표 `station_cd` 는 코드 체계가
  달라, 가장 안전한 다리는 역명이다. (필요 시 역명→station_cd 매핑 보강)
- **방향**: 실시간 `updn_line`(0상행/1하행) ↔ 시간표 `inout_tag`(1상행/2하행) 매핑.
- **요일유형**: 이벤트 날짜 → 평일/토/휴일 → `week_tag`.
- **열차/시각**: 시간표 `train_no`(`C9008`)와 실시간 `train_no`(`9585`)는 **포맷이 달라 직접
  매칭 불가**. 따라서 실제 도착시각에 **가장 가까운 예정 `arrive_time`** 으로 매칭한다.

→ `지연(초) = 실제 도착시각 − 가장 가까운 예정 도착시각`. 이를 시간대 버킷(새벽·출근·점심·
퇴근·밤) × 요일유형별로 집계해 **지연 발생 빈도·지연율**을 낸다. (다음 단계: Spark)
