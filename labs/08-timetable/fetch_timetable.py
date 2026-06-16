#!/usr/bin/env python3
"""서울 9호선 운행 시간표(OA-101) → CSV 차원 데이터 수집기.

실시간 위치 API와 달리 시간표는 거의 고정된 **참조 데이터**라, 계속 폴링하지 않고
1회(또는 주 1회) 받아 차원 테이블(dim_timetable)로 둔다. Kafka 를 거치지 않는다.

수집 흐름:
  1) SearchSTNBySubwayLineInfo 로 전체 역 목록을 받아 09호선만 필터 → 역코드 확보
  2) 각 역 × WEEK_TAG(1평일,2토,3휴일) × INOUT_TAG(1상행,2하행) 로
     SearchSTNTimeTableByIDService 호출 → 예정 시각표 수집
  3) CSV 로 저장 (이후 Spark 가 Iceberg dim_timetable 로 적재)

환경변수:
  SEOUL_API_KEY   서울 열린데이터광장 인증키 (필수)
  SUBWAY_LINE     수집할 호선명 (기본 '09호선')
  OUT_PATH        출력 CSV 경로 (기본 data/timetable/line9_timetable.csv)

사용:
  SEOUL_API_KEY=본인키 python labs/08-timetable/fetch_timetable.py
"""
from __future__ import annotations

import csv
import os
import sys
import time
import urllib.request
import json
from typing import Any

BASE = "http://openapi.seoul.go.kr:8088"

# 시간표 응답에서 보존할 필드 (원본 그대로 + line 태그)
FIELDS = [
    "line", "station_cd", "fr_code", "station_nm", "train_no",
    "arrive_time", "left_time", "origin_station", "dest_station",
    "origin_nm", "dest_nm", "week_tag", "inout_tag",
    "express_yn", "fl_flag", "branch_line",
]

WEEK_TAGS = ["1", "2", "3"]   # 1=평일, 2=토요일, 3=휴일/일요일
INOUT_TAGS = ["1", "2"]       # 1=상행/내선, 2=하행/외선


def get_json(url: str, retries: int = 3) -> dict[str, Any]:
    last = None
    for i in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"GET 실패: {url} ({last})")


def fetch_line_stations(key: str, line: str) -> list[dict[str, str]]:
    """전체 역 목록에서 해당 호선만 필터해 반환."""
    url = f"{BASE}/{key}/json/SearchSTNBySubwayLineInfo/1/1000/"
    data = get_json(url)
    rows = data.get("SearchSTNBySubwayLineInfo", {}).get("row", [])
    stations = [
        {"station_cd": r["STATION_CD"], "fr_code": r.get("FR_CODE", ""), "station_nm": r["STATION_NM"]}
        for r in rows
        if r.get("LINE_NUM") == line
    ]
    # 같은 역이 중복될 수 있어 station_cd 기준 dedup
    uniq = {s["station_cd"]: s for s in stations}
    return sorted(uniq.values(), key=lambda s: s["station_cd"])


def fetch_timetable(key: str, station_cd: str, week: str, inout: str) -> list[dict[str, Any]]:
    """한 역 × 요일유형 × 방향의 예정 시각표(최대 1000행)."""
    url = f"{BASE}/{key}/json/SearchSTNTimeTableByIDService/1/1000/{station_cd}/{week}/{inout}"
    data = get_json(url)
    svc = data.get("SearchSTNTimeTableByIDService")
    if not svc:  # INFO-200(데이터 없음) 등
        return []
    return svc.get("row", [])


def to_record(line: str, raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "line": line,
        "station_cd": raw.get("STATION_CD", ""),
        "fr_code": raw.get("FR_CODE", ""),
        "station_nm": raw.get("STATION_NM", ""),
        "train_no": raw.get("TRAIN_NO", ""),
        "arrive_time": raw.get("ARRIVETIME", ""),
        "left_time": raw.get("LEFTTIME", ""),
        "origin_station": raw.get("ORIGINSTATION", ""),
        "dest_station": raw.get("DESTSTATION", ""),
        "origin_nm": raw.get("SUBWAYSNAME", ""),
        "dest_nm": raw.get("SUBWAYENAME", ""),
        "week_tag": raw.get("WEEK_TAG", ""),
        "inout_tag": raw.get("INOUT_TAG", ""),
        "express_yn": raw.get("EXPRESS_YN", ""),
        "fl_flag": raw.get("FL_FLAG", ""),
        "branch_line": raw.get("BRANCH_LINE", ""),
    }


def main() -> int:
    key = os.getenv("SEOUL_API_KEY", "").strip()
    if not key:
        print("SEOUL_API_KEY 가 비어 있습니다. 환경변수로 키를 넣어주세요.", file=sys.stderr)
        return 1
    line = os.getenv("SUBWAY_LINE", "09호선")
    out_path = os.getenv("OUT_PATH", "data/timetable/line9_timetable.csv")

    print(f"[1/3] {line} 역 목록 조회...")
    stations = fetch_line_stations(key, line)
    print(f"      {len(stations)}개 역")
    if not stations:
        print("역을 못 찾았습니다. SUBWAY_LINE 값(예: '09호선')을 확인하세요.", file=sys.stderr)
        return 1

    records: list[dict[str, Any]] = []
    total = len(stations) * len(WEEK_TAGS) * len(INOUT_TAGS)
    done = 0
    print(f"[2/3] 시간표 수집 (호출 {total}회)...")
    for s in stations:
        for week in WEEK_TAGS:
            for inout in INOUT_TAGS:
                rows = fetch_timetable(key, s["station_cd"], week, inout)
                records.extend(to_record(line, r) for r in rows)
                done += 1
                time.sleep(0.1)  # API 예의상 간격
        print(f"      {s['station_nm']}({s['station_cd']}) 누적 {len(records)}행")

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    print(f"[3/3] CSV 저장 → {out_path}")
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(records)

    print(f"완료: {len(records)}행 / {len(stations)}역  →  {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
