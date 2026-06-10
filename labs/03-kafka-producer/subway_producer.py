#!/usr/bin/env python3
"""서울 지하철 실시간 열차 위치 → Kafka producer.

기존 commerce producer(파일 읽기)와 달리, 서울 열린데이터광장 API를
주기적으로 호출해 실시간 이벤트를 Kafka로 흘려보낸다.

환경변수:
  SEOUL_API_KEY            서울 열린데이터광장 인증키 (필수)
  KAFKA_BOOTSTRAP_SERVER   기본 localhost:9092 (도커 내부는 kafka:19092)
  KAFKA_TOPIC              기본 subway-events
  KAFKA_KEY_FIELD          메시지 키로 쓸 필드 (기본 train_no)
  SUBWAY_LINES             수집할 호선, 콤마구분 (기본 1~9호선)
  POLL_INTERVAL            폴링 간격 초 (기본 30)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from confluent_kafka import Producer

BASE_URL = "http://swopenapi.seoul.go.kr/api/subway"
DEFAULT_LINES = "1호선,2호선,3호선,4호선,5호선,6호선,7호선,8호선,9호선"


class ProduceStats:
    def __init__(self, quiet: bool = False) -> None:
        self.delivered = 0
        self.failed = 0
        self.quiet = quiet

    def delivery_report(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failed += 1
            print(f"delivery failed: {err}", file=sys.stderr)
            return
        self.delivered += 1
        if self.quiet:
            return
        key = msg.key().decode("utf-8") if msg.key() else ""
        print(
            f"delivered topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()} key={key}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce Seoul subway realtime position events into Kafka."
    )
    parser.add_argument(
        "--bootstrap-server",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092"),
        help="Kafka bootstrap server. Use localhost:9092 on host, kafka:19092 in Docker.",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "subway-events"),
        help="Kafka topic name.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("SEOUL_API_KEY", ""),
        help="Seoul Open Data API key. Reads SEOUL_API_KEY env by default.",
    )
    parser.add_argument(
        "--lines",
        default=os.getenv("SUBWAY_LINES", DEFAULT_LINES),
        help="Comma-separated subway lines to poll.",
    )
    parser.add_argument(
        "--key-field",
        default=os.getenv("KAFKA_KEY_FIELD", "train_no"),
        help="Field used as Kafka message key. Empty string for no key.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("POLL_INTERVAL", "30")),
        help="Polling interval in seconds.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Poll all lines once and exit (good for testing / batch).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print events without producing to Kafka.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-message delivery logs.",
    )
    return parser.parse_args()


def fetch_line(api_key: str, line: str, start: int = 0, end: int = 100) -> list[dict[str, Any]]:
    """한 호선의 실시간 위치 목록을 반환."""
    url = f"{BASE_URL}/{api_key}/json/realtimePosition/{start}/{end}/{line}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # API 에러 처리
    if "errorMessage" in data:
        msg = data["errorMessage"]
        code = msg.get("code")
        if code not in ("INFO-000", None):
            # INFO-200(데이터 없음)은 빈 리스트로 취급, 그 외는 예외
            if code == "INFO-200":
                return []
            raise RuntimeError(f"API error [{code}] {msg.get('message')} (line={line})")

    return data.get("realtimePositionList", [])


def to_event(raw: dict[str, Any], line: str) -> dict[str, Any]:
    """API 응답 1건을 Kafka로 보낼 표준 이벤트로 변환.

    event_id 설계:
      recptnDt가 폴링마다 갱신되는 "수신시각"이라 시각 기반 키는 중복을 못 막는다.
      대신 (열차번호 + 역 + 상태 + 날짜)로 키를 만들어,
      같은 열차가 같은 역에 같은 상태로 머무는 동안에는 1건으로 묶고(중복 제거),
      다음 역으로 가거나 상태가 바뀌면(도착→출발) 새 이벤트로 기록한다.
      날짜를 섞어 다른 날 같은 역을 지나갈 때 이전 기록을 덮어쓰지 않게 한다.
    """
    train_no = raw.get("trainNo", "")
    recptn_dt = raw.get("recptnDt", "")
    statn_id = raw.get("statnId", "")
    train_sttus = raw.get("trainSttus", "")
    event_date = recptn_dt[:10]  # 'YYYY-MM-DD'
    event_id = f"{train_no}-{statn_id}-{train_sttus}-{event_date}"

    return {
        "event_id": event_id,
        "line": line,
        "subway_id": raw.get("subwayId"),
        "train_no": train_no,
        "statn_id": raw.get("statnId"),
        "statn_nm": raw.get("statnNm"),
        "statn_tnm": raw.get("statnTnm"),
        "updn_line": raw.get("updnLine"),       # 0:상행/내선 1:하행/외선
        "train_sttus": raw.get("trainSttus"),   # 0진입 1도착 2출발 3전역출발
        "direct_at": raw.get("directAt"),       # 1급행
        "lstcar_at": raw.get("lstcarAt"),       # 1막차
        "recptn_dt": recptn_dt,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def poll_all_lines(api_key: str, lines: list[str]) -> list[dict[str, Any]]:
    """모든 호선을 1회 폴링해서 표준 이벤트 리스트로 반환."""
    events: list[dict[str, Any]] = []
    for line in lines:
        try:
            for raw in fetch_line(api_key, line):
                events.append(to_event(raw, line))
        except Exception as exc:  # 한 호선 실패가 전체를 막지 않도록
            print(f"  [{line}] fetch failed: {exc}", file=sys.stderr)
    return events


def make_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "de5-subway-events-producer",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "compression.type": "snappy",
        }
    )


def send_events(
    producer: Producer | None,
    topic: str,
    key_field: str,
    events: list[dict[str, Any]],
    stats: ProduceStats,
    dry_run: bool,
) -> int:
    sent = 0
    for event in events:
        key_value = event.get(key_field) if key_field else None
        key = str(key_value) if key_value is not None else None
        value = json.dumps(event, ensure_ascii=False, separators=(",", ":"))

        if dry_run:
            print(f"dry-run key={key or ''} value={value}")
        else:
            assert producer is not None
            while True:
                try:
                    producer.produce(
                        topic,
                        key=key,
                        value=value.encode("utf-8"),
                        callback=stats.delivery_report,
                    )
                    break
                except BufferError:
                    producer.poll(1)
            producer.poll(0)
        sent += 1
    return sent


def run(args: argparse.Namespace) -> int:
    if not args.api_key or args.api_key == "your_key_here":
        print("SEOUL_API_KEY is not set. Pass --api-key or set the env var.", file=sys.stderr)
        return 1

    lines = [x.strip() for x in args.lines.split(",") if x.strip()]
    stats = ProduceStats(quiet=args.quiet)
    producer = None if args.dry_run else make_producer(args.bootstrap_server)

    def one_cycle() -> int:
        now = datetime.now().strftime("%H:%M:%S")
        events = poll_all_lines(args.api_key, lines)
        if not events:
            print(f"[{now}] no events fetched")
            return 0
        n = send_events(producer, args.topic, args.key_field, events, stats, args.dry_run)
        if producer is not None:
            producer.flush(30)
        print(f"[{now}] fetched={len(events)} sent={n} topic={args.topic}")
        return n

    if args.once:
        one_cycle()
        return 0

    print(
        f"streaming subway events → {args.topic} "
        f"(interval={args.interval}s, lines={len(lines)}). Ctrl+C to stop.\n"
    )
    try:
        while True:
            one_cycle()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    return 0


def main() -> int:
    args = parse_args()
    if args.interval < 0:
        print("--interval must be >= 0", file=sys.stderr)
        return 1
    try:
        return run(args)
    except Exception as exc:
        print(f"producer failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
