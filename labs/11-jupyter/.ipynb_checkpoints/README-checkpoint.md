# Jupyter(PySpark) — Lakehouse 직접 탐색

Spark 세션 하나로 **Iceberg(Silver/Gold)** 와 **Paimon(Bronze)** 를 모두 조회·변환한다.
StarRocks/Grafana 의 고정 집계 너머로, 분석가가 SQL·DataFrame 으로 자유 분석하는 환경.

## 실행

```bash
# 이미지 빌드(최초 1회) + 기동
docker compose --profile notebook up -d --build jupyter

# 브라우저에서 접속 (토큰 없음)
#   http://localhost:8888/lab  →  explore_iceberg.ipynb 열기
```

노트북 첫 셀이 SparkSession 을 만들며 커넥터 jar 를 ivy 로 받는다(지연 잡과 같은 캐시 공유 →
대개 즉시). netty SIGSEGV 방지 옵션과 Iceberg 벡터화 끔 설정이 세션에 들어가 있다.

## 들어 있는 것 (`explore_iceberg.ipynb`)

- Iceberg 네임스페이스/테이블 목록
- Gold 조회: 연착 빈번 역 Top5, 시간대×요일유형 지연율
- Silver 직접 변환 예시: 방향(상/하행)별 지연 통계
- Paimon Bronze(`subway_position_log`) 조회

## 메모

- 정리: `docker compose --profile notebook down`
- 포트 변경: `.env` 에 `JUPYTER_PORT=8889` 등
- Iceberg 데이터를 보려면 Spark 지연 잡(`run-spark-delay.sh`)이 한 번 돌아 있어야 한다.
