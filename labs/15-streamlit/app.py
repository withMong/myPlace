"""서울 지하철 배차 안정성 BI (Streamlit → StarRocks → Iceberg gold).

데이터: iceberg_catalog.gold.subway_headway_by_station_tod (배차 안정성)
        iceberg_catalog.gold.subway_service_freshness     (파이프라인 freshness)
사전: StarRocks 에 Iceberg 외부 카탈로그(iceberg_catalog)가 생성돼 있어야 함
      (labs/10-delay-olap/00-create-iceberg-catalog.sql).
"""
import os

import pandas as pd
import pymysql
import streamlit as st

HOST = os.getenv("STARROCKS_HOST", "starrocks-fe")
PORT = int(os.getenv("STARROCKS_PORT", "9030"))
USER = os.getenv("STARROCKS_USER", "root")
HW = "iceberg_catalog.gold.subway_headway_by_station_tod"
FR = "iceberg_catalog.gold.subway_service_freshness"


@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password="", charset="utf8mb4")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


st.set_page_config(page_title="서울 지하철 배차 안정성", layout="wide")
st.title("🚇 서울 지하철 배차 안정성 — 1·2·9호선")
st.caption("출퇴근 시간대에 어느 노선·역·방향에서 배차 간격(headway)이 불안정한가? (CV=변동계수, 높을수록 불안정)")

# ── 사이드바 필터 ──
st.sidebar.header("필터")
line = st.sidebar.selectbox("노선", ["(전체)", "1호선", "2호선", "9호선"])
band = st.sidebar.selectbox("시간대", ["(전체)", "출근", "점심", "퇴근", "새벽", "밤"])
topn = st.sidebar.slider("Top N 역", 5, 50, 20)
min_n = st.sidebar.slider("최소 표본 수", 3, 30, 10)
line_f = "" if line == "(전체)" else f" AND line = '{line}'"
band_f = "" if band == "(전체)" else f" AND time_band = '{band}'"

try:
    # ── 1) 헤드라인: 노선·시간대별 평균 CV (구조적 스토리) ──
    st.subheader("① 노선·시간대별 배차 변동성 (평균 CV)")
    cv = q(
        f"""
        SELECT line, time_band, ROUND(AVG(cv),3) AS avg_cv,
               ROUND(AVG(p50_sec),0) AS avg_headway_sec, COUNT(*) AS n_groups
        FROM {HW}
        WHERE headway_samples >= {min_n} {line_f} {band_f}
        GROUP BY line, time_band ORDER BY line, time_band
        """
    )
    c1, c2 = st.columns([3, 2])
    with c1:
        if not cv.empty:
            pivot = cv.pivot(index="line", columns="time_band", values="avg_cv").fillna(0)
            st.bar_chart(pivot)
    with c2:
        st.dataframe(cv, use_container_width=True, hide_index=True)
    st.info("순환선(2호선)이 가장 안정 · 급행혼용(9호선) 중간 · 분기노선(1호선) 가장 불안정 — 노선 구조가 배차 안정성을 좌우.")

    # ── 2) 배차 불안정 역 Top N ──
    st.subheader(f"② 배차 불안정 역 Top {topn} (CV 기준)")
    top = q(
        f"""
        SELECT line, statn_nm, direction, time_band, headway_samples AS n,
               p50_sec, p90_sec, cv, over_1p5x_ratio
        FROM {HW}
        WHERE headway_samples >= {min_n} {line_f} {band_f}
        ORDER BY cv DESC LIMIT {topn}
        """
    )
    if not top.empty:
        st.bar_chart(top.assign(label=top["statn_nm"] + "(" + top["direction"] + "·" + top["time_band"] + ")")
                     .set_index("label")["cv"])
        st.dataframe(top, use_container_width=True, hide_index=True)

    # ── 3) 파이프라인 freshness (수신 heartbeat) ──
    st.subheader("③ 파이프라인 freshness — 분당 수신 heartbeat")
    fr = q(
        f"""
        SELECT line, CAST(minute_ts AS CHAR) AS minute_ts, records
        FROM {FR} ORDER BY minute_ts
        """
    )
    if not fr.empty:
        fr["minute_ts"] = pd.to_datetime(fr["minute_ts"])
        hb = fr.pivot_table(index="minute_ts", columns="line", values="records", aggfunc="sum")
        st.line_chart(hb)
        st.caption("수집 윈도우(출근·점심·퇴근) 동안 선이 끊김 없이 채워지면 파이프라인 건강. 윈도우 사이 빈 구간은 정상(러시아워만 수집).")
except Exception as e:  # noqa: BLE001
    st.error(f"조회 실패: {e}")
    st.caption(
        "StarRocks 가 떠 있고 iceberg_catalog 가 생성됐는지, gold 마트(headway/freshness)가 적재됐는지 확인하세요."
    )
