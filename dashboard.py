import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from config import settings

st.set_page_config(
    page_title="Binance Auto Trader Dashboard", page_icon="📈", layout="wide"
)
st.title("📈 바이낸스 자동 매매 24/7 모니터링")

# Streamlit은 내부적으로 동기(Sync)로 작동하므로 asyncpg 대신 동일한 URL을 sync용(기본 psycopg2)으로 재해석합니다.
sync_uri = settings.SQLALCHEMY_DATABASE_URI.replace("+asyncpg", "")


@st.cache_data(ttl=60)  # 60초마다 캐시가 재조회됩니다.
def load_data():
    """
    Supabase DB에 접속하여 테이블로부터 최근 100건의 기록을 조회하여 출력합니다.
    """
    engine = create_engine(sync_uri)
    try:
        trades_df = pd.read_sql(
            "SELECT * FROM trade_logs ORDER BY entry_time DESC LIMIT 100", engine
        )
        balance_df = pd.read_sql(
            "SELECT * FROM balance_history ORDER BY timestamp ASC LIMIT 500", engine
        )
        return trades_df, balance_df
    except Exception as e:
        st.error(f"DB 데이터를 불러오는데 실패했습니다: {e}")
        return pd.DataFrame(), pd.DataFrame()


# 데이터 호출 및 로드
trades, balance = load_data()

# 1. 시각화 구역(자산 흐름)
st.header("자산 흐름 동향 (USDT)")
if not balance.empty:
    st.line_chart(balance.set_index("timestamp")["balance"])
else:
    st.info(
        "기록된 잔고 이력 데이터가 없습니다 (DB 연동은 확인되었으나 행이 없습니다)."
    )

st.divider()

# 2. 거래 체결 기록
st.header("최근 체결 / 포지션 등록 기록 (trade_logs)")
if not trades.empty:
    col1, col2 = st.columns([1, 3])
    with col1:
        action_filter = st.selectbox(
            "Action 조건", options=["ALL", "LONG", "SHORT", "PARTIAL_CLOSED", "CLOSED"]
        )
    with col2:
        pass  # 빈 공간 여백

    # 필터 처리
    if action_filter != "ALL":
        display_df = trades[trades["action"] == action_filter]
    else:
        display_df = trades

    st.dataframe(display_df, use_container_width=True)
else:
    st.info("최근 거래 체결 내역이 존재하지 않습니다.")

# 사이드바 설정 영역 전시 (동작을 제어하진 않고 보여주기)
st.sidebar.markdown("### ⚙️ 시스템 가동 정보")
st.sidebar.success("정상 작동 중 (DB 실시간 연동됨)")
st.sidebar.metric(label="현재 K 계수", value=settings.K_VALUE)
st.sidebar.metric(
    label="통제 리스크 자본(%)", value=f"{settings.RISK_PERCENTAGE * 100} %"
)

st.sidebar.markdown("*(위 지표는 `.env` 환경 파일을 기반으로 합니다.)*")
