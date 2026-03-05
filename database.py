from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, text, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from config import settings, logger

# 명시적으로 기존 DataBase 스키마를 선언합니다 (Reflection 대신 사용)
# 자동화된 create_all은 호출하지 않으므로 기존 데이터를 파괴하지 않습니다.
Base = declarative_base()


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime)
    action = Column(String(10))  # 'BUY' or 'SELL', 'MANUAL', 'CLOSED' 등의 구분자
    symbol = Column(String(20))  # 예: 'BTC/USDT:USDT'
    price = Column(Float)
    quantity = Column(Float)
    realized_pnl = Column(Float, nullable=True)  # 실현손익 (청산 시에만 기록)
    reason = Column(Text)  # 진입/청산 사유
    dry_run = Column(Boolean, default=True)  # 테스트 거래 여부 구분자
    # 거래 당시의 전략 파라미터 스냅샷 (JSON 문자열)
    # 예: {"k_value": 2.0, "vol_mult": 1.5, "atr_ratio": 1.2, "leverage": 10, "timeframe": "3m", ...}
    params = Column(Text, nullable=True)
    # 진입 시점의 시장 지표 스냅샷 (JSONB 형식으로 저장하여 PostgreSQL 내부 검색/인덱싱 최적화)
    market_data = Column(JSONB, nullable=True)


# ==========================================
# [V16.2 ML Data Pipeline Models]
# ==========================================


class MarketSnapshot(Base):
    """
    1분/3분 캔들 마감 시점의 시장 맥락(Market Context)을 수집하는 Feature 테이블.
    비지도 학습(분류) 및 지도 학습(예측)의 독립 변수(X)로 활용됩니다.
    """

    __tablename__ = "market_snapshots"

    snapshot_id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, index=True)
    symbol = Column(String(20), index=True)

    # 1. 가격 및 오실레이터 (Price & Oscillators)
    rsi = Column(Float, nullable=True)
    macd_hist = Column(Float, nullable=True)
    adx = Column(Float, nullable=True)
    atr_14 = Column(Float, nullable=True)
    atr_200 = Column(Float, nullable=True)

    # 2. 다중 시간 프레임 (MTF)
    ema_1h_dist = Column(Float, nullable=True)  # (현재가 - 1h_EMA) / 1h_EMA
    ema_15m_dist = Column(Float, nullable=True)  # (현재가 - 15m_EMA) / 15m_EMA

    # 3. 오더플로우 및 잔량 (Orderflow & Microstructure)
    cvd_5m_sum = Column(Float, nullable=True)
    cvd_15m_sum = Column(Float, nullable=True)
    cvd_delta_slope = Column(Float, nullable=True)
    bid_ask_imbalance = Column(Float, nullable=True)  # TWAP 30s

    # 4. 기타 시장 정보
    funding_rate_match = Column(Integer, nullable=True)  # 1(일치), 0(불일치), -1(반대)

    # 5. V17 확장 피처
    log_vol_zscore = Column(Float, nullable=True)  # 로그 변환 거래량 Z-Score
    correlation_max = Column(Float, nullable=True)  # 동조화 최대 상관계수

    # 6. V18 스코어링 피처
    nofi_1m = Column(Float, nullable=True)
    buy_ratio = Column(Float, nullable=True)
    long_score = Column(Integer, nullable=True)
    short_score = Column(Integer, nullable=True)


class TradeLog(Base):
    """
    개별 매매의 상세 내역과 성과, 슬리피지를 기록하고
    추후 배치 스크립트가 MFE/MAE 등의 미래 데이터(Label, Y)를 업데이트하는 테이블.
    """

    __tablename__ = "trade_logs"

    trade_id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), index=True)
    direction = Column(String(10))  # 'LONG' or 'SHORT'
    qty = Column(Float)

    # 진입 스펙
    entry_time = Column(DateTime, index=True)
    target_price = Column(Float)  # 진입 시점 당시 의도했던 기준가/최우선호가
    execution_price = Column(Float)  # Chasing 완료 결과 실제 평단가
    slippage = Column(Float)  # (실제 - 목표) 차이 등
    entry_reason = Column(Text)
    execution_time_ms = Column(
        Integer
    )  # 주문 발송부터 체결 완료 알림까지의 지연시간 밀리초

    # 청산 스펙
    exit_time = Column(DateTime, nullable=True)
    exit_price = Column(Float, nullable=True)
    exit_reason = Column(
        Text, nullable=True
    )  # 'TP', 'SL', 'Chandelier', 'Time', 'Manual' 등
    realized_pnl = Column(Float, nullable=True)
    roi_pct = Column(Float, nullable=True)

    # 지연 모델링 레이블 (MFE, MAE 및 수익률) - Offline 배치로 UPDATE 됨
    mfe = Column(
        Float, nullable=True
    )  # 최대 가용 수익 구간 (Maximum Favorable Excursion)
    mae = Column(
        Float, nullable=True
    )  # 최대 노출 손실 구간 (Maximum Adverse Excursion)
    ret_5m = Column(Float, nullable=True)  # 진입 5분 뒤 % 수익률
    ret_15m = Column(Float, nullable=True)  # 진입 15분 뒤 % 수익률
    ret_30m = Column(Float, nullable=True)  # 진입 30분 뒤 % 수익률


class MarketData_1m(Base):
    """
    [V16.6 HFT] 인메모리 파이프라인 전용 1분 단위 압축 스냅샷
    원시 틱 데이터를 DB에 적재하지 않고, OHLCV와 파생지표(OI, 펀딩비, OFI 등)를
    단일 JSON 컬럼으로 통합 보관하여 DB 부하(Write)를 최소화합니다.
    """

    __tablename__ = "market_data_1m"

    timestamp = Column(DateTime, primary_key=True, index=True)
    symbol = Column(String(20), primary_key=True)
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    # 펀딩비, 미결제약정(OI), 머신러닝 피처(OFI 등) 압축 적재
    features = Column(JSONB, nullable=True)


from sqlalchemy.pool import NullPool
import uuid

# Supabase PgBouncer (Transaction mode) 연결 호환성 설정
# - SQLAlchemy 자체 Pool을 비활성화 (NullPool) 하고 PgBouncer가 전담하도록 위임
# - prepared_statement 캐시 기능을 완전 비활성화하여 충돌 방지
# - UUID를 활용해 asyncpg가 생성하는 PreparedStatement 이름을 매번 다르게 고유 적용
engine = create_async_engine(
    settings.SQLALCHEMY_DATABASE_URI,
    poolclass=NullPool,
    echo=False,
    connect_args={
        "statement_cache_size": 0,
        "prepared_statement_cache_size": 0,
        "server_settings": {"application_name": "binance_bot"},
        "prepared_statement_name_func": lambda: f"__asyncpg_{uuid.uuid4().hex}__",
    },
    execution_options={"prepared_statement_cache_size": 0, "compiled_cache": None},
)

AsyncSessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)


async def check_db_connection():
    """
    서버 시작 전 DB 연결 상태 및 테이블 접근 가능 여부를 간단하게 쿼리하여 체크합니다.
    """
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1 FROM trades LIMIT 1"))
            logger.info("Supabase 연결 점검 및 테이블(trades, trade_logs) 준비 완료.")
            return True
    except Exception as e:
        logger.error(f"DB 초기 연결 혹은 테이블 접근 실패. 상세: {e}")
        return False
