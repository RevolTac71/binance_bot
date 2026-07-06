from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, text, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from config import settings, logger

# 명시적으로 기존 DataBase 스키마를 선언합니다 (Reflection 대신 사용)
# 자동화된 create_all은 호출하지 않으므로 기존 데이터를 파괴하지 않습니다.
Base = declarative_base()


# ==========================================
# [V18 ML Data Pipeline Models]
# ==========================================


class TradeLog(Base):
    """
    개별 매매의 상세 내역과 성과, 슬리피지를 기록하는 테이블.
    """

    __tablename__ = "trade_logs"

    trade_id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(50), index=True)
    direction = Column(String(20))  # 'LONG' or 'SHORT'
    action = Column(String(50), nullable=True)  # 'BUY', 'SELL', 'CANCEL', 'MANUAL' 등
    qty = Column(Float)

    # 진입 스펙
    entry_time = Column(DateTime, index=True)
    target_price = Column(Float)  # 진입 시점 당시 의도했던 기준가/최우선호가
    execution_price = Column(Float)  # 실제 평단가
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
    )  # 'TP', 'SL', 'Chandelier', 'Timeout', 'Manual' 등
    realized_pnl = Column(Float, nullable=True)
    roi_pct = Column(Float, nullable=True)

    # DRY_RUN 영속화 및 복구를 위한 필드
    dry_run = Column(Boolean, default=True, index=True)
    tp_price = Column(Float, nullable=True)
    sl_price = Column(Float, nullable=True)

    # 전략 메타데이터
    strategy_name = Column(String(100), nullable=True)
    strategy_params = Column(JSONB, nullable=True)  # 진입 시점 설정 값 (Squeeze threshold 등)
    market_snapshot = Column(JSONB, nullable=True)  # 진입 시점 지표 스냅샷 (RSI, Bandwidth 등)


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
    engine, class_=AsyncSession, expire_on_commit=False
)


async def check_db_connection():
    """
    서버 시작 전 DB 연결 상태 및 테이블 접근 가능 여부를 간단하게 쿼리하여 체크합니다.
    """
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1 FROM trade_logs LIMIT 1"))
            logger.info(
                "Supabase 연결 점검 및 테이블(trade_logs) 준비 완료."
            )
            return True
    except Exception as e:
        logger.error(f"DB 초기 연결 혹은 테이블 접근 실패. 상세: {e}")
        return False
