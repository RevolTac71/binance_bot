from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, text, Boolean
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


class BalanceHistory(Base):
    __tablename__ = "balance_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime)
    balance = Column(Float)  # 달러 등 환산치 (또는 USDT 잔량)


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
            logger.info(
                "Supabase 연결 점검 및 테이블(trades, balance_history) 준비 완료."
            )
            return True
    except Exception as e:
        logger.error(f"DB 초기 연결 혹은 테이블 접근 실패. 상세: {e}")
        return False
