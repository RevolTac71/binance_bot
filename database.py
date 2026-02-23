from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, text
from config import settings, logger

# 명시적으로 기존 DataBase 스키마를 선언합니다 (Reflection 대신 사용)
# 자동화된 create_all은 호출하지 않으므로 기존 데이터를 파괴하지 않습니다.
Base = declarative_base()


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime)
    action = Column(String(10))  # 'BUY' or 'SELL', 'STOPLOSS' 등의 구분자
    symbol = Column(String(20))  # 예: 'BTCUSDT'
    price = Column(Float)
    quantity = Column(Float)
    reason = Column(Text)  # 추가적인 정보 (예: 'ATR Stop', 'Trend Signal 진입' 등)


class BalanceHistory(Base):
    __tablename__ = "balance_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime)
    balance = Column(Float)  # 달러 등 환산치 (또는 USDT 잔량)


# Supabase 등의 TCP 끊김이나 Transaction Pooler 환경을 대비해
# pool_pre_ping과 짧은 주기의 pool_recycle 적용.
engine = create_async_engine(
    settings.SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=True,
    pool_recycle=1800,  # 30분 마다 connection 자동 리프레시로 끊김 방지
    echo=False,
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
