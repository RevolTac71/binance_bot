import asyncio
import time
from config import logger
from database import engine, Base, MarketData_1m


async def migrate_hft_tables():
    """
    [V16.6 HFT] 파이프라인 전용으로 추가된 새로운 SQLAlchemy 모델(`MarketData_1m`)을
    PostgreSQL(Supabase)에 반영하여 테이블을 생성합니다. 기존 테이블의 데이터는 유지됩니다.
    """
    logger.info("Initializing HFT Data Pipeline Tables...")
    try:
        async with engine.begin() as conn:
            # Base에 등록된 모든 모델을 훑어서 없으면 생성
            # 기존 trades, balance_history 등은 IF NOT EXISTS 옵션으로 인해 안전하게 넘어감
            await conn.run_sync(Base.metadata.create_all)
        logger.info("🎉 DB Migration Completed. 'market_data_1m' table is ready.")
    except Exception as e:
        logger.error(f"Migration Failed: {e}")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    s = time.time()
    asyncio.run(migrate_hft_tables())
    logger.info(f"Migration took {time.time() - s:.2f} seconds.")
