import asyncio
import logging
from datetime import timedelta
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from config import Config

DATABASE_URL = Config.SQLALCHEMY_DATABASE_URI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MigrateTimezone")


async def convert_utc_to_kst():
    # connect to the actual db
    engine = create_async_engine(DATABASE_URL)
    session_factory = sessionmaker(engine, class_=AsyncSession)

    async with session_factory() as session:
        try:
            # We add 9 hours to everything before 2026-03-03 14:00:00 (which captures the UTC timestamps from earlier today before the 21:00 deploy)
            update_query = text(
                """
                UPDATE market_data_1m
                SET "timestamp" = "timestamp" + interval '9 hours'
                WHERE "timestamp" < '2026-03-03 14:00:00'
                """
            )
            result = await session.execute(update_query)
            await session.commit()
            logger.info(
                f"Successfully converted timezone for {result.rowcount} rows in market_data_1m."
            )
        except Exception as e:
            await session.rollback()
            logger.error(f"Migration Failed: {e}")
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(convert_utc_to_kst())
