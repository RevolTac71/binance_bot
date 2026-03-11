import asyncio
from sqlalchemy import text
from database import engine, logger


async def migrate():
    """
    market_snapshots 테이블에 부족한 컬럼들을 추가하는 마이그레이션 스크립트 (V3)
    """
    logger.info("Starting DB Migration V3 (market_snapshots columns)...")

    queries = [
        # 1. funding_rate_match 추가 (Integer)
        "ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS funding_rate_match INTEGER;",
        # 2. log_volume_zscore -> log_vol_zscore 이름 변경 시도 (이미 있으면 넘김)
        # PostgreSQL에서는 RENAME COLUMN을 사용하지만, 안전하게 ADD + DROP 또는 IF NOT EXISTS 지원 확인 필요
        # 여기서는 단순 추가로 대응 (기존 log_volume_zscore는 유지하거나 나중에 삭제)
        "ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS log_vol_zscore FLOAT;",
        # 3. nofi_1m 추가 (Float)
        "ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS nofi_1m FLOAT;",
        # 4. 기존 log_volume_zscore 삭제 (데이터 마이그레이션이 필요 없다면 바로 삭제)
        # "ALTER TABLE market_snapshots DROP COLUMN IF EXISTS log_volume_zscore;"
    ]

    try:
        async with engine.begin() as conn:
            for query in queries:
                try:
                    await conn.execute(text(query))
                    logger.info(f"Executed: {query}")
                except Exception as ex:
                    logger.warning(f"Could not execute '{query}': {ex}")

        logger.info("DB Migration V3 completed successfully!")
    except Exception as e:
        logger.error(f"DB Migration V3 failed: {e}")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(migrate())
