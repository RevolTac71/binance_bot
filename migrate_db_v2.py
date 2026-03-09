import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from config import settings


async def migrate():
    engine = create_async_engine(settings.SQLALCHEMY_DATABASE_URI)
    async with engine.begin() as conn:
        print("🔍 Checking TradeLog table schema...")

        # Add last_pnl_at column to trade_logs
        try:
            await conn.execute(
                text(
                    "ALTER TABLE trade_logs ADD COLUMN IF NOT EXISTS last_pnl_at TIMESTAMP WITHOUT TIME ZONE;"
                )
            )
            print("✅ Column 'last_pnl_at' added successfully to 'trade_logs'.")
        except Exception as e:
            print(f"⚠️ Error adding column 'last_pnl_at': {e}")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(migrate())
