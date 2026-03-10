import asyncio
from database import engine
from sqlalchemy import text


async def mega_migrate():
    print("🚀 Database Schema Check & Mega Migration Started...")

    # 추가할 모든 컬럼 리스트 (컬럼명, 타입)
    columns = [
        ("timeframe", "VARCHAR(10)"),
        ("fee_rate", "FLOAT"),
        ("min_score_long", "INTEGER"),
        ("min_score_short", "INTEGER"),
        ("long_tp_mode", "VARCHAR(20)"),
        ("long_sl_mode", "VARCHAR(20)"),
        ("short_tp_mode", "VARCHAR(20)"),
        ("short_sl_mode", "VARCHAR(20)"),
        ("long_tp_pct", "FLOAT"),
        ("long_sl_pct", "FLOAT"),
        ("short_tp_pct", "FLOAT"),
        ("short_sl_pct", "FLOAT"),
    ]

    for col_name, col_type in columns:
        # 각 컬럼마다 개별 트랜잭션으로 처리하여 하나가 실패해도 나머지가 진행되도록 함
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        f"ALTER TABLE market_snapshots ADD COLUMN {col_name} {col_type}"
                    )
                )
                print(f"✅ Column added: {col_name}")
        except Exception as e:
            err_msg = str(e).lower()
            if (
                "already exists" in err_msg
                or "이미 존재" in err_msg
                or "duplicate column" in err_msg
            ):
                print(f"ℹ️ Column already exists: {col_name}")
            else:
                print(f"❌ Failed to add {col_name}: {e}")

    print("\n✨ Migration Finished! Please restart your bot.")


if __name__ == "__main__":
    asyncio.run(mega_migrate())
