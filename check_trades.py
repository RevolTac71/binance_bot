import asyncio
from sqlalchemy import text
from database import engine


async def query():
    async with engine.connect() as conn:
        r = await conn.execute(
            text(
                "SELECT timestamp, action, symbol, ROUND(price::numeric,2) as price, "
                "ROUND(realized_pnl::numeric,4) as pnl, reason FROM trades "
                "ORDER BY timestamp DESC LIMIT 25"
            )
        )
        rows = r.fetchall()
        print("=== 최근 25개 거래 ===")
        for row in rows:
            print(row)

        r2 = await conn.execute(
            text(
                "SELECT action, COUNT(*) as cnt, "
                "ROUND(AVG(realized_pnl)::numeric,4) as avg_pnl, "
                "ROUND(SUM(realized_pnl)::numeric,4) as total "
                "FROM trades WHERE realized_pnl IS NOT NULL "
                "GROUP BY action ORDER BY action"
            )
        )
        print("\n=== 액션별 PnL ===")
        for row in r2.fetchall():
            print(row)

        r3 = await conn.execute(
            text(
                "SELECT "
                "COUNT(*) FILTER (WHERE realized_pnl > 0) as wins, "
                "COUNT(*) FILTER (WHERE realized_pnl < 0) as losses, "
                "ROUND(SUM(realized_pnl)::numeric,4) as net_pnl "
                "FROM trades WHERE realized_pnl IS NOT NULL"
            )
        )
        print("\n=== 승/패/합산 ===")
        for row in r3.fetchall():
            print(row)


asyncio.run(query())
