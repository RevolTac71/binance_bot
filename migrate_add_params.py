"""
Supabase 'trades' 테이블에 'params' 컬럼을 추가하는 마이그레이션 스크립트.
최초 1회만 실행하면 됩니다.
"""

import asyncio
from sqlalchemy import text
from database import engine


async def migrate():
    async with engine.begin() as conn:
        try:
            await conn.execute(
                text("ALTER TABLE trades ADD COLUMN IF NOT EXISTS params TEXT")
            )
            print(
                "✅ 마이그레이션 완료: 'params' 컬럼이 trades 테이블에 추가되었습니다."
            )
        except Exception as e:
            print(f"❌ 마이그레이션 실패: {e}")


if __name__ == "__main__":
    asyncio.run(migrate())
