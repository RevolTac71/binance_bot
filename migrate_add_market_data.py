"""
Supabase 'trades' 테이블에 'market_data' 컬럼을 추가하는 마이그레이션 스크립트.
최초 1회만 실행하면 됩니다.
"""

import asyncio
from sqlalchemy import text
from database import engine


async def migrate():
    async with engine.begin() as conn:
        try:
            # PostgreSQL에 최적화된 JSONB 타입으로 생성
            await conn.execute(
                text("ALTER TABLE trades ADD COLUMN IF NOT EXISTS market_data JSONB")
            )
            print(
                "✅ 마이그레이션 완료: 'market_data' 컬럼(JSONB)이 trades 테이블에 추가되었습니다."
            )
        except Exception as e:
            print(f"❌ 마이그레이션 실패: {e}")


if __name__ == "__main__":
    asyncio.run(migrate())
