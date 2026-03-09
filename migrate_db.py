import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

# SQLAlchemy용 비동기 연결 URL 생성
DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


async def migrate():
    print(f"Connecting to {DB_HOST}...")
    engine = create_async_engine(DATABASE_URL)

    # 추가할 컬럼 목록 (컬럼명, 데이터 타입)
    columns_to_add = [
        # V18.2 지표
        ("nofi_1m", "DOUBLE PRECISION"),
        ("buy_ratio", "DOUBLE PRECISION"),
        # V18.4 전략 파라미터
        ("min_score_long", "INTEGER"),
        ("min_score_short", "INTEGER"),
        ("long_tp_mult", "DOUBLE PRECISION"),
        ("long_sl_mult", "DOUBLE PRECISION"),
        ("short_tp_mult", "DOUBLE PRECISION"),
        ("short_sl_mult", "DOUBLE PRECISION"),
        ("macd_filter_enabled", "BOOLEAN"),
    ]

    async with engine.begin() as conn:
        for col_name, col_type in columns_to_add:
            try:
                # PostgreSQL에서 컬럼 존재 여부 확인 후 ALTER TABLE 실행
                check_query = text(f"""
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='market_snapshots' AND column_name='{col_name}';
                """)
                result = await conn.execute(check_query)
                exists = result.fetchone() is not None

                if not exists:
                    alter_query = text(
                        f"ALTER TABLE market_snapshots ADD COLUMN {col_name} {col_type};"
                    )
                    await conn.execute(alter_query)
                    print(f"✅ Column '{col_name}' added to market_snapshots.")
                else:
                    print(f"ℹ️ Column '{col_name}' already exists in market_snapshots.")
            except Exception as e:
                print(f"❌ Error processing column '{col_name}': {e}")

    await engine.dispose()
    print("Migration finished.")


if __name__ == "__main__":
    asyncio.run(migrate())
