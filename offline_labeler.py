import asyncio
import argparse
from datetime import datetime, timezone, timedelta
from sqlalchemy.future import select
import pandas as pd
import ccxt.async_support as ccxt

from config import settings, logger
from database import TradeLog, AsyncSessionLocal


async def fetch_historical_klines(
    exchange, symbol: str, since_ms: int, until_ms: int
) -> pd.DataFrame:
    """
    지정된 기간 동안의 1분봉 데이터를 가져와 DataFrame으로 반환합니다.
    (MFE, MAE, 5m/15m/30m 후 가격 파싱용)
    """
    all_ohlcv = []
    current_since = since_ms
    limit = 1000

    while current_since < until_ms:
        try:
            ohlcv = await exchange.fetch_ohlcv(
                symbol, timeframe="1m", since=current_since, limit=limit
            )
            if not ohlcv:
                break

            all_ohlcv.extend(ohlcv)
            current_since = ohlcv[-1][0] + 60 * 1000

            if current_since >= until_ms:
                break

            await asyncio.sleep(0.1)  # Rate limit 방지
        except Exception as e:
            logger.error(
                f"[{symbol}] 1분봉 데이터 조회 중 에러 (since={current_since}): {e}"
            )
            await asyncio.sleep(2)

    if not all_ohlcv:
        return pd.DataFrame()

    df = pd.DataFrame(
        all_ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("datetime", inplace=True)
    return df


async def run_offline_labeling(dry_run: bool = False):
    """
    DB에서 아직 라벨링(mfe, mae 등)이 업데이트되지 않은 TradeLog를 찾아,
    해당 기간의 바이낸스 1분봉 데이터를 조회하여 라벨을 계산해 넣습니다.
    (매일 1회 또는 모델 학습 전 오프라인 배치로 돌리는 용도)
    """
    exchange = ccxt.binanceusdm(
        {
            "enableRateLimit": True,
            "options": {
                "defaultType": "future",
                "warnOnFetchOpenOrdersWithoutSymbol": False,
            },
        }
    )

    try:
        async with AsyncSessionLocal() as session:
            # 타겟: 청산이 완료(exit_time 존재)되었으나, 라벨 데이터(mfe 등)가 None인 거래
            # [V19] 히스토리성 보조 레코드(PARTIAL_CLOSED, CLOSED 등)를 제외하고 진입 메인 레코드만 타겟
            stmt = (
                select(TradeLog)
                .where(
                    TradeLog.exit_time.is_not(None),
                    TradeLog.mfe.is_(None),
                    TradeLog.action.in_(["LONG", "SHORT", "ENTRY"]),
                )
                .order_by(TradeLog.entry_time.asc())
            )

            result = await session.execute(stmt)
            trades = result.scalars().all()

            if not trades:
                logger.info("모든 거래가 이미 라벨링되어 있습니다. (대상 없음)")
                return

            logger.info(
                f"총 {len(trades)}건의 라벨링 안 된 TradeLog를 발견했습니다. 처리를 시작합니다..."
            )

            for t in trades:
                symbol = t.symbol
                # DB 시간은 KST(+9)이므로 UTC로 복원 후 타임스탬프 계산
                entry_utc = t.entry_time - timedelta(hours=9)
                exit_utc = t.exit_time - timedelta(hours=9)

                # 진입 시간부터 청산 30분 후까지의 데이터를 로드 (5m, 15m, 30m 후 가격 파싱 위함)
                since_ms = int(entry_utc.timestamp() * 1000)
                until_ms = int((exit_utc + timedelta(minutes=30)).timestamp() * 1000)

                logger.info(
                    f"[{t.id}] {symbol} ({t.entry_time} ~ {t.exit_time}) 데이터 조회 중..."
                )
                df = await fetch_historical_klines(exchange, symbol, since_ms, until_ms)

                if df.empty:
                    logger.warning(
                        f"[{t.id}] {symbol}의 해당 기간 1분봉 데이터가 없어 스킵합니다."
                    )
                    continue

                # 1. 진입 ~ 청산 사이의 MFE / MAE 계산
                # (진입가 대비 유리했던 최대 가격편차, 불리했던 최대 가격편차)
                # DataFrame 인덱스는 UTC 기준이므로 utc.replace(tzinfo=None)로 비교 편의를 맞춥니다.
                mask_holding = (df.index >= entry_utc.replace(tzinfo=None)) & (
                    df.index <= exit_utc.replace(tzinfo=None)
                )
                df_held = df.loc[mask_holding]

                entry_price = (
                    t.execution_price if t.execution_price > 0 else t.target_price
                )
                if entry_price <= 0 or df_held.empty:
                    continue

                high_max = df_held["high"].max()
                low_min = df_held["low"].min()

                mfe = 0.0
                mae = 0.0

                if t.direction == "LONG":
                    mfe = (high_max - entry_price) / entry_price * 100
                    mae = (low_min - entry_price) / entry_price * 100
                    # MAE는 불리한 방향이므로 - 처리할 수도 있지만 절댓값으로 기록
                else:  # SHORT
                    mfe = (entry_price - low_min) / entry_price * 100
                    mae = (entry_price - high_max) / entry_price * 100

                t.mfe = mfe
                t.mae = mae

                # 2. 고정 시간 경과 후의 가격대 파싱 (진입 시점 기준 5, 15, 30분 후)
                # Timestamp 매칭이 완벽하지 않을 수 있으므로 nearest_index 탐색
                entry_idx = entry_utc.replace(tzinfo=None)

                def get_price_after(minutes: int):
                    target_time = entry_idx + timedelta(minutes=minutes)
                    # target_time 이후의 첫 번째 봉 찾기
                    future_df = df[df.index >= target_time]
                    if not future_df.empty:
                        return future_df.iloc[0]["close"]
                    return None

                p5 = get_price_after(5)
                p15 = get_price_after(15)
                p30 = get_price_after(30)

                if p5:
                    t.ret_5m = (
                        (p5 - entry_price) / entry_price * 100
                        if t.direction == "LONG"
                        else (entry_price - p5) / entry_price * 100
                    )
                if p15:
                    t.ret_15m = (
                        (p15 - entry_price) / entry_price * 100
                        if t.direction == "LONG"
                        else (entry_price - p15) / entry_price * 100
                    )
                if p30:
                    t.ret_30m = (
                        (p30 - entry_price) / entry_price * 100
                        if t.direction == "LONG"
                        else (entry_price - p30) / entry_price * 100
                    )

                logger.info(
                    f"[{t.trade_id}] {symbol} 완료: MFE={mfe:.2f}%, MAE={mae:.2f}% / ret5={t.ret_5m if p5 else 'N/A'}"
                )

            if not dry_run:
                await session.commit()
                logger.info(f"==== 라벨링 DB 업데이트(Commit) 완료 ====")
            else:
                logger.info(f"==== [DRY_RUN] Commit은 건너뛰었습니다. ====")

    except Exception as e:
        logger.error(f"오프라인 라벨링 스크립트 실행 중 치명적 에러 발생: {e}")
    finally:
        await exchange.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="오프라인 TradeLog MFE/MAE 라벨링")
    parser.add_argument(
        "--dry", action="store_true", help="DB에 커밋하지 않고 결과만 출력합니다."
    )
    args = parser.parse_args()

    asyncio.run(run_offline_labeling(dry_run=args.dry))
