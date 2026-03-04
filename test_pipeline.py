import asyncio
from data_pipeline import DataPipeline


async def test():
    pipeline = DataPipeline()
    sym = "BTC/USDT:USDT"
    print("Fetching 1h...")
    res_1h = await pipeline.fetch_ohlcv_htf(sym, timeframe="1h", limit=300)
    print("Fetching 15m...")
    res_15m = await pipeline.fetch_ohlcv_htf(sym, timeframe="15m", limit=200)

    print("Calculating indicators...")
    try:
        out_1h, out_15m = pipeline.calculate_htf_indicators(res_1h, res_15m)
        print("Success 1!")
    except Exception as e:
        import traceback

        traceback.print_exc()

    await pipeline.close()


if __name__ == "__main__":
    import sys

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test())
