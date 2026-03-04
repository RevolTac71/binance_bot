import asyncio
import aiohttp


async def test_rest():
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        url = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
        try:
            async with session.get(url) as resp:
                print(f"Status: {resp.status}")
                if resp.status == 200:
                    data = await resp.json()
                    print(f"Data: {data}")
        except Exception as e:
            print(f"REST Fetch Error: {type(e).__name__} - {e}")


if __name__ == "__main__":
    import sys

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test_rest())
