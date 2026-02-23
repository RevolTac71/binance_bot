import asyncio
import functools
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta
from ccxt.base.errors import RateLimitExceeded, RequestTimeout, NetworkError
from config import settings, logger


# -- Exponential Backoff Decorator --
def with_exponential_backoff(max_retries=5, base_delay=1.0, max_delay=60.0):
    """
    API 429 에러(RateLimitExceeded)나 타임아웃 발생 시,
    대기 시간을 점진적으로(지수적) 증가시켜가며 재시도하는 데코레이터입니다.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            delay = base_delay
            while True:
                try:
                    return await func(*args, **kwargs)
                except (RateLimitExceeded, RequestTimeout, NetworkError) as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(
                            f"최대 재시도 횟수({max_retries}) 초과. {func.__name__} 최종 에러: {e}"
                        )
                        raise e

                    logger.warning(
                        f"API 에러 발생 ({e.__class__.__name__}): {e}. {delay}초 대기 후 재시도 ({retries}/{max_retries})..."
                    )
                    await asyncio.sleep(delay)
                    delay = min(max_delay, delay * 2)  # 지수 증가, 최댓값 max_delay

        return wrapper

    return decorator


class DataPipeline:
    def __init__(self):
        # 바이낸스 선물 시장을 타겟으로 가정 (현물인 경우 defaultType: 'spot')
        self.exchange = ccxt.binance(
            {
                "apiKey": settings.BINANCE_API_KEY,
                "secret": settings.BINANCE_API_SECRET,
                "enableRateLimit": True,  # ccxt 내장 속도 제한기 활성화
                "options": {"defaultType": "future"},
            }
        )

    async def close(self):
        """거래소 세션을 안전하게 종료합니다."""
        await self.exchange.close()

    @with_exponential_backoff(max_retries=5)
    async def fetch_ohlcv_df(
        self, symbol: str, timeframe: str = "1h", limit: int = 150
    ) -> pd.DataFrame:
        """
        주어진 심볼의 바이낸스 캔들 데이터를 비동기로 불러와 DataFrame으로 변환합니다.
        (limit은 EMA 50, RSI 14 등 과거치를 충분히 포함하기 위해 150 이상 지정)
        """
        candles = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

        df = pd.DataFrame(
            candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("datetime", inplace=True)
        return df

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        pandas_ta 모듈을 사용하여 매매 전략 판단에 필요한 보조지표를 일괄 계산합니다.
        """
        # 1. 추세 식별을 위한 이동평균선(MA)
        df.ta.sma(length=20, append=True, col_names=("MA_20",))
        df.ta.sma(length=50, append=True, col_names=("MA_50",))

        # 2. RSI 필터
        df.ta.rsi(length=14, append=True, col_names=("RSI_14",))

        # 3. ATR 필터 (변동성 파악 및 역지정가(스탑로스) 계산 용도)
        df.ta.atr(length=14, append=True, col_names=("ATR_14",))

        # 4. 거래량 검증용 20봉 평균 거래량
        # 마지막 로우 직전 캔들 포함하여 과거 20개 이동평균
        df["Volume_MA_20"] = df["volume"].rolling(window=20).mean()

        # 결측치가 포함된 과거 데이터 일부 삭제 (필요 시)
        # df.dropna(inplace=True)

        return df

    # Top 10 알트코인 동적 추출을 이 곳에 구현 (risk_management에도 연계 사용)
    @with_exponential_backoff(max_retries=3)
    async def fetch_top_altcoins_by_volume(
        self, limit: int = 10, exclude_symbols: list = ["BTCUSDT"]
    ) -> list:
        """
        바이낸스 선물 시장에서 24시간 거래금액(Quote Volume) 기준으로 상위 알트코인을 추출합니다.
        """
        tickers = await self.exchange.fetch_tickers()

        # USDT 페어 거래소 티커 필터링
        usdt_pairs = {
            k: v
            for k, v in tickers.items()
            if k.endswith("USDT") and k not in exclude_symbols
        }

        # 24시간 거래대금 (quoteVolume) 내림차순 정렬
        sorted_pairs = sorted(
            usdt_pairs.items(), key=lambda x: x[1].get("quoteVolume", 0), reverse=True
        )

        # 심볼 추출
        top_symbols = [pair[0] for pair in sorted_pairs[:limit]]
        return top_symbols
