import asyncio
import functools
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
import pandas_ta as ta
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
        # 바이낸스 선물(USDⓈ-M Futures) 시장 타겟 (V11)
        self.exchange = ccxt.binance(
            {
                "apiKey": settings.BINANCE_API_KEY,
                "secret": settings.BINANCE_API_SECRET,
                "enableRateLimit": True,  # ccxt 내장 속도 제한기 활성화
                "options": {"defaultType": "future"},  # 현물(spot) -> 선물(future) 변경
            }
        )

    async def close(self):
        """거래소 세션을 안전하게 종료합니다."""
        await self.exchange.close()

    @with_exponential_backoff(max_retries=5)
    async def fetch_ohlcv_since(
        self, symbol: str, timeframe: str = "3m", since: int = None
    ) -> pd.DataFrame:
        """
        주어진 심볼의 바이낸스 캔들 데이터를 비동기로 불러와 DataFrame으로 변환합니다.
        (V11: since 파라미터를 사용하여 09:00 KST 부터의 모든 데이터를 가져옵니다)
        """
        # limit 없이 since부터 현재까지 모두 가져와야 VWAP 누적이 정확함 (기본 500개 1500분=25시간이므로 당일 커버 가능)
        candles = await self.exchange.fetch_ohlcv(symbol, timeframe, since=since)

        df = pd.DataFrame(
            candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("datetime", inplace=True)
        return df

    def calculate_vwap_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        당일 누적 VWAP (Anchored VWAP) 및 표준편차 밴드 계산 (V11).
        주의: df는 반드시 당일 09:00 KST 이후의 데이터만 포함하거나,
        여기서 Session 기준 Groupby 연산을 해야 하지만,
        fetch_ohlcv_since 에서 이미 09:00 KST 이후 데이터만 넘겨준다고 가정합니다.
        """
        if len(df) == 0:
            return df

        df["hlc3"] = (df["high"] + df["low"] + df["close"]) / 3
        df["vol_hlc3"] = df["hlc3"] * df["volume"]
        df["vol_hlc3_sq"] = df["hlc3"] ** 2 * df["volume"]

        # 누적 합계 계산
        df["cum_vol"] = df["volume"].cumsum()
        df["cum_vol_hlc3"] = df["vol_hlc3"].cumsum()
        df["cum_vol_hlc3_sq"] = df["vol_hlc3_sq"].cumsum()

        # VWAP 계산
        df["VWAP"] = df["cum_vol_hlc3"] / df["cum_vol"]

        # 분산(Variance) 계산 = (누적(가격^2 * 거래량) / 누적거래량) - VWAP^2
        variance = (df["cum_vol_hlc3_sq"] / df["cum_vol"]) - (df["VWAP"] ** 2)
        # 음수 분산 방지 (부동소수점 오차)
        variance = np.maximum(0, variance)

        # 표준편차 및 밴드 (VWAP 멀티플라이어 2.0 고정 혹은 설정값)
        vwap_mult = 2.0
        df["StdDev"] = np.sqrt(variance)
        df["Upper_Band"] = df["VWAP"] + df["StdDev"] * vwap_mult
        df["Lower_Band"] = df["VWAP"] - df["StdDev"] * vwap_mult

        # 과매도/과매수 판단을 위한 RSI (14)
        df.ta.rsi(length=14, append=True, col_names=("RSI_14",))

        return df

    # Top 5 알트코인 동적 추출
    @with_exponential_backoff(max_retries=3)
    async def fetch_top_altcoins_by_volume(
        self,
        limit: int = 5,
        exclude_symbols: list = [
            "BTC/USDT:USDT",
            "ETH/USDT:USDT",
            "USDC/USDT:USDT",
            "FDUSD/USDT:USDT",
            "TUSD/USDT:USDT",
            "EUR/USDT:USDT",
            "USDP/USDT:USDT",
        ],
    ) -> list:
        """
        바이낸스 선물 시장에서 24시간 거래금액(Quote Volume) 기준으로 상위 알트코인을 추출합니다.
        스테이블코인이나 무거운 비트, 이더리움은 제외합니다.
        """
        tickers = await self.exchange.fetch_tickers()

        # USDT 기반 선형 선물 티커 필터링 (선물은 "ADA/USDT:USDT" 형태)
        usdt_pairs = {
            k: v
            for k, v in tickers.items()
            if k.endswith("/USDT:USDT") and k not in exclude_symbols
        }

        # 24시간 거래대금 (quoteVolume) 내림차순 정렬
        sorted_pairs = sorted(
            usdt_pairs.items(), key=lambda x: x[1].get("quoteVolume", 0), reverse=True
        )

        # 심볼 추출
        top_symbols = [pair[0] for pair in sorted_pairs[:limit]]
        return top_symbols
