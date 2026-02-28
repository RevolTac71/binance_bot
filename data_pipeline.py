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
        # 바이낸스 선물(USDⓈ-M Futures) 시장 환경 설정 (V11)
        api_key = (
            settings.BINANCE_TESTNET_API_KEY
            if settings.USE_TESTNET
            else settings.BINANCE_API_KEY
        )
        api_secret = (
            settings.BINANCE_TESTNET_API_SECRET
            if settings.USE_TESTNET
            else settings.BINANCE_API_SECRET
        )

        self.exchange = ccxt.binance(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,  # ccxt 내장 속도 제한기 활성화
                "options": {
                    "defaultType": "future",
                    "warnOnFetchOpenOrdersWithoutSymbol": False,  # 초기 동기화 시 전체 대기주문 조회 경고 무시
                },  # 현물(spot) -> 선물(future) 변경
            }
        )

        # Testnet (Sandbox) 모드 활성화 처리
        if settings.USE_TESTNET:
            self.exchange.set_sandbox_mode(True)
            logger.info(
                "🧪 [TESTNET MODE] 바이낸스 선물 테스트넷 환경으로 CCXT 객체 연결이 세팅되었습니다."
            )

    async def close(self):
        """거래소 세션을 안전하게 종료합니다."""
        await self.exchange.close()

    @with_exponential_backoff(max_retries=5)
    async def fetch_ohlcv_since(
        self, symbol: str, timeframe: str = "1m", since: int = None
    ) -> pd.DataFrame:
        """
        주어진 심볼의 바이낸스 캔들 데이터를 비동기로 불러와 DataFrame으로 변환합니다.
        (V15: 1분봉 당일 누적 데이터 수집을 위해 최대 한도 1500개를 끌어옵니다)
        """
        candles = await self.exchange.fetch_ohlcv(
            symbol, timeframe, since=since, limit=1500
        )

        df = pd.DataFrame(
            candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("datetime", inplace=True)
        return df

    def calculate_vwap_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        [V15.0] 일자별(Daily) 00:00 UTC(09:00 KST) 기준 누적 그룹화된 Anchored VWAP 및 제반 지표 연산
        (수집된 전체 1분봉 데이터 프레임을 대상으로 한번에 계산합니다)
        """
        if len(df) == 0:
            return df

        df["hlc3"] = (df["high"] + df["low"] + df["close"]) / 3
        df["vol_hlc3"] = df["hlc3"] * df["volume"]
        df["vol_hlc3_sq"] = df["hlc3"] ** 2 * df["volume"]

        # KST 타임존 적용된 index 기준으로 date를 추출하여 일자별 누적합 계산 (09:00 기준 리셋 효과)
        # 웹소켓 환경 등에서 여러 날짜의 데이터가 섞여 있어도 일자별로 VWAP이 정상 초기화됩니다.
        grouped = df.groupby(df.index.date)
        df["cum_vol"] = grouped["volume"].cumsum()
        df["cum_vol_hlc3"] = grouped["vol_hlc3"].cumsum()
        df["cum_vol_hlc3_sq"] = grouped["vol_hlc3_sq"].cumsum()

        # VWAP 계산
        df["VWAP"] = df["cum_vol_hlc3"] / df["cum_vol"]

        # 분산(Variance) 계산 = (누적(가격^2 * 거래량) / 누적거래량) - VWAP^2
        variance = (df["cum_vol_hlc3_sq"] / df["cum_vol"]) - (df["VWAP"] ** 2)
        variance = np.maximum(0, variance)  # 음수 방지

        # V15.0 표준편차 밴드 멀티플라이어 (K = 2.5)
        vwap_mult = (
            float(settings.K_VALUE) if getattr(settings, "K_VALUE", 2.5) else 2.5
        )
        df["StdDev"] = np.sqrt(variance)
        df["Upper_Band"] = df["VWAP"] + df["StdDev"] * vwap_mult
        df["Lower_Band"] = df["VWAP"] - df["StdDev"] * vwap_mult

        # 과매도/과매수 판단을 위한 RSI (14)
        df.ta.rsi(length=14, append=True, col_names=("RSI_14",))
        # V15.0 거래량 스파이크 판별을 위한 Volume SMA (20)
        df.ta.sma(close=df["volume"], length=20, append=True, col_names=("Vol_SMA_20",))
        # 변동성 필터 및 동적 익손절 거리를 위한 단기 ATR (14)
        df.ta.atr(length=14, append=True, col_names=("ATR_14",))

        # [V15.2] 동적 변동성 필터를 위한 장기 ATR 계산 (기본 200)
        atr_long_len = getattr(settings, "ATR_LONG_LEN", 200)
        # 데이터가 충분하지 않을 경우를 대비해 계산
        if len(df) > atr_long_len:
            df.ta.atr(
                length=atr_long_len, append=True, col_names=(f"ATR_{atr_long_len}",)
            )
        else:
            df[f"ATR_{atr_long_len}"] = df["ATR_14"]

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
