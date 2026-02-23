import pandas as pd
from config import settings


class StrategyEngine:
    def __init__(self, k_value=None):
        """
        돌파 계수 K 값을 받습니다. 주어지지 않으면 환경 설정 파일(.env)의 k 값을 읽습니다.
        """
        self.k = k_value if k_value is not None else settings.K_VALUE

    def check_long_entry(self, df: pd.DataFrame) -> dict:
        """
        주어진 DataFrame (보조지표가 추가된 상태여야 함)을 기반으로 Long 진입 룰을 검사합니다.
        가장 최신봉(제일 마지막 행)을 현재 캔들 시점으로 판단합니다.

        Condition 1 (Trend Filter): 현재 종가가 MA(20) 및 MA(50) 위에 존재.
        Condition 2 (Volatility Breakout): 현재 종가 > 당일 시가 + (이전 고가 - 이전 저가) * k
        Condition 3 (Volume Verification): 거래량 > 당일 제외 과거 20봉 평균 거래량 * 1.5
        Condition 4 (RSI Filter): 14주기 RSI < 70 (과매수 상태가 아닐 것)

        Returns:
            dict: { 'signal': bool, 'reason': str }
        """
        # 최소한 EMA50을 요구하기 때문에 데이터 길이 검증
        if len(df) < 51:
            return {"signal": False, "reason": "데이터 부족 (최소 51개 캔들 필요)"}

        current_candle = df.iloc[-1]
        previous_candle = df.iloc[-2]

        # 1. 가격 데이터 추출
        current_price = current_candle["close"]
        current_open = current_candle["open"]
        prev_high = previous_candle["high"]
        prev_low = previous_candle["low"]
        current_vol = current_candle["volume"]

        # 2. 이동평균 추출
        ma_20 = current_candle["MA_20"]
        ma_50 = current_candle["MA_50"]

        # Condition 1: Trend Filter
        cond1 = (current_price > ma_20) and (current_price > ma_50)

        # Condition 2: Volatility Breakout
        target_price = current_open + (prev_high - prev_low) * self.k
        cond2 = current_price > target_price

        # Condition 3: Volume Verification
        vol_ma_20 = current_candle["Volume_MA_20"]
        cond3 = current_vol >= (vol_ma_20 * 1.5)

        # Condition 4: RSI Filter
        rsi_14 = current_candle["RSI_14"]
        cond4 = rsi_14 < 70

        # 최종 판단
        if cond1 and cond2 and cond3 and cond4:
            return {
                "signal": True,
                "reason": (
                    f"모든 조건 충족 "
                    f"(목표가: {target_price:.4f}, 도달가: {current_price:.4f}, "
                    f"RSI: {rsi_14:.2f}, 거래량 {current_vol:.2f} >= {vol_ma_20 * 1.5:.2f})"
                ),
            }

        return {
            "signal": False,
            "reason": f"진입 불가 (Trend:{cond1}, Breakout:{cond2}, Vol:{cond3}, RSI:{cond4})",
        }
