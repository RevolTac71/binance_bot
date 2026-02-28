import pandas as pd
import numpy as np
import pandas_ta as ta
from config import settings, logger


class StrategyEngine:
    def __init__(self, exchange=None):
        """
        [V15.2] 극단적 꼬리잡기 스캘핑 전략 엔진 (멀티 타임프레임 최적화)
        - 타임프레임: 기본 3m (설정 가능)
        - 필터: 1) 당일 누적 데이터 개수가 30개 미만이면 무시 (Session Filter 강화)
               2) 장단기 동적 ATR 변동성 필터 도입
        - 진입 트리거:
          * 기본: 이전보다 완화된 볼륨 스파이크 + 프라이스 리젝션
          * 익스트림: 극한의 볼륨 스파이크(2.5x) + 밴드 바깥으로 넘쳤을 때 (리젝션 없이도 잡아냄)
        """
        self.exchange = exchange

        # 지표 파라미터 (V15.2)
        self.rsi_os = 30
        self.rsi_ob = 70
        self.session_filter_bars = 30  # 캔들 마감 기준 30봉 (3분봉이면 90분)

    def check_entry(self, symbol: str, df: pd.DataFrame) -> dict:
        """
        데이터 프레임을 분석하여 진입 시그널과 동적 변동성(ATR) 값을 반환합니다.

        Returns:
            dict: {
                "signal": str ('LONG' | 'SHORT' | None),
                "market_price": float,
                "atr_val": float,
                "vwap_mid": float,
                "reason": str
            }
        """
        if len(df) < 50:
            return {"signal": None, "reason": "데이터 부족 (최소 50개 필요)"}

        current = df.iloc[-1]

        market_price = float(current["close"])
        high_price = float(current["high"])
        low_price = float(current["low"])
        volume = float(current["volume"])

        rsi_val = current.get("RSI_14", 50)
        lower_band = current.get("Lower_Band", None)
        upper_band = current.get("Upper_Band", None)
        vwap_mid = current.get("VWAP", market_price)

        vol_sma_20 = current.get("Vol_SMA_20", volume)
        atr_14 = current.get("ATR_14", market_price * 0.005)

        atr_long_len = getattr(settings, "ATR_LONG_LEN", 200)
        atr_long = current.get(f"ATR_{atr_long_len}", atr_14)

        if (
            pd.isna(lower_band)
            or pd.isna(upper_band)
            or pd.isna(atr_14)
            or pd.isna(vol_sma_20)
        ):
            return {"signal": None, "reason": "지표 결측치 발생"}

        signal_type = None
        reason = ""

        # 1. Session Filter
        # 09:00 KST 부터 누적된 캔들 갯수로 판단. (cumsum()을 통해 누적 거래량이 초기화된 시점 이후의 row 갯수)
        # 당일 데이터의 첫 캔들부터 누적 합이 연속해서 증가하므로 index 로 그룹 지어도 되나, 직관적으로 KST 계산.
        current_time = df.index[-1] if df.index.name == "datetime" else current.name
        # 09:00 에 리셋, 즉 시간을 분 단위로 환산해서 09:00 으로부터 경과한 캔들을 세어야 함.
        # 가장 빠른 방법: 오늘 09:00 생성된 df의 rows를 직접 세는 것이 vwap 로직상 가장 정확함 (groupby date).
        today_date = current_time.date()
        if current_time.hour < 9:
            today_date = (current_time - pd.Timedelta(days=1)).date()

        # 해당 날짜에 해당하는 row 개수 계산 (Session Filter)
        bars_since_reset = len(df[df.index.date == today_date])

        is_session_safe = bars_since_reset >= self.session_filter_bars

        if not is_session_safe:
            return {
                "signal": None,
                "reason": f"Session Filter Active (VWAP 리셋 후 {bars_since_reset}/{self.session_filter_bars}봉 째)",
            }

        # 2. V15.2 동적 변동성 필터
        atr_ratio_mult = getattr(settings, "ATR_RATIO_MULT", 1.2)
        is_volatile = atr_14 > (atr_long * atr_ratio_mult)

        if not is_volatile:
            return {
                "signal": None,
                "reason": f"Low Volatility (ATR Short {atr_14:.4f} <= ATR Long {atr_long:.4f} x {atr_ratio_mult})",
            }

        # 3. Volume Spike (V15.2 거래량 스파이크 1.5배 & 극한 스파이크 2.5배)
        vol_mult = getattr(settings, "VOL_MULT", 1.5)
        is_vol_spike = volume > (vol_sma_20 * vol_mult)
        is_extreme_vol = volume > (vol_sma_20 * 2.5)

        # 4. Price Action Rejection & Exteme Outlier Logic
        long_rejection = (low_price <= lower_band) and (market_price > lower_band)
        short_rejection = (high_price >= upper_band) and (market_price < upper_band)

        # V15.2 진입 트리거 조합
        if (rsi_val <= self.rsi_os) and (
            (is_vol_spike and long_rejection)
            or (is_extreme_vol and low_price <= lower_band)
        ):
            signal_type = "LONG"
            reason = f"LONG (RSI: {rsi_val:.1f}<=30, V_Spike: {volume / vol_sma_20:.1f}x. ATR: {atr_14:.4f})"

        elif (rsi_val >= self.rsi_ob) and (
            (is_vol_spike and short_rejection)
            or (is_extreme_vol and high_price >= upper_band)
        ):
            signal_type = "SHORT"
            reason = f"SHORT (RSI: {rsi_val:.1f}>=70, V_Spike: {volume / vol_sma_20:.1f}x. ATR: {atr_14:.4f})"

        else:
            reason = "진입 조건 불충족"

        return {
            "signal": signal_type,
            "market_price": market_price,
            "atr_val": atr_14,
            "vwap_mid": vwap_mid,
            "reason": reason,
        }
