import pandas as pd
from config import settings, logger


class StrategyEngine:
    def __init__(self, exchange=None):
        """
        VWAP 평균 회귀 전략 엔진 (V11.3 - 시장가 진입)
        - 타임프레임: 3분봉
        - 핵심 지표: 당일 누적 VWAP, VWAP StdDev Band, RSI(14)
        - 진입: RSI 극단치 도달 시 현재가(Market)로 즉시 진입
        - 청산: 비대칭 고정 R:R (Long: +1.0% / -0.5%, Short: -1.2% / +0.4%)
        """
        self.exchange = exchange  # ccxt exchange instance for precision

        # 지표 설정
        self.rsi_os = 30  # Long 대기 기준
        self.rsi_ob = 70  # Short 대기 기준

        # 고정 청산 비율 (TP/SL)
        self.long_tp_pct = 0.010  # +1.0%
        self.long_sl_pct = 0.005  # -0.5%
        self.short_tp_pct = 0.012  # -1.2%
        self.short_sl_pct = 0.004  # +0.4%

    def check_entry(self, symbol: str, df: pd.DataFrame) -> dict:
        """
        주어진 3분봉 데이터를 기반으로 진입 시그널 및 대략적인 시장가 예상 가격을 산출합니다.

        Returns:
            dict: {
                "signal": str ('LONG' | 'SHORT' | None),
                "market_price": float (현재 종가 기반 예상 진입 단가),
                "reason": str (로그/메시지용 텍스트)
            }
        """
        if len(df) < 15:
            return {"signal": None, "reason": "데이터 부족"}

        # 가장 최근 완성된 캔들(혹은 진행중인 캔들의 최신 틱)을 기준으로 확인
        # 지정가 대기의 성격상, 가장 최신 봉(index -1)의 정보를 바탕으로 밴드와 RSI를 확인합니다.
        current = df.iloc[-1]

        rsi_val = current.get("RSI_14", 50)
        lower_band = current.get("Lower_Band", None)
        upper_band = current.get("Upper_Band", None)

        if pd.isna(lower_band) or pd.isna(upper_band) or pd.isna(rsi_val):
            return {"signal": None, "reason": "VWAP 밴드 또는 RSI 계산 안됨"}

        # ─── 진입 신호 확인 및 대략적인 시장 가격 반환 ───
        # 시장가 진입이므로, 정확한 TP/SL은 체결 후 execution.py에서 average 체결가 기준으로 계산합니다.
        # 여기서는 사이징(수량 계산)을 위한 현재 종가(close)만 반환합니다.
        market_price = float(current["close"])
        signal_type = None
        reason = ""

        if rsi_val < self.rsi_os and market_price <= lower_band:
            # RSI 과매도 & 가격이 하단 밴드 밑(또는 터치)일 때 시장가 LONG
            signal_type = "LONG"
            reason = f"LONG (RSI: {rsi_val:.1f} < {self.rsi_os}, 가격({market_price})이 하단 밴드 돌파). 시장가 즉시 진입"

        elif rsi_val > self.rsi_ob and market_price >= upper_band:
            # RSI 과매수 & 가격이 상단 밴드 위(또는 터치)일 때 시장가 SHORT
            signal_type = "SHORT"
            reason = f"SHORT (RSI: {rsi_val:.1f} > {self.rsi_ob}, 가격({market_price})이 상단 밴드 돌파). 시장가 즉시 진입"

        else:
            return {
                "signal": None,
                "reason": f"조건 미달성 (RSI: {rsi_val:.1f}, C: {market_price}, LB: {lower_band:.2f}, UB: {upper_band:.2f})",
            }

        return {
            "signal": signal_type,
            "market_price": market_price,
            "reason": reason,
            "current_rsi": rsi_val,
        }
