import pandas as pd
from config import settings, logger


class StrategyEngine:
    def __init__(self, exchange=None):
        """
        VWAP 평균 회귀 전략 엔진 (V11)
        - 타임프레임: 3분봉
        - 핵심 지표: 당일 누적 VWAP, VWAP StdDev Band, RSI(14)
        - 진입: RSI 극단치 도달 시 VWAP 상하단 밴드에 지정가(Limit) 대기
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
        주어진 3분봉 데이터를 기반으로 진입 시그널 및 지정가 주문 가격을 산출합니다.

        Returns:
            dict: {
                "signal": str ('LONG' | 'SHORT' | None),
                "limit_price": float (진입 지정가),
                "tp_price": float (익절가),
                "sl_price": float (손절가),
                "reason": str (로그용 텍스트)
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

        # ─── 진입 가격 설정 (VWAP 밴드 +/- 2 틱 보정) ───
        # 우선 현재 마켓의 tickSize를 조회해야 정확한 2 틱을 더하고 뺄 수 있습니다.
        limit_price = 0.0
        signal_type = None
        reason = ""

        # 시장 정보에서 tickSize 가져오기
        tick_size = 0.001  # 기본값 (fallback)
        if self.exchange and self.exchange.markets and symbol in self.exchange.markets:
            market = self.exchange.markets[symbol]
            tick_size = float(market.get("precision", {}).get("price", 0.001))

        if rsi_val < self.rsi_os:
            signal_type = "LONG"
            # Lower Band + 2 Tick 위치에 지정가
            raw_limit = lower_band + (tick_size * 2)
            limit_price = (
                self.exchange.price_to_precision(symbol, raw_limit)
                if self.exchange
                else raw_limit
            )

            # 고정 비율 TP/SL 계산
            raw_tp = float(limit_price) * (1 + self.long_tp_pct)
            raw_sl = float(limit_price) * (1 - self.long_sl_pct)

            tp_price = (
                float(self.exchange.price_to_precision(symbol, raw_tp))
                if self.exchange
                else raw_tp
            )
            sl_price = (
                float(self.exchange.price_to_precision(symbol, raw_sl))
                if self.exchange
                else raw_sl
            )

            reason = (
                f"LONG 대기 (RSI: {rsi_val:.1f} < {self.rsi_os}). 지정가: {limit_price}"
            )

        elif rsi_val > self.rsi_ob:
            signal_type = "SHORT"
            # Upper Band - 2 Tick 위치에 지정가
            raw_limit = upper_band - (tick_size * 2)
            limit_price = (
                self.exchange.price_to_precision(symbol, raw_limit)
                if self.exchange
                else raw_limit
            )

            # 고정 비율 TP/SL 계산
            raw_tp = float(limit_price) * (1 - self.short_tp_pct)
            raw_sl = float(limit_price) * (1 + self.short_sl_pct)

            tp_price = (
                float(self.exchange.price_to_precision(symbol, raw_tp))
                if self.exchange
                else raw_tp
            )
            sl_price = (
                float(self.exchange.price_to_precision(symbol, raw_sl))
                if self.exchange
                else raw_sl
            )

            reason = f"SHORT 대기 (RSI: {rsi_val:.1f} > {self.rsi_ob}). 지정가: {limit_price}"

        else:
            return {"signal": None, "reason": f"RSI 중립 구간 ({rsi_val:.1f})"}

        return {
            "signal": signal_type,
            "limit_price": float(limit_price),
            "tp_price": float(tp_price),
            "sl_price": float(sl_price),
            "reason": reason,
            "current_rsi": rsi_val,
        }
