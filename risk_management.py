from config import settings, logger


class RiskManager:
    def __init__(self, data_pipeline):
        """
        V15.0 리스크 매니지먼트 (Kelly-Risk Parity 버전)
        - ATR 기반 투입 갯수 산출 (리스크 패리티 방식: 손절 시 고정 비율만 잃음)
        - 설정(config / .env)된 RISK_PERCENTAGE(기본 0.5% = 0.005)를 총 자본의 허용 손실금으로 사용
        """
        self.pipeline = data_pipeline
        self.risk_pct = settings.RISK_PERCENTAGE  # 허용 손실 비율 (예: 0.005)
        self.leverage = settings.LEVERAGE
        self.min_order_usdt = 6.0  # 바이낸스 선물 최소 주문 금액 방어망

    def calculate_position_size(
        self, symbol: str, capital: float, entry_price: float, atr_val: float
    ) -> dict:
        """
        사용자 설정 비율 기반 거래 수량 동적 산출.
        단, TP/SL 거리는 시세 변동폭(ATR)을 반영합니다.

        Returns:
            dict: {
                "size": float,
                "invest_usdt": float,
                "tp_dist": float,
                "sl_dist": float
            }
        """
        if capital <= 0 or entry_price <= 0 or atr_val <= 0:
            return {"size": 0.0, "invest_usdt": 0.0, "tp_dist": 0.0, "sl_dist": 0.0}

        # 1. 1회 투입 증거금 액수 산출 (총 자본금 * 사용자가 설정한 투입 비율)
        # (이전의 Kelly-Risk Parity 방식이 아닌 단순 증거금 분할 투입 방식)
        margin_invest = capital * self.risk_pct

        # 2. 거래당 스탑폭/익절폭 금액 산출 (settings에서 읽어 유연하게 조정 가능)
        sl_mult = getattr(settings, "SL_MULT", 3.0)
        tp_mult = getattr(settings, "TP_MULT", 6.0)
        sl_distance = atr_val * sl_mult
        tp_distance = atr_val * tp_mult

        # 3. 최대 레버리지를 곱한 명목 진입 금액 (Notional Value)
        notional_value = margin_invest * self.leverage

        # 4. 바이낸스 최소 주문 한도(5.5~6.0 USDT) 방어
        if notional_value < self.min_order_usdt:
            notional_value = self.min_order_usdt
            margin_invest = notional_value / self.leverage

        # 5. 가용 증거금 초과(풀시드 초과) 안전장치 (가용 잔고의 95%까지만 최대 허용)
        if margin_invest > capital * 0.95:
            margin_invest = capital * 0.95
            notional_value = margin_invest * self.leverage

        # 6. 최종 계약 수량 산정
        calc_size = notional_value / entry_price

        # 수량 정밀도 포맷 관리 (바이낸스 규격 소수점)
        try:
            sz_str = self.pipeline.exchange.amount_to_precision(symbol, calc_size)
            final_size = float(sz_str)
        except Exception as e:
            final_size = calc_size

        # 실제 투입 증거금 재계산 (소수점 절사 등에 의해 미미하게 달라질 수 있음)
        actual_margin_invest = (final_size * entry_price) / self.leverage

        # 예상되는 달러 손절 금액 (수량 * 스탑폭)
        expected_loss = final_size * sl_distance

        logger.info(
            f"[Position Sizing] {symbol} | 증거금 투입 설정: {self.risk_pct * 100:.2f}% | "
            f"실투입 증거금: {actual_margin_invest:.2f} USDT | 수량: {final_size} | "
            f"TP: +{tp_distance:.4f} / SL: -{sl_distance:.4f} (예상손실: {expected_loss:.2f} USDT)"
        )

        return {
            "size": final_size,
            "invest_usdt": actual_margin_invest,
            "tp_dist": tp_distance,
            "sl_dist": sl_distance,
        }
