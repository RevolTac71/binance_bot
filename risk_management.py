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

        # 1. 1회 손실 허용액 산출 (복리 효과: 자본 성장에 비례)
        risk_amount = capital * self.risk_pct

        # 2. 거래당 스탑폭/익절폭 금액 (1코인당 잃을 수 있는 달러가치)
        sl_distance = atr_val * 1.5
        tp_distance = atr_val * 2.5

        # 3. 목표 수량: 내가 감내할 총 손실액 / 1코인당 손절폭
        calc_size = risk_amount / sl_distance if sl_distance > 0 else 0.0

        # 4. 최대 레버리지 한도 방어막 적용 (Max Notional)
        max_notional = capital * self.leverage
        current_notional = calc_size * entry_price

        if current_notional > max_notional:
            logger.warning(
                f"⚠️ 진입 금액({current_notional:.2f})이 최대 레버리지 한도({max_notional:.2f})를 초과하여 강제 조정됩니다."
            )
            calc_size = max_notional / entry_price
            current_notional = calc_size * entry_price

        # 보호 로직: 바이낸스 5.5~6.0 USDT 미만 진입 거절 방어
        if current_notional < self.min_order_usdt:
            calc_size = self.min_order_usdt / entry_price
            current_notional = self.min_order_usdt

        # 수량 정밀도 포맷 관리 (바이낸스 규격 소수점)
        try:
            sz_str = self.pipeline.exchange.amount_to_precision(symbol, calc_size)
            final_size = float(sz_str)
        except Exception as e:
            final_size = calc_size

        actual_margin_invest = current_notional / self.leverage

        logger.info(
            f"[Position Sizing] {symbol} | 예상 손실액: {risk_amount:.2f} USDT ({self.risk_pct * 100:.2f}%) | 투입 증거금: {actual_margin_invest:.2f} USDT | "
            f"수량: {final_size} | TP: +{tp_distance:.4f} / SL: -{sl_distance:.4f} (ATR: {atr_val:.4f})"
        )

        return {
            "size": final_size,
            "invest_usdt": actual_margin_invest,
            "tp_dist": tp_distance,
            "sl_dist": sl_distance,
        }
