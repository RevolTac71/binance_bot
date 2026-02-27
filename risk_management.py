from config import settings, logger
from data_pipeline import DataPipeline


class RiskManager:
    def __init__(self, data_pipeline: DataPipeline):
        """
        VWAP V11 고정 비율 자금 관리 모듈입니다.
        config.py (혹은 .env) 에서 설정한 RISK_PERCENTAGE (예: 10%) 와 LEVERAGE (예: 3배)
        기반으로 1회 진입 시 투입할 규모를 통제합니다.
        바이낸스 최소 주문 금액 거절 방지를 위해 5.5 USDT 안전망이 추가되었습니다.
        """
        self.pipeline = data_pipeline
        self.risk_pct = settings.RISK_PERCENTAGE
        self.leverage = settings.LEVERAGE
        self.min_order_usdt = 5.5  # 바이낸스 선물 최소 주문 금액 (안전 마진 포함)

    def calculate_position_size(
        self, symbol: str, capital: float, entry_price: float
    ) -> dict:
        """
        V11 고정 비율 투입 수량 및 레버리지 자금 크기를 계산합니다.

        수식:
            margin_usdt = max(capital * risk_pct, 5.5) / leverage
            notional_value = margin_usdt * leverage
            position_size = notional_value / entry_price

        Returns:
            dict: {size, invest_usdt, min_notional_applied}
        """
        if capital <= 0 or entry_price <= 0:
            return {"size": 0.0, "invest_usdt": 0.0, "min_notional_applied": False}

        # 진입 코인 수량의 명목 가치 (Notional Value) 산출
        # (원금 * 위험률 * 레버리지)
        base_notional = capital * self.risk_pct * self.leverage
        min_notional_applied = False

        # 바이낸스 자체 5.5 USDT 현물 가치 최소 통과선 검증
        if base_notional < self.min_order_usdt:
            target_notional = self.min_order_usdt
            min_notional_applied = True
            logger.info(
                f"[{symbol}] 산출된 레버리지 총 금액({base_notional:.2f})이 최소 주문 금액 미달. {self.min_order_usdt} USDT로 보정 적용."
            )
        else:
            target_notional = base_notional

        # 자본금 자체가 레버리지 연동 시 5.5불을 도저히 못 만드는 극소 자본일 때 보호
        if target_notional > (capital * self.leverage):
            logger.warning(
                f"가용 자본({capital:.2f}) 최대 레버리지({self.leverage}x) 파워가 최소 주문금액({self.min_order_usdt}) 자체를 넘지 못해 진입 불가."
            )
            return {"size": 0.0, "invest_usdt": 0.0, "min_notional_applied": False}

        # 최종 진입 코인 수량 산출 (실제 확보할 코인 수)
        raw_size = target_notional / entry_price

        # 수량 정밀도 포맷팅 (stepSize 적용)
        try:
            position_size_str = self.pipeline.exchange.amount_to_precision(
                symbol, raw_size
            )
            position_size = float(position_size_str)
        except Exception as e:
            logger.warning(f"[{symbol}] 수량 정밀도 변환 오류: {e}. Raw: {raw_size}")
            position_size = raw_size  # fallback

        if position_size <= 0:
            return {"size": 0.0, "invest_usdt": 0.0, "min_notional_applied": False}

        # 실제 투자되는 순수 내 돈(Margin) = (수량 * 가격) / 레버리지
        actual_invested = (position_size * entry_price) / self.leverage

        # 리스크 투명성 로깅
        logger.info(
            f"[Position Sizing] [{symbol}] 실제 투입 마진: {actual_invested:.2f} USDT, "
            f"레버리지 명목금액: {target_notional:.2f} USDT, 수량: {position_size}, "
            f"Target Risk: {self.risk_pct * 100:.1f}%, 보정 여부: {min_notional_applied}"
        )

        return {
            "size": position_size,
            "invest_usdt": actual_invested,
            "min_notional_applied": min_notional_applied,
        }
