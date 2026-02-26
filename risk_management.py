from config import settings, logger
from data_pipeline import DataPipeline


class RiskManager:
    def __init__(self, data_pipeline: DataPipeline):
        """
        VWAP V11 고정 비율 자금 관리 모듈입니다.
        1회 진입 시 선물 계좌 총 자금의 3%에 해당하는 금액을 고정 투입합니다.
        바이낸스 최소 주문 금액 거절 방지를 위해 5.5 USDT 안전망이 추가되었습니다.
        """
        self.pipeline = data_pipeline
        self.risk_pct = (
            0.03  # 거래당 자본의 3% 배팅 (레버리지 1배이므로 그대로 명목금액)
        )
        self.min_order_usdt = 5.5  # 바이낸스 선물 최소 주문 금액 (안전 마진 포함)

    def calculate_position_size(
        self, symbol: str, capital: float, entry_price: float
    ) -> dict:
        """
        V11 고정 비율 투입 수량을 계산합니다.

        수식:
            invest_usdt = max(capital * 0.03, 5.5)
            position_size = invest_usdt / entry_price

        Returns:
            dict: {size, invest_usdt, min_notional_applied}
        """
        if capital <= 0 or entry_price <= 0:
            return {"size": 0.0, "invest_usdt": 0.0, "min_notional_applied": False}

        # 기본 3% 투입 금액 산출
        base_invest = capital * self.risk_pct
        min_notional_applied = False

        # 5.5 USDT 최소 제한망 적용
        if base_invest < self.min_order_usdt:
            invest_usdt = self.min_order_usdt
            min_notional_applied = True
            logger.info(
                f"[{symbol}] 산출된 금액({base_invest:.2f})이 최소 주문 금액 미달. {self.min_order_usdt} USDT로 보정 적용."
            )
        else:
            invest_usdt = base_invest

        # 만약 전체 자본금 자체가 5.5 USDT 미만이면 진입 불가
        if capital < self.min_order_usdt:
            logger.warning(
                f"가용 자본({capital:.2f})이 최소 주문금액({self.min_order_usdt}) 자체를 넘지 못해 진입 불가."
            )
            return {"size": 0.0, "invest_usdt": 0.0, "min_notional_applied": False}

        # 진입 코인 수량 산출
        raw_size = invest_usdt / entry_price

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

        # 리스크 투명성 로깅
        logger.info(
            f"[Position Sizing] [{symbol}] 투입: {invest_usdt:.2f} USDT, 수량: {position_size}, "
            f"Target Risk: {self.risk_pct * 100:.1f}%, 보정 여부: {min_notional_applied}"
        )

        return {
            "size": position_size,
            "invest_usdt": invest_usdt,
            "min_notional_applied": min_notional_applied,
        }
