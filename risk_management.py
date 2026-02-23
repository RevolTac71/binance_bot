import pandas as pd
from config import settings, logger
from data_pipeline import DataPipeline


class RiskManager:
    def __init__(self, data_pipeline: DataPipeline):
        """
        리스크 관리를 위해 실시간 가격을 조사해야할 상황이 있으므로
        DataPipeline 객체를 인젝션받습니다.
        """
        self.pipeline = data_pipeline
        self.risk_pct = settings.RISK_PERCENTAGE  # 최대 손실 제한 비율 (기본 0.02 = 2%)

    def calculate_position_size(
        self, capital: float, entry_price: float, stop_loss: float
    ) -> float:
        """
        고정 자산 리스크 비율(2%)을 토대로 진입 수량을 산출합니다.
        (슬리피지 0.05%를 여분 마진으로 추가하여 보수적으로 계산)

        Args:
            capital: 가용 총 잔고 (USDT 기준)
            entry_price: 자산의 진입 예상 가격
            stop_loss: ATR로 계산한 하드 스탑로스 가격
        """
        risk_amount = capital * self.risk_pct

        # 1주 당 손실 (슬리피지 비용 등 추가 가산)
        slippage_estimate = entry_price * 0.0005
        loss_per_coin = abs(entry_price - stop_loss) + slippage_estimate

        if loss_per_coin <= 0:
            return 0.0

        size = risk_amount / loss_per_coin
        return size

    def calculate_stop_loss(self, entry_price: float, atr_14: float) -> float:
        """
        진입 시 최근 14주기 ATR의 2배 거리에 위치한 하드 스탑로스를 계산합니다. (Long 진입 기준)
        """
        return entry_price - (atr_14 * 2)

    async def is_highly_correlated_with_btc(
        self, symbol: str, threshold: float = 0.85
    ) -> bool:
        """
        비트코인(BTCUSDT)과의 피어슨 상관계수를 계산하여 특정 값(0.85) 이상이면
        중복 진입 방지를 위해 True를 반환합니다.
        """
        if symbol == "BTCUSDT":
            return False  # 자기 자신은 필터링하지 않음

        try:
            btc_df = await self.pipeline.fetch_ohlcv_df("BTCUSDT", limit=100)
            alt_df = await self.pipeline.fetch_ohlcv_df(symbol, limit=100)

            # 인덱스(시간) 기준으로 공통 데이터 병합 (join)
            joined = pd.concat([btc_df["close"], alt_df["close"]], axis=1, join="inner")
            joined.columns = ["BTC", "ALT"]

            # pandas corr 메서드를 이용한 pearson 상관계수 도출
            corr = joined["BTC"].corr(joined["ALT"], method="pearson")
            logger.info(
                f"[{symbol}]와 BTC의 피어슨 상관계수 산출: {corr:.3f} (허용치: {threshold})"
            )

            return corr >= threshold

        except Exception as e:
            logger.error(f"상관관계 계산 중 에러 발생: {e}")
            # 에러 발생 시, 매매 기회를 놓치지 않도록 필터 통과하는 것을 기본 원칙으로 함 (False 리턴)
            return False
