from pydantic import BaseModel, ConfigDict, Field
from typing import Optional


class MarketDataSnapshot(BaseModel):
    """
    [V16.6] strategy.py 에서 진입 시점에 캡처되는 보조지표 스냅샷 규격
    데이터 타입과 Key 누락을 방지하여 ML 전처리 파이프라인의 안전성을 보장합니다.
    """

    model_config = ConfigDict(extra="ignore")  # 정의되지 않은 추가 필드는 무시

    adx_5m: Optional[float] = None
    adx_15m: Optional[float] = None
    adx_1h: Optional[float] = None

    rsi: Optional[float] = None
    rsi_15m: Optional[float] = None
    rsi_1h: Optional[float] = None

    atr_14: Optional[float] = None
    atr_200: Optional[float] = None

    sma_20: Optional[float] = None
    ema_20_15m: Optional[float] = None
    ema_50_15m: Optional[float] = None
    ema_20_1h: Optional[float] = None
    ema_50_1h: Optional[float] = None

    vwap: Optional[float] = None
    volume: Optional[float] = None
    twap_imbalance: Optional[float] = None

    mtf_bias_1h: Optional[str] = None
    mtf_bias_15m: Optional[str] = None
    regime: Optional[str] = None


class HFTFeatures1m(BaseModel):
    """
    [V16.8] hft_pipeline.py 에서 1분마다 취합되는 미시구조 지표 스냅샷 규격
    """

    model_config = ConfigDict(extra="ignore")

    ofi_1m: float = Field(description="Order Flow Imbalance 절대값")
    nofi_1m: float = Field(description="정규화된 Order Flow Imbalance (-1.0 ~ 1.0)")
    open_interest: float = Field(description="현재 미결제약정")
    funding_rate: float = Field(description="현재 펀딩비율")
    tick_count: int = Field(description="해당 분봉의 원시 체결 틱 갯수")
    # V17 신규 미시구조 피처
    buy_ratio: float = Field(default=0.5, description="매수 체결 비율 (0~1, 0.5=중립)")
    spread_avg: float = Field(
        default=0.0, description="1분간 평균 호가 스프레드 (ask-bid)"
    )
    log_volume_zscore: float = Field(
        default=0.0, description="로그 변환 거래량 Z-Score"
    )
