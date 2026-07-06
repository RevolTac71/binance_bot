"""
strategy.py  —  StrategyEngine V18
======================================
[V18 주요 변경사항]
1. 다중 시간 프레임(MTF) 필터링:
   - 1H EMA50/200 배열 → 거시적 방향 (BULL / BEAR / NEUTRAL)
   - 15m ADX + MACD   → 추세 강도 및 모멘텀
   - 3m 진입 트리거는 상위 프레임 방향과 일치할 때만 작동

2. 변동성 적응형 샹들리에 청산(Chandelier Exit / Trailing Stop):
   - 시간 기반 이탈(TIME_EXIT_MINUTES) 대신 ATR×배수 기반 동적 손절선

3. 포트폴리오 동시 진입 제한 (PortfolioState):
   - MAX_CONCURRENT_SAME_DIR 초과 시 동일 방향 신규 진입 차단

4. CVD(Cumulative Volume Delta) Placeholder:
   - check_entry()의 cvd_trend 인자로 "BUY_PRESSURE" | "SELL_PRESSURE" | None
   - 실시간 틱 웹소켓 구축 후 실제 값 주입 예정
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional
from scipy.stats import percentileofscore
from schemas import MarketDataSnapshot
from config import settings, logger


# ────────────────────────────────────────────────────────────────────────────
# 포트폴리오 상태 관리 클래스 (V18)
# ────────────────────────────────────────────────────────────────────────────
class PortfolioState:
    """
    봇 전체의 포지션 상태, 트레일링 익스트림 가격, 샹들리에 손절선을 관리합니다.

    주의:
        현재는 순수 인메모리(in-memory) 구현입니다.
        봇이 재시작되면 상태가 초기화되므로, 추후 SQLite/파일 영속화 레이어
        (예: PortfolioState.save() / PortfolioState.load())를 별도로 구현해 주세요.

    positions 딕셔너리 스키마:
        {
            "BTC/USDT:USDT": {
                "direction"        : "LONG" | "SHORT",
                "entry_price"      : float,      # 진입가
                "extreme"          : float,       # Long=최고점 / Short=최저점 추적
                "chandelier_stop"  : float,       # 현재 동적 손절선
                "atr"              : float,       # 진입 시점 ATR (손절 거리 재계산에 사용)
                "is_partial_tp_done": bool,       # V18.2: 분할 익절 완료 여부 (True일 때만 Chandelier 가동)
            }
        }
    """

    def __init__(self):
        self.positions: dict[str, dict] = {}

    # ── 통계 프로퍼티 ──────────────────────────────────────────────────────

    @property
    def open_longs(self) -> int:
        """현재 활성 롱 포지션 수"""
        return sum(1 for p in self.positions.values() if p["direction"] == "LONG")

    @property
    def open_shorts(self) -> int:
        """현재 활성 숏 포지션 수"""
        return sum(1 for p in self.positions.values() if p["direction"] == "SHORT")

    # ── 포지션 등록/해제 ──────────────────────────────────────────────────

    def register_position(
        self,
        symbol: str,
        direction: str,
        entry_price: float,
        atr: float,
    ) -> None:
        """
        신규 포지션을 등록하고 초기 샹들리에 손절선을 설정합니다.

        Args:
            symbol    : 종목 심볼 (예: "BTC/USDT:USDT")
            direction : "LONG" 또는 "SHORT"
            entry_price: 진입가
            atr       : 진입 시점의 ATR 값
        """
        mult = getattr(settings, "CHANDELIER_MULT", 2.0)

        if direction == "LONG":
            extreme = entry_price
            chandelier_stop = entry_price - atr * mult
        else:
            extreme = entry_price
            chandelier_stop = entry_price + atr * mult

        self.positions[symbol] = {
            "direction": direction,
            "entry_price": entry_price,
            "extreme": extreme,
            "chandelier_stop": chandelier_stop,
            "atr": atr,
            "is_partial_tp_done": False,
        }
        logger.info(
            f"[PortfolioState] {symbol} {direction} 포지션 등록 | "
            f"진입가={entry_price:.4f}, 초기 손절선={chandelier_stop:.4f}"
        )

    def close_position(self, symbol: str) -> None:
        """포지션을 해제합니다."""
        if symbol in self.positions:
            dir_ = self.positions[symbol]["direction"]
            del self.positions[symbol]
            logger.info(f"[PortfolioState] {symbol} {dir_} 포지션 해제.")

    # ── 샹들리에(트레일링 스탑) 갱신 ──────────────────────────────────────

    def update_chandelier(
        self,
        symbol: str,
        current_high: float,
        current_low: float,
        current_atr: float,
    ) -> float | None:
        """
        캔들 마감마다 호출하여 극단 가격과 샹들리에 손절선을 갱신합니다.

        동작 원리:
         - LONG: 새 캔들 고가가 이전 최고점을 경신 → 최고점 업데이트
                 새 손절선 = max(최고점 - ATR×mult, 이전 손절선)  ← 손절선이 내려가지 않음
         - SHORT: 새 캔들 저가가 이전 최저점을 경신 → 최저점 업데이트
                  새 손절선 = min(최저점 + ATR×mult, 이전 손절선) ← 손절선이 올라가지 않음

        Returns:
            갱신된 chandelier_stop 가격. symbol이 없으면 None.
        """
        pos = self.positions.get(symbol)
        if pos is None:
            return None

        mult = getattr(settings, "CHANDELIER_MULT", 2.0)
        be_trigger = getattr(settings, "BREAKEVEN_TRIGGER_MULT", 1.5)
        be_profit = getattr(settings, "BREAKEVEN_PROFIT_MULT", 0.2)
        direction = pos["direction"]
        prev_stop = pos["chandelier_stop"]
        entry = pos["entry_price"]

        if direction == "LONG":
            if current_high > pos["extreme"]:
                pos["extreme"] = current_high

            # 본절(Breakeven) 추적 로직 적용
            if (pos["extreme"] - entry) >= (current_atr * be_trigger):
                floor = entry + (current_atr * be_profit)
                new_stop = max(pos["extreme"] - current_atr * mult, floor)
            else:
                new_stop = pos["extreme"] - current_atr * mult

            pos["chandelier_stop"] = max(new_stop, prev_stop)

        else:  # SHORT
            if current_low < pos["extreme"]:
                pos["extreme"] = current_low

            # 본절(Breakeven) 추적 로직 적용
            if (entry - pos["extreme"]) >= (current_atr * be_trigger):
                ceiling = entry - (current_atr * be_profit)
                new_stop = min(pos["extreme"] + current_atr * mult, ceiling)
            else:
                new_stop = pos["extreme"] + current_atr * mult

            pos["chandelier_stop"] = min(new_stop, prev_stop)

        pos["atr"] = current_atr
        return pos["chandelier_stop"]

    def is_chandelier_triggered(self, symbol: str, current_price: float) -> bool:
        """
        현재가가 샹들리에 손절선을 돌파했는지 확인합니다.

        Returns:
            True면 청산 필요. False면 포지션 유지.
        """
        pos = self.positions.get(symbol)
        if pos is None:
            return False

        stop = pos["chandelier_stop"]
        direction = pos["direction"]

        if direction == "LONG" and current_price <= stop:
            return True

        if direction == "SHORT" and current_price >= stop:
            return True

        return False

    def get_stop_price(self, symbol: str) -> float | None:
        """현재 샹들리에 손절선 가격 반환 (없으면 None)."""
        pos = self.positions.get(symbol)
        return pos["chandelier_stop"] if pos else None

    def __repr__(self) -> str:
        lines = [f"PortfolioState | 롱={self.open_longs}, 숏={self.open_shorts}"]
        for sym, pos in self.positions.items():
            lines.append(
                f"  {sym}: {pos['direction']} | 극값={pos['extreme']:.4f} "
                f"| Chandelier={pos['chandelier_stop']:.4f}"
            )
        return "\n".join(lines)


# ────────────────────────────────────────────────────────────────────────────
# 전략 인터페이스 및 구체 전략 클래스 (V18.6)
# ────────────────────────────────────────────────────────────────────────────

class Strategy(ABC):
    """전략 인터페이스"""
    @abstractmethod
    async def check_entry(
        self,
        symbol: str,
        df: pd.DataFrame,
        portfolio: PortfolioState,
        interval: str = "3m",
        df_1h: Optional[pd.DataFrame] = None,
        df_15m: Optional[pd.DataFrame] = None,
        cvd_trend: Optional[str] = None,
        bid_ask_imbalance: float = 0.5,
        hft_features: Optional[dict] = None,
    ) -> dict:
        pass


class BBSqueezeVolatilityBreakout(Strategy):
    """
    볼린저 밴드 수축(Squeeze) 및 변동성 돌파(Volatility Breakout) 전략 (V18.7)
    - 밴드폭 백분위수(Percentile) 적용: 100봉 내 하위 20% 미만으로 좁아졌는가?
    - 실시간 펀딩비 필터 (LONG < 0.1%, SHORT > -0.05%)
    - 볼륨 프로파일 기반 매물대 저항 필터 (POC 저항선 가로막힘 차단)
    """

    def __init__(self, exchange=None, use_funding_filter=True, use_volume_profile_filter=True):
        self.exchange = exchange
        self.use_funding_filter = use_funding_filter
        self.use_volume_profile_filter = use_volume_profile_filter
        
        # 전략 파라미터 초기화
        self.squeeze_threshold = 0.8  # Percentile 폴백용
        self.volume_multiplier = 2.0
        self.atr_multiplier = 1.5
        self.max_squeeze_duration = 12
        
        # 각 티커별 상태 추적 딕셔너리
        self.ticker_status = {}

    def _calculate_poc(self, df: pd.DataFrame) -> float:
        """
        [V18.7] 최근 200개 캔들의 종가와 거래량을 활용하여 볼륨 프로파일의 POC(Point of Control)를 산출합니다.
        ATR 변동성 해상도를 대조 평가하여 Bins의 개수를 유동적(30~200)으로 결정해 가격 왜곡을 최소화합니다.
        """
        recent_df = df.tail(200)
        closes = recent_df["close"].astype(float)
        volumes = recent_df["volume"].astype(float)

        min_price = closes.min()
        max_price = closes.max()
        price_range = max_price - min_price
        
        if price_range <= 0:
            return min_price

        # ATR 변동성 단위 획득 (최근 ATR_20 지표의 5% 수준을 최소 해상도 단위로 simulated 적용)
        last_row = df.iloc[-1]
        atr20 = float(last_row.get("ATR_20", price_range * 0.05))
        if atr20 <= 0:
            atr20 = price_range * 0.05
            
        tick_unit = atr20 * 0.05
        
        # Bins 수 동적 결정 (최소 30개, 최대 200개 해상도로 클리핑)
        num_bins = int(price_range / tick_unit)
        num_bins = min(max(num_bins, 30), 200)

        bin_width = price_range / num_bins
        
        bin_volumes = [0.0] * num_bins
        for close, vol in zip(closes, volumes):
            bin_idx = int((close - min_price) / bin_width)
            if bin_idx >= num_bins:
                bin_idx = num_bins - 1
            if bin_idx < 0:
                bin_idx = 0
            bin_volumes[bin_idx] += vol
            
        poc_idx = bin_volumes.index(max(bin_volumes))
        poc_price = min_price + (poc_idx + 0.5) * bin_width
        return poc_price

    async def check_entry(
        self,
        symbol: str,
        df: pd.DataFrame,
        portfolio: PortfolioState,
        interval: str = "3m",
        df_1h: Optional[pd.DataFrame] = None,
        df_15m: Optional[pd.DataFrame] = None,
        cvd_trend: Optional[str] = None,
        bid_ask_imbalance: float = 0.5,
        hft_features: Optional[dict] = None,
    ) -> dict:
        """
        볼린저 밴드 수축(Squeeze, 100봉 내 20% 백분위수) 상태에서 돌파(Breakout) 조건을 감시합니다.
        펀딩비 과열 및 매물대(POC) 저항 구간일 경우 진입을 필터링하여 패스시킵니다.
        """
        if len(df) < 100:
            return {"signal": None, "reason": "데이터 부족 (최소 100봉 필요)"}

        current = df.iloc[-1]
        close_price = float(current["close"])
        volume = float(current["volume"])

        # 지표 획득
        ma20 = float(current.get("BB_MA20", close_price))
        bandwidth = float(current.get("BB_Bandwidth", 0.0))
        mean_bandwidth = float(current.get("BB_Mean_Bandwidth", 0.0))
        atr20 = float(current.get("ATR_20", 0.0))
        vol_ma20 = float(current.get("Vol_SMA_20", 1.0))

        # 티커 상태 초기화
        if symbol not in self.ticker_status:
            self.ticker_status[symbol] = {"in_squeeze": False, "squeeze_bars": 0}

        status = self.ticker_status[symbol]

        # 1. 존 볼린저 정석: 과거 100봉 동안의 밴드폭들 중에서 현재 밴드폭의 백분위수(Percentile) 판단
        bandwidths = df["BB_Bandwidth"].tail(100).dropna().tolist()
        if len(bandwidths) >= 50:
            from scipy.stats import percentileofscore
            pct = percentileofscore(bandwidths, bandwidth)
            is_squeezed = (pct < 20.0)
            logger.debug(f"[{symbol}] 밴드폭 백분위수: {pct:.1f}% (현재: {bandwidth:.6f})")
        else:
            is_squeezed = (bandwidth <= mean_bandwidth * self.squeeze_threshold)

        if is_squeezed:
            if not status["in_squeeze"]:
                status["in_squeeze"] = True
                status["squeeze_bars"] = 1
                logger.info(f"⏳ [{symbol}] 볼린저 밴드 수축(Squeeze 하위 20% 미만) 돌입! (진입 감시 시작)")
            else:
                status["squeeze_bars"] += 1
                logger.debug(f"⏳ [{symbol}] 스퀴즈 상태 유지 중 ({status['squeeze_bars']}봉째)")
        else:
            if status["in_squeeze"]:
                status["squeeze_bars"] += 1
                logger.debug(f"⏳ [{symbol}] 스퀴즈 돌파 감시 연장 ({status['squeeze_bars']}봉째)")

        # 스퀴즈 타임아웃 해제 (최대 12봉 경과 시 만료)
        if status["in_squeeze"] and status["squeeze_bars"] > self.max_squeeze_duration:
            logger.info(
                f"🔕 [{symbol}] 스퀴즈 감시 시간 만료 (Squeeze Timeout, {status['squeeze_bars']}봉 경과). 상태 리셋."
            )
            status["in_squeeze"] = False
            status["squeeze_bars"] = 0

        signal = None
        reason = "신호 없음"

        # 2. 돌파(Breakout) 조건 검사 (스퀴즈 상태일 때만 유효)
        if status["in_squeeze"]:
            upper_channel = ma20 + (atr20 * self.atr_multiplier)
            lower_channel = ma20 - (atr20 * self.atr_multiplier)
            
            # 거래량 조건: 20일 평균 거래량의 volume_multiplier 배수 돌파
            volume_condition = (volume >= vol_ma20 * self.volume_multiplier)

            if close_price > upper_channel and volume_condition:
                signal = "LONG"
                reason = f"[Squeeze Breakout LONG] 종가({close_price:.4f}) > 상단채널({upper_channel:.4f}) 및 거래량 돌파"
            elif close_price < lower_channel and volume_condition:
                signal = "SHORT"
                reason = f"[Squeeze Breakout SHORT] 종가({close_price:.4f}) < 하단채널({lower_channel:.4f}) 및 거래량 돌파"
            else:
                reason = (
                    f"스퀴즈 감시 중 ({status['squeeze_bars']}/{self.max_squeeze_duration}봉 | "
                    f"종가: {close_price:.4f}, Upper: {upper_channel:.4f}, Lower: {lower_channel:.4f}, "
                    f"거래량 {volume:.1f} (기준 {vol_ma20 * self.volume_multiplier:.1f})"
                )
        else:
            reason = "스퀴즈 비활성 상태"

        # 3. 추가 실전 필터 결합 (Funding Rate & Volume Profile POC)
        if signal:
            # 3.1 펀딩비 필터 검사
            funding_rate = 0.0
            if self.use_funding_filter:
                try:
                    if self.exchange:
                        funding_data = await self.exchange.fetch_funding_rate(symbol)
                        funding_rate = float(funding_data.get("fundingRate", 0.0))
                    else:
                        funding_rate = float((hft_features or {}).get("funding_rate", 0.0))
                except Exception as fe:
                    logger.warning(f"[{symbol}] 실시간 펀딩비 fetch 실패 (폴백 적용): {fe}")
                    funding_rate = float((hft_features or {}).get("funding_rate", 0.0))
                
                if signal == "LONG" and funding_rate > 0.001:
                    logger.info(f"🚫 [{symbol}] LONG 진입 차단: 펀딩비 과열 상태 ({funding_rate*100:.3f}% > 0.1%)")
                    signal = None
                    reason = f"펀딩비 롱과열 차단 ({funding_rate*100:.3f}%)"
                elif signal == "SHORT" and funding_rate < -0.0005:
                    logger.info(f"🚫 [{symbol}] SHORT 진입 차단: 펀딩비 숏스퀴즈 위험 ({funding_rate*100:.3f}% < -0.05%)")
                    signal = None
                    reason = f"펀딩비 숏과열 차단 ({funding_rate*100:.3f}%)"

        if signal:
            # 3.2 볼륨 프로파일 매물대(POC) 필터 검사
            if self.use_volume_profile_filter:
                poc_price = self._calculate_poc(df)
                
                if signal == "LONG" and (close_price < poc_price <= close_price * 1.02):
                    logger.info(f"🚫 [{symbol}] LONG 진입 차단: 매물대 저항대 인접 (POC: {poc_price:.4f}, 범위: {close_price:.4f} ~ {close_price * 1.02:.4f})")
                    signal = None
                    reason = f"매물대 POC 저항 차단 (POC: {poc_price:.4f})"
                elif signal == "SHORT" and (close_price * 0.98 <= poc_price < close_price):
                    logger.info(f"🚫 [{symbol}] SHORT 진입 차단: 매물대 지지대 인접 (POC: {poc_price:.4f}, 범위: {close_price * 0.98:.4f} ~ {close_price:.4f})")
                    signal = None
                    reason = f"매물대 POC 지지 차단 (POC: {poc_price:.4f})"

        # 최종 신호 발생 시 상태 리셋
        if signal:
            status["in_squeeze"] = False
            status["squeeze_bars"] = 0

        # DB 기록용 시장 스냅샷 데이터 생성
        market_data_obj = None
        if signal:
            market_data_obj = {
                "close": close_price,
                "volume": volume,
                "bandwidth": bandwidth,
                "mean_bandwidth": mean_bandwidth,
                "ma20": ma20,
                "atr20": atr20,
                "vol_ma20": vol_ma20,
                "squeeze_bars": status["squeeze_bars"],
                "strategy_params": {
                    "squeeze_threshold": self.squeeze_threshold,
                    "volume_multiplier": self.volume_multiplier,
                    "atr_multiplier": self.atr_multiplier,
                    "max_squeeze_duration": self.max_squeeze_duration,
                    "use_funding_filter": self.use_funding_filter,
                    "use_volume_profile_filter": self.use_volume_profile_filter,
                }
            }

        return {
            "signal": signal,
            "market_price": close_price,
            "atr_val": atr20,
            "reason": reason,
            "market_data": market_data_obj,
            "long_score": 100 if signal == "LONG" else 0,
            "short_score": 100 if signal == "SHORT" else 0,
            "excess_score": 0,
            "entry_type": "BB_SQUEEZE_BREAKOUT",
        }


class StrategyEngine:
    """[V18.7] 전략 패턴 컨텍스트
    - 진입 신호 판단은 구체 전략 인스턴스 BBSqueezeVolatilityBreakout 에 위임합니다.
    - 샹들리에 익절 감시 기능 공통 적용.
    """

    def __init__(self, exchange=None):
        self.exchange = exchange
        self.session_filter_bars = 30
        
        # 구체 전략 패턴 세팅 (펀딩비 및 매물대 필터 기본 활성화)
        self.strategy = BBSqueezeVolatilityBreakout(
            exchange=exchange,
            use_funding_filter=True,
            use_volume_profile_filter=True
        )

    async def check_entry(
        self,
        symbol: str,
        df: pd.DataFrame,
        portfolio: PortfolioState,
        interval: str = "3m",
        df_1h: Optional[pd.DataFrame] = None,
        df_15m: Optional[pd.DataFrame] = None,
        cvd_trend: Optional[str] = None,
        bid_ask_imbalance: float = 0.5,
        hft_features: Optional[dict] = None,
    ) -> dict:
        """독립적인 전략 클래스의 check_entry 로 포워딩합니다."""
        return await self.strategy.check_entry(
            symbol=symbol,
            df=df,
            portfolio=portfolio,
            interval=interval,
            df_1h=df_1h,
            df_15m=df_15m,
            cvd_trend=cvd_trend,
            bid_ask_imbalance=bid_ask_imbalance,
            hft_features=hft_features,
        )

    def check_chandelier_exit(
        self,
        symbol: str,
        portfolio: PortfolioState,
        current_price: float,
        current_high: float,
        current_low: float,
        current_atr: float,
    ) -> dict:
        """샹들리에 동적 익절/손절선을 갱신하고 돌파 여부를 감시합니다."""
        new_stop = portfolio.update_chandelier(
            symbol, current_high, current_low, current_atr
        )
        triggered = portfolio.is_chandelier_triggered(symbol, current_price)

        return {
            "exit": triggered,
            "chandelier_stop": new_stop,
            "reason": "Chandelier Exit 손절선 돌파" if triggered else "정상 유지"
        }
