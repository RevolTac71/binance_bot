"""
strategy.py  —  StrategyEngine V16
======================================
[V16 주요 변경사항]
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

import pandas as pd
import numpy as np
from typing import Optional
from config import settings, logger


# ────────────────────────────────────────────────────────────────────────────
# 포트폴리오 상태 관리 클래스 (V16 신규)
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
        direction = pos["direction"]
        prev_stop = pos["chandelier_stop"]

        if direction == "LONG":
            # 최고점 경신 시 갱신
            if current_high > pos["extreme"]:
                pos["extreme"] = current_high
            # ATR 기반 새 손절선 계산 (항상 상승 방향으로만 이동)
            new_stop = pos["extreme"] - current_atr * mult
            pos["chandelier_stop"] = max(new_stop, prev_stop)

        else:  # SHORT
            # 최저점 경신 시 갱신
            if current_low < pos["extreme"]:
                pos["extreme"] = current_low
            # ATR 기반 새 손절선 계산 (항상 하락 방향으로만 이동)
            new_stop = pos["extreme"] + current_atr * mult
            pos["chandelier_stop"] = min(new_stop, prev_stop)

        pos["atr"] = current_atr  # ATR도 최신 값으로 갱신
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
            logger.info(
                f"[Chandelier] {symbol} LONG 손절선 돌파! "
                f"현재가={current_price:.4f} ≤ 손절선={stop:.4f}"
            )
            return True

        if direction == "SHORT" and current_price >= stop:
            logger.info(
                f"[Chandelier] {symbol} SHORT 손절선 돌파! "
                f"현재가={current_price:.4f} ≥ 손절선={stop:.4f}"
            )
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
# 전략 엔진 (V16 MTF + Chandelier + Portfolio + CVD)
# ────────────────────────────────────────────────────────────────────────────
class StrategyEngine:
    """
    [V16] 다중 시간 프레임 적응형 스캘핑 전략 엔진

    Data Flow:
        main.py (WebSocket)
            └─→ process_closed_kline()
                    ├─ df_3m  : 3분봉 (진입 트리거)
                    ├─ df_1h  : 1시간봉 (거시 방향, htf_refresh_loop가 주기 공급)
                    ├─ df_15m : 15분봉 (추세 강도/모멘텀, htf_refresh_loop가 주기 공급)
                    └─ cvd_trend : "BUY_PRESSURE" | "SELL_PRESSURE" | None (Placeholder)
                            └─→ strategy.check_entry(df_3m, df_1h, df_15m, cvd_trend)
    """

    def __init__(self, exchange=None):
        self.exchange = exchange

        # RSI 과매도/과매수 기준
        self.rsi_os = 30
        self.rsi_ob = 70

        # 세션 필터 (VWAP 리셋 후 최소 안정화 봉수)
        self.session_filter_bars = 30

    # ────────────────────────────────────────────────────────────────────────
    # 1. HTF 추세 방향 판별 (1시간봉 EMA 배열)
    # ────────────────────────────────────────────────────────────────────────
    def get_htf_bias(self, df_1h: Optional[pd.DataFrame]) -> str:
        """
        [V16 MTF] 1H 봉의 EMA50/200 배열로 거시적 시장 방향을 판단합니다.

        판단 기준:
          - EMA50 > EMA200 * 1.002 → "BULL"  (완전한 골든크로스 상태)
          - EMA50 < EMA200 * 0.998 → "BEAR"  (완전한 데드크로스 상태)
          - 그 외 (근접 = 0.2% 이내) → "NEUTRAL" (배열 혼재, 신중한 접근)

        Returns:
            "BULL" | "BEAR" | "NEUTRAL"
        """
        # HTF 데이터가 없거나 부족하면 중립 반환
        if df_1h is None or len(df_1h) < 50:
            return "NEUTRAL"

        last = df_1h.iloc[-1]
        ema50 = last.get("EMA_50", None)
        ema200 = last.get("EMA_200", None)

        if ema50 is None or ema200 is None or pd.isna(ema50) or pd.isna(ema200):
            return "NEUTRAL"

        # EMA200을 기준으로 0.2% 간격 버퍼 적용 (노이즈 필터)
        buf = ema200 * 0.002
        if ema50 > ema200 + buf:
            bias = "BULL"
        elif ema50 < ema200 - buf:
            bias = "BEAR"
        else:
            bias = "NEUTRAL"

        logger.debug(f"[HTF 1H Bias] EMA50={ema50:.4f}, EMA200={ema200:.4f} → {bias}")
        return bias

    # ────────────────────────────────────────────────────────────────────────
    # 2. MTF 추세 강도 / 모멘텀 판별 (15분봉 ADX + MACD)
    # ────────────────────────────────────────────────────────────────────────
    def get_mtf_regime(self, df_15m: Optional[pd.DataFrame]) -> dict:
        """
        [V16 MTF] 15m 봉의 ADX와 MACD로 현재 장세 유형과 모멘텀 방향을 반환합니다.

        Returns:
            dict:
                "regime"      : "TREND" | "RANGE"          (장세 유형)
                "momentum"    : "BULLISH" | "BEARISH" | "NEUTRAL"  (MACD 방향)
                "adx"         : float | None
                "macd"        : float | None
                "macd_signal" : float | None
        """
        result = {
            "regime": "RANGE",  # 데이터 없으면 기본적으로 횡보장으로 가정
            "momentum": "NEUTRAL",
            "adx": None,
            "macd": None,
            "macd_signal": None,
        }

        if df_15m is None or len(df_15m) < 35:
            return result

        last = df_15m.iloc[-1]
        adx = last.get("ADX_14", None)
        macd = last.get("MACD", None)
        macd_s = last.get("MACD_S", None)

        adx_threshold = getattr(settings, "ADX_THRESHOLD", 25.0)

        # ADX 기반 장세 분류
        if adx is not None and not pd.isna(adx):
            result["adx"] = float(adx)
            result["regime"] = "TREND" if adx >= adx_threshold else "RANGE"

        # MACD 기반 모멘텀 방향 분류
        if (
            macd is not None
            and macd_s is not None
            and not pd.isna(macd)
            and not pd.isna(macd_s)
        ):
            result["macd"] = float(macd)
            result["macd_signal"] = float(macd_s)
            if macd > macd_s:
                result["momentum"] = "BULLISH"
            elif macd < macd_s:
                result["momentum"] = "BEARISH"

        logger.debug(
            f"[MTF 15m Regime] ADX={result['adx']}, 장세={result['regime']}, "
            f"MACD={result['macd']}, 모멘텀={result['momentum']}"
        )
        return result

    # ────────────────────────────────────────────────────────────────────────
    # 3. 샹들리에 청산 확인 (외부 루프에서 호출)
    # ────────────────────────────────────────────────────────────────────────
    def check_chandelier_exit(
        self,
        symbol: str,
        portfolio: PortfolioState,
        current_price: float,
        current_high: float,
        current_low: float,
        current_atr: float,
    ) -> dict:
        """
        [V16 Chandelier] state_machine_loop에서 매 캔들 마감 시 호출합니다.
        손절선을 갱신한 뒤, 현재가가 돌파 시 EXIT 신호를 반환합니다.

        Args:
            symbol        : 종목 심볼
            portfolio     : PortfolioState 인스턴스 (공유 참조)
            current_price : 현재 종가
            current_high  : 현재 고가
            current_low   : 현재 저가
            current_atr   : 현재 ATR14 값

        Returns:
            dict:
                "exit"           : bool
                "chandelier_stop": float | None  (갱신된 손절선)
                "reason"         : str
        """
        # 1. 손절선 갱신
        new_stop = portfolio.update_chandelier(
            symbol, current_high, current_low, current_atr
        )

        # 2. 돌파 여부 확인
        triggered = portfolio.is_chandelier_triggered(symbol, current_price)

        return {
            "exit": triggered,
            "chandelier_stop": new_stop,
            "reason": "Chandelier Exit 손절선 돌파" if triggered else "정상 유지",
        }

    # ────────────────────────────────────────────────────────────────────────
    # 4. 메인 진입 판단 함수 (V16 통합 버전)
    # ────────────────────────────────────────────────────────────────────────
    def check_entry(
        self,
        symbol: str,
        df: pd.DataFrame,
        portfolio: PortfolioState,
        df_1h: Optional[pd.DataFrame] = None,
        df_15m: Optional[pd.DataFrame] = None,
        cvd_trend: Optional[str] = None,
    ) -> dict:
        """
        [V16] 다중 시간 프레임 기반 진입 신호를 판단합니다.

        Args:
            symbol     : 종목 심볼
            df         : 3분봉 OHLCV + 지표 데이터프레임 (지표 연산 완료 상태)
            portfolio  : PortfolioState 공유 인스턴스
            df_1h      : 1시간봉 데이터프레임 (EMA50, EMA200 컬럼 포함)
            df_15m     : 15분봉 데이터프레임 (ADX_14, MACD, MACD_S 컬럼 포함)
            cvd_trend  : CVD 오더플로우 방향 (Placeholder)
                         "BUY_PRESSURE"  → 공격적 시장가 매수 우세
                         "SELL_PRESSURE" → 공격적 시장가 매도 우세
                         None            → 미수집 (현재 기본값)

        Returns:
            dict:
                "signal"      : "LONG" | "SHORT" | None
                "market_price": float
                "atr_val"     : float
                "vwap_mid"    : float
                "reason"      : str
        """
        # ── 기초 데이터 검증 ──────────────────────────────────────────────
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

        # 결측치 방어
        if (
            pd.isna(lower_band)
            or pd.isna(upper_band)
            or pd.isna(atr_14)
            or pd.isna(vol_sma_20)
        ):
            return {"signal": None, "reason": "지표 결측치 발생"}

        # ── STEP 1: Session Filter ────────────────────────────────────────
        current_time = df.index[-1] if df.index.name == "datetime" else current.name
        today_date = current_time.date()
        if current_time.hour < 9:
            today_date = (current_time - pd.Timedelta(days=1)).date()

        bars_since_reset = len(df[df.index.date == today_date])
        if bars_since_reset < self.session_filter_bars:
            logger.info(
                f"[{symbol}] ⏳ [STEP1 Session Filter] VWAP 리셋 후 "
                f"{bars_since_reset}/{self.session_filter_bars}봉 — 안정화 대기 중"
            )
            return {
                "signal": None,
                "reason": (
                    f"Session Filter Active "
                    f"(VWAP 리셋 후 {bars_since_reset}/{self.session_filter_bars}봉 째)"
                ),
            }

        # ── STEP 2: 동적 ATR 변동성 필터 ─────────────────────────────────
        atr_ratio_mult = getattr(settings, "ATR_RATIO_MULT", 1.2)
        if atr_14 <= atr_long * atr_ratio_mult:
            logger.info(
                f"[{symbol}] 📉 [STEP2 Volatility Filter] 변동성 부족 — "
                f"ATR14={atr_14:.4f} ≤ ATR{atr_long_len}({atr_long:.4f}) × {atr_ratio_mult}"
            )
            return {
                "signal": None,
                "reason": (
                    f"Low Volatility (ATR Short {atr_14:.4f} "
                    f"<= ATR Long {atr_long:.4f} x {atr_ratio_mult})"
                ),
            }

        # ── STEP 3: [V16 NEW] HTF Bias 판별 (1H EMA 배열) ────────────────
        htf_bias = self.get_htf_bias(df_1h)

        # ── STEP 4: [V16 NEW] MTF Regime 판별 (15m ADX/MACD) ─────────────
        mtf = self.get_mtf_regime(df_15m)
        regime = mtf["regime"]  # "TREND" | "RANGE"
        momentum = mtf["momentum"]  # "BULLISH" | "BEARISH" | "NEUTRAL"

        # HTF/MTF 상태를 캔들마다 INFO로 출력 (봇 작동 여부 확인용)
        logger.info(
            f"[{symbol}] 📊 [MTF 상태] "
            f"1H Bias={htf_bias} | "
            f"Regime={regime} (ADX={mtf['adx']}) | "
            f"Momentum={momentum} | "
            f"RSI={rsi_val:.1f} | Vol={volume / vol_sma_20:.1f}x (기준={vol_mult:.1f}x)"
        ) if not (
            pd.isna(rsi_val) if hasattr(rsi_val, "__class__") else False
        ) else None

        # ── STEP 5: Volume Spike 판별 ─────────────────────────────────────
        vol_mult = getattr(settings, "VOL_MULT", 1.5)  # 일반 돌파: 1.5x~2.0x
        extreme_mult = getattr(
            settings, "EXTREME_VOL_MULT", 2.5
        )  # 극단 소진: 2.5x~3.0x
        is_vol_spike = volume > (vol_sma_20 * vol_mult)
        is_extreme_vol = volume > (vol_sma_20 * extreme_mult)

        # ── STEP 6: Price Action Rejection / Extreme Outlier ──────────────
        long_rejection = (low_price <= lower_band) and (market_price > lower_band)
        short_rejection = (high_price >= upper_band) and (market_price < upper_band)

        # ── STEP 7: [V16 NEW] CVD 오더플로우 검증 (Placeholder) ──────────
        # cvd_trend가 실제로 공급되면 방향 불일치 시 진입 차단
        # 현재는 None이 기본값이므로 필터가 작동하지 않음 (무조건 통과)
        #
        # 향후 구현 예시:
        #   - 바이낸스 aggTrade 웹소켓 수신
        #   - buy_vol, sell_vol 누적 → CVD = buy_vol - sell_vol
        #   - CVD > 0 → "BUY_PRESSURE", CVD < 0 → "SELL_PRESSURE"
        cvd_long_ok = cvd_trend != "SELL_PRESSURE"  # 롱 진입 시 강한 매도 압력이면 차단
        cvd_short_ok = cvd_trend != "BUY_PRESSURE"  # 숏 진입 시 강한 매수 압력이면 차단

        # ── STEP 8: 장세별 분기 + HTF 방향 일치 필터 ──────────────────────
        signal_type = None
        reason = ""

        if regime == "RANGE":
            # 횡보장: 평균 회귀(역추세) 로직 → RSI 과매도/과매수 + VWAP 밴드 반전
            # ★ 엄격한 HTF 방향 필터: BULL이면 롱만 허용 / BEAR이면 숏만 허용
            #   → NEUTRAL(EMA가 근접한 과도기)에서는 진입하지 않음
            long_htf_ok = htf_bias == "BULL"
            short_htf_ok = htf_bias == "BEAR"

            if (
                long_htf_ok
                and cvd_long_ok
                and rsi_val <= self.rsi_os
                and (
                    (is_vol_spike and long_rejection)
                    or (is_extreme_vol and low_price <= lower_band)
                )
            ):
                signal_type = "LONG"
                reason = (
                    f"[RANGE 역추세 롱] RSI={rsi_val:.1f}≤{self.rsi_os} | "
                    f"HTF={htf_bias} | VolSpike={volume / vol_sma_20:.1f}x | "
                    f"ATR={atr_14:.4f} | CVD={cvd_trend}"
                )

            elif (
                short_htf_ok
                and cvd_short_ok
                and rsi_val >= self.rsi_ob
                and (
                    (is_vol_spike and short_rejection)
                    or (is_extreme_vol and high_price >= upper_band)
                )
            ):
                signal_type = "SHORT"
                reason = (
                    f"[RANGE 역추세 숏] RSI={rsi_val:.1f}≥{self.rsi_ob} | "
                    f"HTF={htf_bias} | VolSpike={volume / vol_sma_20:.1f}x | "
                    f"ATR={atr_14:.4f} | CVD={cvd_trend}"
                )

        else:  # TREND 장세: 모멘텀 추종 로직
            # 추세장: HTF Bias와 MACD 모멘텀이 모두 일치할 때만 진입
            long_htf_ok = (htf_bias == "BULL") and (momentum == "BULLISH")
            short_htf_ok = (htf_bias == "BEAR") and (momentum == "BEARISH")

            if (
                long_htf_ok
                and cvd_long_ok
                and rsi_val <= self.rsi_os
                and (
                    (is_vol_spike and long_rejection)
                    or (is_extreme_vol and low_price <= lower_band)
                )
            ):
                signal_type = "LONG"
                reason = (
                    f"[TREND 모멘텀 롱] ADX={mtf['adx']:.1f}≥{getattr(settings, 'ADX_THRESHOLD', 25)} | "
                    f"HTF={htf_bias} | MACD={momentum} | "
                    f"VolSpike={volume / vol_sma_20:.1f}x | CVD={cvd_trend}"
                )

            elif (
                short_htf_ok
                and cvd_short_ok
                and rsi_val >= self.rsi_ob
                and (
                    (is_vol_spike and short_rejection)
                    or (is_extreme_vol and high_price >= upper_band)
                )
            ):
                signal_type = "SHORT"
                reason = (
                    f"[TREND 모멘텀 숏] ADX={mtf['adx']:.1f}≥{getattr(settings, 'ADX_THRESHOLD', 25)} | "
                    f"HTF={htf_bias} | MACD={momentum} | "
                    f"VolSpike={volume / vol_sma_20:.1f}x | CVD={cvd_trend}"
                )

        # ── STEP 9: [V16 NEW] 포트폴리오 동시 진입 제한 ──────────────────
        if signal_type is not None:
            max_same_dir = getattr(settings, "MAX_CONCURRENT_SAME_DIR", 2)
            current_longs = portfolio.open_longs
            current_shorts = portfolio.open_shorts

            if signal_type == "LONG" and current_longs >= max_same_dir:
                discarded_reason = (
                    f"[Portfolio 제한] 롱 포지션 {current_longs}개 이미 존재 "
                    f"(최대={max_same_dir}). 신규 롱 Discard."
                )
                logger.info(f"[{symbol}] {discarded_reason}")
                return {
                    "signal": None,
                    "market_price": market_price,
                    "atr_val": float(atr_14),
                    "vwap_mid": float(vwap_mid),
                    "reason": discarded_reason,
                }

            if signal_type == "SHORT" and current_shorts >= max_same_dir:
                discarded_reason = (
                    f"[Portfolio 제한] 숏 포지션 {current_shorts}개 이미 존재 "
                    f"(최대={max_same_dir}). 신규 숏 Discard."
                )
                logger.info(f"[{symbol}] {discarded_reason}")
                return {
                    "signal": None,
                    "market_price": market_price,
                    "atr_val": float(atr_14),
                    "vwap_mid": float(vwap_mid),
                    "reason": discarded_reason,
                }

        if signal_type is None and not reason:
            reason = (
                f"진입 조건 불충족 | HTF={htf_bias} | Regime={regime} | "
                f"Momentum={momentum} | RSI={rsi_val:.1f}"
            )

        # [V16 진단 로그] 진입 실패 시 상세 원인 출력
        if signal_type is None:
            vol_ratio = volume / vol_sma_20
            long_htf_ok_check = (
                (htf_bias == "BULL")
                if regime == "RANGE"
                else ((htf_bias == "BULL") and (momentum == "BULLISH"))
            )
            short_htf_ok_check = (
                (htf_bias == "BEAR")
                if regime == "RANGE"
                else ((htf_bias == "BEAR") and (momentum == "BEARISH"))
            )

            if htf_bias == "NEUTRAL":
                logger.info(
                    f"[{symbol}] 🚫 [STEP3 HTF Block] 1H EMA 과도기(NEUTRAL) — "
                    f"방향 미확정으로 진입 차단"
                )
            elif not (long_htf_ok_check or short_htf_ok_check):
                logger.info(
                    f"[{symbol}] 🚫 [STEP3 HTF Block] 방향 불일치 — "
                    f"HTF={htf_bias}, Regime={regime}, Momentum={momentum}"
                )
            elif not is_vol_spike and not is_extreme_vol:
                logger.info(
                    f"[{symbol}] 📉 [STEP5 Volume] 거래량 부족 — "
                    f"{vol_ratio:.2f}x (진입 기준 {vol_mult:.1f}x 미달)"
                )
            elif not (long_rejection or short_rejection):
                logger.info(
                    f"[{symbol}] ↩️ [STEP6 PriceAction] 밴드 터치/리젝션 없음 — "
                    f"Close={market_price:.4f}, Lower={lower_band:.4f}, Upper={upper_band:.4f}"
                )
            elif rsi_val > self.rsi_os and rsi_val < self.rsi_ob:
                logger.info(
                    f"[{symbol}] 〽️ [STEP8 RSI] RSI 중립 구간 — "
                    f"RSI={rsi_val:.1f} (과매도≤{self.rsi_os} / 과매수≥{self.rsi_ob} 아님)"
                )
            else:
                logger.info(f"[{symbol}] ✖ [STEP8] 복합 조건 미충족 — {reason}")

        return {
            "signal": signal_type,
            "market_price": market_price,
            "atr_val": float(atr_14),
            "vwap_mid": float(vwap_mid),
            "reason": reason,
        }
