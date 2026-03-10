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

import pandas as pd
import numpy as np
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
# 전략 엔진 (V18 MTF + Chandelier + Portfolio + CVD)
# ────────────────────────────────────────────────────────────────────────────
class StrategyEngine:
    """[V18] 다중 시간 프레임 적응형 스캘핑 전략 엔진

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

        # 세션 필터 (VWAP 리셋 후 최소 안정화 봉수)
        self.session_filter_bars = 30

    # ────────────────────────────────────────────────────────────────────────
    # 1. HTF 추세 방향 판별 (1시간봉 EMA 배열)
    # ────────────────────────────────────────────────────────────────────────
    def get_htf_bias(self, df_1h: Optional[pd.DataFrame]) -> str:
        """
        [V18] 1H 봉의 EMA50/200 배열로 거시적 시장 방향을 판단합니다.

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
        [V18] 15m 봉의 ADX와 MACD로 현재 장세 유형과 모멘텀 방향을 반환합니다.

        Returns:
            dict:
                "regime"      : "TREND" | "RANGE"          (장세 유형)
                "momentum"    : "BULLISH" | "BEARISH" | "NEUTRAL"  (MACD 방향)
                "adx"         : float | None
                "adx_sma"     : float | None
                "macd"        : float | None
                "macd_signal" : float | None
        """
        result = {
            "regime": "RANGE",  # 데이터 없으면 기본적으로 횡보장으로 가정
            "momentum": "NEUTRAL",
            "adx": None,
            "adx_sma": None,
            "macd": None,
            "macd_signal": None,
        }

        if df_15m is None or len(df_15m) < 100:
            return result

        last = df_15m.iloc[-1]
        adx = last.get("ADX_14", None)
        adx_sma = last.get("ADX_SMA_50", None)
        macd = last.get("MACD", None)
        macd_s = last.get("MACD_S", None)

        # ADX 기반 장세 분류 — V18
        adx_pctl = last.get("ADX_PCTL_80", None)
        if adx is not None and not pd.isna(adx):
            result["adx"] = float(adx)
            if adx_sma is not None and not pd.isna(adx_sma):
                result["adx_sma"] = float(adx_sma)

            # V18: 백분위수가 있으면 종목별 고유 기준으로 TREND/RANGE 판별
            if adx_pctl is not None and not pd.isna(adx_pctl):
                result["regime"] = "TREND" if adx >= float(adx_pctl) else "RANGE"
            elif adx_sma is not None and not pd.isna(adx_sma):
                result["regime"] = "TREND" if adx >= float(adx_sma) else "RANGE"
            else:
                result["regime"] = "TREND" if adx >= 20.0 else "RANGE"

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
        [V18] state_machine_loop에서 매 캔들 마감 시 호출합니다.
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

        # [V18.2] Safety Gear: TP1이 체결되기 전에는 Chandelier Exit을 억제 (수익권이 아닐 때 너무 일찍 털리는 현상 방지)
        pos = portfolio.positions.get(symbol)
        if pos and not pos.get("is_partial_tp_done", False):
            if triggered:
                # 30초마다 스팸 방지를 위해 로깅은 하되 청산은 보류 (사용자 인지용)
                logger.info(
                    f"🔍 [Chandelier] {symbol} 손절선 돌파 감지 (단, 1차 익절 도달 전이므로 청산 보류)"
                )

            # 분할 익절 전이면 샹들리에 손절선은 갱신하되, 실제 청산 신호는 내보내지 않음 (거래소 SL이 담당)
            return {
                "exit": False,
                "chandelier_stop": new_stop,
                "reason": "TP1 도달 전 샹들리에 트리거 억제",
            }

        return {
            "exit": triggered,
            "chandelier_stop": new_stop,
            "reason": "Chandelier Exit 손절선 돌파" if triggered else "정상 유지",
        }

    # ────────────────────────────────────────────────────────────────────────
    # 4. 메인 진입 판단 함수 (V18)
    # ────────────────────────────────────────────────────────────────────────
    def check_entry(
        self,
        symbol: str,
        df: pd.DataFrame,
        portfolio: PortfolioState,
        df_1h: Optional[pd.DataFrame] = None,
        df_15m: Optional[pd.DataFrame] = None,
        cvd_trend: Optional[str] = None,
        bid_ask_imbalance: float = 0.5,
        all_htf_15m: Optional[dict] = None,
        hft_features: Optional[dict] = None,
    ) -> dict:
        """
        [V18] 다중 시간 프레임 기반 진입 신호를 판단합니다.
        순서: 1) 데이터 수집/정규화 -> 2) 스코어 산출(무조건 실행) -> 3) 진입 조건 필터링
        """
        # ── 1. 기초 데이터 검증 및 연산 ────────────────────────────────────
        if len(df) < 250:
            return {"signal": None, "reason": "데이터 부족 (최소 250개 필요)"}

        current = df.iloc[-1]
        market_price = float(current["close"])
        vwap_mid = current.get("VWAP", market_price)
        vol_sma_20 = current.get("Vol_SMA_20")
        atr_14 = current.get("ATR_14", market_price * 0.005)
        atr_long_len = getattr(settings, "ATR_LONG_LEN", 200)
        atr_long = current.get(f"ATR_{atr_long_len}", atr_14)

        if vol_sma_20 is None or pd.isna(atr_14) or pd.isna(vol_sma_20):
            return {"signal": None, "reason": "지표 결측치 발생 (초동 방어)"}

        # ── 2. 데이터 정규화 및 피처 주입 (스코어 산출용) ───────────────────
        from scoring import compute_live_percentiles, calculate_entry_score

        # 외부 전달 데이터를 df 컬럼으로 주입
        if "cvd_delta_slope" not in df.columns:
            df["cvd_delta_slope"] = 0.0
            if cvd_trend == "BUY_PRESSURE":
                df.iloc[-1, df.columns.get_loc("cvd_delta_slope")] = 1.0
            elif cvd_trend == "SELL_PRESSURE":
                df.iloc[-1, df.columns.get_loc("cvd_delta_slope")] = -1.0
        if "bid_ask_imbalance" not in df.columns:
            df["bid_ask_imbalance"] = bid_ask_imbalance
        if "ADX_14" not in df.columns and df_15m is not None and not df_15m.empty:
            df["ADX_14"] = df_15m.iloc[-1].get("ADX_14", 20.0)
        if "buy_ratio" not in df.columns:
            df["buy_ratio"] = 0.5
        if "NOFI" not in df.columns:
            df["NOFI"] = 0.0

        if "open_interest" not in df.columns:
            df["open_interest"] = 0.0
        if "tick_count" not in df.columns:
            df["tick_count"] = 0
        if "Log_Vol_ZScore" not in df.columns:
            df["Log_Vol_ZScore"] = 0.0

        if hft_features:
            df.iloc[-1, df.columns.get_loc("open_interest")] = hft_features.get(
                "open_interest", 0.0
            )
            df.iloc[-1, df.columns.get_loc("tick_count")] = hft_features.get(
                "tick_count", 0
            )
            if "nofi_1m" in hft_features:
                df.iloc[-1, df.columns.get_loc("NOFI")] = hft_features["nofi_1m"]
            if "log_volume_zscore" in hft_features:
                df.iloc[-1, df.columns.get_loc("Log_Vol_ZScore")] = hft_features[
                    "log_volume_zscore"
                ]

        # 백분위수 산출
        pctl_window = getattr(settings, "PCTL_WINDOW", 100)
        percentiles = compute_live_percentiles(df, window=pctl_window)

        # [V18.4] 상위 봉(15m) MACD_H 백분위 직접 주입 (3m df에는 15m MACD 역사가 없으므로)
        if df_15m is not None and "MACD_H" in df_15m.columns:
            recent_macd = df_15m["MACD_H"].tail(pctl_window).dropna()
            if len(recent_macd) >= 20:
                curr_macd = recent_macd.iloc[-1]
                macd_p = float(
                    percentileofscore(recent_macd.values, float(curr_macd), kind="rank")
                )
                percentiles["macd_hist_pctl"] = macd_p

        # ATR 부스트 및 HTF/MTF 정보 수집
        atr_ratio_mult = getattr(settings, "ATR_RATIO_MULT", 1.2)
        atr_boost_flag = atr_14 > (atr_long * atr_ratio_mult)
        mtf = self.get_mtf_regime(df_15m)
        htf_bias_str = self.get_htf_bias(df_1h)
        mtf_moment_str = mtf.get("momentum", "NEUTRAL")
        mtf_regime_str = mtf.get("regime", "RANGE")

        percentiles["atr_boost_flag"] = atr_boost_flag
        percentiles["htf_bias"] = (
            1 if htf_bias_str == "BULL" else (-1 if htf_bias_str == "BEAR" else 0)
        )
        percentiles["mtf_moment"] = (
            1
            if mtf_moment_str == "BULLISH"
            else (-1 if mtf_moment_str == "BEARISH" else 0)
        )
        percentiles["mtf_regime"] = 1 if mtf_regime_str == "TREND" else 0
        percentiles["vwap_dist"] = market_price - vwap_mid

        # ── 3. 스코어링 엔진 실행 (무조건 실행 - 로깅 보장) ─────────────────
        adx_boost = getattr(settings, "ADX_BOOST_PCTL", 70.0)
        score_result = calculate_entry_score(percentiles, adx_boost_pctl=adx_boost)

        long_score = score_result["long_score"]
        short_score = score_result["short_score"]
        raw_signal = score_result["signal"]
        score_detail = score_result["detail"]

        # [V18.3] 진입 유형(entry_type) 선제적 판별 (로그용)
        # MACD 점수가 1점 이상이면 TREND, 아니면 SCALP로 분류 (V18.4 기준 조정 가능)
        entry_type = (
            "TREND_MACD"
            if (
                score_result.get("l_macd", 0) >= 1 or score_result.get("s_macd", 0) >= 1
            )
            else "SCALP_CVD"
        )

        # ── 4. 진입 조건 필터링 (Pure Scoring 기반) ───────────────────────
        min_score_long = getattr(settings, "MIN_SCORE_LONG", 18)
        min_score_short = getattr(settings, "MIN_SCORE_SHORT", 17)
        signal_type = None
        reason = ""

        # 롱 진입 검증
        if raw_signal == 1 and long_score >= min_score_long:
            # [V18.4] MACD 하드 필터 체크
            if settings.MACD_FILTER_ENABLED:
                macd_p = percentiles.get("macd_hist_pctl", 50.0)
                if macd_p <= 50.0:
                    raw_signal = None  # 필터 탈락
                    logger.info(
                        f"[{symbol}] LONG 시그널 발생했으나 MACD 하드 필터에 의해 차단됨 (Pctl: {macd_p:.1f} <= 50)"
                    )

        # 숏 진입 검증
        if raw_signal == -1 and short_score >= min_score_short:
            # [V18.4] MACD 하드 필터 체크
            if settings.MACD_FILTER_ENABLED:
                macd_p = percentiles.get("macd_hist_pctl", 50.0)
                if macd_p >= 50.0:
                    raw_signal = None  # 필터 탈락
                    logger.info(
                        f"[{symbol}] SHORT 시그널 발생했으나 MACD 하드 필터에 의해 차단됨 (Pctl: {macd_p:.1f} >= 50)"
                    )

        # 필터 통과 후 최종 시그널 결정
        if raw_signal == 1 and long_score >= min_score_long:
            signal_type = "LONG"
            reason = f"[V18 SCORE LONG] {score_detail} (≥{min_score_long}, Type={entry_type})"
        elif raw_signal == -1 and short_score >= min_score_short:
            signal_type = "SHORT"
            reason = f"[V18 SCORE SHORT] {score_detail} (≥{min_score_short}, Type={entry_type})"

        # 탈락 사유 기록 (조건 미달 시)
        if not signal_type:
            if raw_signal == 1:
                reason = f"[{entry_type}] LONG 점수 미달: {long_score} < {min_score_long} ({score_detail})"
            elif raw_signal == -1:
                reason = f"[{entry_type}] SHORT 점수 미달: {short_score} < {min_score_short} ({score_detail})"
            else:
                reason = f"[{entry_type}] No Signal ({score_detail})"

        # entry_type은 위에서 선제 판별됨

        # [V18.2] Excess Score 산출 (임계치 대비 여유 점수)
        excess_score = 0
        if signal_type == "LONG":
            excess_score = int(long_score - min_score_long)
        elif signal_type == "SHORT":
            excess_score = int(short_score - min_score_short)

        # 최종 진단 로그
        if not reason and not signal_type:
            reason = f"V18 점수 미달 (L={long_score}/{min_score_long}, S={short_score}/{min_score_short})"

        logger.info(
            f"[{symbol}] [V18 Analysis] {reason if not signal_type else 'MATCH'} "
            f"(Strength: +{excess_score if signal_type else 0}, Type: {entry_type})"
        )

        # [V18] 시장 데이터 스냅샷 (Pydantic)
        market_data_obj = None
        if signal_type:
            market_data_obj = MarketDataSnapshot(
                rsi=float(current.get("RSI", 50)),
                volume=float(current["volume"]),
                atr_14=float(atr_14),
                adx_15m=float(mtf["adx"]) if mtf.get("adx") else None,
                mtf_bias_1h=str(htf_bias_str),
                regime=str(mtf_regime_str),
                twap_imbalance=float(bid_ask_imbalance),
                long_score=int(long_score),
                short_score=int(short_score),
                excess_score=int(excess_score),
                entry_type=str(entry_type),
                # [V18.4] Settings Snapshot
                timeframe=str(getattr(settings, "TIMEFRAME", "3m")),
                min_score_long=int(min_score_long),
                min_score_short=int(min_score_short),
                long_tp_mult=float(getattr(settings, "LONG_TP_MULT", 5.0)),
                long_sl_mult=float(getattr(settings, "LONG_SL_MULT", 1.5)),
                short_tp_mult=float(getattr(settings, "SHORT_TP_MULT", 5.0)),
                short_sl_mult=float(getattr(settings, "SHORT_SL_MULT", 1.5)),
                macd_filter_enabled=bool(
                    getattr(settings, "MACD_FILTER_ENABLED", True)
                ),
            ).model_dump(exclude_none=True)

        return {
            "signal": signal_type,
            "market_price": market_price,
            "atr_val": float(atr_14),
            "vwap_mid": float(vwap_mid),
            "reason": reason,
            "market_data": market_data_obj,
            "long_score": long_score,
            "short_score": short_score,
            "excess_score": excess_score,
            "nofi_1m": float(df["NOFI"].iloc[-1]),
            "buy_ratio": float(df["buy_ratio"].iloc[-1]),
            "entry_type": entry_type,
        }
