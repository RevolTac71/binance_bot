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
from schemas import MarketDataSnapshot
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

        # ADX 기반 장세 분류 — V17: 백분위수 활용
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
        bid_ask_imbalance: float = 0.5,
        all_htf_15m: Optional[dict] = None,
        hft_features: Optional[dict] = None,
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
            bid_ask_imbalance: 오더북 불균형
            all_htf_15m: 전체 심볼의 15분봉 데이터 (상관관계 산출용)
            hft_features: hft_pipeline에서 수집한 OI, Tick Count 등 정보
            dict:
                "signal"      : "LONG" | "SHORT" | None
                "market_price": float
                "atr_val"     : float
                "vwap_mid"    : float
                "reason"      : str
        """
        # ── 기초 데이터 검증 ──────────────────────────────────────────────
        if len(df) < 250:
            return {"signal": None, "reason": "데이터 부족 (최소 250개 필요)"}

        current = df.iloc[-1]

        market_price = float(current["close"])
        high_price = float(current["high"])
        low_price = float(current["low"])
        volume = float(current["volume"])

        rsi_val = current.get("RSI", 50.0)
        vwap_mid = current.get("VWAP", market_price)
        vol_sma_20 = current.get("Vol_SMA_20")
        atr_14 = current.get("ATR_14", market_price * 0.005)

        atr_long_len = getattr(settings, "ATR_LONG_LEN", 200)
        atr_long = current.get(f"ATR_{atr_long_len}", atr_14)

        # 결측치 방어 (NoneType 예외 차단 로직 강화)
        if vol_sma_20 is None or pd.isna(atr_14) or pd.isna(vol_sma_20):
            return {
                "signal": None,
                "reason": "지표 결측치 발생 (방어 로직 등 데이터 부족)",
            }

        # ── STEP 1: Session Filter ────────────────────────────────────────
        # [V16.1] 24-Hour Rolling VWAP 설계 도입으로 인해, 00:00 기준 리셋 대기시간(Session Block) 삭제

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
        # ── STEP 2.5: MTF 추세 강도 / 모멘텀 판별 복원 ─────────────────────
        mtf = self.get_mtf_regime(df_15m)
        regime = mtf.get("regime")

        # ══════════════════════════════════════════════════════════════
        # V18 스코어링 진입 엔진 (STEP 3~8 직렬 AND 게이트 전면 교체)
        # ══════════════════════════════════════════════════════════════

        from scoring import compute_live_percentiles, calculate_entry_score
        import numpy as np

        # 1. 실시간 백분위수 산출 (O(window) 최적화)
        pctl_window = getattr(settings, "PCTL_WINDOW", 100)

        # 콜드스타트 방어: 데이터 부족 시 진입 유보
        if len(df) < 20:
            return {
                "signal": None,
                "reason": f"V18 콜드스타트 — 데이터 {len(df)}개 (최소 20개 필요)",
            }

        # 외부 전달 데이터를 df 컬럼으로 주입 (백분위수 산출 소스)
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

        # [V18] HFT 피처 주입
        if hft_features:
            df["open_interest"] = hft_features.get("open_interest", 0.0)
            df["tick_count"] = hft_features.get("tick_count", 0)
            if "log_volume_zscore" in hft_features:
                df["Log_Vol_ZScore"] = hft_features["log_volume_zscore"]
        else:
            df["open_interest"] = 0.0
            df["tick_count"] = 0

        # 백분위수 산출
        percentiles = compute_live_percentiles(df, window=pctl_window)

        # HTF bias를 환경 부스트용 펀딩비 방향으로 활용
        htf_bias = self.get_htf_bias(df_1h)
        funding_rate_match = 0
        if htf_bias == "BULL":
            funding_rate_match = 1
        elif htf_bias == "BEAR":
            funding_rate_match = -1
        percentiles["funding_rate_match"] = funding_rate_match

        # 2. 스코어링 엔진 실행
        adx_boost = getattr(settings, "ADX_BOOST_PCTL", 70.0)
        score_result = calculate_entry_score(percentiles, adx_boost_pctl=adx_boost)

        long_score = score_result["long_score"]
        short_score = score_result["short_score"]
        raw_signal = score_result["signal"]
        score_detail = score_result["detail"]
        l_macd = score_result.get("l_macd", 0)
        s_macd = score_result.get("s_macd", 0)

        # 3. 최소 점수 임계값 필터
        min_score = getattr(settings, "MIN_ENTRY_SCORE", 5)
        signal_type = None
        reason = ""

        if raw_signal == 1 and long_score >= min_score:
            is_scalp = l_macd < 2
            if is_scalp:
                has_hft_signal = (
                    score_result.get("l_cvd", 0) >= 1
                    or score_result.get("l_nofi", 0) >= 1
                )
                if not has_hft_signal:
                    return {
                        "signal": None,
                        "reason": f"V18 SCALP 거부 — CVD/OFI 점수 없음 ({score_detail})",
                    }

            signal_type = "LONG"
            reason = f"[V18 SCORE LONG] {score_detail} (≥{min_score})"
        elif raw_signal == -1 and short_score >= min_score:
            is_scalp = s_macd < 2
            if is_scalp:
                has_hft_signal = (
                    score_result.get("s_cvd", 0) >= 1
                    or score_result.get("s_nofi", 0) >= 1
                )
                if not has_hft_signal:
                    return {
                        "signal": None,
                        "reason": f"V18 SCALP 거부 — CVD/OFI 점수 없음 ({score_detail})",
                    }

            signal_type = "SHORT"
            reason = f"[V18 SCORE SHORT] {score_detail} (≥{min_score})"

        # V18 진단 로그
        adx_pctl_val = percentiles.get("adx_pctl", 0)
        rsi_val_log = percentiles.get("rsi", 50)
        vol_z_val = percentiles.get("vol_zscore", 0)

        if signal_type is not None:
            logger.info(
                f"[{symbol}] 🎯 [V18] {signal_type} 진입! "
                f"{score_detail} | RSI={rsi_val_log:.1f} | VolZ={vol_z_val:.1f}"
            )
        else:
            best = max(long_score, short_score)
            best_dir = "L" if long_score >= short_score else "S"
            if not reason:
                reason = (
                    f"V18 점수 미달 | {best_dir}={best}/{min_score} | "
                    f"HTF={htf_bias} | RSI={rsi_val_log:.1f}"
                )
            logger.info(
                f"[{symbol}] 📊 [V18] {best_dir}={best}/{min_score} | "
                f"RSI={rsi_val_log:.1f} | ADX%={adx_pctl_val:.0f} | VolZ={vol_z_val:.1f}"
            )

        # [V16.8] Pydantic Schema Validation
        market_data_obj = None
        if signal_type is not None:
            market_data_obj = MarketDataSnapshot(
                rsi=float(rsi_val),
                sma_20=float(vol_sma_20) if vol_sma_20 is not None else None,
                volume=float(volume),
                atr_14=float(atr_14),
                adx_15m=float(mtf["adx"])
                if "adx" in mtf and mtf["adx"] is not None
                else None,
                mtf_bias_1h=str(htf_bias) if htf_bias else None,
                regime=str(regime) if regime else None,
                twap_imbalance=float(bid_ask_imbalance),
            ).model_dump(exclude_none=True)

        # [V18] 진입 원인 판별 (SL/TP 차등 적용용)
        # MACD 점수가 높으면 추세 추종형, 그 외(CVD/OFI 등)가 높으면 스캘핑(반전)형으로 분류
        entry_type = "TREND_MACD" if l_macd >= 2 or s_macd >= 2 else "SCALP_CVD"

        return {
            "signal": signal_type,
            "market_price": market_price,
            "atr_val": float(atr_14),
            "vwap_mid": float(vwap_mid),
            "reason": reason,
            "market_data": market_data_obj,
            "long_score": long_score,
            "short_score": short_score,
            "nofi_1m": float(df["NOFI"].iloc[-1]),
            "buy_ratio": float(df["buy_ratio"].iloc[-1]),
            "entry_type": entry_type,
        }
