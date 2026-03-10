"""
V18 스코어링 기반 시그널 생성 엔진
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
각 보조지표의 백분위수 조건 충족도를 점수화하고,
합산 점수(total_score)가 임계값 이상일 때만
매매 시그널(Long/Short)을 발생시킵니다.

V18 변경:
- vol_zscore 점수 항목 추가 (≥1.5 → +1, ≥2.5 → +2)
- 단일 행 dict 입력용 래퍼 calculate_entry_score() 추가
- scipy.stats.percentileofscore 기반 실시간 백분위수 산출 유틸
"""

import numpy as np
import pandas as pd
from scipy.stats import percentileofscore
from config import settings


def compute_live_percentiles(df: pd.DataFrame, window: int = 100) -> dict:
    """
    실시간 백분위수 산출 (O(window) 최적화).
    전체 df에 rolling을 걸지 않고, 최근 window개 슬라이스에서
    현재값의 백분위수만 단일 계산합니다.

    Parameters
    ----------
    df : pd.DataFrame
        최소 window개 이상의 행을 포함하는 DataFrame.
        필요 컬럼: MACD_H, cvd_delta_slope, bid_ask_imbalance,
                    NOFI, RSI, buy_ratio, ADX_14, vol_zscore, open_interest, tick_count
    window : int
        백분위수 산출에 사용할 과거 기간 (기본 100)

    Returns
    -------
    dict : 각 지표의 백분위수 값 (0~100)
    """
    # 실제 사용 가능한 윈도우 크기 (콜드스타트 방어)
    actual_window = min(window, len(df))

    if actual_window < 20:
        # 최소 20개 미만이면 백분위수 산출 불가 → 중립값 반환
        return {
            "macd_hist_pctl": 50.0,
            "cvd_delta_slope_pctl": 50.0,
            "bid_ask_imbalance_pctl": 50.0,
            "nofi_1m_pctl": 50.0,
            "rsi": 50.0,
            "buy_ratio_pctl": 50.0,
            "adx_pctl": 50.0,
            "vol_zscore": 0.0,
            "oi_pctl": 50.0,
            "tick_pctl": 50.0,
        }

    # 최근 window개 슬라이스
    recent = df.tail(actual_window)
    current = df.iloc[-1]

    # 각 지표별 백분위수 산출 (현재값이 과거 window 기간 중 몇 %에 위치하는지)
    def pctl(col_name, default=50.0):
        if col_name not in recent.columns:
            return default
        arr = recent[col_name].dropna()
        if len(arr) < 10:
            return default
        val = current.get(col_name, default)
        if pd.isna(val):
            return default
        return float(percentileofscore(arr.values, float(val), kind="rank"))

    return {
        "macd_hist_pctl": pctl("MACD_H"),
        "cvd_delta_slope_pctl": pctl("cvd_delta_slope"),
        "bid_ask_imbalance_pctl": pctl("bid_ask_imbalance"),
        "nofi_1m_pctl": pctl("NOFI"),
        "rsi": float(
            current.get("RSI", current.get("RSI_14", 50.0))
        ),  # RSI는 원시값 사용
        "buy_ratio_pctl": pctl("buy_ratio"),
        "adx_pctl": pctl("ADX_14"),
        "vol_zscore": float(current.get("Log_Vol_ZScore", 0.0)),
        "oi_pctl": pctl("open_interest"),
        "tick_pctl": pctl("tick_count"),
    }


def evaluate_rule_score(val: float, rules: list, direction: str = "upper") -> int:
    """
    단일 지표에 대해 리스트 규칙을 순차 평가하여 가장 높은 점수를 반환합니다.
    rules: [(threshold, score), (threshold, score, "val"), ...]
    direction: "upper" (>= threshold) 또는 "lower" (<= threshold)
    """
    if not rules:
        return 0

    max_score = 0
    for rule in rules:
        threshold = rule[0]
        score = rule[1]

        if direction == "upper":
            if val >= threshold:
                max_score = max(max_score, score)
        else:  # lower
            if val <= threshold:
                max_score = max(max_score, score)

    return max_score


def calculate_entry_score(indicators: dict, adx_boost_pctl: float = 70.0) -> dict:
    """
    V18.4 리스트 규칙 기반 스코어링 엔진
    """
    # 1. 지표 추출
    macd_p = indicators.get("macd_hist_pctl", 50)
    cvd_p = indicators.get("cvd_delta_slope_pctl", 50)
    imbal_p = indicators.get("bid_ask_imbalance_pctl", 50)
    nofi_p = indicators.get("nofi_1m_pctl", 50)
    rsi_val = indicators.get("rsi", 50)
    buy_p = indicators.get("buy_ratio_pctl", 50)
    adx_p = indicators.get("adx_pctl", 50)
    fr = indicators.get("funding_rate_match", 0)
    vol_z = indicators.get("vol_zscore", 0.0)
    oi_p = indicators.get("oi_pctl", 50)
    tick_p = indicators.get("tick_pctl", 50)
    atr_boost_flag = indicators.get("atr_boost_flag", False)

    htf_bias = indicators.get("htf_bias", 0)
    mtf_moment = indicators.get("mtf_moment", 0)
    mtf_reg = indicators.get("mtf_regime", 0)
    vwap_dist = indicators.get("vwap_dist", 0.0)

    rl = settings.SC_RULES_LONG
    rs = settings.SC_RULES_SHORT
    w = settings.SCORING_WEIGHTS

    # ━━━━━ LONG 스코어링 ━━━━━
    l_macd = evaluate_rule_score(macd_p, rl["trend"]["macd_hist"], "upper")
    l_cvd = evaluate_rule_score(cvd_p, rl["trend"]["cvd_delta_slope"], "upper")
    l_imbal = evaluate_rule_score(imbal_p, rl["trend"]["bid_ask_imbalance"], "upper")
    l_nofi = evaluate_rule_score(nofi_p, rl["trend"]["nofi_1m"], "upper")
    l_oi = evaluate_rule_score(oi_p, rl["trend"]["open_interest"], "upper")
    l_tick = evaluate_rule_score(tick_p, rl["trend"]["tick_count"], "upper")
    l_vol = evaluate_rule_score(vol_z, rl["trend"]["log_volume_zscore"], "upper")

    l_rsi = evaluate_rule_score(rsi_val, rl["mean_reversion"]["rsi"], "lower")
    l_buy = evaluate_rule_score(buy_p, rl["mean_reversion"]["buy_ratio"], "lower")

    # 가중치 기반 (기존 유지)
    l_adx = w["adx_boost"]["1"] if adx_p >= adx_boost_pctl else 0
    l_fr = w["fr_boost"]["2"] if fr == -1 else 0
    l_atr = w["atr"]["2"] if atr_boost_flag else 0
    l_htf = w["htf_bias"]["2"] if htf_bias == 1 else 0
    l_mtf_m = w["mtf_moment"]["2"] if mtf_moment == 1 else 0
    l_mtf_r = w["mtf_regime"]["1"] if mtf_reg == 1 else 0
    l_vwap = w["vwap_dist"]["2"] if vwap_dist > 0 else 0

    long_score = (
        l_macd
        + l_cvd
        + l_imbal
        + l_nofi
        + l_rsi
        + l_buy
        + l_vol
        + l_adx
        + l_fr
        + l_oi
        + l_tick
        + l_atr
        + l_htf
        + l_mtf_m
        + l_mtf_r
        + l_vwap
    )

    # ━━━━━ SHORT 스코어링 ━━━━━
    # 숏은 추세 지표의 경우 100 - pctl (대칭) 기준으로 평가
    s_macd = evaluate_rule_score(100 - macd_p, rs["trend"]["macd_hist"], "upper")
    s_cvd = evaluate_rule_score(100 - cvd_p, rs["trend"]["cvd_delta_slope"], "upper")
    s_imbal = evaluate_rule_score(
        100 - imbal_p, rs["trend"]["bid_ask_imbalance"], "upper"
    )
    s_nofi = evaluate_rule_score(100 - nofi_p, rs["trend"]["nofi_1m"], "upper")
    s_oi = evaluate_rule_score(
        oi_p, rs["trend"]["open_interest"], "upper"
    )  # OI/Tick은 절대 활성도
    s_tick = evaluate_rule_score(tick_p, rs["trend"]["tick_count"], "upper")
    s_vol = evaluate_rule_score(vol_z, rs["trend"]["log_volume_zscore"], "upper")

    # 평균 회귀 지표 (숏은 고점에서 하락 반전이므로 Upper 기준 사용)
    s_rsi = evaluate_rule_score(rsi_val, rs["mean_reversion"]["rsi"], "upper")
    s_buy = evaluate_rule_score(buy_p, rs["mean_reversion"]["buy_ratio"], "upper")

    s_adx = w["adx_boost"]["1"] if adx_p >= adx_boost_pctl else 0
    s_fr = w["fr_boost"]["2"] if fr == -1 else 0
    s_atr = w["atr"]["2"] if atr_boost_flag else 0
    s_htf = w["htf_bias"]["2"] if htf_bias == -1 else 0
    s_mtf_m = w["mtf_moment"]["2"] if mtf_moment == -1 else 0
    s_mtf_r = w["mtf_regime"]["1"] if mtf_reg == 1 else 0
    s_vwap = w["vwap_dist"]["2"] if vwap_dist < 0 else 0

    short_score = (
        s_macd
        + s_cvd
        + s_imbal
        + s_nofi
        + s_rsi
        + s_buy
        + s_vol
        + s_adx
        + s_fr
        + s_oi
        + s_tick
        + s_atr
        + s_htf
        + s_mtf_m
        + s_mtf_r
        + s_vwap
    )

    # 점수 상세 내역 (로그용)
    if long_score >= short_score:
        detail = (
            f"L[MACD={l_macd} CVD={l_cvd} Imbal={l_imbal} OFI={l_nofi} RSI={l_rsi} Vol={l_vol} "
            f"OI={l_oi} Tick={l_tick} HTF={l_htf} MOM={l_mtf_m} Reg={l_mtf_r} VWAP={l_vwap} ATR={l_atr}]={long_score}"
        )
    else:
        detail = (
            f"S[MACD={s_macd} CVD={s_cvd} Imbal={s_imbal} OFI={s_nofi} RSI={s_rsi} Vol={s_vol} "
            f"OI={s_oi} Tick={s_tick} HTF={s_htf} MOM={s_mtf_m} Reg={s_mtf_r} VWAP={s_vwap} ATR={s_atr}]={short_score}"
        )

    return {
        "long_score": long_score,
        "short_score": short_score,
        "signal": 1
        if long_score > short_score
        else (-1 if short_score > long_score else 0),
        "detail": detail,
        # V18: 필수 조건 체크를 위한 개별 점수 노출 (상대 순수 점수 반환)
        "l_cvd": l_cvd,
        "l_nofi": l_nofi,
        "s_cvd": s_cvd,
        "s_nofi": s_nofi,
        "l_macd": l_macd,
        "s_macd": s_macd,
        "l_atr": l_atr,
        "s_atr": s_atr,
    }


# ━━━━━━ 백테스트용 벡터화 함수 (기존 유지) ━━━━━━


def generate_scoring_signals(
    df: pd.DataFrame,
    min_score: int = 5,
) -> pd.DataFrame:
    """
    백테스트 전용 — 전체 DataFrame에 대해 벡터화 스코어링.
    """
    result = df.copy()

    macd = result.get("macd_hist_pctl", pd.Series(50, index=result.index)).values
    cvd = result.get("cvd_delta_slope_pctl", pd.Series(50, index=result.index)).values
    imbal = result.get(
        "bid_ask_imbalance_pctl", pd.Series(50, index=result.index)
    ).values
    nofi = result.get("nofi_1m_pctl", pd.Series(50, index=result.index)).values
    rsi = result.get(
        "rsi", result.get("rsi_pctl", pd.Series(50, index=result.index))
    ).values
    buy_r = result.get("buy_ratio_pctl", pd.Series(50, index=result.index)).values
    adx_p = result.get("adx_pctl", pd.Series(50, index=result.index)).values
    fr = result.get("funding_rate_match", pd.Series(0, index=result.index)).values
    vol_z = result.get("vol_zscore", pd.Series(0, index=result.index)).values

    # Long
    l_macd = np.where(macd >= 85, 2, np.where(macd >= 70, 1, 0))
    l_cvd = np.where(cvd >= 85, 2, np.where(cvd >= 70, 1, 0))
    l_imbal = np.where(imbal >= 80, 2, np.where(imbal >= 65, 1, 0))
    l_nofi = np.where(nofi >= 85, 2, np.where(nofi >= 70, 1, 0))
    l_rsi = np.where(rsi <= 15, 2, np.where(rsi <= 30, 1, 0))
    l_buy = np.where(buy_r <= 10, 2, np.where(buy_r <= 25, 1, 0))
    l_vol = np.where(vol_z >= 2.5, 2, np.where(vol_z >= 1.5, 1, 0))
    l_adx = np.where(adx_p >= 70, 1, 0)
    l_fr = np.where(fr == 1, 1, 0)
    long_score = (
        l_macd + l_cvd + l_imbal + l_nofi + l_rsi + l_buy + l_vol + l_adx + l_fr
    )

    # Short
    s_macd = np.where(macd <= 15, 2, np.where(macd <= 30, 1, 0))
    s_cvd = np.where(cvd <= 15, 2, np.where(cvd <= 30, 1, 0))
    s_imbal = np.where(imbal <= 20, 2, np.where(imbal <= 35, 1, 0))
    s_nofi = np.where(nofi <= 15, 2, np.where(nofi <= 30, 1, 0))
    s_rsi = np.where(rsi >= 85, 2, np.where(rsi >= 70, 1, 0))
    s_buy = np.where(buy_r >= 90, 2, np.where(buy_r >= 75, 1, 0))
    s_vol = np.where(vol_z >= 2.5, 2, np.where(vol_z >= 1.5, 1, 0))
    s_adx = np.where(adx_p >= 70, 1, 0)
    s_fr = np.where(fr == -1, 1, 0)
    short_score = (
        s_macd + s_cvd + s_imbal + s_nofi + s_rsi + s_buy + s_vol + s_adx + s_fr
    )

    result["long_score"] = long_score
    result["short_score"] = short_score

    signal = np.where(
        (long_score >= min_score) & (long_score > short_score),
        1,
        np.where((short_score >= min_score) & (short_score > long_score), -1, 0),
    )

    result["total_score"] = np.where(
        signal == 1, long_score, np.where(signal == -1, short_score, 0)
    )
    result["signal"] = signal

    return result
