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


def calculate_entry_score(indicators: dict, adx_boost_pctl: float = 70.0) -> dict:
    """
    단일 캔들 시점의 지표 dict로부터 Long/Short 점수를 산출합니다.
    strategy.py의 check_entry()에서 직접 호출하는 래퍼입니다.

    Parameters
    ----------
    indicators : dict
        compute_live_percentiles()의 반환값 + funding_rate_match + atr_boost_flag
    adx_boost_pctl : float
        ADX 부스트 임계 백분위수 (기본 70)

    Returns
    -------
    dict : {"long_score": int, "short_score": int, "signal": int, "detail": str}
    """
    macd = indicators.get("macd_hist_pctl", 50)
    cvd = indicators.get("cvd_delta_slope_pctl", 50)
    imbal = indicators.get("bid_ask_imbalance_pctl", 50)
    nofi = indicators.get("nofi_1m_pctl", 50)
    rsi = indicators.get("rsi", 50)
    buy_r = indicators.get("buy_ratio_pctl", 50)
    adx_p = indicators.get("adx_pctl", 50)
    fr = indicators.get("funding_rate_match", 0)
    vol_z = indicators.get("vol_zscore", 0.0)
    oi_p = indicators.get("oi_pctl", 50)
    tick_p = indicators.get("tick_pctl", 50)
    atr_boost_flag = indicators.get("atr_boost_flag", False)
    # 신규 항목: HTF/MTF 및 VWAP (기본값 중립)
    htf_bias = indicators.get("htf_bias", 0)  # 1: BULL, -1: BEAR
    mtf_moment = indicators.get("mtf_moment", 0)  # 1: BULL, -1: BEAR
    mtf_reg = indicators.get("mtf_regime", 0)  # 1: TREND, 0: RANGE
    vwap_dist = indicators.get("vwap_dist", 0.0)  # (+)면 VWAP 위, (-)면 아래

    # ━━━━━ LONG 스코어링 ━━━━━
    t = settings.SCORING_THRESHOLDS
    w = settings.SCORING_WEIGHTS

    # MACD 히스토그램
    l_macd = (
        w["macd"]["4"]
        if macd >= t["macd_pctl"].get("+4", 90)
        else (
            w["macd"]["2"]
            if macd >= t["macd_pctl"]["+2"]
            else (w["macd"]["1"] if macd >= t["macd_pctl"]["+1"] else 0)
        )
    )
    # CVD 델타 기울기
    l_cvd = (
        w["cvd"]["2"]
        if cvd >= t["cvd_pctl"]["+2"]
        else (w["cvd"]["1"] if cvd >= t["cvd_pctl"]["+1"] else 0)
    )
    # 호가 불균형
    l_imbal = (
        w["imbalance"]["2"]
        if imbal >= t["imbalance"]["+2"]
        else (w["imbalance"]["1"] if imbal >= t["imbalance"]["+1"] else 0)
    )
    # 정규화 OFI
    l_nofi = (
        w["nofi"]["2"]
        if nofi >= t["nofi_pctl"]["+2"]
        else (w["nofi"]["1"] if nofi >= t["nofi_pctl"]["+1"] else 0)
    )
    # RSI (과매도 기준)
    l_rsi = (
        w["rsi"]["2"]
        if rsi <= t["rsi"]["+2"]
        else (w["rsi"]["1"] if rsi <= t["rsi"]["+1"] else 0)
    )
    # 매수 비율 (역발상: 하위 백분위수)
    l_buy = (
        w["buy_ratio"]["2"]
        if buy_r <= t["buy_ratio"]["+2"]
        else (w["buy_ratio"]["1"] if buy_r <= t["buy_ratio"]["+1"] else 0)
    )
    # 거래량 Z-Score
    l_vol = (
        w["vol_z"]["2"]
        if vol_z >= t["vol_zscore"]["+2"]
        else (w["vol_z"]["1"] if vol_z >= t["vol_zscore"]["+1"] else 0)
    )
    # 환경 부스트 및 신규 피처
    l_adx = w["adx_boost"]["1"] if adx_p >= adx_boost_pctl else 0
    l_fr = w["fr_boost"]["2"] if fr == -1 else 0
    l_oi = (
        w["oi"]["2"]
        if oi_p >= t["oi_pctl"]["+2"]
        else (w["oi"]["1"] if oi_p >= t["oi_pctl"]["+1"] else 0)
    )
    l_tick = (
        w["tick"]["2"]
        if tick_p >= t["tick_pctl"]["+2"]
        else (w["tick"]["1"] if tick_p >= t["tick_pctl"]["+1"] else 0)
    )
    # ATR 변동성 필터
    l_atr = w["atr"]["2"] if atr_boost_flag else 0

    # ━━━━━ MTF / HTF / VWAP 추가 점수 ━━━━━
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

    # ━━━━━ SHORT 스코어링 (대칭 반전) ━━━━━

    s_macd = (
        w["macd"]["4"]
        if macd <= (100 - t["macd_pctl"].get("+4", 90))
        else (
            w["macd"]["2"]
            if macd <= (100 - t["macd_pctl"]["+2"])
            else (w["macd"]["1"] if macd <= (100 - t["macd_pctl"]["+1"]) else 0)
        )
    )
    s_cvd = (
        w["cvd"]["2"]
        if cvd <= (100 - t["cvd_pctl"]["+2"])
        else (w["cvd"]["1"] if cvd <= (100 - t["cvd_pctl"]["+1"]) else 0)
    )
    s_imbal = (
        w["imbalance"]["2"]
        if imbal <= (100 - t["imbalance"]["+2"])
        else (w["imbalance"]["1"] if imbal <= (100 - t["imbalance"]["+1"]) else 0)
    )
    s_nofi = (
        w["nofi"]["2"]
        if nofi <= (100 - t["nofi_pctl"]["+2"])
        else (w["nofi"]["1"] if nofi <= (100 - t["nofi_pctl"]["+1"]) else 0)
    )
    # RSI
    s_rsi = (
        w["rsi"]["2"]
        if rsi >= (100 - t["rsi"]["+2"])
        else (w["rsi"]["1"] if rsi >= (100 - t["rsi"]["+1"]) else 0)
    )
    # 매수 비율
    s_buy = (
        w["buy_ratio"]["2"]
        if buy_r >= (100 - t["buy_ratio"]["+2"])
        else (w["buy_ratio"]["1"] if buy_r >= (100 - t["buy_ratio"]["+1"]) else 0)
    )
    # 거래량
    s_vol = (
        w["vol_z"]["2"]
        if vol_z >= t["vol_zscore"]["+2"]
        else (w["vol_z"]["1"] if vol_z >= t["vol_zscore"]["+1"] else 0)
    )
    s_adx = w["adx_boost"]["1"] if adx_p >= adx_boost_pctl else 0
    s_fr = w["fr_boost"]["2"] if fr == 1 else 0
    s_oi = (
        w["oi"]["2"]
        if oi_p >= t["oi_pctl"]["+2"]
        else (w["oi"]["1"] if oi_p >= t["oi_pctl"]["+1"] else 0)
    )
    s_tick = (
        w["tick"]["2"]
        if tick_p >= t["tick_pctl"]["+2"]
        else (w["tick"]["1"] if tick_p >= t["tick_pctl"]["+1"] else 0)
    )
    s_atr = w["atr"]["2"] if atr_boost_flag else 0

    # ━━━━━ MTF / HTF / VWAP 추가 점수 ━━━━━
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
    if long_score > short_score:
        detail = (
            f"L[MACD={l_macd} CVD={l_cvd} Imbal={l_imbal} OFI={l_nofi} RSI={l_rsi} Vol={l_vol} "
            f"HTF={l_htf} MTF={l_mtf_m} Reg={l_mtf_r} VWAP={l_vwap} ATR={l_atr}]={long_score}"
        )
    else:
        detail = (
            f"S[MACD={s_macd} CVD={s_cvd} Imbal={s_imbal} OFI={s_nofi} RSI={s_rsi} Vol={s_vol} "
            f"HTF={s_htf} MTF={s_mtf_m} Reg={s_mtf_r} VWAP={s_vwap} ATR={s_atr}]={short_score}"
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
