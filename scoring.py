"""
V17 스코어링 기반 시그널 생성 엔진
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
각 보조지표의 백분위수 조건 충족도를 점수화하고,
합산 점수(total_score)가 임계값 이상일 때만
매매 시그널(Long/Short)을 발생시킵니다.

모든 연산은 NumPy 벡터화로 처리하여
대용량 DataFrame에서도 O(n) 선형 성능을 보장합니다.
"""

import numpy as np
import pandas as pd


def generate_scoring_signals(
    df: pd.DataFrame,
    min_score: int = 5,
) -> pd.DataFrame:
    """
    백분위수 기반 보조지표 스코어링 → 시그널 생성.

    Parameters
    ----------
    df : pd.DataFrame
        아래 컬럼들이 이미 백분위수(0~100)로 변환되어 있어야 합니다:
        - macd_hist_pctl, cvd_delta_slope_pctl, bid_ask_imbalance_pctl
        - nofi_1m_pctl, rsi (원시값 또는 rsi_pctl), buy_ratio_pctl
        - adx_pctl, funding_rate_match (원시값: 1/0/-1)
    min_score : int
        시그널 발생 최소 합산 점수 (기본 5)

    Returns
    -------
    pd.DataFrame
        원본 df에 다음 컬럼이 추가된 사본:
        - long_score, short_score, total_score
        - signal (1=Long, -1=Short, 0=None)
    """
    result = df.copy()

    # ━━ 컬럼 안전 참조 (없으면 중립 기본값) ━━━━━━━━━━━━━━━━━━━━━━━
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

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  LONG 스코어링 (모멘텀 상승 + 과매도 역추세)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    # MACD 히스토그램: 70 이상 +1, 85 이상 +2
    l_macd = np.where(macd >= 85, 2, np.where(macd >= 70, 1, 0))

    # CVD 델타 기울기: 70 이상 +1, 85 이상 +2
    l_cvd = np.where(cvd >= 85, 2, np.where(cvd >= 70, 1, 0))

    # 호가 불균형: 65 이상 +1, 80 이상 +2
    l_imbal = np.where(imbal >= 80, 2, np.where(imbal >= 65, 1, 0))

    # 정규화 OFI: 70 이상 +1, 85 이상 +2
    l_nofi = np.where(nofi >= 85, 2, np.where(nofi >= 70, 1, 0))

    # RSI (과매도): 30 이하 +1, 15 이하 +2
    l_rsi = np.where(rsi <= 15, 2, np.where(rsi <= 30, 1, 0))

    # 매수 비율 (역발상): 25 이하 +1, 10 이하 +2
    l_buy = np.where(buy_r <= 10, 2, np.where(buy_r <= 25, 1, 0))

    # 환경 부스트
    l_adx = np.where(adx_p >= 70, 1, 0)  # 추세 강도 부스트
    l_fr = np.where(fr == 1, 1, 0)  # 펀딩비 방향 일치

    # Long 합산
    long_score = l_macd + l_cvd + l_imbal + l_nofi + l_rsi + l_buy + l_adx + l_fr

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SHORT 스코어링 (대칭 반전)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    # MACD 히스토그램: 30 이하 +1, 15 이하 +2
    s_macd = np.where(macd <= 15, 2, np.where(macd <= 30, 1, 0))

    # CVD 델타 기울기: 30 이하 +1, 15 이하 +2
    s_cvd = np.where(cvd <= 15, 2, np.where(cvd <= 30, 1, 0))

    # 호가 불균형: 35 이하 +1, 20 이하 +2
    s_imbal = np.where(imbal <= 20, 2, np.where(imbal <= 35, 1, 0))

    # 정규화 OFI: 30 이하 +1, 15 이하 +2
    s_nofi = np.where(nofi <= 15, 2, np.where(nofi <= 30, 1, 0))

    # RSI (과매수): 70 이상 +1, 85 이상 +2
    s_rsi = np.where(rsi >= 85, 2, np.where(rsi >= 70, 1, 0))

    # 매수 비율 (과열): 75 이상 +1, 90 이상 +2
    s_buy = np.where(buy_r >= 90, 2, np.where(buy_r >= 75, 1, 0))

    # 환경 부스트
    s_adx = np.where(adx_p >= 70, 1, 0)  # 추세 강도 (방향 무관)
    s_fr = np.where(fr == -1, 1, 0)  # 펀딩비 숏 쏠림

    # Short 합산
    short_score = s_macd + s_cvd + s_imbal + s_nofi + s_rsi + s_buy + s_adx + s_fr

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  최종 시그널 생성 (Long 우선, 동점이면 None)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    result["long_score"] = long_score
    result["short_score"] = short_score

    # 양쪽 모두 임계값 이상이면 점수가 높은 쪽 채택, 동점이면 0
    signal = np.where(
        (long_score >= min_score) & (long_score > short_score),
        1,
        np.where((short_score >= min_score) & (short_score > long_score), -1, 0),
    )

    # 최종 점수: 채택된 방향의 점수
    result["total_score"] = np.where(
        signal == 1, long_score, np.where(signal == -1, short_score, 0)
    )
    result["signal"] = signal

    return result
