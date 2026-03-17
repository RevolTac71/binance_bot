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
import json
from typing import Any, Literal
from scipy.stats import percentileofscore
from config import settings


ComparisonOperator = Literal[">=", "<=", ">", "<", "=="]


def load_config(filepath: str) -> dict[str, Any]:
    """JSON 설정 파일을 로드해 파싱 결과(dict)를 반환합니다."""
    with open(filepath, "r", encoding="utf-8") as fp:
        parsed = json.load(fp)

    if not isinstance(parsed, dict):
        raise ValueError(f"Config root must be a JSON object: {filepath}")

    return parsed


def _compare(value: float, threshold: float, op: ComparisonOperator) -> bool:
    if op == ">=":
        return value >= threshold
    if op == "<=":
        return value <= threshold
    if op == ">":
        return value > threshold
    if op == "<":
        return value < threshold
    if op == "==":
        return value == threshold
    return value >= threshold


def _resolve_operator(
    side: str,
    regime: str,
    feature: str,
    scoring_rules: dict[str, Any],
) -> ComparisonOperator:
    """
    feature 비교 연산자를 규칙에서 우선 탐색하고, 없으면 전략 기본값으로 결정합니다.

    지원되는 연산자 설정 예시:
    - scoring_rules["operators"]["long"]["mean_reversion"]["rsi"] = "<="
    - scoring_rules["operators"]["long"]["rsi"] = "<="
    - scoring_rules["operators"]["rsi"] = "<="
    """
    operators = scoring_rules.get("operators", {})
    op_candidate = None

    if isinstance(operators, dict):
        side_ops = operators.get(side)
        if isinstance(side_ops, dict):
            regime_ops = side_ops.get(regime)
            if isinstance(regime_ops, dict):
                op_candidate = regime_ops.get(feature)
            if op_candidate is None:
                op_candidate = side_ops.get(feature)

        if op_candidate is None:
            op_candidate = operators.get(feature)

    if op_candidate in {">=", "<=", ">", "<", "=="}:
        return op_candidate

    if regime == "mean_reversion":
        return "<=" if side == "long" else ">="
    return ">="


def calculate_score(market_data: dict[str, Any], side: str, rules: dict[str, Any]) -> int:
    """
    계층형 scoring_rules를 순회하며 동적으로 점수를 계산합니다.

    Parameters
    ----------
    market_data : dict
        현재 캔들의 지표 값. 예: {"macd_hist": 78.2, "rsi": 31.0}
    side : str
        "long" 또는 "short"
    rules : dict
        load_config로 로드한 전체 설정 또는 scoring_rules 딕셔너리

    Returns
    -------
    int
        side 기준 총 점수
    """
    side_key = side.lower().strip()
    if side_key not in {"long", "short"}:
        raise ValueError("side must be either 'long' or 'short'")

    scoring_rules = rules.get("scoring_rules", rules)
    if not isinstance(scoring_rules, dict):
        return 0

    side_rules = scoring_rules.get(side_key, {})
    if not isinstance(side_rules, dict):
        return 0

    total_score = 0

    for regime, features in side_rules.items():
        if regime == "operators" or not isinstance(features, dict):
            continue

        for feature, tiers in features.items():
            if not isinstance(tiers, list):
                continue
            if feature not in market_data:
                continue

            feature_value = market_data[feature]
            if feature_value is None:
                continue

            try:
                current_value = float(feature_value)
            except (TypeError, ValueError):
                continue

            operator = _resolve_operator(side_key, regime, feature, scoring_rules)
            best_weight = 0

            for tier in tiers:
                if not isinstance(tier, (list, tuple)) or len(tier) < 2:
                    continue

                threshold, weight = tier[0], tier[1]
                try:
                    threshold_value = float(threshold)
                    weight_value = int(weight)
                except (TypeError, ValueError):
                    continue

                if _compare(current_value, threshold_value, operator):
                    best_weight = max(best_weight, weight_value)

            total_score += best_weight

    return int(total_score)


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


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_score_detail(prefix: str, parts: dict[str, int], total_score: int) -> str:
    active_parts = [f"{feature}={score}" for feature, score in parts.items() if score]
    if not active_parts:
        return f"{prefix}[none]=0"
    return f"{prefix}[{' '.join(active_parts)}]={total_score}"


def _score_bot_ready_side(side_config: dict[str, Any], feature_values: dict[str, float]) -> tuple[int, dict[str, int]]:
    if not isinstance(side_config, dict):
        return 0, {}

    features = side_config.get("features", {})
    if not isinstance(features, dict):
        return 0, {}

    total_score = 0
    score_parts: dict[str, int] = {}

    for feature_name, rule in features.items():
        if not isinstance(rule, dict):
            score_parts[feature_name] = 0
            continue

        enabled = bool(rule.get("enabled", False))
        score = int(rule.get("score", 0) or 0)
        threshold = rule.get("threshold")
        op = str(rule.get("op", ">=")).strip()

        if not enabled or threshold is None or feature_name not in feature_values:
            score_parts[feature_name] = 0
            continue

        value = feature_values[feature_name]
        if _compare(value, _to_float(threshold), op):
            score_parts[feature_name] = score
            total_score += score
        else:
            score_parts[feature_name] = 0

    return total_score, score_parts


def _calculate_bot_ready_entry_score(indicators: dict[str, Any]) -> dict[str, Any] | None:
    bot_ready_config = getattr(settings, "BOT_READY_ENTRY_CONFIG", {})
    if not isinstance(bot_ready_config, dict):
        return None

    long_config = bot_ready_config.get("long")
    short_config = bot_ready_config.get("short")
    if not isinstance(long_config, dict) or not isinstance(short_config, dict):
        return None

    feature_values = {
        "rsi": _to_float(indicators.get("rsi"), 50.0),
        "adx_15m": _to_float(indicators.get("adx_15m"), 0.0),
        "twap_imbalance": _to_float(indicators.get("twap_imbalance"), 0.0),
        "bid_ask_imbalance": _to_float(indicators.get("bid_ask_imbalance"), 0.0),
        "cvd_delta_slope": _to_float(indicators.get("cvd_delta_slope"), 0.0),
        "nofi_1m": _to_float(indicators.get("nofi_1m"), 0.0),
        "buy_ratio": _to_float(indicators.get("buy_ratio"), 0.0),
        "open_interest": _to_float(indicators.get("open_interest"), 0.0),
        "tick_count": _to_float(indicators.get("tick_count"), 0.0),
        "log_volume_zscore": _to_float(indicators.get("log_volume_zscore"), 0.0),
        "macd_hist": _to_float(indicators.get("macd_hist"), 0.0),
    }

    long_score, long_parts = _score_bot_ready_side(long_config, feature_values)
    short_score, short_parts = _score_bot_ready_side(short_config, feature_values)

    detail = (
        _format_score_detail("L", long_parts, long_score)
        if long_score >= short_score
        else _format_score_detail("S", short_parts, short_score)
    )

    return {
        "long_score": int(long_score),
        "short_score": int(short_score),
        "signal": 1
        if long_score > short_score
        else (-1 if short_score > long_score else 0),
        "detail": detail,
        "scores": {
            "long": long_parts,
            "short": short_parts,
        },
        "percentiles": indicators,
        "l_cvd": long_parts.get("cvd_delta_slope", 0),
        "l_nofi": long_parts.get("nofi_1m", 0),
        "s_cvd": short_parts.get("cvd_delta_slope", 0),
        "s_nofi": short_parts.get("nofi_1m", 0),
        "l_macd": long_parts.get("macd_hist", 0),
        "s_macd": short_parts.get("macd_hist", 0),
        "l_atr": 0,
        "s_atr": 0,
    }


def calculate_entry_score(indicators: dict, adx_boost_pctl: float = 70.0) -> dict:
    """
    V18.4 리스트 규칙 기반 스코어링 엔진
    """
    bot_ready_result = _calculate_bot_ready_entry_score(indicators)
    if bot_ready_result is not None:
        return bot_ready_result

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
        # [V19] 상세 스코어 구성 요소 (DB 적재 및 최적화용)
        "scores": {
            "long": {
                "macd": l_macd, "cvd": l_cvd, "imbal": l_imbal, "nofi": l_nofi,
                "rsi": l_rsi, "buy": l_buy, "vol": l_vol, "adx": l_adx,
                "fr": l_fr, "oi": l_oi, "tick": l_tick, "atr": l_atr,
                "htf": l_htf, "mom": l_mtf_m, "reg": l_mtf_r, "vwap": l_vwap
            },
            "short": {
                "macd": s_macd, "cvd": s_cvd, "imbal": s_imbal, "nofi": s_nofi,
                "rsi": s_rsi, "buy": s_buy, "vol": s_vol, "adx": s_adx,
                "fr": s_fr, "oi": s_oi, "tick": s_tick, "atr": s_atr,
                "htf": s_htf, "mom": s_mtf_m, "reg": s_mtf_r, "vwap": s_vwap
            }
        },
        "percentiles": indicators, # 원본 백분위수 데이터 전달
        # 하위 호환성 유지
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
