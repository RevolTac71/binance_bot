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
    V18.4 리스트 규칙 기반 스코어링 엔진 (Current System)
    """
    bot_ready_result = _calculate_bot_ready_entry_score(indicators)
    if bot_ready_result is not None:
        return bot_ready_result

    # Fallback if bot_ready_config is missing
    return {
        "long_score": 0, "short_score": 0, "signal": 0,
        "detail": "Missing bot_ready_entry_config",
        "scores": {"long": {}, "short": {}},
        "percentiles": indicators
    }


# ━━━━━━ 백테스트용 벡터화 함수 (기존 유지) ━━━━━━


