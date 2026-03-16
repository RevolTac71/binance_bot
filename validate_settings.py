import argparse
import codecs
import json
from pathlib import Path
from typing import Any


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_type(data: dict, key: str, expected: str, errors: list[str]) -> None:
    if key not in data:
        errors.append(f"missing key: {key}")
        return

    value = data[key]
    ok = False
    if expected == "number":
        ok = _is_number(value)
    elif expected == "int":
        ok = isinstance(value, int) and not isinstance(value, bool)
    elif expected == "bool":
        ok = isinstance(value, bool)
    elif expected == "str":
        ok = isinstance(value, str)
    elif expected == "object":
        ok = isinstance(value, dict)

    if not ok:
        errors.append(
            f"type mismatch: {key} -> expected {expected}, got {type(value).__name__}"
        )


def _validate_tiers(path: str, tiers: Any, errors: list[str]) -> None:
    if not isinstance(tiers, list):
        errors.append(f"{path} must be a list")
        return

    for idx, tier in enumerate(tiers):
        if not isinstance(tier, list):
            errors.append(f"{path}[{idx}] must be a list")
            continue
        if len(tier) < 2:
            errors.append(f"{path}[{idx}] must have at least 2 elements")
            continue
        if not _is_number(tier[0]):
            errors.append(f"{path}[{idx}][0] threshold must be numeric")
        if not _is_number(tier[1]):
            errors.append(f"{path}[{idx}][1] weight must be numeric")


def _validate_scoring_rules(data: dict, errors: list[str]) -> None:
    rules = data.get("scoring_rules")
    if not isinstance(rules, dict):
        errors.append("scoring_rules must be an object")
        return

    for side in ("long", "short"):
        side_obj = rules.get(side)
        if not isinstance(side_obj, dict):
            errors.append(f"scoring_rules.{side} must be an object")
            continue

        for regime in ("trend", "mean_reversion"):
            regime_obj = side_obj.get(regime)
            if not isinstance(regime_obj, dict):
                errors.append(f"scoring_rules.{side}.{regime} must be an object")
                continue

            for feature, tiers in regime_obj.items():
                _validate_tiers(f"scoring_rules.{side}.{regime}.{feature}", tiers, errors)

    weights = rules.get("weights")
    if not isinstance(weights, dict):
        errors.append("scoring_rules.weights must be an object")
        return

    for feature, band_map in weights.items():
        if not isinstance(band_map, dict):
            errors.append(f"scoring_rules.weights.{feature} must be an object")
            continue
        for band, weight in band_map.items():
            if not isinstance(band, str):
                errors.append(f"scoring_rules.weights.{feature} band key must be string")
            if not _is_number(weight):
                errors.append(
                    f"scoring_rules.weights.{feature}.{band} weight must be numeric"
                )


def validate_settings(settings_path: Path) -> int:
    errors: list[str] = []

    if not settings_path.exists():
        print(f"[FAIL] settings file not found: {settings_path}")
        return 1

    raw_bytes = settings_path.read_bytes()
    has_utf8_bom = raw_bytes.startswith(codecs.BOM_UTF8)
    if has_utf8_bom:
        errors.append(
            "UTF-8 BOM detected: current bot loader in config.py uses utf-8 and may ignore this file"
        )

    try:
        data = json.loads(raw_bytes.decode("utf-8-sig"))
    except json.JSONDecodeError as e:
        print(f"[FAIL] invalid JSON: {e}")
        return 1

    if not isinstance(data, dict):
        print("[FAIL] root must be a JSON object")
        return 1

    expected_top = {
        "strategy_version": "str",
        "timeframe": "str",
        "risk_percentage": "number",
        "leverage": "int",
        "macd_filter_enabled": "bool",
        "min_score_long": "int",
        "min_score_short": "int",
        "scoring_rules": "object",
    }
    for key, expected in expected_top.items():
        _check_type(data, key, expected, errors)

    _validate_scoring_rules(data, errors)

    # Try real loader path used by bot.
    try:
        from config import Config

        cfg = Config()

        attr_map = {
            "strategy_version": "STRATEGY_VERSION",
            "timeframe": "TIMEFRAME",
            "leverage": "LEVERAGE",
            "min_score_long": "MIN_SCORE_LONG",
            "min_score_short": "MIN_SCORE_SHORT",
            "risk_percentage": "RISK_PERCENTAGE",
        }
        for key, attr in attr_map.items():
            if key not in data:
                continue
            expected = data[key]
            actual = getattr(cfg, attr, None)

            if isinstance(expected, float):
                if not isinstance(actual, (int, float)) or abs(float(actual) - expected) > 1e-12:
                    errors.append(
                        f"loader mismatch: {key} file={expected} but Config.{attr}={actual}"
                    )
            else:
                if actual != expected:
                    errors.append(
                        f"loader mismatch: {key} file={expected} but Config.{attr}={actual}"
                    )

        print(
            "[INFO] Config loaded: "
            f"TIMEFRAME={cfg.TIMEFRAME}, LEVERAGE={cfg.LEVERAGE}, "
            f"MIN_SCORE_LONG={cfg.MIN_SCORE_LONG}, MIN_SCORE_SHORT={cfg.MIN_SCORE_SHORT}"
        )
    except Exception as e:
        errors.append(f"config loader failed: {e}")

    if errors:
        print("[FAIL] settings validation failed")
        for i, msg in enumerate(errors, start=1):
            print(f"  {i}. {msg}")
        return 1

    print("[PASS] settings.json is valid and bot loader can read it")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate bot settings.json format")
    parser.add_argument(
        "--file",
        default="settings.json",
        help="Path to settings JSON file (default: settings.json)",
    )
    args = parser.parse_args()

    return validate_settings(Path(args.file))


if __name__ == "__main__":
    raise SystemExit(main())
