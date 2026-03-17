import json
import logging
import os
import urllib.parse
from datetime import datetime
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo

# KST Timezone 설정
KST = ZoneInfo("Asia/Seoul")

BASE_DIR = os.path.dirname(__file__)
SETTINGS_JSON_PATH = os.path.join(BASE_DIR, "settings.json")
LEGACY_DOTENV_PATH = os.path.join(BASE_DIR, ".env")

SECRET_KEYS = {
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "BINANCE_TESTNET_API_KEY",
    "BINANCE_TESTNET_API_SECRET",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "DB_PASSWORD",
    "BINANCE_KEY",
    "BINANCE_SECRET",
}

ENV_ONLY_KEYS = SECRET_KEYS | {
    "DB_USER",
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
}


def _parse_env_file(file_path: str) -> dict:
    parsed = {}
    if not os.path.exists(file_path):
        return parsed

    with open(file_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            parsed[key.strip()] = value.strip()
    return parsed


def _load_settings_data() -> dict:
    if os.path.exists(SETTINGS_JSON_PATH):
        try:
            # Accept both UTF-8 and UTF-8 with BOM.
            with open(SETTINGS_JSON_PATH, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            pass

    # settings.json 이 없을 경우 .env를 1회 마이그레이션 소스로 사용
    env_data = _parse_env_file(LEGACY_DOTENV_PATH)
    env_data = {k: v for k, v in env_data.items() if k not in ENV_ONLY_KEYS}
    if env_data:
        _save_settings_data(env_data)
    return env_data


def _save_settings_data(data: dict) -> None:
    with open(SETTINGS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)


def _to_storage_value(value):
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return str(value)


def _to_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _load_secret_data() -> dict:
    parsed = _parse_env_file(LEGACY_DOTENV_PATH)
    secrets = {k: v for k, v in parsed.items() if k in ENV_ONLY_KEYS}
    # OS 환경변수는 .env 보다 우선합니다.
    for key in ENV_ONLY_KEYS:
        env_val = os.environ.get(key)
        if env_val not in (None, ""):
            secrets[key] = env_val
    return secrets


def _save_secret_to_env_file(key: str, value: str) -> None:
    lines = []
    if os.path.exists(LEGACY_DOTENV_PATH):
        with open(LEGACY_DOTENV_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()

    target = f"{key}={value}\n"
    replaced = False
    for i, raw_line in enumerate(lines):
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        current_key = line.split("=", 1)[0].replace("export ", "").strip()
        if current_key == key:
            lines[i] = target
            replaced = True
            break

    if not replaced:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] = lines[-1] + "\n"
        lines.append(target)

    with open(LEGACY_DOTENV_PATH, "w", encoding="utf-8") as f:
        f.writelines(lines)


_SETTINGS_DATA = _load_settings_data()
_SECRET_DATA = _load_secret_data()


def _resolve_settings_key(key: str) -> str:
    """settings.json에 저장할 실제 키명을 결정합니다.

    기존 키가 있으면 그 casing을 유지하고,
    없으면 현재 settings.json 스타일(소문자 스네이크 케이스)에 맞춰 저장합니다.
    """
    candidates = [key, str(key).lower(), str(key).upper()]
    for candidate in candidates:
        if candidate in _SETTINGS_DATA:
            return candidate
    return str(key).lower()


def update_env_variable(key: str, value, silent: bool = False):
    """
    실행 중 설정값을 갱신하고 settings.json 파일에 영구 반영합니다.
    함수명은 기존 호출 호환성을 위해 유지합니다.
    """
    stored = _to_storage_value(value)
    try:
        # settings.json을 단일 기준으로 저장합니다.
        storage_key = _resolve_settings_key(key)
        _SETTINGS_DATA[storage_key] = stored
        _save_settings_data(_SETTINGS_DATA)

        # 런타임 호환을 위해 환경변수도 동기화합니다.
        os.environ[key] = stored

        # 과거 .env 의존 경로를 깨지 않기 위해 민감 키는 레거시 파일에도 미러링합니다.
        if key in ENV_ONLY_KEYS:
            _SECRET_DATA[key] = stored
            _save_secret_to_env_file(key, stored)

        if not silent:
            logger.info(
                f"💾 [Persistence] 설정 '{key}' 가 {stored}로 저장되었습니다. (settings key: {storage_key})"
            )
    except Exception as e:
        logger.error(f"❌ [Persistence] settings.json 갱신 실패 ({key}): {e}")


class Config:
    def __init__(self):
        self._data = _SETTINGS_DATA
        self._secrets = _SECRET_DATA
        self._known_keys = set()
        self._settings_mtime = self._get_settings_mtime()

        # 1. Binance API Settings
        self.BINANCE_API_KEY = self._get_str("BINANCE_API_KEY", None, aliases=["BINANCE_KEY"])
        self.BINANCE_API_SECRET = self._get_str(
            "BINANCE_API_SECRET", None, aliases=["BINANCE_SECRET"]
        )
        self.USE_TESTNET = self._get_bool("USE_TESTNET", False)
        self.BINANCE_TESTNET_API_KEY = self._get_str("BINANCE_TESTNET_API_KEY", None)
        self.BINANCE_TESTNET_API_SECRET = self._get_str("BINANCE_TESTNET_API_SECRET", None)

        # 2. Database Settings
        self.DB_USER = self._get_str("DB_USER", "postgres.uvqhpiilmameyjortqoc")
        raw_password = self._get_str("DB_PASSWORD", "")
        self.DB_PASSWORD = urllib.parse.quote_plus(raw_password)
        self.DB_HOST = self._get_str("DB_HOST", "aws-1-ap-southeast-2.pooler.supabase.com")
        self.DB_PORT = self._get_str("DB_PORT", "6543")
        self.DB_NAME = self._get_str("DB_NAME", "postgres")

        self.SQLALCHEMY_DATABASE_URI = (
            f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

        # 3. Telegram Settings
        self.TELEGRAM_BOT_TOKEN = self._get_str("TELEGRAM_BOT_TOKEN", None)
        self.TELEGRAM_CHAT_ID = self._get_str("TELEGRAM_CHAT_ID", None)

        # 4. Global Parameters
        self.STRATEGY_VERSION = self._get_str("STRATEGY_VERSION", "V18")
        self.TIMEFRAME = self._get_str("TIMEFRAME", "3m")
        self.RISK_PERCENTAGE = self._get_float("RISK_PERCENTAGE", 0.005)
        self.LEVERAGE = self._get_int("LEVERAGE", 5)
        self.TIME_EXIT_MINUTES = self._get_int("TIME_EXIT_MINUTES", 60)
        self.DRY_RUN = self._get_bool("DRY_RUN", True)
        self.PAUSED = self._get_bool("PAUSED", False)

        # 5. Kelly & Chasing
        self.KELLY_SIZING = self._get_bool("KELLY_SIZING", False)
        self.KELLY_MIN_TRADES = self._get_int("KELLY_MIN_TRADES", 20)
        self.KELLY_MAX_FRACTION = self._get_float("KELLY_MAX_FRACTION", 0.05)
        self.CHASING_WAIT_SEC = self._get_float("CHASING_WAIT_SEC", 2.5)
        self.CHASING_MAX_RETRY = self._get_int("CHASING_MAX_RETRY", 10)
        self.CHASING_MARKET_THRESHOLD = self._get_int("CHASING_MARKET_THRESHOLD", 2)

        # 6. Exit Modes & Multipliers
        self.PARTIAL_TP_RATIO = self._get_float(
            "PARTIAL_TP_RATIO", 0.5, aliases=["S_PARTIAL_TP_RATIO"]
        )
        self.LONG_TP_MODE = self._get_str("LONG_TP_MODE", "ATR")
        self.LONG_SL_MODE = self._get_str("LONG_SL_MODE", "ATR")
        self.SHORT_TP_MODE = self._get_str("SHORT_TP_MODE", "PERCENT")
        self.SHORT_SL_MODE = self._get_str("SHORT_SL_MODE", "ATR")

        self.L_TP_MULT = self._get_float("L_TP_MULT", 5.0, aliases=["LONG_TP_MULT", "TP_MULT"])
        self.L_SL_MULT = self._get_float("L_SL_MULT", 1.5, aliases=["LONG_SL_MULT", "SL_MULT"])
        self.S_TP_MULT = self._get_float("S_TP_MULT", 5.0, aliases=["SHORT_TP_MULT", "TP_MULT"])
        self.S_SL_MULT = self._get_float("S_SL_MULT", 1.5, aliases=["SHORT_SL_MULT", "SL_MULT"])

        self.L_TP_PCT = self._get_float("L_TP_PCT", 0.05, aliases=["LONG_TP_PCT"])
        self.L_SL_PCT = self._get_float("L_SL_PCT", 0.02, aliases=["LONG_SL_PCT"])
        self.S_TP_PCT = self._get_float("S_TP_PCT", 0.03, aliases=["SHORT_TP_PCT"])
        self.S_SL_PCT = self._get_float("S_SL_PCT", 0.015, aliases=["SHORT_SL_PCT"])

        self.FEE_RATE = self._get_float("FEE_RATE", 0.00045)
        self.BREAKEVEN_TRIGGER_MULT = self._get_float("BREAKEVEN_TRIGGER_MULT", 1.5)
        self.BREAKEVEN_PROFIT_MULT = self._get_float("BREAKEVEN_PROFIT_MULT", 0.2)

        # 7. System Settings
        self.MACD_FILTER_ENABLED = self._get_bool(
            "MACD_FILTER_ENABLED", True, aliases=["MTF_FILTER"]
        )
        self.SYMBOL_REFRESH_INTERVAL = self._get_int("SYMBOL_REFRESH_INTERVAL", 3)
        self.CURRENT_TARGET_SYMBOLS = []
        self.BLACKLIST_SYMBOLS = self._get_list("BLACKLIST_SYMBOLS")

        # Dashboard/Telegram 표시값
        self.K_VALUE = self._get_float("K_VALUE", 1.6)
        self.CHANDELIER_MULT = self._get_float("CHANDELIER_MULT", 2.5)
        self.CHANDELIER_ATR_LEN = self._get_int("CHANDELIER_ATR_LEN", 14)

        # 8. Scoring & Boosts
        self.MIN_SCORE_LONG = self._get_int("MIN_SCORE_LONG", 18)
        self.MIN_SCORE_SHORT = self._get_int("MIN_SCORE_SHORT", 17)
        self.ADX_BOOST_PCTL = self._get_float("ADX_BOOST_PCTL", 70.0)
        self.PCTL_WINDOW = self._get_int("PCTL_WINDOW", 100)
        self.ATR_RATIO_MULT = self._get_float("ATR_RATIO_MULT", 1.2)
        self.ATR_LONG_LEN = self._get_int("ATR_LONG_LEN", 200)
        self.LOSS_COOLDOWN_MINUTES = self._get_int("LOSS_COOLDOWN_MINUTES", 15)
        self.MAX_TRADES = self._get_int("MAX_TRADES", 3)
        self.MAX_CONCURRENT_SAME_DIR = self._get_int("MAX_CONCURRENT_SAME_DIR", 2)

        # MTF
        self.HTF_TIMEFRAME_1H = self._get_str("HTF_TIMEFRAME_1H", "1h")
        self.HTF_TIMEFRAME_15M = self._get_str("HTF_TIMEFRAME_15M", "15m")

        # 이전 버전 키는 경고만 억제하고 동작에는 영향이 없도록 알려진 키로 등록합니다.
        self._register_deprecated_keys()
        self._remember_keys("SCORING_RULES", aliases=["scoring_rules"])
        self._remember_keys("SCORING_THRESHOLDS", aliases=["scoring_thresholds"])

        self.rebuild_scoring_rules()
        self._log_unknown_keys()

    def _get_settings_mtime(self):
        try:
            return os.path.getmtime(SETTINGS_JSON_PATH)
        except OSError:
            return None

    def refresh_runtime_scoring_settings(self, force: bool = False) -> bool:
        """
        실행 중 settings.json 변경을 감지해 스코어링 관련 설정을 즉시 반영합니다.
        빈번한 호출을 고려해 mtime 변경 시에만 디스크를 다시 읽습니다.
        """
        current_mtime = self._get_settings_mtime()
        if current_mtime is None:
            return False

        if (
            not force
            and self._settings_mtime is not None
            and current_mtime <= self._settings_mtime
        ):
            return False

        fresh_data = _load_settings_data()
        if not isinstance(fresh_data, dict):
            return False

        global _SETTINGS_DATA
        _SETTINGS_DATA = fresh_data
        self._data = fresh_data
        self._settings_mtime = current_mtime

        # check_entry / scoring 경로에서 즉시 사용하는 값만 재적용
        self.MACD_FILTER_ENABLED = self._get_bool("MACD_FILTER_ENABLED", True, aliases=["MTF_FILTER"])
        self.MIN_SCORE_LONG = self._get_int("MIN_SCORE_LONG", 18)
        self.MIN_SCORE_SHORT = self._get_int("MIN_SCORE_SHORT", 17)
        self.ADX_BOOST_PCTL = self._get_float("ADX_BOOST_PCTL", 70.0)
        self.PCTL_WINDOW = self._get_int("PCTL_WINDOW", 100)
        self.ATR_RATIO_MULT = self._get_float("ATR_RATIO_MULT", 1.2)
        self.ATR_LONG_LEN = self._get_int("ATR_LONG_LEN", 200)
        self.rebuild_scoring_rules()
        return True

    # [V18.6] API 장애 시 사용할 기본 메이저 알트코인 리스트
    DEFAULT_FALLBACK_SYMBOLS = [
        "SOL/USDT:USDT",
        "ADA/USDT:USDT",
        "XRP/USDT:USDT",
        "DOT/USDT:USDT",
        "AVAX/USDT:USDT",
        "LINK/USDT:USDT",
        "DOGE/USDT:USDT",
        "MATIC/USDT:USDT",
        "TRX/USDT:USDT",
        "LTC/USDT:USDT",
        "BCH/USDT:USDT",
        "ICP/USDT:USDT",
        "NEAR/USDT:USDT",
        "APT/USDT:USDT",
        "ARB/USDT:USDT",
    ]

    def _remember_keys(self, key: str, aliases=None):
        self._known_keys.add(key)
        self._known_keys.add(str(key).lower())
        self._known_keys.add(str(key).upper())
        if aliases:
            for alias in aliases:
                self._known_keys.add(alias)
                self._known_keys.add(str(alias).lower())
                self._known_keys.add(str(alias).upper())

    def _register_deprecated_keys(self):
        deprecated = {
            "ADX_THRESHOLD",  # legacy key; replaced by newer scoring/boost params
        }
        for key in deprecated:
            self._remember_keys(key)

    def _first_raw(self, key: str, aliases=None):
        for candidate in [key] + (aliases or []):
            variants = [candidate, str(candidate).lower(), str(candidate).upper()]
            for variant in variants:
                if variant in self._data and self._data[variant] not in (None, ""):
                    return self._data[variant]
                if variant in self._secrets and self._secrets[variant] not in (None, ""):
                    return self._secrets[variant]
        return None

    def _get_str(self, key: str, default, aliases=None):
        self._remember_keys(key, aliases)
        raw = self._first_raw(key, aliases)
        if raw is None:
            return default
        return str(raw)

    def _get_int(self, key: str, default: int, aliases=None):
        self._remember_keys(key, aliases)
        raw = self._first_raw(key, aliases)
        if raw is None:
            return default
        try:
            return int(raw)
        except (TypeError, ValueError):
            return default

    def _get_float(self, key: str, default: float, aliases=None):
        self._remember_keys(key, aliases)
        raw = self._first_raw(key, aliases)
        if raw is None:
            return default
        try:
            return float(raw)
        except (TypeError, ValueError):
            return default

    def _get_bool(self, key: str, default: bool, aliases=None):
        self._remember_keys(key, aliases)
        raw = self._first_raw(key, aliases)
        return _to_bool(raw, default)

    def _get_list(self, key: str, aliases=None):
        self._remember_keys(key, aliases)
        raw = self._first_raw(key, aliases)
        if raw is None:
            return []
        if isinstance(raw, list):
            return [str(v).strip().upper() for v in raw if str(v).strip()]
        return [s.strip().upper() for s in str(raw).split(",") if s.strip()]

    def _log_unknown_keys(self):
        unknown = sorted(k for k in self._data.keys() if k not in self._known_keys)
        if unknown:
            logger.warning("[Config] 미사용/알수없음 설정 키 감지: %s", ", ".join(unknown))

    def rebuild_scoring_rules(self):
        """settings.json 으로부터 스코어링 규칙을 다시 빌드합니다."""
        raw_scoring_rules = self._first_raw("SCORING_RULES", aliases=["scoring_rules"])

        def _normalize_tiers(tiers):
            if not isinstance(tiers, list):
                return []

            normalized = []
            for tier in tiers:
                if not isinstance(tier, (list, tuple)) or len(tier) < 2:
                    continue

                try:
                    threshold = float(tier[0])
                    weight = float(tier[1])
                except (TypeError, ValueError):
                    continue

                if len(tier) >= 3:
                    normalized.append((threshold, weight, tier[2]))
                else:
                    normalized.append((threshold, weight))
            return normalized

        def _normalize_regime_map(regime_map):
            if not isinstance(regime_map, dict):
                return {}
            return {feature: _normalize_tiers(tiers) for feature, tiers in regime_map.items()}

        if isinstance(raw_scoring_rules, dict):
            long_raw = raw_scoring_rules.get("long", {})
            short_raw = raw_scoring_rules.get("short", {})
            weights_raw = raw_scoring_rules.get("weights", {})

            if isinstance(long_raw, dict) and isinstance(short_raw, dict):
                self.SC_RULES_LONG = {
                    "trend": _normalize_regime_map(long_raw.get("trend", {})),
                    "mean_reversion": _normalize_regime_map(
                        long_raw.get("mean_reversion", {})
                    ),
                }
                self.SC_RULES_SHORT = {
                    "trend": _normalize_regime_map(short_raw.get("trend", {})),
                    "mean_reversion": _normalize_regime_map(
                        short_raw.get("mean_reversion", {})
                    ),
                }

                if isinstance(weights_raw, dict):
                    self.SCORING_WEIGHTS = weights_raw
                else:
                    self.SCORING_WEIGHTS = {
                        "atr": {"2": self._get_int("WEIGHT_ATR_2", 2)},
                        "adx_boost": {"1": self._get_int("WEIGHT_ADX_1", 1)},
                        "fr_boost": {"2": self._get_int("WEIGHT_FR_2", 2)},
                        "htf_bias": {"2": self._get_int("WEIGHT_HTF_BIAS", 2)},
                        "mtf_moment": {"2": self._get_int("WEIGHT_MTF_MOMENT", 2)},
                        "mtf_regime": {"1": self._get_int("WEIGHT_MTF_REGIME", 1)},
                        "vwap_dist": {"2": self._get_int("WEIGHT_VWAP_DIST", 2)},
                    }
                return

        self.L_MACD_T1 = self._get_float("L_MACD_T1", 65)
        self.L_MACD_W1 = self._get_float("L_MACD_W1", 1)
        self.L_MACD_T2 = self._get_float("L_MACD_T2", 75)
        self.L_MACD_W2 = self._get_float("L_MACD_W2", 2)
        self.L_MACD_T4 = self._get_float("L_MACD_T4", 85)
        self.L_MACD_W4 = self._get_float("L_MACD_W4", 4)
        self.L_CVD_T1 = self._get_float("L_CVD_T1", 70)
        self.L_CVD_W1 = self._get_float("L_CVD_W1", 1)
        self.L_CVD_T2 = self._get_float("L_CVD_T2", 85)
        self.L_CVD_W2 = self._get_float("L_CVD_W2", 2)
        self.L_IMBAL_T1 = self._get_float("L_IMBAL_T1", 80)
        self.L_IMBAL_W1 = self._get_float("L_IMBAL_W1", 1)
        self.L_NOFI_T1 = self._get_float("L_NOFI_T1", 85)
        self.L_NOFI_W1 = self._get_float("L_NOFI_W1", 1)
        self.L_OI_T1 = self._get_float("L_OI_T1", 70)
        self.L_OI_W1 = self._get_float("L_OI_W1", 2)
        self.L_OI_T2 = self._get_float("L_OI_T2", 85)
        self.L_OI_W2 = self._get_float("L_OI_W2", 4)
        self.L_TICK_T1 = self._get_float("L_TICK_T1", 85)
        self.L_TICK_W1 = self._get_float("L_TICK_W1", 1)
        self.L_VOL_T1 = self._get_float("L_VOL_T1", 1.9)
        self.L_VOL_W1 = self._get_float("L_VOL_W1", 1)
        self.L_BUY_T1 = self._get_int("L_BUY_T1", 15)
        self.L_BUY_W1 = self._get_int("L_BUY_W1", 1)

        self.S_MACD_T1 = self._get_float("S_MACD_T1", 65)
        self.S_MACD_W1 = self._get_float("S_MACD_W1", 1)
        self.S_MACD_T2 = self._get_float("S_MACD_T2", 75)
        self.S_MACD_W2 = self._get_float("S_MACD_W2", 2)
        self.S_MACD_T4 = self._get_float("S_MACD_T4", 85)
        self.S_MACD_W4 = self._get_float("S_MACD_W4", 4)
        self.S_CVD_T1 = self._get_float("S_CVD_T1", 70)
        self.S_CVD_W1 = self._get_float("S_CVD_W1", 1)
        self.S_CVD_T2 = self._get_float("S_CVD_T2", 85)
        self.S_CVD_W2 = self._get_float("S_CVD_W2", 2)
        self.S_IMBAL_T1 = self._get_float("S_IMBAL_T1", 65)
        self.S_IMBAL_W1 = self._get_float("S_IMBAL_W1", 1)
        self.S_IMBAL_T2 = self._get_float("S_IMBAL_T2", 80)
        self.S_IMBAL_W2 = self._get_float("S_IMBAL_W2", 2)
        self.S_NOFI_T1 = self._get_float("S_NOFI_T1", 70)
        self.S_NOFI_W1 = self._get_float("S_NOFI_W1", 1)
        self.S_NOFI_T2 = self._get_float("S_NOFI_T2", 85)
        self.S_NOFI_W2 = self._get_float("S_NOFI_W2", 2)
        self.S_OI_T1 = self._get_float("S_OI_T1", 70)
        self.S_OI_W1 = self._get_float("S_OI_W1", 1)
        self.S_OI_T2 = self._get_float("S_OI_T2", 85)
        self.S_OI_W2 = self._get_float("S_OI_W2", 2)
        self.S_TICK_T1 = self._get_float("S_TICK_T1", 70)
        self.S_TICK_W1 = self._get_float("S_TICK_W1", 1)
        self.S_TICK_T2 = self._get_float("S_TICK_T2", 85)
        self.S_TICK_W2 = self._get_float("S_TICK_W2", 2)
        self.S_VOL_T1 = self._get_float("S_VOL_T1", 1.4)
        self.S_VOL_W1 = self._get_float("S_VOL_W1", 1)
        self.S_VOL_T2 = self._get_float("S_VOL_T2", 1.9)
        self.S_VOL_W2 = self._get_float("S_VOL_W2", 2)
        self.S_RSI_T1 = self._get_float("S_RSI_T1", 35)
        self.S_RSI_W1 = self._get_float("S_RSI_W1", 1)
        self.S_RSI_T2 = self._get_float("S_RSI_T2", 25)
        self.S_RSI_W2 = self._get_float("S_RSI_W2", 2)
        self.S_BUY_T1 = self._get_float("S_BUY_T1", 25)
        self.S_BUY_W1 = self._get_float("S_BUY_W1", 1)
        self.S_BUY_T2 = self._get_float("S_BUY_T2", 10)
        self.S_BUY_W2 = self._get_float("S_BUY_W2", 2)

        self.SC_RULES_LONG = {
            "trend": {
                "macd_hist": [
                    (self.L_MACD_T1, self.L_MACD_W1),
                    (self.L_MACD_T2, self.L_MACD_W2),
                    (self.L_MACD_T4, self.L_MACD_W4),
                ],
                "cvd_delta_slope": [
                    (self.L_CVD_T1, self.L_CVD_W1),
                    (self.L_CVD_T2, self.L_CVD_W2),
                ],
                "bid_ask_imbalance": [(self.L_IMBAL_T1, self.L_IMBAL_W1)],
                "nofi_1m": [(self.L_NOFI_T1, self.L_NOFI_W1)],
                "open_interest": [
                    (self.L_OI_T1, self.L_OI_W1),
                    (self.L_OI_T2, self.L_OI_W2),
                ],
                "tick_count": [(self.L_TICK_T1, self.L_TICK_W1)],
                "log_volume_zscore": [(self.L_VOL_T1, self.L_VOL_W1, "val")],
            },
            "mean_reversion": {"rsi": [], "buy_ratio": [(self.L_BUY_T1, self.L_BUY_W1)]},
        }

        self.SC_RULES_SHORT = {
            "trend": {
                "macd_hist": [
                    (self.S_MACD_T1, self.S_MACD_W1),
                    (self.S_MACD_T2, self.S_MACD_W2),
                    (self.S_MACD_T4, self.S_MACD_W4),
                ],
                "cvd_delta_slope": [
                    (self.S_CVD_T1, self.S_CVD_W1),
                    (self.S_CVD_T2, self.S_CVD_W2),
                ],
                "bid_ask_imbalance": [
                    (self.S_IMBAL_T1, self.S_IMBAL_W1),
                    (self.S_IMBAL_T2, self.S_IMBAL_W2),
                ],
                "nofi_1m": [
                    (self.S_NOFI_T1, self.S_NOFI_W1),
                    (self.S_NOFI_T2, self.S_NOFI_W2),
                ],
                "open_interest": [
                    (self.S_OI_T1, self.S_OI_W1),
                    (self.S_OI_T2, self.S_OI_W2),
                ],
                "tick_count": [
                    (self.S_TICK_T1, self.S_TICK_W1),
                    (self.S_TICK_T2, self.S_TICK_W2),
                ],
                "log_volume_zscore": [
                    (self.S_VOL_T1, self.S_VOL_W1, "val"),
                    (self.S_VOL_T2, self.S_VOL_W2, "val"),
                ],
            },
            "mean_reversion": {
                "rsi": [(self.S_RSI_T1, self.S_RSI_W1), (self.S_RSI_T2, self.S_RSI_W2)],
                "buy_ratio": [
                    (self.S_BUY_T1, self.S_BUY_W1),
                    (self.S_BUY_T2, self.S_BUY_W2),
                ],
            },
        }

        self.SCORING_WEIGHTS = {
            "atr": {"2": self._get_int("WEIGHT_ATR_2", 2)},
            "adx_boost": {"1": self._get_int("WEIGHT_ADX_1", 1)},
            "fr_boost": {"2": self._get_int("WEIGHT_FR_2", 2)},
            "htf_bias": {"2": self._get_int("WEIGHT_HTF_BIAS", 2)},
            "mtf_moment": {"2": self._get_int("WEIGHT_MTF_MOMENT", 2)},
            "mtf_regime": {"1": self._get_int("WEIGHT_MTF_REGIME", 1)},
            "vwap_dist": {"2": self._get_int("WEIGHT_VWAP_DIST", 2)},
        }


class KSTFormatter(logging.Formatter):
    """로그 출력 시간을 항상 한국 시간(KST)으로 표시하기 위한 커스텀 포맷터"""

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=KST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


def get_logger(name="BinanceBot"):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    formatter = KSTFormatter(
        "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    c_handler = logging.StreamHandler()
    c_handler.setFormatter(formatter)
    logger.addHandler(c_handler)

    f_handler = RotatingFileHandler(
        "app.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    f_handler.setFormatter(formatter)
    logger.addHandler(f_handler)

    try:
        from notification import TelegramLogHandler

        tg_handler = TelegramLogHandler()
        tg_handler.setLevel(logging.ERROR)
        tg_handler.setFormatter(formatter)
        logger.addHandler(tg_handler)
    except Exception as e:
        print(f"Failed to load TelegramLogHandler: {e}")

    return logger


# Global settings and logger ready
logger = get_logger()
settings = Config()
