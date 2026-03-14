import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv, set_key
import urllib.parse
import threading
import requests
import requests

# Load environment variables (override=True를 통해 .env 파일의 수정사항이 항상 우선 적용되도록 함)
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path, override=True)


def update_env_variable(key: str, value: str, silent: bool = False):
    """
    실행 중 메모리의 환경변수를 갱신하고 동시에 .env 파일에도 덮어씁니다.
    """
    os.environ[key] = str(value)
    try:
        if not os.path.exists(dotenv_path):
            with open(dotenv_path, "a", encoding="utf-8") as f:
                pass

        success = set_key(dotenv_path, key, str(value))

        if success and not silent:
            logger.info(
                f"💾 [Persistence] 환경변수 '{key}' 가 {value}로 영구 저장되었습니다."
            )
    except Exception as e:
        logger.error(f"❌ [Persistence] .env 파일 갱신 실패 ({key}): {e}")


# KST Timezone 설정
KST = ZoneInfo("Asia/Seoul")


class Config:
    def __init__(self):
        # 1. Binance API Settings
        self.BINANCE_API_KEY = os.getenv("BINANCE_API_KEY") or os.getenv("BINANCE_KEY")
        self.BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET") or os.getenv("BINANCE_SECRET")
        self.USE_TESTNET = os.getenv("USE_TESTNET", "False").lower() == "true"
        self.BINANCE_TESTNET_API_KEY = os.getenv("BINANCE_TESTNET_API_KEY")
        self.BINANCE_TESTNET_API_SECRET = os.getenv("BINANCE_TESTNET_API_SECRET")

        # 2. Database Settings
        self.DB_USER = os.getenv("DB_USER", "postgres.uvqhpiilmameyjortqoc")
        _raw_password = os.getenv("DB_PASSWORD", "")
        self.DB_PASSWORD = urllib.parse.quote_plus(_raw_password)
        self.DB_HOST = os.getenv("DB_HOST", "aws-1-ap-southeast-2.pooler.supabase.com")
        self.DB_PORT = os.getenv("DB_PORT", "6543")
        self.DB_NAME = os.getenv("DB_NAME", "postgres")

        self.SQLALCHEMY_DATABASE_URI = (
            f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

        # 3. Telegram Settings
        self.TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
        self.TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

        # 4. Global Parameters
        self.STRATEGY_VERSION = os.getenv("STRATEGY_VERSION", "V18")
        self.TIMEFRAME = os.getenv("TIMEFRAME", "3m")
        self.RISK_PERCENTAGE = float(os.getenv("RISK_PERCENTAGE", "0.005"))
        self.LEVERAGE = int(os.getenv("LEVERAGE", "5"))
        self.TIME_EXIT_MINUTES = int(os.getenv("TIME_EXIT_MINUTES", "60"))
        self.DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"
        
        # 5. Kelly & Chasing
        self.KELLY_SIZING = os.getenv("KELLY_SIZING", "False").lower() == "true"
        self.KELLY_MIN_TRADES = int(os.getenv("KELLY_MIN_TRADES", "20"))
        self.KELLY_MAX_FRACTION = float(os.getenv("KELLY_MAX_FRACTION", "0.05"))
        self.CHASING_WAIT_SEC = float(os.getenv("CHASING_WAIT_SEC", "2.5"))
        self.CHASING_MAX_RETRY = int(os.getenv("CHASING_MAX_RETRY", "10"))
        self.CHASING_MARKET_THRESHOLD = int(os.getenv("CHASING_MARKET_THRESHOLD", "2"))
        
        # 6. Exit Modes & Multipliers
        self.PARTIAL_TP_RATIO = float(os.getenv("PARTIAL_TP_RATIO", "0.5"))
        self.LONG_TP_MODE = os.getenv("LONG_TP_MODE", "ATR")
        self.LONG_SL_MODE = os.getenv("LONG_SL_MODE", "ATR")
        self.SHORT_TP_MODE = os.getenv("SHORT_TP_MODE", "PERCENT")
        self.SHORT_SL_MODE = os.getenv("SHORT_SL_MODE", "ATR")
        
        self.L_TP_MULT = float(os.getenv("L_TP_MULT", "5.0"))
        self.L_SL_MULT = float(os.getenv("L_SL_MULT", "1.5"))
        self.S_TP_MULT = float(os.getenv("S_TP_MULT", "5.0"))
        self.S_SL_MULT = float(os.getenv("S_SL_MULT", "1.5"))
        
        self.L_TP_PCT = float(os.getenv("L_TP_PCT", "0.05"))
        self.L_SL_PCT = float(os.getenv("L_SL_PCT", "0.02"))
        self.S_TP_PCT = float(os.getenv("S_TP_PCT", "0.03"))
        self.S_SL_PCT = float(os.getenv("S_SL_PCT", "0.015"))
        
        self.FEE_RATE = float(os.getenv("FEE_RATE", "0.00045"))
        self.BREAKEVEN_TRIGGER_MULT = float(os.getenv("BREAKEVEN_TRIGGER_MULT", "1.5"))
        self.BREAKEVEN_PROFIT_MULT = float(os.getenv("BREAKEVEN_PROFIT_MULT", "0.2"))
        
        # 7. System Settings
        self.MACD_FILTER_ENABLED = os.getenv("MACD_FILTER_ENABLED", "True").lower() == "true"
        self.SYMBOL_REFRESH_INTERVAL = int(os.getenv("SYMBOL_REFRESH_INTERVAL", "3"))
        self.CURRENT_TARGET_SYMBOLS = []
        _blacklist = os.getenv("BLACKLIST_SYMBOLS", "")
        self.BLACKLIST_SYMBOLS = [s.strip().upper() for s in _blacklist.split(",") if s.strip()]
        
        # 8. Scoring & Boosts
        self.MIN_SCORE_LONG = int(os.getenv("MIN_SCORE_LONG", "18"))
        self.MIN_SCORE_SHORT = int(os.getenv("MIN_SCORE_SHORT", "17"))
        self.ADX_BOOST_PCTL = float(os.getenv("ADX_BOOST_PCTL", "70.0"))
        self.PCTL_WINDOW = int(os.getenv("PCTL_WINDOW", "100"))
        self.ATR_RATIO_MULT = float(os.getenv("ATR_RATIO_MULT", "1.2"))
        self.ATR_LONG_LEN = int(os.getenv("ATR_LONG_LEN", "200"))
        self.LOSS_COOLDOWN_MINUTES = int(os.getenv("LOSS_COOLDOWN_MINUTES", "15"))
        self.MAX_TRADES = int(os.getenv("MAX_TRADES", "3"))
        self.MAX_CONCURRENT_SAME_DIR = int(os.getenv("MAX_CONCURRENT_SAME_DIR", "2"))
        
        # MTF
        self.HTF_TIMEFRAME_1H = os.getenv("HTF_TIMEFRAME_1H", "1h")
        self.HTF_TIMEFRAME_15M = os.getenv("HTF_TIMEFRAME_15M", "15m")

        self.rebuild_scoring_rules()

    # [V18.6] API 장애 시 사용할 기본 메이저 알트코인 리스트
    DEFAULT_FALLBACK_SYMBOLS = [
        "SOL/USDT:USDT", "ADA/USDT:USDT", "XRP/USDT:USDT", "DOT/USDT:USDT",
        "AVAX/USDT:USDT", "LINK/USDT:USDT", "DOGE/USDT:USDT", "MATIC/USDT:USDT",
        "TRX/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "ICP/USDT:USDT",
        "NEAR/USDT:USDT", "APT/USDT:USDT", "ARB/USDT:USDT"
    ]

    def rebuild_scoring_rules(self):
        """환경변수로부터 스코어링 규칙을 다시 빌드합니다."""
        self.L_MACD_T1 = (os.getenv("L_MACD_T1", "65"))
        self.L_MACD_W1 = float(os.getenv("L_MACD_W1", "1"))
        self.L_MACD_T2 = float(os.getenv("L_MACD_T2", "75"))
        self.L_MACD_W2 = float(os.getenv("L_MACD_W2", "2"))
        self.L_MACD_T4 = float(os.getenv("L_MACD_T4", "85"))
        self.L_MACD_W4 = float(os.getenv("L_MACD_W4", "4"))
        self.L_CVD_T1 = float(os.getenv("L_CVD_T1", "70"))
        self.L_CVD_W1 = float(os.getenv("L_CVD_W1", "1"))
        self.L_CVD_T2 = float(os.getenv("L_CVD_T2", "85"))
        self.L_CVD_W2 = float(os.getenv("L_CVD_W2", "2"))
        self.L_IMBAL_T1 = float(os.getenv("L_IMBAL_T1", "80"))
        self.L_IMBAL_W1 = float(os.getenv("L_IMBAL_W1", "1"))
        self.L_NOFI_T1 = float(os.getenv("L_NOFI_T1", "85"))
        self.L_NOFI_W1 = float(os.getenv("L_NOFI_W1", "1"))
        self.L_OI_T1 = float(os.getenv("L_OI_T1", "70"))
        self.L_OI_W1 = float(os.getenv("L_OI_W1", "2"))
        self.L_OI_T2 = float(os.getenv("L_OI_T2", "85"))
        self.L_OI_W2 = float(os.getenv("L_OI_W2", "4"))
        self.L_TICK_T1 = float(os.getenv("L_TICK_T1", "85"))
        self.L_TICK_W1 = float(os.getenv("L_TICK_W1", "1"))
        self.L_VOL_T1 = float(os.getenv("L_VOL_T1", "1.9"))
        self.L_VOL_W1 = float(os.getenv("L_VOL_W1", "1"))
        self.L_BUY_T1 = int(os.getenv("L_BUY_T1", "15"))
        self.L_BUY_W1 = int(os.getenv("L_BUY_W1", "1"))

        self.S_MACD_T1 = float(os.getenv("S_MACD_T1", "65"))
        self.S_MACD_W1 = float(os.getenv("S_MACD_W1", "1"))
        self.S_MACD_T2 = float(os.getenv("S_MACD_T2", "75"))
        self.S_MACD_W2 = float(os.getenv("S_MACD_W2", "2"))
        self.S_MACD_T4 = float(os.getenv("S_MACD_T4", "85"))
        self.S_MACD_W4 = float(os.getenv("S_MACD_W4", "4"))
        self.S_CVD_T1 = float(os.getenv("S_CVD_T1", "70"))
        self.S_CVD_W1 = float(os.getenv("S_CVD_W1", "1"))
        self.S_CVD_T2 = float(os.getenv("S_CVD_T2", "85"))
        self.S_CVD_W2 = float(os.getenv("S_CVD_W2", "2"))
        self.S_IMBAL_T1 = float(os.getenv("S_IMBAL_T1", "65"))
        self.S_IMBAL_W1 = float(os.getenv("S_IMBAL_W1", "1"))
        self.S_IMBAL_T2 = float(os.getenv("S_IMBAL_T2", "80"))
        self.S_IMBAL_W2 = float(os.getenv("S_IMBAL_W2", "2"))
        self.S_NOFI_T1 = float(os.getenv("S_NOFI_T1", "70"))
        self.S_NOFI_W1 = float(os.getenv("S_NOFI_W1", "1"))
        self.S_NOFI_T2 = float(os.getenv("S_NOFI_T2", "85"))
        self.S_NOFI_W2 = float(os.getenv("S_NOFI_W2", "2"))
        self.S_OI_T1 = float(os.getenv("S_OI_T1", "70"))
        self.S_OI_W1 = float(os.getenv("S_OI_W1", "1"))
        self.S_OI_T2 = float(os.getenv("S_OI_T2", "85"))
        self.S_OI_W2 = float(os.getenv("S_OI_W2", "2"))
        self.S_TICK_T1 = float(os.getenv("S_TICK_T1", "70"))
        self.S_TICK_W1 = float(os.getenv("S_TICK_W1", "1"))
        self.S_TICK_T2 = float(os.getenv("S_TICK_T2", "85"))
        self.S_TICK_W2 = float(os.getenv("S_TICK_W2", "2"))
        self.S_VOL_T1 = float(os.getenv("S_VOL_T1", "1.4"))
        self.S_VOL_W1 = float(os.getenv("S_VOL_W1", "1"))
        self.S_VOL_T2 = float(os.getenv("S_VOL_T2", "1.9"))
        self.S_VOL_W2 = float(os.getenv("S_VOL_W2", "2"))
        self.S_RSI_T1 = float(os.getenv("S_RSI_T1", "35"))
        self.S_RSI_W1 = float(os.getenv("S_RSI_W1", "1"))
        self.S_RSI_T2 = float(os.getenv("S_RSI_T2", "25"))
        self.S_RSI_W2 = float(os.getenv("S_RSI_W2", "2"))
        self.S_BUY_T1 = float(os.getenv("S_BUY_T1", "25"))
        self.S_BUY_W1 = float(os.getenv("S_BUY_W1", "1"))
        self.S_BUY_T2 = float(os.getenv("S_BUY_T2", "10"))
        self.S_BUY_W2 = float(os.getenv("S_BUY_W2", "2"))

        self.SC_RULES_LONG = {
            "trend": {
                "macd_hist": [(self.L_MACD_T1, self.L_MACD_W1), (self.L_MACD_T2, self.L_MACD_W2), (self.L_MACD_T4, self.L_MACD_W4)],
                "cvd_delta_slope": [(self.L_CVD_T1, self.L_CVD_W1), (self.L_CVD_T2, self.L_CVD_W2)],
                "bid_ask_imbalance": [(self.L_IMBAL_T1, self.L_IMBAL_W1)],
                "nofi_1m": [(self.L_NOFI_T1, self.L_NOFI_W1)],
                "open_interest": [(self.L_OI_T1, self.L_OI_W1), (self.L_OI_T2, self.L_OI_W2)],
                "tick_count": [(self.L_TICK_T1, self.L_TICK_W1)],
                "log_volume_zscore": [(self.L_VOL_T1, self.L_VOL_W1, "val")],
            },
            "mean_reversion": {"rsi": [], "buy_ratio": [(self.L_BUY_T1, self.L_BUY_W1)]},
        }

        self.SC_RULES_SHORT = {
            "trend": {
                "macd_hist": [(self.S_MACD_T1, self.S_MACD_W1), (self.S_MACD_T2, self.S_MACD_W2), (self.S_MACD_T4, self.S_MACD_W4)],
                "cvd_delta_slope": [(self.S_CVD_T1, self.S_CVD_W1), (self.S_CVD_T2, self.S_CVD_W2)],
                "bid_ask_imbalance": [(self.S_IMBAL_T1, self.S_IMBAL_W1), (self.S_IMBAL_T2, self.S_IMBAL_W2)],
                "nofi_1m": [(self.S_NOFI_T1, self.S_NOFI_W1), (self.S_NOFI_T2, self.S_NOFI_W2)],
                "open_interest": [(self.S_OI_T1, self.S_OI_W1), (self.S_OI_T2, self.S_OI_W2)],
                "tick_count": [(self.S_TICK_T1, self.S_TICK_W1), (self.S_TICK_T2, self.S_TICK_W2)],
                "log_volume_zscore": [(self.S_VOL_T1, self.S_VOL_W1, "val"), (self.S_VOL_T2, self.S_VOL_W2, "val")],
            },
            "mean_reversion": {"rsi": [(self.S_RSI_T1, self.S_RSI_W1), (self.S_RSI_T2, self.S_RSI_W2)], "buy_ratio": [(self.S_BUY_T1, self.S_BUY_W1), (self.S_BUY_T2, self.S_BUY_W2)]},
        }

        self.SCORING_WEIGHTS = {
            "atr": {"2": int(os.getenv("WEIGHT_ATR_2", "2"))},
            "adx_boost": {"1": int(os.getenv("WEIGHT_ADX_1", "1"))},
            "fr_boost": {"2": int(os.getenv("WEIGHT_FR_2", "2"))},
            "htf_bias": {"2": int(os.getenv("WEIGHT_HTF_BIAS", "2"))},
            "mtf_moment": {"2": int(os.getenv("WEIGHT_MTF_MOMENT", "2"))},
            "mtf_regime": {"1": int(os.getenv("WEIGHT_MTF_REGIME", "1"))},
            "vwap_dist": {"2": int(os.getenv("WEIGHT_VWAP_DIST", "2"))},
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
    
    # [V18.6] KST 전용 포맷터 적용
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
settings = Config()
logger = get_logger()
