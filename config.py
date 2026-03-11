import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv, set_key
import urllib.parse
import threading
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
        self.rebuild_scoring_rules()

    # Binance API Settings
    BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
    BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

    # Binance Testnet Settings
    USE_TESTNET = os.getenv("USE_TESTNET", "False").lower() == "true"
    BINANCE_TESTNET_API_KEY = os.getenv("BINANCE_TESTNET_API_KEY")
    BINANCE_TESTNET_API_SECRET = os.getenv("BINANCE_TESTNET_API_SECRET")

    # Supabase Database Settings
    DB_USER = os.getenv("DB_USER", "postgres.uvqhpiilmameyjortqoc")
    # 비밀번호에 특수문자가 있을 수 있으므로 URL 인코딩 처리
    _raw_password = os.getenv("DB_PASSWORD", "")
    DB_PASSWORD = urllib.parse.quote_plus(_raw_password)
    DB_HOST = os.getenv("DB_HOST", "aws-1-ap-southeast-2.pooler.supabase.com")
    DB_PORT = os.getenv("DB_PORT", "6543")
    DB_NAME = os.getenv("DB_NAME", "postgres")

    # SQLAlchemy Database URI (asyncpg 사용을 고려해 포맷 구성, 필요시 psycopg2로 전환 가능)
    SQLALCHEMY_DATABASE_URI = (
        f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Telegram Settings
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    # Strategy Global Parameters
    STRATEGY_VERSION = os.getenv("STRATEGY_VERSION", "V18")  # 전략 버전 식별자
    TIMEFRAME = os.getenv("TIMEFRAME", "3m")
    RISK_PERCENTAGE = float(os.getenv("RISK_PERCENTAGE", "0.005"))
    LEVERAGE = int(os.getenv("LEVERAGE", "5"))
    TIME_EXIT_MINUTES = int(
        os.getenv("TIME_EXIT_MINUTES", "60")
    )  # 기본값 90 -> 60 수정

    # [V18.4] 숏/롱 분리형 규칙 기반 스코어링 시스템
    def rebuild_scoring_rules(self):
        """
        환경변수로부터 스코어링 규칙(Threshold, Weight)을 다시 빌드합니다.
        /setparam 명령어로 특정 파라미터가 바뀐 뒤 이 메서드를 호출하면 즉시 반영됩니다.
        """
        # 개별 파라미터 로드
        self.L_MACD_T1 = int(os.getenv("L_MACD_T1", "65"))
        self.L_MACD_W1 = int(os.getenv("L_MACD_W1", "1"))
        self.L_MACD_T2 = int(os.getenv("L_MACD_T2", "75"))
        self.L_MACD_W2 = int(os.getenv("L_MACD_W2", "2"))
        self.L_MACD_T4 = int(os.getenv("L_MACD_T4", "85"))
        self.L_MACD_W4 = int(os.getenv("L_MACD_W4", "4"))

        self.L_CVD_T1 = int(os.getenv("L_CVD_T1", "70"))
        self.L_CVD_W1 = int(os.getenv("L_CVD_W1", "1"))
        self.L_CVD_T2 = int(os.getenv("L_CVD_T2", "85"))
        self.L_CVD_W2 = int(os.getenv("L_CVD_W2", "2"))

        self.L_IMBAL_T1 = int(os.getenv("L_IMBAL_T1", "80"))
        self.L_IMBAL_W1 = int(os.getenv("L_IMBAL_W1", "1"))

        self.L_NOFI_T1 = int(os.getenv("L_NOFI_T1", "85"))
        self.L_NOFI_W1 = int(os.getenv("L_NOFI_W1", "1"))

        self.L_OI_T1 = int(os.getenv("L_OI_T1", "70"))
        self.L_OI_W1 = int(os.getenv("L_OI_W1", "2"))
        self.L_OI_T2 = int(os.getenv("L_OI_T2", "85"))
        self.L_OI_W2 = int(os.getenv("L_OI_W2", "4"))

        self.L_TICK_T1 = int(os.getenv("L_TICK_T1", "85"))
        self.L_TICK_W1 = int(os.getenv("L_TICK_W1", "1"))

        self.L_VOL_T1 = float(os.getenv("L_VOL_T1", "1.9"))
        self.L_VOL_W1 = int(os.getenv("L_VOL_W1", "1"))

        self.L_BUY_T1 = int(os.getenv("L_BUY_T1", "15"))
        self.L_BUY_W1 = int(os.getenv("L_BUY_W1", "1"))

        # SHORT
        self.S_MACD_T1 = int(os.getenv("S_MACD_T1", "65"))
        self.S_MACD_W1 = int(os.getenv("S_MACD_W1", "1"))
        self.S_MACD_T2 = int(os.getenv("S_MACD_T2", "75"))
        self.S_MACD_W2 = int(os.getenv("S_MACD_W2", "2"))
        self.S_MACD_T4 = int(os.getenv("S_MACD_T4", "85"))
        self.S_MACD_W4 = int(os.getenv("S_MACD_W4", "4"))

        self.S_CVD_T1 = int(os.getenv("S_CVD_T1", "70"))
        self.S_CVD_W1 = int(os.getenv("S_CVD_W1", "1"))
        self.S_CVD_T2 = int(os.getenv("S_CVD_T2", "85"))
        self.S_CVD_W2 = int(os.getenv("S_CVD_W2", "2"))

        self.S_IMBAL_T1 = int(os.getenv("S_IMBAL_T1", "65"))
        self.S_IMBAL_W1 = int(os.getenv("S_IMBAL_W1", "1"))
        self.S_IMBAL_T2 = int(os.getenv("S_IMBAL_T2", "80"))
        self.S_IMBAL_W2 = int(os.getenv("S_IMBAL_W2", "2"))

        self.S_NOFI_T1 = int(os.getenv("S_NOFI_T1", "70"))
        self.S_NOFI_W1 = int(os.getenv("S_NOFI_W1", "1"))
        self.S_NOFI_T2 = int(os.getenv("S_NOFI_T2", "85"))
        self.S_NOFI_W2 = int(os.getenv("S_NOFI_W2", "2"))

        self.S_OI_T1 = int(os.getenv("S_OI_T1", "70"))
        self.S_OI_W1 = int(os.getenv("S_OI_W1", "1"))
        self.S_OI_T2 = int(os.getenv("S_OI_T2", "85"))
        self.S_OI_W2 = int(os.getenv("S_OI_W2", "2"))

        self.S_TICK_T1 = int(os.getenv("S_TICK_T1", "70"))
        self.S_TICK_W1 = int(os.getenv("S_TICK_W1", "1"))
        self.S_TICK_T2 = int(os.getenv("S_TICK_T2", "85"))
        self.S_TICK_W2 = int(os.getenv("S_TICK_W2", "2"))

        self.S_VOL_T1 = float(os.getenv("S_VOL_T1", "1.4"))
        self.S_VOL_W1 = int(os.getenv("S_VOL_W1", "1"))
        self.S_VOL_T2 = float(os.getenv("S_VOL_T2", "1.9"))
        self.S_VOL_W2 = int(os.getenv("S_VOL_W2", "2"))

        self.S_RSI_T1 = int(os.getenv("S_RSI_T1", "35"))
        self.S_RSI_W1 = int(os.getenv("S_RSI_W1", "1"))
        self.S_RSI_T2 = int(os.getenv("S_RSI_T2", "25"))
        self.S_RSI_W2 = int(os.getenv("S_RSI_W2", "2"))

        self.S_BUY_T1 = int(os.getenv("S_BUY_T1", "25"))
        self.S_BUY_W1 = int(os.getenv("S_BUY_W1", "1"))
        self.S_BUY_T2 = int(os.getenv("S_BUY_T2", "10"))
        self.S_BUY_W2 = int(os.getenv("S_BUY_W2", "2"))

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
                "open_interest": [(self.L_OI_T1, self.L_OI_W1), (self.L_OI_T2, self.L_OI_W2)],
                "tick_count": [(self.L_TICK_T1, self.L_TICK_W1)],
                "log_volume_zscore": [(self.L_VOL_T1, self.L_VOL_W1, "val")],
            },
            "mean_reversion": {
                "rsi": [],
                "buy_ratio": [(self.L_BUY_T1, self.L_BUY_W1)],
            },
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
                "nofi_1m": [(self.S_NOFI_T1, self.S_NOFI_W1), (self.S_NOFI_T2, self.S_NOFI_W2)],
                "open_interest": [(self.S_OI_T1, self.S_OI_W1), (self.S_OI_T2, self.S_OI_W2)],
                "tick_count": [(self.S_TICK_T1, self.S_TICK_W1), (self.S_TICK_T2, self.S_TICK_W2)],
                "log_volume_zscore": [
                    (self.S_VOL_T1, self.S_VOL_W1, "val"),
                    (self.S_VOL_T2, self.S_VOL_W2, "val"),
                ],
            },
            "mean_reversion": {
                "rsi": [(self.S_RSI_T1, self.S_RSI_W1), (self.S_RSI_T2, self.S_RSI_W2)],
                "buy_ratio": [(self.S_BUY_T1, self.S_BUY_W1), (self.S_BUY_T2, self.S_BUY_W2)],
            },
        }

        # 하위 호환 및 환경 부스트/거시 가중치 유지 (규칙 엔진으로 미통합된 항목들)
        self.SCORING_WEIGHTS = {
            "atr": {"2": int(os.getenv("WEIGHT_ATR_2", "2"))},
            "adx_boost": {"1": int(os.getenv("WEIGHT_ADX_1", "1"))},
            "fr_boost": {"2": int(os.getenv("WEIGHT_FR_2", "2"))},
            "htf_bias": {"2": int(os.getenv("WEIGHT_HTF_BIAS", "2"))},
            "mtf_moment": {"2": int(os.getenv("WEIGHT_MTF_MOMENT", "2"))},
            "mtf_regime": {"1": int(os.getenv("WEIGHT_MTF_REGIME", "1"))},
            "vwap_dist": {"2": int(os.getenv("WEIGHT_VWAP_DIST", "2"))},
        }

    # ── V18 체결 & 사이징 파라미터 ────────────────────────────────────────
    # Half-Kelly 동적 사이징 (승률·손익비 기반 투입 비중 자동 조절)
    KELLY_SIZING = os.getenv("KELLY_SIZING", "False").lower() == "true"
    KELLY_MIN_TRADES = int(os.getenv("KELLY_MIN_TRADES", "20"))  # 최소 표본 수
    KELLY_MAX_FRACTION = float(
        os.getenv("KELLY_MAX_FRACTION", "0.05")
    )  # 최대 투입 비율 캡

    # Chasing 지정가 체결 대기 시간 (초) - BTC 대응을 위해 2.5초로 단축 (V18.2)
    CHASING_WAIT_SEC = float(os.getenv("CHASING_WAIT_SEC", "2.5"))
    CHASING_MAX_RETRY = int(os.getenv("CHASING_MAX_RETRY", "10"))
    CHASING_MARKET_THRESHOLD = int(os.getenv("CHASING_MARKET_THRESHOLD", "2"))

    # ── V18 분할 익절 파라미터 ────────────────────────────────────────────
    # TP 도달 시 전체 물량 중 이 비율만 1차 익절, 잔량은 Chandelier 추적
    PARTIAL_TP_RATIO = float(os.getenv("PARTIAL_TP_RATIO", "0.5"))  # 50% 분할 익절

    # Dry Run Mode (True면 실제 매매하지 않고 DB 기록만 함)
    DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"

    # ── V18 시스템 & 설정 ──────────────────────────────────────────────
    MACD_FILTER_ENABLED = os.getenv("MACD_FILTER_ENABLED", "True").lower() == "true"
    SYMBOL_REFRESH_INTERVAL = int(os.getenv("SYMBOL_REFRESH_INTERVAL", "3"))
    CURRENT_TARGET_SYMBOLS = []  # 메인 루프에서 동적으로 채워짐 (AttributeError 방지)
    
    # [V18.5] 블랙리스트 심볼 (쉼표로 구분된 문자열을 리스트로 변환)
    _blacklist = os.getenv("BLACKLIST_SYMBOLS", "")
    BLACKLIST_SYMBOLS = [s.strip().upper() for s in _blacklist.split(",") if s.strip()]

    # ── V18 스코어링 & 필터 파라미터 (V18.3/4 추가분) ───────────────────

    # ── V18 스코어링 & 필터 파라미터 (V18.3/4 추가분) ───────────────────
    MIN_SCORE_LONG = int(os.getenv("MIN_SCORE_LONG", "18"))
    MIN_SCORE_SHORT = int(os.getenv("MIN_SCORE_SHORT", "17"))
    ADX_BOOST_PCTL = float(os.getenv("ADX_BOOST_PCTL", "70.0"))
    PCTL_WINDOW = int(os.getenv("PCTL_WINDOW", "100"))
    ATR_RATIO_MULT = float(os.getenv("ATR_RATIO_MULT", "1.2"))
    ATR_LONG_LEN = int(os.getenv("ATR_LONG_LEN", "200"))

    # ── V18 리스크 제어 & 동시 포지션 ───────────────────────────────
    MAX_TRADES = int(os.getenv("MAX_TRADES", "3"))
    MAX_CONCURRENT_SAME_DIR = int(os.getenv("MAX_CONCURRENT_SAME_DIR", "2"))
    LOSS_COOLDOWN_MINUTES = int(os.getenv("LOSS_COOLDOWN_MINUTES", "15"))
    BREAKEVEN_TRIGGER_MULT = float(os.getenv("BREAKEVEN_TRIGGER_MULT", "1.5"))
    BREAKEVEN_PROFIT_MULT = float(os.getenv("BREAKEVEN_PROFIT_MULT", "0.2"))

    # ── V18.5 방향별/익손절별 세분화 모드 ──────────────────────────────
    LONG_TP_MODE = os.getenv("LONG_TP_MODE", "ATR")  # 'ATR' or 'PERCENT'
    LONG_SL_MODE = os.getenv("LONG_SL_MODE", "ATR")  # 'ATR' or 'PERCENT'
    SHORT_TP_MODE = os.getenv("SHORT_TP_MODE", "PERCENT")  # 'ATR' or 'PERCENT'
    SHORT_SL_MODE = os.getenv("SHORT_SL_MODE", "ATR")  # 'ATR' or 'PERCENT'

    LONG_TP_MULT = float(os.getenv("L_TP_MULT", "5.0"))
    LONG_SL_MULT = float(os.getenv("L_SL_MULT", "1.5"))
    SHORT_TP_MULT = float(os.getenv("S_TP_MULT", "5.0"))
    SHORT_SL_MULT = float(os.getenv("S_SL_MULT", "1.5"))

    # [V18.4] 고정 수익 비율(%) 기반 설정 (EXIT_MODE='PERCENT' 시 사용)
    LONG_TP_PCT = float(os.getenv("L_TP_PCT", "0.05"))  # 5%
    LONG_SL_PCT = float(os.getenv("L_SL_PCT", "0.02"))  # 2%
    SHORT_TP_PCT = float(os.getenv("S_TP_PCT", "0.03"))  # 3%
    SHORT_SL_PCT = float(os.getenv("S_SL_PCT", "0.015"))  # 1.5%

    # [V18.4] 수수료율 설정 (사용자 요청 반영: 0.045% = 0.00045)
    FEE_RATE = float(os.getenv("FEE_RATE", "0.00045"))

    # 청산 관련 기타
    CHANDELIER_MULT = float(os.getenv("CHANDELIER_MULT", "3.0"))
    CHANDELIER_ATR_LEN = int(os.getenv("CHANDELIER_ATR_LEN", "14"))

    # 하위 호환용 (기존 코드에서 참조할 경우의 Fallback)
    SL_MULT = float(os.getenv("SL_MULT", "1.5"))
    TP_MULT = float(os.getenv("TP_MULT", "5.0"))

    # Telegram Interactive Pause Mode
    PAUSED = os.getenv("PAUSED", "False").lower() == "true"
    HTF_TIMEFRAME_1H = os.getenv("HTF_TIMEFRAME_1H", "1h")
    HTF_TIMEFRAME_15M = os.getenv("HTF_TIMEFRAME_15M", "15m")


class TelegramLogHandler(logging.Handler):
    """
    에러 발생 시 텔레그램으로 메세지를 전송하는 커스텀 로깅 핸들러입니다.
    비동기 웹소켓 충돌 방지를 위해 별도 스레드(threading)를 사용하여 requests로 전송합니다.
    """

    _last_sent_at = 0
    _lock = threading.Lock()

    def emit(self, record):
        if record.levelno < logging.ERROR:
            return

        bot_token = getattr(settings, "TELEGRAM_BOT_TOKEN", None)
        chat_id = getattr(settings, "TELEGRAM_CHAT_ID", None)

        if not bot_token or not chat_id:
            return

        # [V18.5] 텔레그램 429 방지를 위한 1초 쿨다운 로직 추가
        import time

        with self._lock:
            now = time.time()
            if now - self._last_sent_at < 1.0:
                return
            self._last_sent_at = now

        try:
            msg = self.format(record)
            if len(msg) > 3500:
                msg = msg[:3500] + "\n...[생략됨]"

            text = f"🚨 <b>[BOT ERROR LOG]</b>\n<pre>{msg}</pre>"
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}

            # 메인 루프 블로킹을 막기 위해 데몬 스레드로 발송 처리
            threading.Thread(
                target=requests.post, args=(url,), kwargs={"json": payload}, daemon=True
            ).start()
        except Exception:
            pass


def get_logger(name="BinanceBot"):
    """
    KST 타임존 기반으로 콘솔 및 파일 로그를 동시 출력하는 로거 생성 반환 (Telegram 전송 추가)
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False  # Root 로거로의 전파 중복 차단

    class KSTFormatter(logging.Formatter):
        def converter(self, timestamp):
            dt = datetime.fromtimestamp(timestamp, tz=KST)
            return dt.timetuple()

        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, tz=KST)
            if datefmt:
                return dt.strftime(datefmt)
            else:
                return dt.isoformat()

    formatter = KSTFormatter(
        fmt="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console Handler
    c_handler = logging.StreamHandler()
    c_handler.setFormatter(formatter)
    logger.addHandler(c_handler)

    f_handler = RotatingFileHandler(
        "app.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    f_handler.setFormatter(formatter)
    logger.addHandler(f_handler)

    # Telegram Error Handler (최상위 ERROR 등급 전용)
    tg_handler = TelegramLogHandler()
    tg_handler.setLevel(logging.ERROR)
    # 별도 포맷을 사용하거나 기본 포맷을 사용 (여기서는 기본 포맷)
    tg_handler.setFormatter(formatter)
    logger.addHandler(tg_handler)

    return logger


# Global settings and logger ready
settings = Config()
logger = get_logger()
