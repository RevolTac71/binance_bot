import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv, set_key
import urllib.parse
import threading
import requests

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)


def update_env_variable(key: str, value: str):
    """
    실행 중 메모리의 환경변수를 갱신하고 동시에 .env 파일에도 덮어씁니다.
    """
    os.environ[key] = str(value)
    if os.path.exists(dotenv_path):
        set_key(dotenv_path, key, str(value))
    else:
        logger.warning(f".env 파일을 찾을 수 없어 {key} 설정이 영구 저장되지 않습니다.")


# KST Timezone 설정
KST = ZoneInfo("Asia/Seoul")


class Config:
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
    STRATEGY_VERSION = os.getenv("STRATEGY_VERSION", "V16")  # 전략 버전 식별자
    TIMEFRAME = os.getenv("TIMEFRAME", "3m")
    K_VALUE = float(os.getenv("K_VALUE", "2.0"))
    RISK_PERCENTAGE = float(os.getenv("RISK_PERCENTAGE", "0.005"))
    LEVERAGE = int(os.getenv("LEVERAGE", "5"))
    TIME_EXIT_MINUTES = int(
        os.getenv("TIME_EXIT_MINUTES", "90")
    )  # 하위 호환 유지 (Chandelier Exit 전환 후 비활성 예정)

    # V15.2 New Parameters
    VOL_MULT = float(
        os.getenv("VOL_MULT", "1.5")
    )  # 일반 거래량 스파이크 배수 (1.5x~2.0x)
    EXTREME_VOL_MULT = float(
        os.getenv("EXTREME_VOL_MULT", "2.5")
    )  # 극단 소진 진입 배수 (2.5x~3.0x)
    ATR_RATIO_MULT = float(os.getenv("ATR_RATIO_MULT", "1.2"))
    ATR_LONG_LEN = int(os.getenv("ATR_LONG_LEN", "200"))

    # SL/TP 배율 (ATR 대비) - 기존 1.5/2.5에서 확장
    # SL을 넓혀 일시적 되돌림에 손절되지 않도록 함
    SL_MULT = float(os.getenv("SL_MULT", "3.0"))  # ATR × 3.0 = 손절 거리
    TP_MULT = float(os.getenv("TP_MULT", "6.0"))  # ATR × 6.0 = 익절 거리 (R:R = 2:1)

    # 동일 종목 연속 손실 시 쿨다운 (분)
    LOSS_COOLDOWN_MINUTES = int(os.getenv("LOSS_COOLDOWN_MINUTES", "15"))

    # ── V16 MTF 필터 파라미터 ─────────────────────────────────────────────
    # 상위 타임프레임 설정 (CCXT 포맷)
    HTF_TIMEFRAME_1H = os.getenv("HTF_TIMEFRAME_1H", "1h")  # 1시간봉 (거시 추세)
    HTF_TIMEFRAME_15M = os.getenv(
        "HTF_TIMEFRAME_15M", "15m"
    )  # 15분봉 (추세 강도·모멘텀)
    # ADX 기준값: 이 이상이면 추세장(모멘텀 추종), 미만이면 횡보장(역추세/평균회귀)
    ADX_THRESHOLD = float(os.getenv("ADX_THRESHOLD", "20.0"))

    # ── V16 샹들리에 청산(Chandelier Exit / Trailing Stop) 파라미터 ────────
    # 진입 후 최고점(Long) 또는 최저점(Short)에서 ATR × 배수 만큼 후퇴 시 손절
    CHANDELIER_MULT = float(os.getenv("CHANDELIER_MULT", "2.0"))
    CHANDELIER_ATR_LEN = int(os.getenv("CHANDELIER_ATR_LEN", "14"))  # ATR 산출 기간

    # ── V16 포트폴리오 동시 진입 제한 ────────────────────────────────────
    # 동일 방향(롱 또는 숏) 포지션이 이 개수 이상이면 추가 진입 차단
    MAX_CONCURRENT_SAME_DIR = int(os.getenv("MAX_CONCURRENT_SAME_DIR", "2"))

    # Dry Run Mode (True면 실제 매매하지 않고 DB 기록만 함)
    DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"

    # Telegram Interactive Pause Mode
    IS_PAUSED = False


class TelegramLogHandler(logging.Handler):
    """
    에러 발생 시 텔레그램으로 메세지를 전송하는 커스텀 로깅 핸들러입니다.
    비동기 웹소켓 충돌 방지를 위해 별도 스레드(threading)를 사용하여 requests로 전송합니다.
    """

    def emit(self, record):
        if record.levelno < logging.ERROR:
            return

        bot_token = getattr(settings, "TELEGRAM_BOT_TOKEN", None)
        chat_id = getattr(settings, "TELEGRAM_CHAT_ID", None)

        if not bot_token or not chat_id:
            return

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
