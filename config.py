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
    try:
        # .env 파일이 없으면 빈 파일로 자동 생성하여 영구 저장 보장
        if not os.path.exists(dotenv_path):
            with open(dotenv_path, "a", encoding="utf-8") as f:
                pass
        set_key(dotenv_path, key, str(value))
    except Exception as e:
        logger.error(f".env 파일 갱신 실패 ({key}): {e}")


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
    STRATEGY_VERSION = os.getenv("STRATEGY_VERSION", "V18")  # 전략 버전 식별자
    TIMEFRAME = os.getenv("TIMEFRAME", "3m")
    RISK_PERCENTAGE = float(os.getenv("RISK_PERCENTAGE", "0.005"))
    LEVERAGE = int(os.getenv("LEVERAGE", "5"))
    TIME_EXIT_MINUTES = int(
        os.getenv("TIME_EXIT_MINUTES", "90")
    )  # 하위 호환 유지 (Chandelier Exit 전환 후 비활성 예정)

    ATR_RATIO_MULT = float(os.getenv("ATR_RATIO_MULT", "1.2"))
    ATR_LONG_LEN = int(os.getenv("ATR_LONG_LEN", "200"))

    # SL/TP 배율 (ATR 대비) - 기존 1.5/2.5에서 확장
    # SL을 넓혀 일시적 되돌림에 손절되지 않도록 함
    SL_MULT = float(
        os.getenv("SL_MULT", "1.5")
    )  # ATR × 1.5 = 손절 거리 (V17 스코어링 기준)
    TP_MULT = float(
        os.getenv("TP_MULT", "5.0")
    )  # ATR × 5.0 = 익절 거리 (V17 스코어링 기준) (R:R = 2:1)

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

    # 전체 최대 동시 진입 허용 개수 (알트코인 연쇄 손절 방지용)
    MAX_TRADES = int(os.getenv("MAX_TRADES", "3"))

    # 본절(Breakeven) 추적 로직 (V16+)
    BREAKEVEN_TRIGGER_MULT = float(os.getenv("BREAKEVEN_TRIGGER_MULT", "1.5"))
    BREAKEVEN_PROFIT_MULT = float(os.getenv("BREAKEVEN_PROFIT_MULT", "0.2"))

    # ── V18 스코어링 진입 엔진 파라미터 ────────────────────────────────
    MIN_ENTRY_SCORE = int(os.getenv("MIN_ENTRY_SCORE", "5"))  # 진입 최소 합산 점수
    PCTL_WINDOW = int(os.getenv("PCTL_WINDOW", "100"))  # 백분위수 산출 윈도우
    ADX_BOOST_PCTL = float(
        os.getenv("ADX_BOOST_PCTL", "70")
    )  # 추세 부스트 임계 백분위수

    # ── V17 체결 & 사이징 파라미터 ────────────────────────────────────────
    # Half-Kelly 동적 사이징 (승률·손익비 기반 투입 비중 자동 조절)
    KELLY_SIZING = os.getenv("KELLY_SIZING", "False").lower() == "true"
    KELLY_MIN_TRADES = int(os.getenv("KELLY_MIN_TRADES", "20"))  # 최소 표본 수
    KELLY_MAX_FRACTION = float(
        os.getenv("KELLY_MAX_FRACTION", "0.05")
    )  # 최대 투입 비율 캡

    # Chasing 지정가 체결 대기 시간 (초)
    CHASING_WAIT_SEC = float(os.getenv("CHASING_WAIT_SEC", "5.0"))

    # ── V17 분할 익절 파라미터 ────────────────────────────────────────────
    # TP 도달 시 전체 물량 중 이 비율만 1차 익절, 잔량은 Chandelier 추적
    PARTIAL_TP_RATIO = float(os.getenv("PARTIAL_TP_RATIO", "0.5"))  # 50% 분할 익절

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
