import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import urllib.parse

# Load environment variables
load_dotenv()

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

    # Kakao Settings
    KAKAO_ACCESS_TOKEN = os.getenv("KAKAO_ACCESS_TOKEN")
    KAKAO_REFRESH_TOKEN = os.getenv("KAKAO_REFRESH_TOKEN")
    KAKAO_REST_API_KEY = os.getenv("KAKAO_REST_API_KEY")

    # Strategy Global Parameters
    K_VALUE = float(os.getenv("K_VALUE", "0.5"))
    RISK_PERCENTAGE = float(os.getenv("RISK_PERCENTAGE", "0.10"))
    LEVERAGE = int(os.getenv("LEVERAGE", "3"))

    # Dry Run Mode (True면 실제 매매하지 않고 DB 기록만 함)
    DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"


def get_logger(name="BinanceBot"):
    """
    KST 타임존 기반으로 콘솔 및 파일 로그를 동시 출력하는 로거 생성 반환
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

    # File Handler (10MB max, keep 5 backups)
    f_handler = RotatingFileHandler(
        "app.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    f_handler.setFormatter(formatter)
    logger.addHandler(f_handler)

    return logger


# Global settings and logger ready
settings = Config()
logger = get_logger()
