import aiohttp
from config import logger, settings


class TelegramNotifier:
    def __init__(self):
        self.bot_token = settings.TELEGRAM_BOT_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    async def send_message(self, text: str):
        """
        텔레그램 봇을 통해 지정된 Chat ID로 비동기 메시지를 전송합니다.
        """
        if not self.bot_token or not self.chat_id:
            logger.warning(
                "텔레그램 봇 토큰이나 Chat ID가 설정되지 않아 알림을 스킵합니다."
            )
            return

        url = f"{self.base_url}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logger.info(f"텔레그램 메시지 전송 성공: {text[:20]}...")
                    else:
                        text_err = await response.text()
                        logger.error(
                            f"텔레그램 전송 실패 [{response.status}]: {text_err}"
                        )
        except Exception as e:
            logger.error(f"텔레그램 메시지 전송 중 오류 발생: {e}")


# 전역 싱글톤 객체로 사용
notifier = TelegramNotifier()
