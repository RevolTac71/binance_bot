import asyncio
import aiohttp
import logging
import html
import threading
import re
import time


class TelegramNotifier:
    def __init__(self):
        from config import settings
        self.bot_token = settings.TELEGRAM_BOT_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=25, connect=10, sock_read=20)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    @staticmethod
    def _strip_html_tags(text: str) -> str:
        # Telegram HTML parse 오류 시 안전한 평문으로 폴백하기 위한 최소 정리
        return re.sub(r"<[^>]+>", "", text)

    @staticmethod
    def _chunk_text(text: str, limit: int = 3900) -> list[str]:
        if len(text) <= limit:
            return [text]

        chunks = []
        remaining = text
        while len(remaining) > limit:
            split_at = remaining.rfind("\n", 0, limit)
            if split_at < 0:
                split_at = limit
            chunks.append(remaining[:split_at])
            remaining = remaining[split_at:].lstrip("\n")

        if remaining:
            chunks.append(remaining)
        return chunks

    async def send_message(self, text: str, max_retries: int = 3):
        """
        텔레그램 봇을 통해 지정된 Chat ID로 비동기 메시지를 전송합니다.
        429 에러(Too Many Requests) 발생 시 retry_after 지시만큼 대기 후 재시도합니다.
        """
        from config import logger

        if not self.bot_token or not self.chat_id:
            logger.warning(
                "텔레그램 봇 토큰이나 Chat ID가 설정되지 않아 알림을 스킵합니다."
            )
            return

        url = f"{self.base_url}/sendMessage"
        chunks = self._chunk_text(text)

        for i, chunk in enumerate(chunks, start=1):
            chunk_prefix = f"[{i}/{len(chunks)}]\n" if len(chunks) > 1 else ""
            message = f"{chunk_prefix}{chunk}"

            payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"}
            sent = False

            for attempt in range(max_retries):
                try:
                    session = await self._get_session()
                    async with session.post(url, json=payload) as response:
                        if response.status == 200:
                            logger.info(f"텔레그램 메시지 전송 성공: {message[:20]}...")
                            sent = True
                            break

                        if response.status == 429:
                            data = await response.json()
                            retry_after = data.get("parameters", {}).get("retry_after", 30)
                            logger.warning(
                                f"⚠️ 텔레그램 속도 제한(429) 감지. {retry_after}초 후 재시도 ({attempt + 1}/{max_retries})"
                            )
                            await asyncio.sleep(retry_after + 1)
                            continue

                        text_err = await response.text()
                        logger.error(
                            f"텔레그램 전송 실패 [{response.status}]: {text_err}"
                        )

                        # HTML 파싱 오류(400)인 경우 안전한 평문으로 1회 폴백
                        if response.status == 400 and "can't parse entities" in text_err.lower():
                            payload = {
                                "chat_id": self.chat_id,
                                "text": self._strip_html_tags(message),
                            }
                            logger.warning("HTML 파싱 오류 감지. parse_mode 없이 재시도합니다.")

                        await asyncio.sleep(2**attempt)

                except Exception as e:
                    logger.error(
                        f"텔레그램 메시지 전송 중 오류 발생 (시도 {attempt + 1}): {e}"
                    )
                    await asyncio.sleep(2**attempt)

            if not sent:
                logger.error(f"❌ 텔레그램 메시지 청크 전송 최종 실패: {message[:30]}...")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# 전역 싱글톤 객체로 사용
notifier = TelegramNotifier()


class TelegramLogHandler(logging.Handler):
    """
    Python logging 핸들러: ERROR 등급 이상의 로그를 실시간으로 텔레그램에 전송합니다.
    """
    def __init__(self):
        super().__init__()
        self._last_sent_messages = {}  # {message_hash: timestamp}
        self._lock = threading.Lock()

    def emit(self, record):
        # 무한 루프 방지: notification 모듈에서 발생한 에러는 텔레그램으로 보내지 않음
        if record.module == "notification":
            return

        # 텔레그램 핸들러 자체의 에러는 무시 (무한 루프 위험)
        if "텔레그램" in record.getMessage():
            return

        try:
            log_entry = self.format(record)
            
            # 동일 메시지 폭주 방지 (1분 쿨타임)
            msg_hash = hash(log_entry)
            now = time.time()
            with self._lock:
                last_sent = self._last_sent_messages.get(msg_hash, 0)
                if now - last_sent < 60:
                    return
                self._last_sent_messages[msg_hash] = now
                
                # 오래된 캐시 정리 (1시간 이상 된 것)
                if len(self._last_sent_messages) > 1000:
                    self._last_sent_messages = {
                        k: v for k, v in self._last_sent_messages.items() 
                        if now - v < 3600
                    }

            # HTML 특수 문자 이스케이프 (중첩 태그로 인한 400 에러 방지)
            safe_log = html.escape(log_entry)
            # 비동기 전송을 위해 백그라운드 태스크로 실행
            asyncio.create_task(notifier.send_message(f"⚠️ <b>[BOT ERROR]</b>\n<pre>{safe_log}</pre>"))
        except Exception:
            self.handleError(record)
