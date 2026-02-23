import json
import aiohttp
from config import logger, settings


class KakaoNotifier:
    def __init__(self):
        self.access_token = settings.KAKAO_ACCESS_TOKEN
        self.refresh_token = settings.KAKAO_REFRESH_TOKEN
        self.rest_api_key = settings.KAKAO_REST_API_KEY

    async def refresh_access_token(self):
        """
        카카오톡 Access Token을 Refresh Token을 사용해 갱신합니다.
        """
        url = "https://kauth.kakao.com/oauth/token"
        data = {
            "grant_type": "refresh_token",
            "client_id": self.rest_api_key,
            "refresh_token": self.refresh_token,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as response:
                    result = await response.json()
                    if "access_token" in result:
                        self.access_token = result["access_token"]
                        logger.info("카카오톡 Access Token 갱신에 성공했습니다.")
                    else:
                        logger.error(f"카카오톡 토큰 갱신 실패: {result}")
        except Exception as e:
            logger.error(f"카카오톡 토큰 갱신 예외 발생: {e}")

    async def send_message(self, text: str):
        """
        나에게 카카오톡 텍스트 메시지를 비동기적으로 전송합니다.
        권한(Token) 만료 시 자동 갱신을 1회 재시도합니다.
        """
        url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        data = {
            "template_object": json.dumps(
                {
                    "object_type": "text",
                    "text": text,
                    "link": {
                        "web_url": "https://developers.kakao.com",
                        "mobile_web_url": "https://developers.kakao.com",
                    },
                }
            )
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, data=data) as response:
                    # 토큰 만료 에러 (401) 발생 시 즉시 리프레시 후 1회 재시도 (재귀 호출 대신 로컬 재시도)
                    if response.status == 401:
                        logger.warning(
                            "카카오톡 메시지 전송 실패 (401 Auth Error). 토큰 리프레시를 시도합니다."
                        )
                        await self.refresh_access_token()
                        headers["Authorization"] = f"Bearer {self.access_token}"

                        # 갱신된 토큰으로 다시 HTTP POST 시도
                        async with session.post(
                            url, headers=headers, data=data
                        ) as retry_response:
                            if retry_response.status == 200:
                                logger.info(
                                    f"카카오톡 메시지 전송 성공 (재시도 완료): {text[:20]}..."
                                )
                            else:
                                text_err = await retry_response.text()
                                logger.error(f"카카오톡 재전송 실패: {text_err}")

                    elif response.status == 200:
                        logger.info(f"카카오톡 메시지 전송 성공: {text[:20]}...")
                    else:
                        text_err = await response.text()
                        logger.error(
                            f"카카오톡 전송 실패 [{response.status}]: {text_err}"
                        )

        except Exception as e:
            logger.error(f"카카오톡 메시지 전송 중 통신 오류 발생: {e}")


# 전역 싱글톤 객체로 사용
notifier = KakaoNotifier()
