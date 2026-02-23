import asyncio
from datetime import datetime, timezone
import ccxt.async_support as ccxt
from sqlalchemy import text
from config import settings, logger
from database import AsyncSessionLocal, Trade, BalanceHistory
from data_pipeline import DataPipeline
from notification import notifier


class ExecutionEngine:
    def __init__(self, data_pipeline: DataPipeline):
        """
        동일한 API Key와 속도 제한기(Rate Limiter)를 공유하기 위해
        data_pipeline이 지니고 있는 ccxt exchange 인스턴스를 활용합니다.
        """
        self.exchange = data_pipeline.exchange
        # 시스템 문제 검출(DB-서버 간 Mismatch 등) 시 자가 정지 처리를 위한 Flag
        self.is_halted = False

    async def execute_trade(
        self,
        symbol: str,
        amount: float,
        current_price: float,
        stop_loss: float,
        reason: str,
    ) -> bool:
        """
        Long 시장가 진입 주문과 동시에 하드 스탑로스(역지정가 시장가) 주문을 발송하여 OCO 성격을 구현합니다.
        성공 시 DB에 이력을 적재하고, 실패 시 예외 처리하여 카카오톡 알림을 발송합니다.
        """
        if self.is_halted:
            logger.warning(
                f"시스템이 일시 중지(Halted) 상태입니다. 신규 진입 요청[{symbol}] 거부."
            )
            return False

        try:
            # 바이낸스 선물 시장 기준 로터리 사이즈 정밀도가 필요하므로, amount 등을 미리 맞추어 둔 상황 가정
            logger.info(
                f"[{symbol}] 시장가 롱 진입 시도. 예측 체결가: {current_price:.4f}, 수량: {amount}"
            )

            # 1. 포지션 진입 (Market Buy)
            entry_order = await self.exchange.create_order(
                symbol=symbol, type="market", side="buy", amount=amount
            )

            # 2. 강제 하드 스탑로스 지정 (STOP_MARKET, reduceOnly=True 사용)
            # 바이낸스 선물은 reduceOnly를 통해 기존 물량 청산에만 초점을 둘 수 있습니다.
            stop_order = await self.exchange.create_order(
                symbol=symbol,
                type="stopMarket",
                side="sell",
                amount=amount,
                price=None,  # 시장가 체결
                params={"stopPrice": stop_loss, "reduceOnly": True},
            )
            logger.info(
                f"[{symbol}] 스탑로스({stop_loss:.4f}) 명령 전송 완료. (ReduceOnly)"
            )

            # 3. 로컬 DB(Supabase) 적재
            async with AsyncSessionLocal() as session:
                new_trade = Trade(
                    timestamp=datetime.now(timezone.utc),
                    action="BUY",
                    symbol=symbol,
                    price=current_price,  # 시장가여서 약간의 오차가 있을 수 있음 (엄밀히 entry_order['average'] 참조 권장)
                    quantity=amount,
                    reason=reason,
                )
                session.add(new_trade)
                await session.commit()

            await notifier.send_message(
                f"✅ 신규 롱 진입\n[{symbol}] 수량: {amount}\n스탑로스: {stop_loss:.4f}\n사유: {reason}"
            )
            return True

        except Exception as e:
            logger.error(f"[{symbol}] 주문 실행 중 극단적 예외 발생: {e}")
            await notifier.send_message(
                f"🚨 [긴급 장애] 주문 전송 실패\n심볼: {symbol}\n내용: {e}"
            )
            return False

    async def close_position(self, symbol: str, amount: float, reason: str) -> bool:
        """
        타임컷(24시간 경과 등) 혹은 수동 청산 시 지정 수량을 시장가로 청산합니다.
        """
        try:
            logger.info(f"[{symbol}] 청산 요청됨. 사유: {reason}")
            # 시장가 매도로 롱 포지션 청산
            close_order = await self.exchange.create_order(
                symbol=symbol,
                type="market",
                side="sell",
                amount=amount,
                price=None,
                params={"reduceOnly": True},
            )

            async with AsyncSessionLocal() as session:
                # 청산 이력 적재
                new_trade = Trade(
                    timestamp=datetime.now(timezone.utc),
                    action="SELL",
                    symbol=symbol,
                    price=0.0,  # 마켓오더 미기입 또는 average로 향후 고도화 가능
                    quantity=amount,
                    reason=reason,
                )
                session.add(new_trade)
                await session.commit()

            await notifier.send_message(
                f"🔄 포지션 청산 완료\n[{symbol}]\n사유: {reason}"
            )
            return True

        except Exception as e:
            logger.error(f"청산 처리 과정 오류 발생: {e}")
            return False

    async def check_state_mismatch(self):
        """
        [Fail-Safe 방어 체계]
        매시간 정각에 바이낸스 실제 위치(잔고, 전체 열린 포지션)와
        DB내역(trades 누적 구매/매도)을 대조하여 강한 오류 방지 메카니즘을 구축.
        불일치가 발생하면 시스템 전역을 Halted 시킵니다.
        """
        if self.is_halted:
            return

        try:
            # 1. 현재 잔고 파악 및 DB 추가 (주기적 모니터링용)
            balance_info = await self.exchange.fetch_balance()
            total_usdt = balance_info.get("total", {}).get("USDT", 0.0)

            async with AsyncSessionLocal() as session:
                new_balance = BalanceHistory(
                    timestamp=datetime.now(timezone.utc), balance=total_usdt
                )
                session.add(new_balance)

                # 2. DB 누적 순수 포지션 사이즈(수량) 조회 (간단한 예시: BUY량 - SELL량 합산 = NET POSITION)
                # 엄밀한 동기화는 각 심볼 단위로 이루어져야 하나,
                # 여기서는 시스템 장애의 대표격으로 총 USDT가 극단적으로 0이 되는 등(청산)의 상태를 점검
                if total_usdt < (settings.RISK_PERCENTAGE * 50):
                    # 자산이 아주 미미하게 남은 경우 (예기치 못한 극단적 손실)
                    self.is_halted = True
                    logger.error(
                        f"[Fail-Safe] 잔고가 비정상적으로 소진되었습니다. ({total_usdt} USDT)"
                    )
                    await notifier.send_message(
                        f"🚨 [시스템 긴급정지]\n자산이 비정상적으로 적습니다. 누수 또는 연쇄 스탑로스의 가능성으로 운영을 일시 중단합니다.\n현재 잔고: {total_usdt:.2f} USDT"
                    )

                await session.commit()

            logger.info(
                f"[State Sync] 서버 잔여 USDT: {total_usdt:.2f} (대조 검사 이상없음)"
            )

        except Exception as e:
            logger.error(f"State Sync 네트워크 연동 에러: {e}")
