import asyncio
from datetime import datetime
from config import logger, settings
from database import check_db_connection
from data_pipeline import DataPipeline
from strategy import StrategyEngine
from risk_management import RiskManager
from execution import ExecutionEngine
from notification import notifier


async def state_sync_loop(execution_engine: ExecutionEngine):
    """
    매시간 정각에 바이낸스 잔고/포지션과 데이터베이스의 기록값을 비교하여
    오류가 발생했는지(Fail-Safe) 검증합니다.
    """
    while True:
        now = datetime.now()
        # 다음 정각까지 기다리는 계산 공식
        seconds_until_next_hour = 3600 - (now.minute * 60 + now.second)

        # 테스트를 위해 짧은 주기를 원할시 아래처럼 강제로 1시간 이내 단축가능하나
        # 여기서는 매시간 정각 기준 대기를 실행합니다.
        await asyncio.sleep(seconds_until_next_hour)

        logger.info("[Main-Worker] 매시간 정기 상태 동기화 및 Fail-safe 점검 시작")
        await execution_engine.check_state_mismatch()


async def trading_loop(
    pipeline: DataPipeline,
    strategy: StrategyEngine,
    risk: RiskManager,
    execution: ExecutionEngine,
):
    """
    메인 매매 무한 루프: 알트코인 스캔, 시세 조회, 시그널 판별, 체결 및 알림을 담당합니다.
    """
    # 1. 최초 구동 시 10개 상위 거래량 코인 추출
    top_alts = await pipeline.fetch_top_altcoins_by_volume(limit=10)
    logger.info(f"[Initial] 24H 거래량 상위 10개 추출 리스트: {top_alts}")

    last_alts_update = datetime.now()

    while True:
        try:
            # 매 24시간 마다(1일) 상위 알트코인 리스트를 갱신
            if (datetime.now() - last_alts_update).total_seconds() >= 86400:
                top_alts = await pipeline.fetch_top_altcoins_by_volume(limit=10)
                last_alts_update = datetime.now()
                logger.info(f"🔄 상위 10개 알트코인 관심 종목 갱신됨: {top_alts}")

            # Binance 잔고 상황 (자본금 판단 및 리스크 비율 계산용)
            balance_info = await pipeline.exchange.fetch_balance()
            capital = balance_info.get("total", {}).get("USDT", 0.0)

            # Fail-safe 동작 시 더 이상 진입/검사를 하지 않음
            if execution.is_halted:
                logger.warning(
                    "[Main-FailSafe] 시스템이 정지 상태(Halted)입니다. 매매 검토를 대기합니다."
                )
                await asyncio.sleep(60)
                continue

            # 분석할 모든 코인(선정된 상위 Top 10)에 대해 순회 검토
            for symbol in top_alts:
                # 1시간 봉(1h)을 기준으로 전략을 검증한다고 가정
                df = await pipeline.fetch_ohlcv_df(symbol, timeframe="1h")

                # 지표 추가 (Trend, 변동성 계수, Volume, RSI 등)
                df = pipeline.calculate_indicators(df)

                # 전략 엔진 호출
                decision = strategy.check_long_entry(df)

                if decision["signal"]:
                    # 상관계수 필터
                    is_correlated = await risk.is_highly_correlated_with_btc(
                        symbol, threshold=0.85
                    )
                    if is_correlated:
                        logger.info(
                            f"[Filter] {symbol}는 비트코인과의 커플링 심화(>0.85)로 종목 배제됨."
                        )
                        continue

                    # 진입가와 최근 ATR값
                    current_price = df.iloc[-1]["close"]
                    atr_14 = df.iloc[-1]["ATR_14"]

                    # 스탑로스폭과 리스크 베팅 수량 계산
                    stop_loss = risk.calculate_stop_loss(current_price, atr_14)
                    size = risk.calculate_position_size(
                        capital, current_price, stop_loss
                    )

                    # 슬리피지/스탑로스가 타이트하여 수량이 0.0 으로 잡힌 경우
                    if size <= 0:
                        logger.info(
                            f"[Reject] {symbol} 자본대비 계산된 진입수량이 음수 혹은 0입니다. 진입 취소"
                        )
                        continue

                    # 바이낸스 실서버 주문 요청
                    logger.info(
                        f"[Execute] 🎯 {symbol} 전략 부합 확정. 시장가 매수(수량={size}) 진행합니다."
                    )
                    await execution.execute_trade(
                        symbol=symbol,
                        amount=size,
                        current_price=current_price,
                        stop_loss=stop_loss,
                        reason=decision["reason"],
                    )

            # 전체 종목 1사이클 스캐닝이 끝나면, 짧게 (5분) 대기 후 다시 시세 관측 루프 시작
            logger.info(
                "모니터링 1 Cycle 검수 완료. 차기 캔들/종가 갱신을 위해 5분간 휴지기에 들어갑니다."
            )
            await asyncio.sleep(300)

        except Exception as e:
            logger.error(f"[Main-Loop Error] 복구할 수 없는 예외 발생: {e}")
            await asyncio.sleep(60)  # 무한루프 점유 방지를 위한 쓰로틀링 대기


async def main():
    logger.info("============== BINANCE 24/7 AUTO TRADER START ==============")

    # DB 및 사전 세팅 체커
    is_db_connected = await check_db_connection()
    if not is_db_connected:
        logger.error(
            "🛑 Supabase 접속 불가 또는 테이블 오류 발생으로 구동을 강제 종료합니다."
        )
        return

    # 초기 카톡 메시지 송신
    await notifier.send_message(
        "🚀 [시작] 바이낸스 변동성+추세 추종 봇이 KST 기준 서버를 부팅했습니다."
    )

    # 핵심 컴포넌트 준비
    pipeline = DataPipeline()
    strategy = StrategyEngine()
    risk = RiskManager(pipeline)
    execution = ExecutionEngine(pipeline)

    try:
        # 비동기 병렬 태스크(Task) 스케줄링
        task_sync = asyncio.create_task(state_sync_loop(execution))
        task_trade = asyncio.create_task(
            trading_loop(pipeline, strategy, risk, execution)
        )

        await asyncio.gather(task_sync, task_trade)
    except KeyboardInterrupt:
        logger.warning("CTRL+C(키보드 인터럽트)로 시스템이 정지되었습니다.")
    finally:
        await pipeline.close()
        logger.info("거래소 API 객체 릴리즈 및 시스템 종료 절차 통과 완료.")


if __name__ == "__main__":
    # Windows 등 환경에서 async RuntimeError 우연 방어
    import sys

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
