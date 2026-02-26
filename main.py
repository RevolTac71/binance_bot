import asyncio
import time
from datetime import datetime, timezone, timedelta
from config import logger, settings
from database import check_db_connection, Trade, AsyncSessionLocal
from data_pipeline import DataPipeline
from strategy import StrategyEngine
from risk_management import RiskManager
from execution import ExecutionEngine
from notification import notifier


def get_today_0900_kst_timestamp() -> int:
    """
    현재 시각을 기준으로 가장 최근의 09:00 KST (00:00 UTC) 타임스탬프(ms)를 계산합니다.
    (Anchored VWAP 계산의 Base Time)
    """
    now_utc = datetime.now(timezone.utc)

    # KST 기준 변환 (+9 시간)
    kst_offset = timedelta(hours=9)
    now_kst = now_utc + kst_offset

    # 당일 09:00 KST 생성
    target_kst = now_kst.replace(hour=9, minute=0, second=0, microsecond=0)

    # 만약 현재 KST가 09:00 이전이라면(예: KST 08:00),
    # 직전 기준일은 하루 전 09:00 KST가 되어야 함.
    if now_kst < target_kst:
        target_kst -= timedelta(days=1)

    # 다시 UTC로 변환
    target_utc = target_kst - kst_offset
    return int(target_utc.timestamp() * 1000)


def is_funding_fee_cutoff() -> bool:
    """
    펀딩비 체결 (매 01:00, 09:00, 17:00 KST)에 따른 리스크 회피 시간 필터.
    해당 정각의 5분 전 (XX:55:00) 부터 정각 후 30초 (XX:00:30) 까지는 True(위험 구역)를 반환.
    """
    now_utc = datetime.now(timezone.utc)
    now_kst = now_utc + timedelta(hours=9)

    hour = now_kst.hour
    minute = now_kst.minute
    second = now_kst.second

    # 정산 시간 리스트 (KST)
    funding_hours = [1, 9, 17]

    # 1) 정산 시간 직전의 55분 ~ 59분
    if minute >= 55:
        next_hour = (hour + 1) % 24
        if next_hour in funding_hours:
            return True

    # 2) 정산 정각 후 30초 이내 (변동성/Latency 버퍼)
    if hour in funding_hours and minute == 0 and second <= 30:
        return True

    return False


async def process_single_symbol(
    symbol: str,
    pipeline: DataPipeline,
    strategy: StrategyEngine,
    risk: RiskManager,
    execution: ExecutionEngine,
    capital: float,
):
    """
    개별 코인에 대한 VWAP 계산 및 전략 진입 판별 코루틴.
    """
    # 진행 중인 포지션이 있거나 대기 주문이 있다면 스킵
    if symbol in execution.active_positions or symbol in execution.pending_entries:
        return

    try:
        # 1. 동적 타임스탬프를 이용해 기준 시간(09:00 KST)부터 현재까지의 3분봉 모두 가져오기
        since_ts = get_today_0900_kst_timestamp()

        df_3m = await pipeline.fetch_ohlcv_since(symbol, timeframe="3m", since=since_ts)

        if df_3m.empty:
            return

        # 2. 당일 누적 VWAP, 밴드, RSI 계산
        df_3m = pipeline.calculate_vwap_indicators(df_3m)

        # 3. 전략 엔진을 통해 지정가 대기 타점(Band +-2 ticks) 확인
        decision = strategy.check_entry(symbol, df_3m)

        if decision["signal"]:
            limit_price = decision["limit_price"]
            tp_price = decision["tp_price"]
            sl_price = decision["sl_price"]
            reason = decision["reason"]

            # 진입가격(limit_price)을 전달하여 고정비율 수량 계산 시 참조
            sizing = risk.calculate_position_size(symbol, capital, limit_price)

            if sizing["size"] <= 0:
                logger.info(f"[{symbol}] 포지션 사이징 불가(수량 0 산출). 진입 생략.")
                return

            qty = sizing["size"]
            side = "buy" if decision["signal"] == "LONG" else "sell"

            logger.info(
                f"[Execute] 🎯 {symbol} 지정가 타점 포착! "
                f"{side.upper()}(수량={qty}, 대기 지정가={limit_price}, 투입={sizing['invest_usdt']:.2f} USDT)"
            )

            # Post-Only 한정가 API 주문 전송
            await execution.place_limit_entry_order(
                symbol, side, qty, limit_price, tp_price, sl_price, reason
            )

    except Exception as e:
        logger.error(f"[{symbol}] 개별 스캔 중 에러: {e}")


async def trading_loop(
    pipeline: DataPipeline,
    strategy: StrategyEngine,
    risk: RiskManager,
    execution: ExecutionEngine,
):
    """
    VWAP 선물 메인 매매 폴링 루프:
    - 5분 전 펀딩비 컷오프 확인 및 대기주문 취소
    - 잔고 부족 시 스킵
    - Top 5 알트코인 병렬 조회 및 로직 병렬 실행 (asyncio.gather)
    """
    # 1. 추출
    top_alts = await pipeline.fetch_top_altcoins_by_volume(limit=5)
    logger.info(f"[Initial] 24H 거래량 Top 5 (선물): {top_alts}")
    last_alts_update = datetime.now()

    while True:
        try:
            # === 1. 시간 필터 체크 (펀딩비 컷오프) ===
            if is_funding_fee_cutoff():
                logger.warning(
                    "⏱️ [Time Filter] 펀딩비 컷오프 적용 구간. 신규 스캔 정지 및 대기 주문 취소."
                )

                # 미체결 지정가 주문이 있다면 전면 취소
                pending_symbols = list(execution.pending_entries.keys())
                for sym in pending_symbols:
                    await execution.cancel_pending_order(
                        sym, reason="펀딩비 타임 필터에 의한 강제 취소"
                    )

                await asyncio.sleep(10)  # 10초 대기 후 재점검
                continue

            # === 2. 종목 리스트 갱신 (4시간마다) ===
            if (datetime.now() - last_alts_update).total_seconds() >= 14400:
                top_alts = await pipeline.fetch_top_altcoins_by_volume(limit=5)
                last_alts_update = datetime.now()
                logger.info(f"🔄 Top 5 관심 종목 갱신: {top_alts}")

            # === 3. 잔고 조회 ===
            balance_info = await pipeline.exchange.fetch_balance()
            capital = balance_info.get("total", {}).get("USDT", 0.0)

            # --- FOR DRY RUN TESTING ONLY ---
            if settings.DRY_RUN:
                capital = 1000.0

            if capital < risk.min_order_usdt:
                logger.warning(
                    f"⚠️ 전체 선물 잔고 부족({capital:.2f} USDT). 신규 진입/스캔 중지."
                )
                await asyncio.sleep(60)
                continue

            # === 4. 병렬 진입 스캔 (상태 관리된 종목 제외) ===
            tasks = []
            for symbol in top_alts:
                tasks.append(
                    process_single_symbol(
                        symbol, pipeline, strategy, risk, execution, capital
                    )
                )

            # 약간의 간격을 두며 병렬 실행 (Rate Limit 보호 목적)
            if tasks:
                await asyncio.gather(*tasks)

            # 1사이클 스캔 휴식 (Rate Limit 및 CPU 자원 보호)
            await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"[Main-Loop Error] 예외 발생: {e}")
            await asyncio.sleep(10)


async def state_machine_loop(execution: ExecutionEngine):
    """
    지정가 대기 취소/체결 판별 및 TP/SL 포워딩을 수행하는 별도의 폴링 루프
    """
    while True:
        try:
            await execution.check_pending_orders_state()
            await execution.check_active_positions_state()
            # 서버 미스매치 점검 (잔고 고립 등)
            await execution.check_state_mismatch()

            await asyncio.sleep(3)  # 상태 조회를 3초 단위로 타이트하게
        except Exception as e:
            logger.error(f"[State Machine Error]: {e}")
            await asyncio.sleep(5)


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
        "🚀 [시작] 바이낸스 현물(Spot) 스캘핑 봇 서버가 부팅되었습니다."
    )

    # 핵심 컴포넌트 준비
    pipeline = DataPipeline()
    strategy = StrategyEngine()
    risk = RiskManager(pipeline)
    execution = ExecutionEngine(pipeline)

    try:
        # 진행 중이던 포지션 복구 및 쓰레기 대기주문 정리
        await execution.sync_state_from_exchange()

        # 비동기 병렬 태스크(Task) 스케줄링
        task_state = asyncio.create_task(state_machine_loop(execution))
        task_trade = asyncio.create_task(
            trading_loop(pipeline, strategy, risk, execution)
        )

        await asyncio.gather(task_state, task_trade)
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
