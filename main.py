import asyncio
import time
import json
import aiohttp
from datetime import datetime, timezone, timedelta
import pandas as pd
from config import logger, settings
from database import check_db_connection, Trade, AsyncSessionLocal
from data_pipeline import DataPipeline
from strategy import StrategyEngine
from risk_management import RiskManager
from execution import ExecutionEngine
from notification import notifier
from telegram_commands import setup_telegram_bot


def get_today_0900_kst_timestamp() -> int:
    """
    현재 시각을 기준으로 가장 최근의 당일 09:00 KST (00:00 UTC) 타임스탬프(ms)를 계산합니다.
    (V15.0 Anchored VWAP 계산 베이스 타임)
    """
    now_utc = datetime.now(timezone.utc)
    kst_offset = timedelta(hours=9)
    now_kst = now_utc + kst_offset

    target_kst = now_kst.replace(hour=9, minute=0, second=0, microsecond=0)
    if now_kst < target_kst:
        target_kst -= timedelta(days=1)

    target_utc = target_kst - kst_offset
    return int(target_utc.timestamp() * 1000)


def is_funding_fee_cutoff() -> bool:
    """
    펀딩비 체결 (매 01:00, 09:00, 17:00 KST)에 따른 리스크 회피 시간 필터.
    해당 정각의 5분 전 (XX:55:00) 부터 정각 후 30초 (XX:00:30) 까지
    """
    now_utc = datetime.now(timezone.utc)
    now_kst = now_utc + timedelta(hours=9)

    hour = now_kst.hour
    minute = now_kst.minute
    second = now_kst.second

    funding_hours = [1, 9, 17]

    if minute >= 55:
        next_hour = (hour + 1) % 24
        if next_hour in funding_hours:
            return True

    if hour in funding_hours and minute == 0 and second <= 30:
        return True

    return False


# In-memory DataFrame Storage for 15 symbols
df_map = {}


async def warm_up_data(symbols: list, pipeline: DataPipeline):
    """최초 접속 혹은 재접속 시 이전 데이터를 로드하여 지표 연속성을 확보합니다."""
    global df_map
    since_ts = get_today_0900_kst_timestamp() - (
        100 * 60 * 1000
    )  # 09:00부터지만, 지표들 계산을 위해 100봉 정도 더 여유있게 가져옴

    tasks = []
    for sym in symbols:
        # V15.2 동적 타임프레임 데이터 로드 (최대 1500 가져와서 장기 ATR 계산도 충당)
        tasks.append(
            pipeline.fetch_ohlcv_since(
                sym, timeframe=settings.TIMEFRAME, since=since_ts
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for sym, res in zip(symbols, results):
        if isinstance(res, Exception):
            logger.error(f"[{sym}] 웜업 데이터 로딩 실패: {res}")
            continue

        df_map[sym] = res
        logger.info(
            f"[{sym}] {settings.TIMEFRAME} 캔들 초기 데이터 {len(res)}개 장전 완료."
        )


async def process_closed_kline(
    symbol: str,
    kline: dict,
    pipeline: DataPipeline,
    strategy: StrategyEngine,
    risk: RiskManager,
    execution: ExecutionEngine,
):
    """웹소켓으로 수신된 '마감된(x: True)' 캔들을 기존 df에 병합하고 판단을 내립니다."""
    if symbol not in df_map:
        return

    # 이미 활성 포지션 처리 중이거나 대기 중이면 생략
    if symbol in execution.active_positions or symbol in execution.pending_entries:
        return

    try:
        new_ts = int(kline["t"])
        new_dt = pd.to_datetime(new_ts, unit="ms")

        # 새 캔들 row
        new_row = pd.DataFrame(
            [
                {
                    "datetime": new_dt,
                    "open": float(kline["o"]),
                    "high": float(kline["h"]),
                    "low": float(kline["l"]),
                    "close": float(kline["c"]),
                    "volume": float(kline["v"]),
                }
            ]
        ).set_index("datetime")

        df = df_map[symbol]

        # 캔들 병합 (웹소켓 중복 수신 방어)
        if new_dt in df.index:
            df.loc[new_dt] = new_row.iloc[0]
        else:
            # pd.concat 대신 간단히 loc 추가 (성능 이점 위함)
            df.loc[new_dt] = new_row.iloc[0]

        # 최대 1500개 유지 (당일 1440개 커버)
        df_map[symbol] = df.tail(1500)
        curr_df = df_map[symbol]

        if is_funding_fee_cutoff():
            # 펀딩비 시간대면 캔들 저장만 하고 진입은 하지 않음
            return

        # 1. 지표 연산
        df_ind = pipeline.calculate_vwap_indicators(curr_df.copy())

        # 2. V15.0 전략 엔진 의사결정
        decision = strategy.check_entry(symbol, df_ind)

        if decision["signal"]:
            balance_info = await pipeline.exchange.fetch_balance()
            capital = balance_info.get("total", {}).get("USDT", 0.0)

            if settings.DRY_RUN:
                capital = 1000.0

            if capital < risk.min_order_usdt:
                logger.warning(f"⚠️ 전체 선물 잔고 부족({capital:.2f} USDT). 패스.")
                return

            market_price = decision["market_price"]
            reason = decision["reason"]
            atr_val = decision.get("atr_val", market_price * 0.005)

            # 3. 투입 사이즈 산출 (V15는 고정 자본 10% 사용)
            sizing = risk.calculate_position_size(
                symbol, capital, market_price, atr_val
            )

            if sizing["size"] <= 0:
                return

            qty = sizing["size"]
            side = "buy" if decision["signal"] == "LONG" else "sell"

            logger.info(
                f"[Execute] 🎯 {symbol} 진입 타점 포착! "
                f"{side.upper()} (qty={qty}, price={market_price})"
            )

            # 4. 시장가 즉시 진입 및 Time Exit 타이머 동시 스케줄링
            await execution.place_market_entry_order(
                symbol=symbol,
                side=side,
                amount=qty,
                reason=reason,
                tp_dist=sizing["tp_dist"],
                sl_dist=sizing["sl_dist"],
            )

    except Exception as e:
        logger.error(f"[{symbol}] KLINE 마감 처리 중 에러: {e}")


async def websocket_loop(
    pipeline: DataPipeline,
    strategy: StrategyEngine,
    risk: RiskManager,
    execution: ExecutionEngine,
):
    """
    [V15.2] Aiohttp를 활용한 동적 타임프레임(15종목) 무지연 이벤트 루프
    """
    base_symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT"]

    # 볼륨 최상위 13개 알트코인 동적 추출
    alts = await pipeline.fetch_top_altcoins_by_volume(
        limit=13, exclude_symbols=base_symbols
    )
    target_symbols = base_symbols + alts

    logger.info(f"📡 [V15.0] 포트폴리오 15종목 동적 선정 결과: {target_symbols}")

    # 웜업 (당일 캔들 누적)
    await warm_up_data(target_symbols, pipeline)

    # CCXT 심볼 포맷('BTC/USDT:USDT') <-> 바이낸스 소켓 포맷('btcusdt') 상호 변환기
    ccxt_to_binance = {
        sym: sym.split("/")[0].lower() + "usdt" for sym in target_symbols
    }
    binance_to_ccxt = {v: k for k, v in ccxt_to_binance.items()}

    # 바이낸스 Streams 생성
    tf = getattr(settings, "TIMEFRAME", "3m")
    streams = [f"{v}@kline_{tf}" for v in ccxt_to_binance.values()]
    ws_url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)

    while True:
        try:
            logger.info(f"⚡ 무지연 WebSocket 스트림({tf} 15종목) 접속 시도 중...")
            async with aiohttp.ClientSession() as session:
                # Binance 푸시핑에 응답하기 위한 heartbeat
                async with session.ws_connect(ws_url, heartbeat=20.0) as ws:
                    logger.info("🟢 웹소켓 연결 완료! 실시간 트레이딩 봇 가동 시작.")

                    async for msg in ws:
                        if getattr(settings, "IS_PAUSED", False):
                            continue

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if "data" in data and "k" in data["data"]:
                                # 캔들 페이로드 파싱
                                kline = data["data"]["k"]
                                is_closed = kline["x"]  # 1분봉 캔들 마감 여부

                                # 마감캔들에 대해서만 후행성 제거 및 확정 스캔을 수행합니다
                                if is_closed:
                                    binance_sym = kline["s"].lower()
                                    ccxt_sym = binance_to_ccxt.get(binance_sym)
                                    if ccxt_sym:
                                        # 블로킹 방지를 위한 독립 태스크(Task) 스핀업
                                        asyncio.create_task(
                                            process_closed_kline(
                                                ccxt_sym,
                                                kline,
                                                pipeline,
                                                strategy,
                                                risk,
                                                execution,
                                            )
                                        )

                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            logger.error(
                                f"웹소켓 끊어짐 (Code: {ws.close_code}). 5초 후 재시도..."
                            )
                            break

        except Exception as e:
            logger.error(f"웹소켓 루프 최상위 오류: {e}. 5초 후 재접속 시도...")
            await asyncio.sleep(5)
            # 재접속 시 중간 데이터 공백(Gap)을 메꾸기 위해 웜업을 재수행
            await warm_up_data(target_symbols, pipeline)


async def state_machine_loop(execution: ExecutionEngine):
    """
    지정가 대기 취소/체결 판별 및 TP/SL 포워딩을 수행하는 별도의 폴링 루프
    """
    while True:
        try:
            await execution.check_pending_orders_state()
            await execution.check_active_positions_state()
            await execution.check_state_mismatch()
            await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"[State Machine Error]: {e}")
            await asyncio.sleep(5)


async def main():
    logger.info("============== BINANCE V15.0 HFT AUTO TRADER START ==============")

    is_db_connected = await check_db_connection()
    if not is_db_connected:
        logger.error(
            "🛑 Supabase 접속 불가 또는 테이블 오류 발생으로 구동을 강제 종료합니다."
        )
        return

    await notifier.send_message(
        f"🚀 [시작] 바이낸스 V15.2 {settings.TIMEFRAME} 스캘핑 봇 웹소켓 대기열 접속 중..."
    )

    pipeline = DataPipeline()
    strategy = StrategyEngine()
    risk = RiskManager(pipeline)
    execution = ExecutionEngine(pipeline)

    try:
        await execution.sync_state_from_exchange()

        app = setup_telegram_bot(execution)
        if app:
            await app.initialize()
            await app.start()
            await app.updater.start_polling()

        # [V15.2] 메인 웹소켓 루프와 스테이트 머신 병렬 가동
        # return_exceptions=True: 하나의 태스크 예외가 전체 봇을 종료하지 않도록 보호
        async def guarded(coro, name):
            try:
                await coro
            except Exception as e:
                logger.error(f"[{name}] 태스크 비정상 종료: {e}")
                # 크리티컬 태스크 종료 시 전체 봇을 비정상 종료코드로 내려서 watchdog이 재시작하도록 유도
                raise

        task_state = asyncio.create_task(
            guarded(state_machine_loop(execution), "StateMachine")
        )
        task_trade = asyncio.create_task(
            guarded(
                websocket_loop(pipeline, strategy, risk, execution), "WebSocketLoop"
            )
        )

        results = await asyncio.gather(task_state, task_trade, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.critical(f"[Main] 핵심 태스크 예외로 인해 봇이 종료됩니다: {r}")

    except KeyboardInterrupt:
        logger.warning("CTRL+C(키보드 인터럽트)로 시스템이 정지되었습니다.")
    finally:
        if "app" in locals() and app:
            logger.info("텔레그램 인터랙티브 커맨더를 안전하게 종료합니다...")
            try:
                if app.updater and app.updater.running:
                    await app.updater.stop()
            except Exception as e:
                logger.warning(f"Telegram Updater 종료 중 예외 발생: {e}")

            try:
                await app.stop()
                await app.shutdown()
            except Exception as e:
                logger.warning(f"Telegram App 종료 중 예외 발생: {e}")

        try:
            await pipeline.close()
        except Exception as e:
            logger.warning(f"거래소 연결 종료 중 예외 발생: {e}")

        logger.info("거래소 API 객체 릴리즈 및 시스템 종료 절차 통과 완료.")


if __name__ == "__main__":
    import sys

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
