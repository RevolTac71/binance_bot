import asyncio
import time
import json
import aiohttp
from datetime import datetime, timezone, timedelta
import pandas as pd
from config import logger, settings
from database import check_db_connection, Trade, AsyncSessionLocal
from data_pipeline import DataPipeline
from strategy import StrategyEngine, PortfolioState
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


# ── In-memory DataFrame Storage ─────────────────────────────────────────────
# 3분봉 데이터 (15종목 × 1500개)
df_map: dict[str, pd.DataFrame] = {}

# [V16 MTF] 상위 타임프레임 데이터 (종목별 1H / 15m)
htf_df_1h: dict[str, pd.DataFrame] = {}
htf_df_15m: dict[str, pd.DataFrame] = {}

# [V16] 포트폴리오 전역 상태 (단일 인스턴스 공유)
portfolio = PortfolioState()

# [V16.1] CVD 실시간 틱 누적 공간
cvd_data: dict[str, float] = {}
# 캔들 마감 시점의 CVD 스냅샷 저장 (추세 판단용)
cvd_history: dict[str, list] = {}


async def warm_up_data(symbols: list, pipeline: DataPipeline):
    """
    최초 접속 혹은 재접속 시 이전 데이터를 로드하여 지표 연속성을 확보합니다.
    [V16] 3분봉에 더해 1H / 15m 상위 타임프레임 데이터도 함께 웜업합니다.
    """
    global df_map, htf_df_1h, htf_df_15m

    since_ts = get_today_0900_kst_timestamp() - (
        100 * 60 * 1000
    )  # 09:00부터지만, 지표들 계산을 위해 100봉 정도 더 여유있게 가져옴

    # 3분봉 로드 태스크
    tasks_3m = [
        pipeline.fetch_ohlcv_since(sym, timeframe=settings.TIMEFRAME, since=since_ts)
        for sym in symbols
    ]

    # [V16 MTF] 1H·15m 로드 태스크 (동시 병렬 처리)
    tasks_1h = [
        pipeline.fetch_ohlcv_htf(sym, timeframe=settings.HTF_TIMEFRAME_1H, limit=300)
        for sym in symbols
    ]
    tasks_15m = [
        pipeline.fetch_ohlcv_htf(sym, timeframe=settings.HTF_TIMEFRAME_15M, limit=200)
        for sym in symbols
    ]

    results_3m, results_1h, results_15m = await asyncio.gather(
        asyncio.gather(*tasks_3m, return_exceptions=True),
        asyncio.gather(*tasks_1h, return_exceptions=True),
        asyncio.gather(*tasks_15m, return_exceptions=True),
    )

    for sym, res in zip(symbols, results_3m):
        if isinstance(res, Exception):
            logger.error(f"[{sym}] 웜업 3m 데이터 로딩 실패: {res}")
            continue
        df_map[sym] = res
        logger.info(
            f"[{sym}] {settings.TIMEFRAME} 캔들 초기 데이터 {len(res)}개 장전 완료."
        )

    for sym, res_1h, res_15m in zip(symbols, results_1h, results_15m):
        # 1H 데이터 + 지표 연산
        if isinstance(res_1h, Exception):
            logger.warning(f"[{sym}] 웜업 1H 데이터 로딩 실패: {res_1h}")
            htf_df_1h[sym] = None
        else:
            htf_df_1h[sym] = res_1h

        # 15m 데이터
        if isinstance(res_15m, Exception):
            logger.warning(f"[{sym}] 웜업 15m 데이터 로딩 실패: {res_15m}")
            htf_df_15m[sym] = None
        else:
            htf_df_15m[sym] = res_15m

        # 두 프레임 모두 있을 때 지표 연산
        if htf_df_1h.get(sym) is not None and htf_df_15m.get(sym) is not None:
            htf_df_1h[sym], htf_df_15m[sym] = pipeline.calculate_htf_indicators(
                htf_df_1h[sym], htf_df_15m[sym]
            )
            logger.info(f"[{sym}] HTF(1H/15m) 지표 웜업 완료.")


async def process_closed_kline(
    symbol: str,
    kline: dict,
    pipeline: DataPipeline,
    strategy: StrategyEngine,
    risk: RiskManager,
    execution: ExecutionEngine,
):
    """
    웹소켓으로 수신된 '마감된(x: True)' 캔들을 기존 df에 병합하고 판단을 내립니다.
    [V16] HTF 데이터(df_1h, df_15m)와 PortfolioState를 strategy에 함께 전달합니다.
    """
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
            df.loc[new_dt] = new_row.iloc[0]

        # 최대 1500개 유지 (당일 1440개 커버)
        df_map[symbol] = df.tail(1500)
        curr_df = df_map[symbol]

        if is_funding_fee_cutoff():
            # 펀딩비 시간대면 캔들 저장만 하고 진입은 하지 않음
            return

        # 1. 3분봉 지표 연산
        df_ind = pipeline.calculate_vwap_indicators(curr_df.copy())

        # [V16 MTF] 상위 타임프레임 데이터 참조 (htf_refresh_loop가 주기적으로 갱신)
        df_1h = htf_df_1h.get(symbol)
        df_15m = htf_df_15m.get(symbol)

        # [V16.1 CVD] 실시간 누적 거래량 델타 추세 연산
        current_cvd = cvd_data.get(symbol, 0.0)
        hist = cvd_history.setdefault(symbol, [])
        hist.append(current_cvd)
        if len(hist) > 10:
            hist.pop(0)

        cvd_trend = None
        if len(hist) >= 2:
            # 방금 캔들의 CVD가 직전 캔들의 CVD보다 높으면 매수 우위, 낮으면 매도 우위
            if hist[-1] > hist[-2]:
                cvd_trend = "BUY_PRESSURE"
            elif hist[-1] < hist[-2]:
                cvd_trend = "SELL_PRESSURE"

        # 2. V16 전략 엔진 의사결정 (HTF + CVD + Portfolio 통합 필터)
        decision = strategy.check_entry(
            symbol=symbol,
            df=df_ind,
            portfolio=portfolio,
            df_1h=df_1h,
            df_15m=df_15m,
            cvd_trend=cvd_trend,
        )

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

            # 3. 투입 사이즈 산출
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

            # 4. 시장가 즉시 진입
            await execution.place_market_entry_order(
                symbol=symbol,
                side=side,
                amount=qty,
                reason=reason,
                tp_dist=sizing["tp_dist"],
                sl_dist=sizing["sl_dist"],
            )

            # [V16] 포트폴리오 상태에 포지션 등록 (Chandelier 추적 시작)
            portfolio.register_position(
                symbol=symbol,
                direction=decision["signal"],
                entry_price=market_price,
                atr=atr_val,
            )

    except Exception as e:
        logger.error(f"[{symbol}] KLINE 마감 처리 중 에러: {e}")


async def htf_refresh_loop(symbols: list, pipeline: DataPipeline):
    """
    [V16 MTF] 15분마다 1H·15m 상위 타임프레임 데이터를 갱신하는 독립 루프.
    WebSocket 루프와 별도로 asyncio.create_task()로 병렬 가동됩니다.

    갱신 주기: 15분 (15m 봉 마감 주기와 동일하게 설정)
    실패 시:   경고 로그만 남기고 계속 실행 (봇 전체 다운 방지)
    """
    global htf_df_1h, htf_df_15m

    while True:
        # 15분 대기 후 갱신 (첫 실행은 warm_up에서 이미 로드되었으므로 대기 먼저)
        await asyncio.sleep(15 * 60)

        logger.info("[HTF Refresh] 상위 타임프레임 데이터 갱신 시작...")
        tasks_1h = [
            pipeline.fetch_ohlcv_htf(
                sym, timeframe=settings.HTF_TIMEFRAME_1H, limit=300
            )
            for sym in symbols
        ]
        tasks_15m = [
            pipeline.fetch_ohlcv_htf(
                sym, timeframe=settings.HTF_TIMEFRAME_15M, limit=200
            )
            for sym in symbols
        ]

        results_1h, results_15m = await asyncio.gather(
            asyncio.gather(*tasks_1h, return_exceptions=True),
            asyncio.gather(*tasks_15m, return_exceptions=True),
        )

        updated_count = 0
        for sym, res_1h, res_15m in zip(symbols, results_1h, results_15m):
            if isinstance(res_1h, Exception):
                logger.warning(f"[HTF Refresh] {sym} 1H 갱신 실패: {res_1h}")
                continue
            if isinstance(res_15m, Exception):
                logger.warning(f"[HTF Refresh] {sym} 15m 갱신 실패: {res_15m}")
                continue

            htf_df_1h[sym], htf_df_15m[sym] = pipeline.calculate_htf_indicators(
                res_1h, res_15m
            )
            updated_count += 1

        logger.info(f"[HTF Refresh] {updated_count}/{len(symbols)}종목 HTF 갱신 완료.")


async def chandelier_monitoring_loop(
    strategy: StrategyEngine, execution: ExecutionEngine, pipeline: DataPipeline
):
    """
    [V16 Chandelier] 매 캔들 주기(~30초)마다 활성 포지션의 샹들리에 손절선을 점검합니다.
    손절선 돌파 시 시장가 청산 요청을 트리거합니다.

    동작 방식:
        - PortfolioState에 등록된 포지션 순회
        - strategy.check_chandelier_exit() 호출 → 돌파 여부 판단
        - 돌파 시 execution.close_position_market() 호출 (봇 내부 기준 시장가 청산)
    참고:
        거래소에 기 발주된 SL 주문과 병행 운용됩니다.
        Chandelier Exit은 '봇 감시 전용 추가 안전망'으로 작동하며,
        거래소 SL이 먼저 체결되면 portfolio 상태가 sync되어 중복 청산을 방지합니다.
    """
    while True:
        await asyncio.sleep(30)  # 30초 주기 점검

        # 포트폴리오에 등록된 심볼 목록 복사 (순회 중 dict 변경 방지)
        tracked_symbols = list(portfolio.positions.keys())

        for symbol in tracked_symbols:
            pos = portfolio.positions.get(symbol)
            if pos is None:
                continue

            # 현재 시세를 df_map에서 참조 (API 호출 없이 인메모리 활용)
            df = df_map.get(symbol)
            if df is None or len(df) == 0:
                continue

            last_bar = df.iloc[-1]
            curr_price = float(last_bar["close"])
            curr_high = float(last_bar["high"])
            curr_low = float(last_bar["low"])
            curr_atr = last_bar.get("ATR_14", curr_price * 0.005)

            if pd.isna(curr_atr):
                continue

            # 샹들리에 손절선 갱신 + 돌파 여부 확인
            ce_result = strategy.check_chandelier_exit(
                symbol=symbol,
                portfolio=portfolio,
                current_price=curr_price,
                current_high=curr_high,
                current_low=curr_low,
                current_atr=float(curr_atr),
            )

            if ce_result["exit"]:
                logger.warning(
                    f"[Chandelier Exit] 🚨 {symbol} 청산 트리거! "
                    f"현재가={curr_price:.4f}, 손절선={ce_result['chandelier_stop']:.4f}"
                )
                # 포지션 방향에 따라 청산 주문 발송
                direction = pos["direction"]
                close_side = "sell" if direction == "LONG" else "buy"

                try:
                    if symbol in execution.active_positions:
                        stop_price = ce_result["chandelier_stop"]
                        logger.warning(
                            f"[Chandelier Exit] {symbol} 시장가 강제 청산 시도 | "
                            f"사이드={close_side}, 손절선={stop_price:.4f}"
                        )

                        if not settings.DRY_RUN:
                            # 1. 잔여 TP/SL 주문 일괄 취소
                            try:
                                await execution.exchange.cancel_all_orders(symbol)
                            except Exception as cancel_err:
                                logger.warning(
                                    f"[Chandelier Exit] {symbol} 잔여 주문 취소 실패(무시): {cancel_err}"
                                )

                            # 2. reduce-only 시장가 청산 주문 발송
                            pos_info = execution.active_positions.get(symbol, {})
                            # 수량은 execution 내부에서 관리되지 않으므로
                            # 거래소에서 직접 조회하여 처리
                            positions = await execution.exchange.fetch_positions(
                                [symbol]
                            )
                            close_amount = 0.0
                            for p in positions:
                                if (
                                    p["symbol"] == symbol
                                    and float(p.get("contracts", 0)) > 0
                                ):
                                    close_amount = float(p["contracts"])
                                    break

                            if close_amount > 0:
                                await execution.exchange.create_order(
                                    symbol=symbol,
                                    type="market",
                                    side=close_side,
                                    amount=close_amount,
                                    params={"reduceOnly": True},
                                )
                            else:
                                logger.warning(
                                    f"[Chandelier Exit] {symbol} 청산 수량 조회 실패. 스킵."
                                )
                        else:
                            logger.info(
                                f"🧪 [DRY RUN] {symbol} Chandelier Exit 가상 시장가 청산."
                            )

                        await notifier.send_message(
                            f"🔴 <b>Chandelier Exit 발동</b>\n"
                            f"[{symbol}] 현재가={current_price:.4f} | 손절선={ce_result['chandelier_stop']:.4f}\n"
                            f"트레일링 스탑 돌파로 시장가 청산 완료."
                        )

                        # execution 내부 상태에서도 제거
                        if symbol in execution.active_positions:
                            del execution.active_positions[symbol]

                    # 포트폴리오 상태에서도 제거
                    portfolio.close_position(symbol)

                except Exception as e:
                    logger.error(f"[Chandelier Exit] {symbol} 청산 중 에러: {e}")


async def websocket_loop(
    pipeline: DataPipeline,
    strategy: StrategyEngine,
    risk: RiskManager,
    execution: ExecutionEngine,
):
    """
    [V16] Aiohttp를 활용한 동적 타임프레임(15종목) 무지연 이벤트 루프
    """
    base_symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT"]

    # 볼륨 최상위 13개 알트코인 동적 추출
    alts = await pipeline.fetch_top_altcoins_by_volume(
        limit=13, exclude_symbols=base_symbols
    )
    target_symbols = base_symbols + alts

    logger.info(f"📡 [V16] 포트폴리오 15종목 동적 선정 결과: {target_symbols}")

    # 웜업 (3m 당일 캔들 + 1H/15m HTF 캔들 동시)
    await warm_up_data(target_symbols, pipeline)

    # CCXT 심볼 포맷('BTC/USDT:USDT') <-> 바이낸스 소켓 포맷('btcusdt') 상호 변환기
    ccxt_to_binance = {
        sym: sym.split("/")[0].lower() + "usdt" for sym in target_symbols
    }
    binance_to_ccxt = {v: k for k, v in ccxt_to_binance.items()}

    # 바이낸스 Streams 생성
    tf = getattr(settings, "TIMEFRAME", "3m")
    # 1. 캔들 스트림
    streams = [f"{v}@kline_{tf}" for v in ccxt_to_binance.values()]
    # 2. 실시간 체결(CVD) 스트림 추가
    agg_streams = [f"{v}@aggTrade" for v in ccxt_to_binance.values()]
    streams.extend(agg_streams)

    ws_url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)

    # [V16] HTF 주기 갱신 루프를 독립 태스크로 가동
    asyncio.create_task(htf_refresh_loop(target_symbols, pipeline))
    logger.info("[V16] HTF 15분 갱신 루프 태스크 가동.")

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
                            stream_name = data.get("stream", "")

                            # [V16.1] CVD 실시간 틱 처리 (@aggTrade)
                            if "@aggTrade" in stream_name:
                                trade = data["data"]
                                binance_sym = trade["s"].lower()
                                ccxt_sym = binance_to_ccxt.get(binance_sym)
                                if ccxt_sym:
                                    is_maker = trade["m"]
                                    qty = float(trade["q"])
                                    # m=True(메이커가 매도자=시장가 매수) -> 음수 누적? 아니오
                                    # 바이낸스에서 m=True는 Maker가 Buyer측(매도자가 시장가로 긁음)을 의미하므로 Sell Pressure (Delta < 0)
                                    # m=False는 Maker가 Seller측(매수자가 시장가로 긁음)을 의미하므로 Buy Pressure (Delta > 0)
                                    delta = -qty if is_maker else qty
                                    cvd_data[ccxt_sym] = (
                                        cvd_data.get(ccxt_sym, 0.0) + delta
                                    )

                            # 기존 캔들 처리 (@kline)
                            elif "data" in data and "k" in data["data"]:
                                # 캔들 페이로드 파싱
                                kline = data["data"]["k"]
                                is_closed = kline["x"]  # 캔들 마감 여부

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
    [V16] PortfolioState 동기화: execution에서 포지션이 청산되면 portfolio에서도 제거
    """
    while True:
        try:
            await execution.check_pending_orders_state()
            await execution.check_active_positions_state()
            await execution.check_state_mismatch()

            # [V16] execution과 portfolio 상태 동기화
            # execution.active_positions에 없는 심볼이 portfolio에 남아 있으면 제거
            for sym in list(portfolio.positions.keys()):
                if sym not in execution.active_positions:
                    logger.info(
                        f"[State Sync] {sym}이 execution에서 청산됨 → portfolio에서 제거."
                    )
                    portfolio.close_position(sym)

            await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"[State Machine Error]: {e}")
            await asyncio.sleep(5)


async def main():
    logger.info("============== BINANCE V16 MTF SCALPING BOT START ==============")

    is_db_connected = await check_db_connection()
    if not is_db_connected:
        logger.error(
            "🛑 Supabase 접속 불가 또는 테이블 오류 발생으로 구동을 강제 종료합니다."
        )
        return

    await notifier.send_message(
        f"🚀 [시작] 바이낸스 V16 MTF {settings.TIMEFRAME} 스캘핑 봇 웹소켓 대기열 접속 중..."
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

        # [V16] 메인 웹소켓 루프 / 스테이트 머신 / 샹들리에 모니터링 병렬 가동
        async def guarded(coro, name):
            try:
                await coro
            except Exception as e:
                logger.error(f"[{name}] 태스크 비정상 종료: {e}")
                raise

        task_state = asyncio.create_task(
            guarded(state_machine_loop(execution), "StateMachine")
        )
        task_trade = asyncio.create_task(
            guarded(
                websocket_loop(pipeline, strategy, risk, execution), "WebSocketLoop"
            )
        )
        task_chandelier = asyncio.create_task(
            guarded(
                chandelier_monitoring_loop(strategy, execution, pipeline),
                "ChandelierMonitor",
            )
        )

        results = await asyncio.gather(
            task_state, task_trade, task_chandelier, return_exceptions=True
        )
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
