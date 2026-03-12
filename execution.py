import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from config import settings, logger
from database import TradeLog, AsyncSessionLocal
from sqlalchemy import select
from data_pipeline import DataPipeline
from notification import notifier
from strategy import PortfolioState
from utils import clean_json_data


class ExecutionEngine:
    def __init__(self, data_pipeline: DataPipeline, portfolio: PortfolioState):
        self.exchange = data_pipeline.exchange
        self.portfolio = portfolio
        self.strategy = data_pipeline  # strategy 모듈의 퍼센트를 참조하기 위한 포인터용 설계 대비 (임시)
        # 시스템 문제 검출(DB-서버 간 Mismatch 등) 시 자가 정지 처리를 위한 Flag
        self.is_halted = False

        # 대기 중인 진입 지정가 주문 추적. 구조:
        # { "SOL/USDT:USDT": {
        #     "order_id": "12345",
        #     "signal": "LONG",
        #     "limit_price": 150.0,
        #     "tp_price": 151.5,
        #     "sl_price": 149.25,
        #     "amount": 0.5,
        #     "status": "open" # 'open', 'closed', 'canceled'
        # }}
        self.pending_entries: dict = {}

        # 활성 상태인 포지션 메모리 (중복 진입 차단용)
        self.active_positions: dict = {}

        # 종목별 손실 후 쿨다운 만료 시각 저장 (연속 손실 방지)
        # { "SOL/USDT:USDT": datetime.utcnow() + cooldown_minutes }
        self.loss_cooldown: dict = {}

        # 봇 재시작 없이 특정 심볼을 무시하는 블랙리스트
        self.blacklist: set = set()

    def _snapshot_params(self) -> str:
        """
        거래 시점의 전략 파라미터를 JSON 문자열로 직렬화하여 반환합니다.
        나중에 백테스트 / ML 모델링 시 각 거래의 환경 변수를 재현하는 데 활용됩니다.
        """
        return json.dumps(
            {
                # 기본 전략 파라미터
                "strategy_version": getattr(settings, "STRATEGY_VERSION", "UNKNOWN"),
                "timeframe": getattr(settings, "TIMEFRAME", "3m"),
                "risk_percentage": getattr(settings, "RISK_PERCENTAGE", None),
                "leverage": getattr(settings, "LEVERAGE", None),
                # ATR 및 변동성 파라미터
                "atr_ratio_mult": getattr(settings, "ATR_RATIO_MULT", None),
                "atr_long_len": getattr(settings, "ATR_LONG_LEN", None),
                # 손익 관리 파라미터
                "sl_mult": getattr(settings, "SL_MULT", None),
                "tp_mult": getattr(settings, "TP_MULT", None),
                "chandelier_mult": getattr(settings, "CHANDELIER_MULT", None),
                "chandelier_atr_len": getattr(settings, "CHANDELIER_ATR_LEN", None),
                "breakeven_trigger_mult": getattr(
                    settings, "BREAKEVEN_TRIGGER_MULT", None
                ),
                "breakeven_profit_mult": getattr(
                    settings, "BREAKEVEN_PROFIT_MULT", None
                ),
                # V18 스코어링 엔진 파라미터
                "macd_filter_enabled": getattr(settings, "MACD_FILTER_ENABLED", True),
                "min_score_long": getattr(settings, "MIN_SCORE_LONG", 18),
                "min_score_short": getattr(settings, "MIN_SCORE_SHORT", 17),
                "pctl_window": getattr(settings, "PCTL_WINDOW", None),
                "adx_boost_pctl": getattr(settings, "ADX_BOOST_PCTL", None),
                "scoring_thresholds": getattr(settings, "SCORING_THRESHOLDS", {}),
                # V18.5 방향별/익손절별 세분화 모드
                "long_tp_mode": getattr(settings, "LONG_TP_MODE", "ATR"),
                "long_sl_mode": getattr(settings, "LONG_SL_MODE", "ATR"),
                "short_tp_mode": getattr(settings, "SHORT_TP_MODE", "PERCENT"),
                "short_sl_mode": getattr(settings, "SHORT_SL_MODE", "ATR"),
                "long_tp_mult": getattr(settings, "LONG_TP_MULT", 5.0),
                "long_sl_mult": getattr(settings, "LONG_SL_MULT", 1.5),
                "short_tp_mult": getattr(settings, "SHORT_TP_MULT", 5.0),
                "short_sl_mult": getattr(settings, "SHORT_SL_MULT", 1.5),
                "long_tp_pct": getattr(settings, "LONG_TP_PCT", 0.05),
                "long_sl_pct": getattr(settings, "LONG_SL_PCT", 0.02),
                "short_tp_pct": getattr(settings, "SHORT_TP_PCT", 0.03),
                "short_sl_pct": getattr(settings, "SHORT_SL_PCT", 0.015),
                "fee_rate": getattr(settings, "FEE_RATE", 0.00045),
                # 포트폴리오 관리 파라미터
                "max_concurrent_same_dir": getattr(
                    settings, "MAX_CONCURRENT_SAME_DIR", None
                ),
                "max_trades": getattr(settings, "MAX_TRADES", None),
                "loss_cooldown_minutes": getattr(
                    settings, "LOSS_COOLDOWN_MINUTES", None
                ),
                # 체결 관리 파라미터
                "kelly_sizing": getattr(settings, "KELLY_SIZING", False),
                "kelly_min_trades": getattr(settings, "KELLY_MIN_TRADES", None),
                "kelly_max_fraction": getattr(settings, "KELLY_MAX_FRACTION", None),
                "chasing_wait_sec": getattr(settings, "CHASING_WAIT_SEC", None),
                "partial_tp_ratio": getattr(settings, "PARTIAL_TP_RATIO", None),
                # MTF 필터 파라미터
                "htf_timeframe_1h": getattr(settings, "HTF_TIMEFRAME_1H", None),
                "htf_timeframe_15m": getattr(settings, "HTF_TIMEFRAME_15M", None),
                # [V19] 상세 스코어링 규칙 정적 스냅샷 (최적화 분석용)
                "scoring_rules": {
                    "long": getattr(settings, "SC_RULES_LONG", {}),
                    "short": getattr(settings, "SC_RULES_SHORT", {}),
                    "weights": getattr(settings, "SCORING_WEIGHTS", {}),
                },
            },
            ensure_ascii=False,
        )

    async def sync_state_from_exchange(self):
        """
        봇 재시작 시, 거래소의 실제 상태(포지션, 미체결 주문)를 읽어와 내부 상태를 복구합니다.
        프로그램 종료/장애 발생 후 재기동 시 포지션을 이어받기 위해 반드시 필요한 절차입니다.
        """
        if settings.DRY_RUN:
            logger.info(
                "🧪 [DRY RUN] 가상 실행 중이므로 거래소 초기 동기화를 생략합니다."
            )
            # [V18.1] 가상 포지션 복구 로직으로 즉시 이동
            await self._recover_dry_run_positions()
            return

        try:
            logger.info("🔄 거래소 서버와 기존 상태 동기화 중...")

            # 1. 활성 포지션 복구
            positions = await self.exchange.fetch_positions()
            active_count = 0

            async with AsyncSessionLocal() as session:
                for p in positions:
                    symbol = p.get("symbol")
                    contracts = float(p.get("contracts", 0.0))
                    if contracts > 0:
                        # [V18.4] DB에서 해당 종목의 마지막 활성(exit_time IS NULL) 로그 조회
                        stmt = (
                            select(TradeLog)
                            .where(
                                TradeLog.symbol == symbol, TradeLog.exit_time == None
                            )
                            .order_by(TradeLog.entry_time.desc())
                            .limit(1)
                        )
                        res = await session.execute(stmt)
                        log = res.scalars().first()

                        entry_time = (
                            log.entry_time
                            if log
                            else (datetime.now(timezone.utc) + timedelta(hours=9))
                        )
                        last_ts = (
                            int(log.last_pnl_at.timestamp() * 1000)
                            if (log and log.last_pnl_at)
                            else int(time.time() * 1000)
                        )

                        self.active_positions[symbol] = {
                            "signal": "LONG" if p.get("side") == "long" else "SHORT",
                            "amount": contracts,
                            "entry_time": entry_time,
                            "last_summed_ts": last_ts,
                            "is_partial_tp_done": False,  # 필요시 DB 컬럼 추가 가능하나 현재는 봇 판단에 맡김
                            "tp_price": log.tp_price if log else 0.0,
                            "sl_price": log.sl_price if log else 0.0,
                        }
                        active_count += 1

                        # [V18.4] 밀리초 Timestamp 대신 읽기 쉬운 시간과 누적 PnL 표시
                        readable_time = datetime.fromtimestamp(last_ts / 1000).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        last_pnl = log.realized_pnl if log else 0.0
                        logger.info(
                            f"✅ [REAL 복구] {symbol} 메타데이터 복원 완료 (진입: {entry_time}, 누적PnL: {last_pnl:.2f} USDT, 기준시막: {readable_time})"
                        )
        except Exception as e:
            error_msg = str(e).lower()
            if "invalid api-key" in error_msg or "-2015" in error_msg:
                logger.error("❌ [API ERROR] 바이낸스 API 키가 유효하지 않거나 권한이 없습니다.")
                logger.error("👉 확인 사항: 1. API 키/시크릿 오타, 2. Futures 권한 활성화, 3. IP 화이트리스트 설정")
            else:
                logger.error(f"거래소 동기화 중(sync_state_from_exchange) 예외 발생: {e}")

        # [V18.5] 15분 손실 쿨다운 복구 로직 추가 (영속화)

        if not settings.DRY_RUN:
            try:
                # 2. 고립된 진입 대기 주문(Pending Entries) 정리
                # 안전을 위해 봇 재시작 시 포지션이 없는 종목의 미체결 주문은 모두 취소합니다.

                logger.info(
                    "내 계좌의 전체 대기 주문을 스캔하여 고립된 찌꺼기 주문을 정리합니다..."
                )
                canceled_count = 0

                # CCXT의 warnOnFetchOpenOrdersWithoutSymbol 옵션을 껐기 때문에 Rate Limit 경고 없이
                # 현재 내 계좌의 모든 Open Order를 한 번의 호출로 매우 빠르게 가져옵니다.
                open_orders = await self.exchange.fetch_open_orders()

                for order in open_orders:
                    symbol = order.get("symbol")
                    order_id = order.get("id")

                    # 진짜 TP/SL 주문인지 식별하기 위해 reduceOnly 속성 확인
                    is_reduce_only = order.get("reduceOnly")
                    if str(is_reduce_only).lower() == "true":
                        is_reduce_only = True
                    elif order.get("info", {}).get("reduceOnly") in [
                        True,
                        "true",
                        "True",
                    ]:
                        is_reduce_only = True
                    else:
                        is_reduce_only = False

                    # 판단 로직:
                    # 1. 이미 활성 포지션이 있고, 해당 주문이 '포지션 축소용(reduceOnly)'이라면 -> 정상적인 TP/SL이므로 살림
                    if symbol in self.active_positions and is_reduce_only:
                        continue

                    # 그 외: 포지션이 없거나, 포지션이 있더라도 reduceOnly가 아닌 '순수 신규 진입' 타점이 그대로 남은 경우 -> 찌꺼기이므로 파쇄
                    await self.exchange.cancel_order(order_id, symbol)
                    canceled_count += 1
                    logger.info(
                        f"🧹 [정리 완료] 찌꺼기 진입 주문 강제 취소 (포지션 유무 무관): {symbol} (Order ID: {order_id})"
                    )

                # 2.2 고립된 Algo 주문 (STOP_MARKET 등) 정리 로직 추가
                # 바이낸스 퓨처스 업데이트로 일반 OpenOrders 통신망과 Algo 통신망이 분리됨.
                algo_orders = await self.exchange.request(
                    path="openAlgoOrders",
                    api="fapiPrivate",
                    method="GET",
                    params={},
                )

                # 반환형이 배열 또는 {'orders': [...]} 인지 확인 후 정리
                algo_items = (
                    algo_orders.get("orders", algo_orders)
                    if isinstance(algo_orders, dict)
                    else algo_orders
                )

                for algo in algo_items:
                    raw_binance_symbol = algo.get("symbol")
                    algo_id = algo.get("algoId")

                    is_reduce_only = algo.get("reduceOnly")
                    if str(is_reduce_only).lower() == "true":
                        is_reduce_only = True
                    else:
                        is_reduce_only = False

                    # Map raw_binance_symbol back to CCXT symbol
                    ccxt_matched_sym = None
                    for ap_sym in self.active_positions.keys():
                        # Market 객체에서 id를 추출하거나 /를 제거해 비교
                        if ap_sym.replace("/", "").split(":")[0] == raw_binance_symbol:
                            ccxt_matched_sym = ap_sym
                            break

                    # 포지션이 있으면서 reduce_only 파라미터가 켜진(조건부 청산) 주문은 살림
                    if ccxt_matched_sym and is_reduce_only:
                        continue

                    # 고립된 Algo 주문 정리
                    await self.exchange.request(
                        path="algoOrder",
                        api="fapiPrivate",
                        method="DELETE",
                        params={"symbol": raw_binance_symbol, "algoId": algo_id},
                    )
                    canceled_count += 1
                    logger.info(
                        f"🧹 [Algo 정리 완료] 고립된 조건부(SL 등) 찌꺼기 알고 주문 취소: {raw_binance_symbol} (Algo ID: {algo_id})"
                    )

                logger.info(
                    f"🔄 동기화 완료: 복구된 포지션 {active_count}개, 정리된 찌꺼기 대기 주문 {canceled_count}개."
                )

            except Exception as e:
                logger.error(
                    f"거래소 동기화 중(sync_state_from_exchange) 예외 발생: {e}"
                )

        # [V18.5] 15분 손실 쿨다운 복구 로직 추가 (영속화)
        try:
            now_kst = datetime.utcnow() + timedelta(hours=9)
            cooldown_min = getattr(settings, "LOSS_COOLDOWN_MINUTES", 15)

            async with AsyncSessionLocal() as session:
                # 최근 쿨다운 기간 이내의 손실 기록(realized_pnl < 0) 조회
                cooldown_limit = now_kst - timedelta(minutes=cooldown_min)
                stmt = (
                    select(TradeLog)
                    .where(
                        TradeLog.realized_pnl < 0, TradeLog.exit_time >= cooldown_limit
                    )
                    .order_by(TradeLog.exit_time.desc())
                )

                res = await session.execute(stmt)
                logs = res.scalars().all()

                # 심볼별 최신 손실 기록 기준으로 쿨다운 복원
                for log in logs:
                    if log.symbol not in self.loss_cooldown:
                        expiry = log.exit_time + timedelta(minutes=cooldown_min)
                        if expiry > now_kst:
                            self.loss_cooldown[log.symbol] = expiry
                            logger.info(
                                f"✅ [Cooldown 복구] {log.symbol} 손실 쿨다운 복원 완료 (만료: {expiry})"
                            )
        except Exception as e:
            logger.error(f"쿨다운 상태 복구 중 에러: {e}")

    async def _recover_dry_run_positions(self):
        """[V18.1] DRY_RUN 가상 포지션 복구 로직 (별도 메서드화)"""
        try:
            async with AsyncSessionLocal() as session:
                stmt = select(TradeLog).where(
                    TradeLog.dry_run == True, TradeLog.exit_time == None
                )
                result = await session.execute(stmt)
                recovered_logs = result.scalars().all()

                for log in recovered_logs:
                    self.active_positions[log.symbol] = {
                        "signal": log.direction,
                        "amount": log.qty,
                        "limit_price": log.execution_price,
                        "tp_price": log.tp_price or 0.0,
                        "sl_price": log.sl_price or 0.0,
                        "entry_time": log.entry_time,
                        "last_summed_ts": int(time.time() * 1000),
                        "is_partial_tp_done": False,
                        "reason": log.entry_reason,
                        "market_data": None,
                    }
                    logger.info(
                        f"✅ [DRY RUN 복구] 기존 가상 포지션 감지 및 복구: {log.symbol} ({log.direction}, 수량: {log.qty})"
                    )
        except Exception as e:
            logger.error(f"DRY RUN 포지션 복구 중 에러: {e}")

    async def setup_margin_and_leverage(self, symbol: str):
        """
        바이낸스 선물에서 해당 코인의 레버리지를 1배로, 마진 모드를 격리(Isolated)로 확실하게 고정합니다.
        """
        # [V18.5] DRY_RUN 모드에서는 실계좌 설정을 건드리지 않고 즉시 반환합니다. (안전 및 에러 방지)
        if settings.DRY_RUN:
            return

        try:
            # 1. 격리 마진(Isolated) 강제 설정
            await self.exchange.set_margin_mode("isolated", symbol)
            logger.info(f"[{symbol}] 마진 모드: 격리(ISOLATED) 강제 설정 완료.")
        except Exception as e:
            error_msg = str(e).lower()
            # 이미 격리로 설정되어 있는 경우 또는 오픈 주문이 있어 변경이 불가능한 경우 (무시)
            if (
                "no need to change margin type" in error_msg
                or "margin type already" in error_msg
                or "-4067" in error_msg
                or "position side cannot be changed" in error_msg
            ):
                pass
            else:
                logger.warning(
                    f"[{symbol}] 마진 모드(ISOLATED) 설정 중 경고 (수동 확인 필요): {e}"
                )

        try:
            # 2. 레버리지 설정 (Config 파일에서 설정한 값으로 적용)
            await self.exchange.set_leverage(settings.LEVERAGE, symbol)
            logger.info(f"[{symbol}] 레버리지: {settings.LEVERAGE}x 설정 완료.")
        except Exception as e:
            logger.warning(f"[{symbol}] 레버리지 설정 중 정보: {e}")

    async def place_chasing_entry_order(
        self,
        symbol: str,
        side: str,  # 'buy' or 'sell'
        amount: float,
        reason: str,
        tp_dist: float = 0.0,
        sl_dist: float = 0.0,
        market_data: dict = None,
    ) -> bool:
        """
        [V18] 시장가(Market) 대신 포스트 온리(Maker) 지정가로 호가를 추격(Chasing)하며 진입합니다.
        3.5초 내 미체결 시 취소하고 최우선 호가로 재생성하여 수수료(Taker Fee)를 절약합니다.
        체결 성공 시, 실제 체결가(average price)를 기반으로 TP/SL 주문을 연이어 등록합니다.
        """
        if self.is_halted:
            logger.warning(
                f"시스템이 일시 중지(Halted) 상태입니다. 신규 진입 요청[{symbol}] 거부."
            )
            return False

        # 블랙리스트 체크
        if symbol in self.blacklist:
            logger.info(
                f"[{symbol}] 블랙리스트(차단)에 등록된 종목이므로 진입을 스킵합니다."
            )
            return False

        # [V18.2] 중복 진입 방어 강화: 이미 Chasing 시도 중이라면 중복 요청 거부
        if symbol in self.pending_entries:
            logger.info(
                f"[{symbol}] 이미 Chasing 진입 시도 중(Pending)입니다. 중복 요청을 무시합니다."
            )
            return False

        # 포지션이 이미 존재하면 추가 진입 억제
        if symbol in self.active_positions:
            logger.info(f"[{symbol}] 이미 활성 포지션이 존재합니다. 진입 생략.")
            return False

        # 포트폴리오 동시 진입 최대 개수(MAX_TRADES) 체크
        max_trades = getattr(settings, "MAX_TRADES", 3)
        if len(self.active_positions) >= max_trades:
            logger.info(
                f"[{symbol}] 전체 활성 포지션 한도 도달 "
                f"(현재 {len(self.active_positions)}/최대 {max_trades}). 연쇄 손실 방지를 위해 진입 생략."
            )
            return False

        # 연속 손실 쿨다운 체크
        now_kst = datetime.utcnow() + timedelta(hours=9)
        cooldown_until = self.loss_cooldown.get(symbol)
        if cooldown_until and now_kst < cooldown_until:
            remaining = int((cooldown_until - now_kst).total_seconds() / 60)
            logger.info(f"[{symbol}] 손실 쿨다운 중. {remaining}분 후 진입 가능. 스킵.")
            return False

        try:
            # [HOTFIX] 진입 시도 시작 시 즉각 pending_entries에 등록하여 수동진입 오탐지 방지
            self.pending_entries[symbol] = {
                "side": side,
                "amount": amount,
                "reason": reason,
                "timestamp": time.time(),
            }

            logger.info(
                f"[{symbol}] 스마트 지정가(Chasing) 진입 시도. "
                f"수량: {amount} (DRY_RUN: {settings.DRY_RUN})"
            )
            start_time_ms = int(time.time() * 1000)

            # 레버리지 및 마진 환경 사전 세팅
            await self.setup_margin_and_leverage(symbol)

            signal_type = "LONG" if side == "buy" else "SHORT"
            average_price = 0.0

            if not settings.DRY_RUN:
                max_retries = getattr(settings, "CHASING_MAX_RETRY", 10)
                market_threshold = getattr(settings, "CHASING_MARKET_THRESHOLD", 3)
                remaining_amount = amount
                total_cost = 0.0
                filled_amount = 0.0

                for attempt in range(max_retries):
                    try:
                        # [V18.2] Hybrid Chasing: 일정 횟수 실패 시 시장가로 전환하여 진입 보장
                        is_market_fallback = attempt >= market_threshold

                        if is_market_fallback:
                            logger.info(
                                f"[{symbol}] Chasing {attempt + 1}/{max_retries}: 지정가 체결 실패로 인한 '시장가' 전환 진입 수행"
                            )
                            # [V18.4] 시장가 전환 시에도 수량 정밀도(BTC 등 0.001 단위) 및 minQty 체크 적용
                            formatted_remaining = self.exchange.amount_to_precision(
                                symbol, remaining_amount
                            )
                            market = self.exchange.market(symbol)
                            min_qty = (
                                market.get("limits", {})
                                .get("amount", {})
                                .get("min", 0.0)
                            )

                            if float(formatted_remaining) < min_qty:
                                logger.warning(
                                    f"[{symbol}] 잔여 수량({formatted_remaining})이 최소 주문 수량({min_qty}) 미달로 시장가 진입을 중단합니다."
                                )
                                break

                            entry_order = await self.exchange.create_order(
                                symbol=symbol,
                                type="market",
                                side=side,
                                amount=float(formatted_remaining),
                            )
                        else:
                            # 1. 호가창(Orderbook) 조회하여 최우선 호가 파악
                            ob = await self.exchange.fetch_order_book(symbol, limit=5)
                            if side == "buy":
                                target_price = float(ob["bids"][0][0])
                            else:
                                target_price = float(ob["asks"][0][0])

                            price_str = self.exchange.price_to_precision(
                                symbol, target_price
                            )
                            amount_str = self.exchange.amount_to_precision(
                                symbol, remaining_amount
                            )

                            if float(amount_str) <= 0:
                                break

                            # 2. Limit Maker (Post-Only) 주문 제출
                            logger.info(
                                f"[{symbol}] Chasing {attempt + 1}/{max_retries}: {target_price}에 Post-Only 지정가 {amount_str}개 제출"
                            )
                            entry_order = await self.exchange.create_order(
                                symbol=symbol,
                                type="limit",
                                side=side,
                                amount=float(amount_str),
                                price=float(price_str),
                                params={"timeInForce": "GTX"},  # Post-Only 강제
                            )
                        order_id = entry_order.get("id")

                        pass

                        # 3. V18
                        chasing_wait = getattr(settings, "CHASING_WAIT_SEC", 5.0)
                        await asyncio.sleep(chasing_wait)

                        # 4. 체결 상태 확인
                        fetched_order = await self.exchange.fetch_order(
                            order_id, symbol
                        )
                        status = fetched_order.get("status")
                        filled = float(fetched_order.get("filled", 0.0))

                        if filled > 0:
                            avg = float(
                                fetched_order.get(
                                    "average", fetched_order.get("price", 0.0)
                                )
                            )
                            if avg > 0:
                                total_cost += filled * avg
                                filled_amount += filled

                        if status == "closed":
                            break
                        elif status in ["open", "canceled", "rejected"]:
                            if status == "open":
                                try:
                                    await self.exchange.cancel_order(order_id, symbol)
                                    pass
                                except Exception as e:
                                    logger.warning(
                                        f"[{symbol}] 주문 취소 중 예외 발생 (이미 체결됨?): {e}"
                                    )

                            # 취소 후 상태 한 번 더 갱신하여 취소 직전 체결분 마저 합산
                            fetched_after = await self.exchange.fetch_order(
                                order_id, symbol
                            )
                            final_filled = float(fetched_after.get("filled", 0.0))

                            newly_filled = final_filled - filled
                            if newly_filled > 0:
                                avg2 = float(
                                    fetched_after.get(
                                        "average", fetched_after.get("price", 0.0)
                                    )
                                )
                                if avg2 > 0:
                                    total_cost += newly_filled * avg2
                                    filled_amount += newly_filled

                            remaining_amount = amount - filled_amount
                            if (
                                remaining_amount <= float(amount_str) * 0.05
                            ):  # 95% 이상 체결되면 종료
                                break
                    except Exception as e:
                        # -5022 Post Only Rejection 등은 Chasing 루프에서 흔히 발생하므로 Warning 처리하여 텔레그램 스팸 방지
                        logger.warning(
                            f"[{symbol}] Chasing 루프 내 에러(재시도됨): {e}"
                        )
                        pass
                        await asyncio.sleep(2)  # 밴 방지

                if filled_amount > 0:
                    average_price = total_cost / filled_amount
                else:
                    average_price = 0.0

            else:
                # DRY RUN 일 경우 현재 시장가(Ticker)를 체결가로 임시 가정
                ticker = await self.exchange.fetch_ticker(symbol)
                average_price = float(ticker.get("last", 0.0))

            if average_price <= 0:
                logger.error(
                    f"[{symbol}] 지정가 Chasing을 완주했으나 단 한 주도 체결되지 않거나 단가를 확인할 수 없습니다! 진입 포기."
                )
                return False

            # 체결 완료 로깅 및 알림
            logger.info(
                f"🎯 [{symbol}] 스마트 메이커(Post-Only) 진입 성공! 평균 단가: {average_price:.4f}. TP/SL 즉각 계산 및 전송 개시."
            )

            # V12: 진입 단가에서 ATR 거리(tp_dist, sl_dist)만큼 가감산
            if signal_type == "LONG":
                raw_tp = average_price + tp_dist
                raw_sl = average_price - sl_dist
            else:
                raw_tp = average_price - tp_dist
                raw_sl = average_price + sl_dist

            # [V18.4] 진입 시점 기록 (Time-Exit 및 복구용)
            now_kst = datetime.now(timezone.utc) + timedelta(hours=9)

            # 호가 단위(precisions) 보정
            tp_price = (
                float(self.exchange.price_to_precision(symbol, raw_tp))
                if self.exchange
                else raw_tp
            )
            sl_price = (
                float(self.exchange.price_to_precision(symbol, raw_sl))
                if self.exchange
                else raw_sl
            )

            # TP/SL 생성 코루틴으로 정보 패스
            # target_price: 처음 진입 시도 당시의 최우선 호가(의도한 기준가)
            # 여기서는 fetch_order_book 호출 직전의 값이 유실되었으므로 (최후호출값만 남으므로),
            # 단순히 average_price를 limit_price로 세팅하거나, 첫 번째 limit_price를 로깅해둬야 하지만, 간단히 유지
            entry_info = {
                "signal": signal_type,
                "amount": amount,
                "limit_price": average_price,
                "tp_price": tp_price,
                "sl_price": sl_price,
                "execution_time_ms": int(time.time() * 1000) - start_time_ms,
                "reason": reason,
                "market_data": market_data,
            }

            # --- [HOTFIX] 진입 직후 봇 추적망에 즉각 편입하여 수동진입 감지(오작동) 방지 ---
            self.active_positions[symbol] = {
                "signal": signal_type,
                "amount": amount,
                "limit_price": average_price,
                "tp_price": tp_price,
                "sl_price": sl_price,
                "entry_time": now_kst,
                "last_summed_ts": int(
                    time.time() * 1000
                ),  # 진입 시점을 첫 합산 기준으로 설정
                "is_partial_tp_done": False,
            }

            # [V18.4] 기존 비동기 데몬 방식 제거 (check_active_positions_state 루프에서 통합 감시)
            # if getattr(settings, "TIME_EXIT_MINUTES", 0) > 0:
            #     asyncio.create_task(...)

            # 동기적(await)으로 TP/SL 즉시 생성 (대기열 통하지 않음)
            success = await self.place_tp_sl_orders(symbol, entry_info)

            return True

        except Exception as e:
            logger.error(
                f"[{symbol}] 시장가 진입 로직 처리 중 예외 발생: {e}",
                exc_info=True,
            )
            # TP/SL 생성 도중 에러가 나더라도 포지션은 체결되어 있으므로 봇 루프에서 관리되도록 True 반환
            return True
        finally:
            # 진입 프로세스(성공/실패/예외 전구간) 종료 후 대기열에서 제거
            if symbol in self.pending_entries:
                self.pending_entries.pop(symbol, None)

    async def cancel_pending_order(
        self, symbol: str, reason: str = "취소 요청"
    ) -> bool:
        """
        신호 해제, 혹은 정산 시간 등 특정 사유로 미체결 지정가 진입 주문을 취소합니다.
        """
        if symbol not in self.pending_entries:
            return False

        order_info = self.pending_entries[symbol]
        order_id = order_info["order_id"]

        try:
            logger.info(
                f"[{symbol}] 미체결 대기 주문 취소. 사유: {reason} (DRY: {settings.DRY_RUN})"
            )

            if not settings.DRY_RUN and order_id != "DRY_RUN_ID":
                await self.exchange.cancel_order(order_id, symbol)

            # DB에 취소 기록 남기기 (히스토리용)
            try:
                async with AsyncSessionLocal() as session:
                    new_log = TradeLog(
                        entry_time=(datetime.utcnow() + timedelta(hours=9)),
                        exit_time=(datetime.utcnow() + timedelta(hours=9)),
                        action="CANCELED",
                        symbol=symbol,
                        direction=order_info.get("signal", "UNKNOWN"),
                        target_price=order_info.get("limit_price", 0.0),
                        execution_price=order_info.get("limit_price", 0.0),
                        slippage=0.0,
                        execution_time_ms=0,
                        qty=order_info.get("amount", 0.0),
                        entry_reason=f"진입 주문 취소: {reason}",
                        realized_pnl=0.0,
                        dry_run=settings.DRY_RUN,
                        params=self._snapshot_params(),
                    )
                    session.add(new_log)
                    await session.commit()
            except Exception as db_err:
                logger.error(f"[{symbol}] 주문 취소 DB 기록 중 에러 (무시됨): {db_err}")

            del self.pending_entries[symbol]
            return True
        except Exception as e:
            logger.error(f"[{symbol}] 지정가 주문 취소 중 에러: {e}")
            if "Unknown order" in str(e):
                # 거래소에서 이미 만료/취소된 경우이므로 메모리에서 지움
                del self.pending_entries[symbol]
                return True
            return False

    async def place_tp_sl_orders(self, symbol: str, entry_info: dict) -> bool:
        """
        체결이 완료된 포지션에 대해 Reduce-Only 파라미터가 포함된 TP/SL 주문을 전송합니다.
        """
        signal_type = entry_info["signal"]
        total_amount = entry_info["amount"]
        tp_price = entry_info["tp_price"]
        sl_price = entry_info["sl_price"]

        # [V18.4] 가격 정밀도 선제 적용 (소수점 오차로 인한 주문 거절 방지)
        tp_price = float(self.exchange.price_to_precision(symbol, tp_price))
        sl_price = float(self.exchange.price_to_precision(symbol, sl_price))

        # DB 업데이트 (정밀도 보정된 가격으로 덮어씀)
        entry_price = entry_info["limit_price"]

        # V18
        partial_ratio = getattr(settings, "PARTIAL_TP_RATIO", 0.5)
        tp_amount = total_amount * partial_ratio
        sl_amount = total_amount  # SL은 전량 (안전망)

        # Long이면 매도(Sell)로 청산, Short이면 매수(Buy)로 청산
        exit_side = "sell" if signal_type == "LONG" else "buy"

        # [V18.4] settings.FEE_RATE 사용 (수치화)
        taker_fee = getattr(settings, "FEE_RATE", 0.00045)
        maker_fee = taker_fee * 0.4  # 일반적인 메이커 비율 (0.018 / 0.045 = 0.4)

        # Pnl = (exit - entry) / entry  * 레버리지(1)
        if signal_type == "LONG":
            tp_pct = (tp_price - entry_price) / entry_price
            sl_pct = (sl_price - entry_price) / entry_price
        else:
            tp_pct = (entry_price - tp_price) / entry_price
            sl_pct = (entry_price - sl_price) / entry_price

        real_tp_pct = tp_pct - maker_fee  # TP는 Limit이므로 Maker 수수료 부담
        real_sl_pct = sl_pct - taker_fee  # SL은 Stop Market이므로 Taker 수수료 부담

        logger.info(
            f"[{symbol}] TP/SL Orders. "
            f"실제 익절률(수수료 차감 후): {real_tp_pct * 100:.2f}%, "
            f"실제 손절률(수수료 차감 후): {real_sl_pct * 100:.2f}% (Taker 수수료 0.045% 포함. R:R={abs(real_tp_pct / real_sl_pct) if real_sl_pct != 0 else 0:.2f})"
        )

        try:
            # DB 기록 (진입) - DRY_RUN 이더라도 테스트 내역을 DB에 기록
            async with AsyncSessionLocal() as session:
                dr_prefix = "[DRY_RUN] " if settings.DRY_RUN else ""
                now_kst = datetime.utcnow() + timedelta(hours=9)

                # market_data와 params를 안전하게 세척 후 기록
                raw_market_data = entry_info.get("market_data")
                cleaned_market_data = clean_json_data(raw_market_data)
                
                # params는 이미 _snapshot_params()에서 json.dumps()된 문자열일 수 있으나
                # 만약 dict라면 세척 후 저장 (TradeLog.params는 String 혹은 JSONB일 수 있음)
                raw_params = self._snapshot_params()
                if isinstance(raw_params, str):
                    try:
                        # 이미 JSON 문자열이면 파싱 후 다시 세척하여 직렬화 (이중 안전장치)
                        params_dict = json.loads(raw_params)
                        cleaned_params = json.dumps(clean_json_data(params_dict), ensure_ascii=False)
                    except:
                        cleaned_params = raw_params
                else:
                    cleaned_params = json.dumps(clean_json_data(raw_params), ensure_ascii=False)

                # [V19] 모든 기록을 TradeLog로 단일화
                new_tradelog = TradeLog(
                    symbol=symbol,
                    direction=signal_type,
                    action=signal_type,  # 'LONG' or 'SHORT'
                    qty=total_amount,
                    entry_time=now_kst,
                    target_price=entry_price,
                    execution_price=entry_price,
                    slippage=0.0,
                    entry_reason=f"{dr_prefix}V18 시장가/추격 진입 완료 (분할TP {partial_ratio * 100:.0f}%)",
                    execution_time_ms=entry_info.get("execution_time_ms", 0),
                    dry_run=settings.DRY_RUN,
                    tp_price=tp_price,
                    sl_price=sl_price,
                    market_data=cleaned_market_data,
                    params=cleaned_params,
                )
                session.add(new_tradelog)
                await session.commit()

            # TP 수량 정밀도 보정 및 최소 수량(minQty) 체크
            try:
                tp_amount_str = self.exchange.amount_to_precision(symbol, tp_amount)
                tp_amount_final = float(tp_amount_str)

                # [V18.4] 최소 수량 미달 시 최소값으로 보정 (BNB 등 소수점/수량 엄격한 종목 대응)
                market = self.exchange.market(symbol)
                min_qty = market.get("limits", {}).get("amount", {}).get("min", 0.0)
                if tp_amount_final > 0 and tp_amount_final < min_qty:
                    tp_amount_final = min_qty
            except Exception:
                tp_amount_final = tp_amount

            if not settings.DRY_RUN:
                tp_order_id = None
                sl_order_id = None

                if tp_amount_final > 0:
                    try:
                        tp_order = await self.exchange.create_order(
                            symbol=symbol,
                            type="limit",
                            side=exit_side,
                            amount=tp_amount_final,
                            price=tp_price,
                            params={"reduceOnly": True},
                        )
                        tp_order_id = tp_order.get("id")
                    except Exception as tp_err:
                        logger.warning(
                            f"[{symbol}] 지정가(Limit) TP 생성 거절됨. 시장가 우회(TAKE_PROFIT_MARKET)로 재시도합니다. 사유: {tp_err}"
                        )
                        try:
                            tp_order = await self.exchange.create_order(
                                symbol=symbol,
                                type="take_profit_market",
                                side=exit_side,
                                amount=tp_amount_final,
                                params={"stopPrice": tp_price, "reduceOnly": True},
                            )
                            tp_order_id = tp_order.get("id")
                        except Exception as tp_algo_err:
                            err_msg = str(tp_algo_err)
                            if "-4120" in err_msg or "Algo Order API" in err_msg:
                                formatted_amount_tp = self.exchange.amount_to_precision(
                                    symbol, tp_amount_final
                                )
                                formatted_price_tp = self.exchange.price_to_precision(
                                    symbol, tp_price
                                )
                                raw_sym = self.exchange.market(symbol)["id"]
                                req_tp = {
                                    "symbol": raw_sym,
                                    "side": exit_side.upper(),
                                    "type": "TAKE_PROFIT_MARKET",
                                    "quantity": formatted_amount_tp,
                                    "stopPrice": formatted_price_tp,
                                    "reduceOnly": "true",
                                    "algoType": "CONDITIONAL",
                                }
                                tp_res = await self.exchange.request(
                                    path="algoOrder",
                                    api="fapiPrivate",
                                    method="POST",
                                    params=req_tp,
                                )
                                tp_order_id = tp_res.get("algoId")
                            else:
                                raise tp_algo_err

                # 2. Stop Loss (STOP_MARKET 방식, reduceOnly)
                try:
                    sl_order = await self.exchange.create_order(
                        symbol=symbol,
                        type="stop_market",
                        side=exit_side,
                        amount=total_amount,
                        params={"stopPrice": sl_price, "reduceOnly": True},
                    )
                    sl_order_id = sl_order.get("id")
                except Exception as e:
                    err_msg = str(e)
                    if "-4120" in err_msg or "Algo Order API endpoints" in err_msg:
                        logger.info(
                            f"ℹ️ [{symbol}] 일반 Stop Market 거절됨(-4120). 신규 AlgoOrder API로 SL(손절) 전송을 시도합니다."
                        )

                        formatted_amount = self.exchange.amount_to_precision(
                            symbol, total_amount
                        )
                        formatted_price = self.exchange.price_to_precision(
                            symbol, sl_price
                        )
                        raw_symbol = self.exchange.market(symbol)["id"]

                        req = {
                            "symbol": raw_symbol,
                            "side": exit_side.upper(),
                            "type": "STOP_MARKET",
                            "quantity": formatted_amount,
                            "triggerPrice": formatted_price,
                            "reduceOnly": "true",
                            "algoType": "CONDITIONAL",
                        }
                        sl_res = await self.exchange.request(
                            path="algoOrder",
                            api="fapiPrivate",
                            method="POST",
                            params=req,
                            headers={},
                        )
                        sl_order_id = sl_res.get("orderId", sl_res.get("algoId"))
                    else:
                        raise e
            else:
                tp_order_id = "DRY_TP"
                sl_order_id = "DRY_SL"
                logger.info(f"🧪 [DRY RUN] {symbol} TP/SL 가상 주문 완료 및 DB 기록됨")

            # 텔레그램 알림 전송 (REAL/DRY 공통)
            dr_tag = "🧪 [DRY RUN] " if settings.DRY_RUN else "✅ "
            reason = entry_info.get("reason", "자동 진입")

            # [V18.2] 신호 강도(Excess Score) 추출
            market_snapshot = entry_info.get("market_data", {})
            excess_score = market_snapshot.get("excess_score", 0)
            strength_tag = (
                f" (강도: +{excess_score})" if excess_score > 0 else " (커트라인)"
            )

            await notifier.send_message(
                f"{dr_tag}포지션 진입 완료\n"
                f"[{symbol}] {signal_type}{strength_tag}\n"
                f"사유: {reason}\n"
                f"체결가: {entry_price:.4f}\n"
                f"TP 지정가: {tp_price} (수량: {tp_amount_final}, {partial_ratio * 100:.0f}%)\n"
                f"SL 시장가: {sl_price} (전량)\n"
                f"잔량 {(1 - partial_ratio) * 100:.0f}%는 Chandelier 추적\n"
                f"Real R:R: 1 : {abs(real_tp_pct / real_sl_pct) if real_sl_pct != 0 else 0:.2f}"
            )

            # 포지션 상태 갱신 (ID 추적 포함)
            entry_info.update(
                {
                    "tp_order_id": str(tp_order_id) if tp_order_id else None,
                    "sl_order_id": str(sl_order_id) if sl_order_id else None,
                    "is_partial_tp_done": False,
                    "last_summed_ts": self.exchange.milliseconds(),  # PnL 합산 기준점
                }
            )
            self.active_positions[symbol] = entry_info

            return True

        except Exception as e:
            logger.error(f"[{symbol}] TP/SL 세팅 중 예외 발생: {e}")
            return False

    async def check_pending_orders_state(self):
        """
        (더 이상 신규 진입 시 사용되지 않으나, 기존 대기 주문 잔여물 정리를 위해 빈 메서드로 유지)
        """
        pass

    async def check_active_positions_state(self):
        """
        활성 포지션을 주기적으로 점검하여, TP/SL에 의해 포지션이 종료되었는지 확인하고
        종료되었다면 잔여 주문(TP/SL 중 미발동분)을 일괄 취소한 뒤 DB에 매도(청산) 기록과 최신 PnL을 남깁니다.
        또한 수동으로 진입한 포지션을 추적 망에 자동으로 끌어옵니다.
        """
        from datetime import datetime, timezone, timedelta

        symbols_to_remove = []

        if not settings.DRY_RUN:
            try:
                positions = await self.exchange.fetch_positions()
                position_map = {
                    p["symbol"]: float(p.get("contracts", 0)) for p in positions
                }

                # 수동(외부) 진입 포지션 색출
                for p in positions:
                    sym = p["symbol"]
                    contracts = float(p.get("contracts", 0))
                    # [V18.1] 봇이 현재 진입 중(pending_entries)이거나 관리 중(active_positions)이면 수동 진입으로 간주하지 않음
                    if (
                        contracts > 0
                        and sym not in self.active_positions
                        and sym not in self.pending_entries
                    ):
                        self.active_positions[sym] = {
                            "amount": contracts,
                            "last_summed_ts": self.exchange.milliseconds(),
                            "is_partial_tp_done": False,
                        }
                        entry_price = float(p.get("entryPrice", 0))
                        side = p.get("side", "long").upper()

                        logger.info(
                            f"[{sym}] 수동/외부 진입 감지. 봇 메모리에 편입합니다."
                        )
                        async with AsyncSessionLocal() as session:
                            new_log = TradeLog(
                                entry_time=(datetime.utcnow() + timedelta(hours=9)),
                                exit_time=None,
                                action="MANUAL",
                                symbol=sym,
                                direction=side,
                                target_price=entry_price,
                                execution_price=entry_price,
                                slippage=0.0,
                                execution_time_ms=0,
                                qty=contracts,
                                entry_reason=f"외부/수동 진입 감지 ({side})",
                                realized_pnl=0.0,
                                dry_run=settings.DRY_RUN,
                                params=self._snapshot_params(),
                            )
                            session.add(new_log)
                            await session.commit()

                        await notifier.send_message(
                            f"✋ 수동 포지션 진입 감지\n[{sym}] {side}\n"
                            f"계약 수: {contracts}\n"
                            f"진입 단가: {entry_price:.4f}\n"
                            f"봇 시스템(DB) 추적망에 편입되었습니다."
                        )
            except Exception as e:
                logger.error(f"활성 포지션 검증 중 거래소 조회 에러: {e}")
                return
        else:
            position_map = {}
            if not self.active_positions:
                return

        for symbol in list(self.active_positions.keys()):
            if settings.DRY_RUN:
                try:
                    ticker = await self.exchange.fetch_ticker(symbol)
                    current_price = float(ticker.get("last", 0.0))
                    pos_info = self.active_positions.get(symbol)

                    if current_price > 0 and isinstance(pos_info, dict):
                        signal_type = pos_info.get("signal")
                        tp_price = pos_info.get("tp_price") or 0.0
                        sl_price = pos_info.get("sl_price") or 0.0
                        is_partial_tp_done = pos_info.get("is_partial_tp_done", False)

                        triggered = False
                        reason = ""

                        # 1. TP 감시 (분할 익절 고려)
                        if signal_type == "LONG":
                            if not is_partial_tp_done and current_price >= tp_price:
                                # 1차 TP 도달
                                reason = "1차 Take Profit 도달 (50% 가상 익절)"
                                await self.close_position_virtually(
                                    symbol,
                                    reason=reason,
                                    close_price=current_price,
                                    partial=True,
                                )
                                continue
                            elif current_price <= sl_price:
                                triggered = True
                                reason = "Stop Loss 도달"
                        elif signal_type == "SHORT":
                            if not is_partial_tp_done and current_price <= tp_price:
                                # 1차 TP 도달
                                reason = "1차 Take Profit 도달 (50% 가상 익절)"
                                await self.close_position_virtually(
                                    symbol,
                                    reason=reason,
                                    close_price=current_price,
                                    partial=True,
                                )
                                continue
                            elif current_price >= sl_price:
                                triggered = True
                                reason = "Stop Loss 도달"

                        if triggered:
                            logger.info(
                                f"🧪 [DRY_RUN] {symbol} 가상 {reason} 감지! 현재가: {current_price}"
                            )
                            await self.close_position_virtually(
                                symbol, reason=reason, close_price=current_price
                            )
                except Exception as e:
                    logger.error(f"[{symbol}] DRY_RUN 가상 TP/SL 감시 중 에러: {e}")
                continue

            current_contracts = position_map.get(symbol, 0.0)

            # [V18] 부분 익절 감지 및 수익 누적 (REAL 모드 전용)
            if not settings.DRY_RUN and isinstance(self.active_positions[symbol], dict):
                prev_qty = self.active_positions[symbol].get("amount", 0.0)
                if (
                    current_contracts > 0 and current_contracts < prev_qty * 0.9
                ):  # 10% 이상 줄었을 때 (수수료/오차 방어)
                    logger.info(
                        f"⚖️ [{symbol}] 포지션 수량 감소 감지 ({prev_qty} -> {current_contracts}). 수익 정산을 시작합니다."
                    )
                    try:
                        trades = await self.exchange.fetch_my_trades(symbol, limit=20)
                        partial_pnl = 0.0
                        # last_summed_ts 이후의 모든 실현 손익 합산 (중복 방지)
                        last_ts = self.active_positions[symbol].get("last_summed_ts", 0)
                        new_last_ts = last_ts

                        for t in trades:
                            t_ts = t["timestamp"]
                            if t_ts > last_ts:
                                # [V18.4] 수수료(commission) 차감 적용하여 Net PnL 산출
                                raw_pnl = float(
                                    t.get("info", {}).get("realizedPnl", 0.0)
                                )
                                comm = float(t.get("fee", {}).get("cost", 0.0))
                                if comm == 0:  # CCXT 구조상 t["fee"]가 없을 경우 대비
                                    comm = float(
                                        t.get("info", {}).get("commission", 0.0)
                                    )

                                net_pnl = raw_pnl - comm
                                if net_pnl != 0:
                                    partial_pnl += net_pnl
                                if t_ts > new_last_ts:
                                    new_last_ts = t_ts

                        if partial_pnl != 0:
                            # 타임스탬프 갱신
                            self.active_positions[symbol]["last_summed_ts"] = (
                                new_last_ts
                            )

                            async with AsyncSessionLocal() as session:
                                stmt = (
                                    select(TradeLog)
                                    .where(
                                        TradeLog.symbol == symbol,
                                        TradeLog.exit_time.is_(None),
                                    )
                                    .order_by(TradeLog.entry_time.desc())
                                    .limit(1)
                                )
                                res = await session.execute(stmt)
                                trade_log = res.scalars().first()
                                if trade_log:
                                    trade_log.realized_pnl = (
                                        trade_log.realized_pnl or 0.0
                                    ) + partial_pnl
                                    # [V18.4] PnL 합산 기준시점 DB 업데이트 (영속화)
                                    trade_log.last_pnl_at = datetime.fromtimestamp(
                                        new_last_ts / 1000
                                    )
                                    # 메모리상 수량 업데이트 및 분할 익절 플래그 세팅 (모의투자와 알고리즘 통일)
                                    self.active_positions[symbol]["amount"] = (
                                        current_contracts
                                    )
                                    self.active_positions[symbol][
                                        "is_partial_tp_done"
                                    ] = True

                                    # [V18.2] PortfolioState(샹들리에 추적용) 플래그도 함께 업데이트
                                    if symbol in self.portfolio.positions:
                                        self.portfolio.positions[symbol][
                                            "is_partial_tp_done"
                                        ] = True

                                    # [V19] TradeLog 테이블에 분할 익절 내역 기록 추가 (히스토리용 별도 레코드 생성)
                                    new_log = TradeLog(
                                        entry_time=(
                                            datetime.utcnow() + timedelta(hours=9)
                                        ),
                                        exit_time=(
                                            datetime.utcnow() + timedelta(hours=9)
                                        ),
                                        action="PARTIAL_CLOSED",
                                        symbol=symbol,
                                        direction=self.active_positions[symbol].get(
                                            "signal"
                                        ),
                                        target_price=self.active_positions[symbol].get(
                                            "tp_price", 0.0
                                        ),
                                        execution_price=self.active_positions[
                                            symbol
                                        ].get("tp_price", 0.0),
                                        slippage=0.0,
                                        execution_time_ms=0,
                                        qty=prev_qty - current_contracts,
                                        entry_reason="분할 익절(Partial TP) 체결 감지",
                                        realized_pnl=partial_pnl,
                                        dry_run=False,
                                        params=self._snapshot_params(),
                                    )
                                    session.add(new_log)
                                    await session.commit()

                                    logger.info(
                                        f"💰 [{symbol}] 부분 익절 수익 누적 및 Trade 기록 완료: +{partial_pnl:.4f} USDT (누적: {trade_log.realized_pnl:.4f})"
                                    )
                    except Exception as p_err:
                        logger.error(f"[{symbol}] 부분 익절 수익 누정 중 에러: {p_err}")

            # [V18.2] REAL 모드 Fail-safe 가격 감시 (모의투자와 알고리즘 통일)
            # 거래소 TP/SL 주문이 없거나 취소된 경우를 대비한 봇 수준의 이중 안전장치
            if not settings.DRY_RUN and isinstance(self.active_positions[symbol], dict):
                try:
                    ticker = await self.exchange.fetch_ticker(symbol)
                    current_price = float(ticker.get("last", 0.0))
                    pos_info = self.active_positions[symbol]
                    tp_price = pos_info.get("tp_price", 0.0)
                    sl_price = pos_info.get("sl_price", 0.0)
                    is_partial_tp_done = pos_info.get("is_partial_tp_done", False)

                    # 1. Fail-safe TP (분할 익절)
                    if pos_info.get("signal") == "LONG":
                        if (
                            not is_partial_tp_done
                            and tp_price > 0
                            and current_price >= tp_price
                        ):
                            logger.warning(
                                f"🚨 [{symbol}] REAL Fail-safe TP 감지! 시장가 분할 청산을 시도합니다."
                            )
                            await self.close_position_market(
                                symbol,
                                amount=pos_info["amount"] * settings.PARTIAL_TP_RATIO,
                                reason="Fail-safe Partial TP",
                            )
                        elif sl_price > 0 and current_price <= sl_price:
                            logger.warning(
                                f"🚨 [{symbol}] REAL Fail-safe SL 감지! 시장가 전량 청산을 시도합니다."
                            )
                            await self.close_position_market(
                                symbol,
                                amount=pos_info["amount"],
                                reason="Fail-safe Full SL",
                            )
                    elif pos_info.get("signal") == "SHORT":
                        if (
                            not is_partial_tp_done
                            and tp_price > 0
                            and current_price <= tp_price
                        ):
                            logger.warning(
                                f"🚨 [{symbol}] REAL Fail-safe TP 감지! 시장가 분할 청산을 시도합니다."
                            )
                            await self.close_position_market(
                                symbol,
                                amount=pos_info["amount"] * settings.PARTIAL_TP_RATIO,
                                reason="Fail-safe Partial TP",
                            )
                        elif sl_price > 0 and current_price >= sl_price:
                            logger.warning(
                                f"🚨 [{symbol}] REAL Fail-safe SL 감지! 시장가 전량 청산을 시도합니다."
                            )
                            await self.close_position_market(
                                symbol,
                                amount=pos_info["amount"],
                                reason="Fail-safe Full SL",
                            )

                    # [V18.4] 영구적 Time-Exit 감시 (봇 재시작 시에도 entry_time이 DB에서 복구되어야 작동)
                    wait_mins = getattr(settings, "TIME_EXIT_MINUTES", 0)
                    entry_time = pos_info.get("entry_time")

                    # [V18.5] Time-Exit는 실제 거래소에 포지션이 있을 때만 동작 (ReduceOnly Rejection 방지)
                    if wait_mins > 0 and entry_time and current_contracts > 0:
                        if isinstance(
                            entry_time, str
                        ):  # DB에서 문자열로 넘어올 경우 대비
                            entry_time = datetime.fromisoformat(
                                entry_time.replace("Z", "+00:00")
                            )

                        now_kst = datetime.now(timezone.utc) + timedelta(hours=9)
                        # Naive/Aware 통일 필요 (entry_time이 Naive라고 가정 시)
                        if entry_time.tzinfo is None:
                            now_kst = now_kst.replace(tzinfo=None)

                        elapsed_mins = (now_kst - entry_time).total_seconds() / 60
                        if elapsed_mins >= wait_mins:
                            logger.warning(
                                f"⏰ [{symbol}] {wait_mins}분 보유 시간 초과 ({elapsed_mins:.1f}분 경과) → 시장가 강제 탈출 시도."
                            )
                            await self.close_position_market(
                                symbol,
                                amount=current_contracts,  # 메모리값 대신 현재 거래소 실수량 사용
                                reason=f"Time Exit ({wait_mins}분 시간 초과)",
                            )
                except Exception as fs_err:
                    logger.error(f"[{symbol}] REAL Fail-safe 감시 중 에러: {fs_err}")

            if current_contracts == 0.0:
                try:
                    # 포지션이 청산됨 -> 반대쪽 찌꺼기 잔여 주문(TP or SL 중 발동 안된 쪽) 일괄 취소
                    try:
                        await self.exchange.cancel_all_orders(symbol)
                        logger.info(
                            f"[{symbol}] 포지션 청산으로 인한 잔여 대기주문 일괄 취소 완료."
                        )
                    except Exception as cancel_e:
                        logger.warning(
                            f"[{symbol}] 잔여 주문 자동 취소 실패 (무시 가능): {cancel_e}"
                        )

                    # 신규 추가: 포지션 청산 시 조건부 Algo 주문(STOP_MARKET) 찌꺼기도 강제 파쇄
                    try:
                        raw_sym = self.exchange.market(symbol)["id"]
                        algo_orders = await self.exchange.request(
                            path="openAlgoOrders",
                            api="fapiPrivate",
                            method="GET",
                            params={"symbol": raw_sym},
                        )
                        algo_items = (
                            algo_orders.get("orders", algo_orders)
                            if isinstance(algo_orders, dict)
                            else algo_orders
                        )
                        for algo in algo_items:
                            await self.exchange.request(
                                path="algoOrder",
                                api="fapiPrivate",
                                method="DELETE",
                                params={
                                    "symbol": raw_sym,
                                    "algoId": algo.get("algoId"),
                                },
                            )
                        if algo_items:
                            logger.info(
                                f"[{symbol}] 포지션 청산으로 인한 잔여 조건부(Algo) 대기주문 일괄 취소 완료."
                            )
                    except Exception as algo_cancel_e:
                        logger.warning(
                            f"[{symbol}] 잔여 조건부(Algo) 주문 취소 실패 (무시 가능): {algo_cancel_e}"
                        )

                    trades = await self.exchange.fetch_my_trades(symbol, limit=20)
                    realized_pnl = 0.0
                    close_price = 0.0
                    close_qty = 0.0
                    last_order_id = ""

                    if trades:
                        # [V18.4] last_summed_ts 이후의 모든 실현 손익 합산 (중복 방지)
                        pos_mem = self.active_positions.get(symbol)
                        last_ts = 0
                        if isinstance(pos_mem, dict):
                            last_ts = pos_mem.get("last_summed_ts", 0)

                        processed_trades = []
                        for t in trades:
                            t_ts = t["timestamp"]
                            if t_ts > last_ts:
                                pnl = float(t.get("info", {}).get("realizedPnl", 0.0))
                                realized_pnl += pnl
                                processed_trades.append(t)

                        if processed_trades:
                            last_trade = processed_trades[-1]  # 가장 최근 체결
                            close_price = float(last_trade.get("price", 0.0))
                            close_qty = sum(
                                [
                                    float(tr.get("amount", 0.0))
                                    for tr in processed_trades
                                ]
                            )
                            last_order_id = str(last_trade.get("order", ""))
                        else:
                            # 필터링 후 남은게 없다면 (드문 경우) 마지막 한 거래 기준으로 폴백
                            last_trade = trades[-1]
                            close_price = float(last_trade.get("price", 0.0))
                            close_qty = float(last_trade.get("amount", 0.0))
                            last_order_id = str(last_trade.get("order", ""))
                            realized_pnl = float(
                                last_trade.get("info", {}).get("realizedPnl", 0.0)
                            )

                        # [V18.1] 청산 사유 정밀 식별
                        exit_reason = "자동 청산 (감지)"
                        pos_mem = self.active_positions.get(symbol)
                        if isinstance(pos_mem, dict):
                            stored_tp = pos_mem.get("tp_order_id")
                            stored_sl = pos_mem.get("sl_order_id")

                            if (
                                last_order_id
                                and stored_tp
                                and last_order_id == stored_tp
                            ):
                                exit_reason = "Take Profit (자동 익절)"
                            elif (
                                last_order_id
                                and stored_sl
                                and last_order_id == stored_sl
                            ):
                                exit_reason = "Stop Loss (자동 손절)"
                            else:
                                # [V18.2] close_position_market 등을 통해 미리 심어둔 사유가 있다면 최우선 채택
                                stored_exit_reason = pos_mem.get("exit_reason")
                                if stored_exit_reason:
                                    exit_reason = stored_exit_reason
                                else:
                                    exit_reason = (
                                        "Manual / External Exit (수동/외부 청산)"
                                    )
                    else:
                        exit_reason = "포지션 종료 감지 (개입/자동)"

                    logger.info(
                        f"🏁 [{symbol}] 포지션 종료 확인. 사유: {exit_reason}, DB 기록: PnL {realized_pnl:.4f} USDT"
                    )

                    async with AsyncSessionLocal() as session:
                        now_kst = datetime.utcnow() + timedelta(hours=9)
                        new_hist = TradeLog(
                            entry_time=now_kst,
                            exit_time=now_kst,
                            action="CLOSED",
                            symbol=symbol,
                            direction=pos_mem.get("signal", "UNKNOWN")
                            if pos_mem
                            else "UNKNOWN",
                            target_price=close_price,
                            execution_price=close_price,
                            slippage=0.0,
                            execution_time_ms=0,
                            qty=close_qty,
                            entry_reason=f"청산 완료: {exit_reason}",
                            realized_pnl=realized_pnl,
                            dry_run=settings.DRY_RUN,
                            params=self._snapshot_params(),
                        )
                        session.add(new_hist)

                        # [V18] 기존 TradeLog 찾아 청산/성과 데이터 업데이트
                        if not settings.DRY_RUN:
                            try:
                                stmt = (
                                    select(TradeLog)
                                    .where(
                                        TradeLog.symbol == symbol,
                                        TradeLog.exit_time.is_(None),
                                    )
                                    .order_by(TradeLog.entry_time.desc())
                                    .limit(1)
                                )

                                result = await session.execute(stmt)
                                trade_log = result.scalars().first()

                                if trade_log:
                                    trade_log.exit_time = now_kst
                                    trade_log.exit_price = close_price
                                    # [V18] 마지막 청산 수익만 덮어쓰지 않고 기존 누적값(부분익절)에 합산
                                    trade_log.realized_pnl = (
                                        trade_log.realized_pnl or 0.0
                                    ) + realized_pnl
                                    trade_log.exit_reason = exit_reason

                                    # 수익률 펀더멘탈 기록 (분할 익절 포함 전체 누적 수익률 계산)
                                    if (
                                        trade_log.execution_price
                                        and trade_log.execution_price > 0
                                        and trade_log.qty
                                        and trade_log.qty > 0
                                    ):
                                        total_notional = (
                                            trade_log.execution_price * trade_log.qty
                                        )
                                        trade_log.roi_pct = (
                                            trade_log.realized_pnl / total_notional
                                        ) * 100
                            except Exception as ml_err:
                                logger.error(
                                    f"[{symbol}] TradeLog 업데이트 중 예외: {ml_err}"
                                )

                        await session.commit()

                        await notifier.send_message(
                            f"🏁 포지션 청산 완료 감지\n[{symbol}]\n"
                            f"사유: {exit_reason}\n"
                            f"종료가: {close_price:.4f}\n"
                            f"실현손익(PnL): {realized_pnl:.4f} USDT"
                        )

                    symbols_to_remove.append(symbol)

                    # 손실이면 해당 종목 쿨다운 설정 (연속 SL 방지)
                    if realized_pnl < 0:
                        now_kst = datetime.utcnow() + timedelta(hours=9)
                        cooldown_min = getattr(settings, "LOSS_COOLDOWN_MINUTES", 15)
                        self.loss_cooldown[symbol] = now_kst + timedelta(
                            minutes=cooldown_min
                        )
                        logger.info(
                            f"[{symbol}] 손실 청산 → {cooldown_min}분 쿨다운 적용. (만료: {self.loss_cooldown[symbol]}) "
                            f"PnL: {realized_pnl:.4f} USDT"
                        )

                except Exception as e:
                    logger.error(
                        f"[{symbol}] 포지션 청산 확인 및 DB 기록 중 예외 발생: {e}"
                    )

        # 처리 완료된 포지션은 메모리 감시열에서 제거
        for sym in symbols_to_remove:
            del self.active_positions[sym]

    async def close_position_market(
        self, symbol: str, amount: float = 0.0, reason: str = "시장가 청산"
    ):
        """
        [V18.2] 통합 시장가 청산 메서드 (REAL / DRY_RUN 공통)
        전량 또는 부분 전량을 시장가로 즉시 매도합니다.
        """
        if symbol not in self.active_positions:
            logger.warning(f"[{symbol}] 이미 종료된 포지션이라 청산을 생략합니다.")
            return

        if settings.DRY_RUN:
            # DRY_RUN은 기존 가상 청산 메서드로 위임
            is_partial = False
            if amount > 0:
                pos_info = self.active_positions.get(symbol)
                if isinstance(pos_info, dict):
                    full_qty = pos_info.get("amount", 0.0)
                    if amount < full_qty * 0.9:  # 대략적으로 부분인지 판별
                        is_partial = True

            await self.close_position_virtually(
                symbol, reason=reason, partial=is_partial
            )
            return

        # --- REAL 모드 실제 주문 ---
        try:
            pos_info = self.active_positions.get(symbol)
            if not isinstance(pos_info, dict):
                logger.error(
                    f"[{symbol}] 포지션 정보가 딕셔너리가 아닙니다. 청산 불가."
                )
                return

            side = pos_info.get("signal", "LONG")
            exit_side = "sell" if side == "LONG" else "buy"

            # 1. 잔여 주문 취소
            try:
                await self.exchange.cancel_all_orders(symbol)
            except Exception as e:
                logger.warning(f"[{symbol}] 청산 전 주문 취소 실패(무시): {e}")

            # 2. 수량 확정 (전달된 amount가 없거나 정밀도 이슈 대비 실시간 재조회)
            # [V18.5] ReduceOnly rejection 방지를 위해 실제 포지션 수량 재확인
            positions = await self.exchange.fetch_positions([symbol])
            exchange_qty = 0.0
            for p in positions:
                if p["symbol"] == symbol:
                    exchange_qty = float(p.get("contracts", 0))
                    break

            if exchange_qty <= 0:
                logger.info(
                    f"[{symbol}] 거래소 포지션 수량이 0이므로 청산을 중단합니다."
                )
                # 메모리상 포지션이 남아있다면 제거 대상에 포함되도록 유도 (다음 루프에서 처리되거나 여기서 직접 처리)
                return

            final_amount = amount if amount > 0 else exchange_qty
            # 청산 시도 수량이 실제 수량보다 많으면 실제 수량으로 보정
            if final_amount > exchange_qty:
                final_amount = exchange_qty

            if symbol in self.active_positions:
                self.active_positions[symbol]["exit_reason"] = reason

            # 3. 시장가 주문 (Reduce-Only)
            await self.exchange.create_order(
                symbol=symbol,
                type="market",
                side=exit_side,
                amount=final_amount,
                params={"reduceOnly": True},
            )

            logger.info(
                f"🚀 [{symbol}] {reason} 시장가 주문 완료. (수량: {final_amount})"
            )

            # 4. DB 기록 (부분 청산인 경우 히스토리용 TradeLog, 전량인 경우 루프에서 처리)
            # (여기서는 주문만 날리고, 실제 메모리 정리는 check_active_positions_state 폴링에 맡김)
            async with AsyncSessionLocal() as session:
                new_log = TradeLog(
                    entry_time=(datetime.utcnow() + timedelta(hours=9)),
                    exit_time=(datetime.utcnow() + timedelta(hours=9)),
                    action="CLOSED" if amount <= 0 else "PARTIAL_CLOSED",
                    symbol=symbol,
                    direction=side,
                    target_price=0.0,
                    execution_price=0.0,  # 나중에 싱크됨
                    slippage=0.0,
                    execution_time_ms=0,
                    qty=final_amount,
                    entry_reason=reason,
                    realized_pnl=0.0,
                    dry_run=False,
                    params=self._snapshot_params(),
                )
                session.add(new_log)
                await session.commit()

            await notifier.send_message(
                f"🔴 <b>시장가 청산 발동</b>\n[{symbol}] {reason}\n수량: {final_amount}\n체결 감지 루프에서 최종 정산 대기 중."
            )

        except Exception as e:
            logger.error(f"[{symbol}] 시장가 청산 중 치명적 에러: {e}")

    async def close_position_virtually(
        self,
        symbol: str,
        reason: str = "가상 청산",
        close_price: float = 0.0,
        partial: bool = False,
    ):
        """
        [V18] DRY_RUN 모드 전용: 실제 거래소 주문 없이 봇 내부 상태만 종료하고 DB에 기록합니다.
        무한 루프 방지를 위해 Chandelier Exit, Time Exit, 가상 TP/SL 감시 등에서 호출됩니다.
        """
        if symbol not in self.active_positions:
            return

        logger.info(f"🧪 [DRY RUN] {symbol} 가상 청산 실행. 사유: {reason}")

        try:
            pos_info = self.active_positions[symbol]
            realized_pnl = 0.0
            close_qty = 0.0
            signal = "UNKNOWN"
            amount = 0.0

            # 가상 종가가 주어지지 않았을 경우 현재가 조회
            if close_price <= 0.0:
                try:
                    ticker = await self.exchange.fetch_ticker(symbol)
                    close_price = float(ticker.get("last", 0.0))
                except Exception as e:
                    logger.error(f"[{symbol}] 가상 청산 중 종가 조회 실패: {e}")

            # PNL 및 수익률 계산 (pos_info 딕셔너리가 존재하는 경우)
            if isinstance(pos_info, dict) and close_price > 0.0:
                entry_price = pos_info.get("limit_price", 0.0)
                amount = pos_info.get("amount", 0.0)
                signal = pos_info.get("signal", "LONG")
                close_qty = amount

                # [V18.4] settings.FEE_RATE 사용
                taker_fee = getattr(settings, "FEE_RATE", 0.00045)
                # 가상 청산이므로 보수적으로 Taker fee 2회(진입/청산) 부과 가정
                fees = (entry_price * amount * taker_fee) + (
                    close_price * amount * taker_fee
                )

                if signal == "LONG":
                    gross_pnl = (close_price - entry_price) * amount
                else:
                    gross_pnl = (entry_price - close_price) * amount

                # 분할 익절인 경우 수량의 절반만 계산
                if partial:
                    gross_pnl *= settings.PARTIAL_TP_RATIO
                    fees *= settings.PARTIAL_TP_RATIO

                realized_pnl = gross_pnl - fees

                logger.info(
                    f"🧪 [DRY RUN] {symbol} {'부분' if partial else '전체'} 가상 PNL 정산 완료: 진입가={entry_price}, 청산가={close_price}, "
                    f"수량={amount * (settings.PARTIAL_TP_RATIO if partial else 1.0)}, Signal={signal}, Net PNL={realized_pnl:.4f}"
                )
            else:
                logger.warning(
                    f"🧪 [DRY RUN] PNL 정산 실패: pos_info data={pos_info}, close_price={close_price}"
                )

            async with AsyncSessionLocal() as session:
                logger.info(
                    f"🧪 [DRY RUN] TradeLog 기록 준비: realized_pnl={realized_pnl:.4f}, close_price={close_price:.4f}"
                )
                now_kst = datetime.utcnow() + timedelta(hours=9)

                # [V19] DRY_RUN 종료 기록은 기존 TradeLog 업데이트와 신규 로그 생성을 병행하거나 통합 관리
                # 여기서는 '전체 청결'을 의미하는 신규 레코드를 하나 더 남겨 히스토리 보존 (기존 방식 Trade 대체)
                new_hist = TradeLog(
                    entry_time=now_kst,
                    exit_time=now_kst,
                    action="PARTIAL_CLOSED" if partial else "CLOSED",
                    symbol=symbol,
                    direction=signal,
                    target_price=close_price,
                    execution_price=close_price,
                    slippage=0.0,
                    execution_time_ms=0,
                    qty=close_qty,
                    entry_reason=f"[DRY_RUN] {reason}",
                    realized_pnl=realized_pnl,
                    dry_run=True,
                    params=self._snapshot_params(),
                )
                session.add(new_hist)

                # ML Pipeline 전용 TradeLog 업데이트
                try:
                    stmt = (
                        select(TradeLog)
                        .where(
                            TradeLog.symbol == symbol,
                            TradeLog.exit_time.is_(None),
                        )
                        .order_by(TradeLog.entry_time.desc())
                        .limit(1)
                    )
                    result = await session.execute(stmt)
                    trade_log = result.scalars().first()

                    if trade_log:
                        trade_log.exit_time = now_kst
                        trade_log.exit_price = close_price
                        # [V18] 누적 수익 처리
                        trade_log.realized_pnl = (
                            trade_log.realized_pnl or 0.0
                        ) + realized_pnl
                        trade_log.exit_reason = f"[DRY_RUN] {reason}"
                        if not partial:
                            trade_log.exit_time = now_kst

                        logger.info(
                            f"🧪 [DRY RUN] TradeLog 디버그 전 - roi_pct 갱신 전, exc_price: {trade_log.execution_price}"
                        )
                        if trade_log.execution_price and trade_log.execution_price > 0:
                            # 전체 ROI 계산을 위해 '수익금 / (진입가 * 총수량)' 공식 사용
                            total_notional = trade_log.execution_price * trade_log.qty
                            trade_log.roi_pct = (
                                trade_log.realized_pnl / total_notional
                            ) * 100
                            logger.info(
                                f"🧪 [DRY RUN] TradeLog 디버그 후 - 누적 PNL: {trade_log.realized_pnl}, calculated roi_pct: {trade_log.roi_pct}"
                            )
                        else:
                            logger.warning(
                                f"🧪 [DRY RUN] TradeLog에 execution_price가 없어 ROI를 계산할 수 없습니다. (값: {trade_log.execution_price})"
                            )
                    else:
                        logger.warning(
                            f"🧪 [DRY RUN] symbol {symbol}에 대해 아직 청산되지 않은 TradeLog 레코드를 찾을 수 없습니다."
                        )
                except Exception as ml_err:
                    logger.error(
                        f"[{symbol}] DRY_RUN TradeLog 업데이트 중 예외: {ml_err}"
                    )

                await session.commit()

            await notifier.send_message(
                f"🧪 [DRY RUN] 포지션 가상 {'부분 ' if partial else ''}청산 완료\n[{symbol}]\n"
                f"종료가: {close_price:.4f}\n"
                f"금회 수익: {realized_pnl:.4f} USDT\n"
                f"사유: {reason}"
            )

            if not partial:
                if symbol in self.active_positions:
                    del self.active_positions[symbol]
            else:
                # 1차 익절 완료 시 수량 감소 및 플래그 세팅
                if symbol in self.active_positions:
                    self.active_positions[symbol]["amount"] -= close_qty
                    self.active_positions[symbol]["is_partial_tp_done"] = True

                # [V18.2] PortfolioState(샹들리에 추적용) 플래그도 가상 업데이트
                if symbol in self.portfolio.positions:
                    self.portfolio.positions[symbol]["is_partial_tp_done"] = True

                logger.info(
                    f"🧪 [DRY RUN] {symbol} 1차 익절 완료. 잔량 {self.active_positions[symbol]['amount']} 추적 계속."
                )

        except Exception as e:
            logger.error(f"[{symbol}] 가상 청산 처리 중 에러: {e}")

    async def check_state_mismatch(self):
        """
        [Fail-Safe 방어 체계]
        거래소 실잔고와 DB/메모리 기록 사이의 불일치를 감지합니다.
        DRY_RUN 모드에서는 실제 거래소 API를 호출하지 않습니다.
        """
        # DRY_RUN 모드에서는 거래소 조회 불필요 — 스킵
        if settings.DRY_RUN:
            return

        try:
            # [V19] 명시적으로 선물(future) 계좌만 조회하여 Margin API 호출 및 타임아웃 방지
            balance_info = await self.exchange.fetch_balance({"type": "future"})
            usdt_total = balance_info.get("total", {}).get("USDT", 0.0)

            # 보유 선물 포지션 조회 (CCXT fetch_positions)
            positions = await self.exchange.fetch_positions()
            active_open = [p for p in positions if float(p.get("contracts", 0)) > 0]

            # 향후 로직 고도화: 실제 서버 포지션과 self.active_positions 불일치 방어
            pass

        except Exception as e:
            logger.error(f"State Mismatch 체크 중 오류: {e}")

    async def _time_exit_daemon(
        self, symbol: str, entry_side: str, amount: float, wait_minutes: int
    ):
        """
        [V15.0] 스캘핑 특화: 진입 후 일정 시간(기본 10분)이 지나도 TP/SL에 닿아 청산되지 않은 포지션은,
        평균회귀(Mean Reversion) 모멘텀이 죽은 것으로 간주하여 즉각 시장가로 강제 청산합니다.
        """
        logger.info(
            f"⏳ [{symbol}] Time Exit 데몬 시작. {wait_minutes}분 뒤 체류 상태 확인 예정."
        )
        await asyncio.sleep(wait_minutes * 60)

        # 지정된 분 경과 후, 여전히 포지션이 살아있는지 확인
        if symbol in self.active_positions:
            logger.warning(
                f"⏰ [{symbol}] 설정된 시간({wait_minutes}분) 경과! 모멘텀 고갈 판단하여 시장가 강제 탈출 시도."
            )

            # 반대 방향 주문(매도/매수)
            exit_side = "sell" if entry_side == "buy" else "buy"

            try:
                # [V18.2] 통합 시장가 청산 메서드 호출 (주문 취소 및 REAL/DRY 분기 처리 포함)
                await self.close_position_market(
                    symbol,
                    amount=amount,
                    reason=f"Time Exit ({wait_minutes}분 모멘텀 이탈)",
                )

            except Exception as e:
                logger.error(f"[{symbol}] Time Exit 탈출 로직 중 에러: {e}")
