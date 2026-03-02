import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from config import settings, logger
from database import Trade, TradeLog, OrderEvent, AsyncSessionLocal
from sqlalchemy.future import select
from data_pipeline import DataPipeline
from notification import notifier


class ExecutionEngine:
    def __init__(self, data_pipeline: DataPipeline):
        self.exchange = data_pipeline.exchange
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
                "strategy_version": getattr(settings, "STRATEGY_VERSION", "UNKNOWN"),
                "k_value": getattr(settings, "K_VALUE", None),
                "vol_mult": getattr(settings, "VOL_MULT", None),
                "atr_ratio": getattr(settings, "ATR_RATIO_MULT", None),
                "atr_long_len": getattr(settings, "ATR_LONG_LEN", None),
                "leverage": getattr(settings, "LEVERAGE", None),
                "risk_pct": getattr(settings, "RISK_PERCENTAGE", None),
                "timeframe": getattr(settings, "TIMEFRAME", None),
                "time_exit_min": getattr(settings, "TIME_EXIT_MINUTES", None),
                "sl_mult": getattr(settings, "SL_MULT", None),
                "tp_mult": getattr(settings, "TP_MULT", None),
                # V16 신규 파라미터
                "adx_threshold": getattr(settings, "ADX_THRESHOLD", None),
                "chandelier_mult": getattr(settings, "CHANDELIER_MULT", None),
                "max_same_dir": getattr(settings, "MAX_CONCURRENT_SAME_DIR", None),
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
            return

        try:
            logger.info("🔄 거래소 서버와 기존 상태 동기화 중...")

            # 1. 활성 포지션 복구
            positions = await self.exchange.fetch_positions()
            active_count = 0
            for p in positions:
                symbol = p.get("symbol")
                contracts = float(p.get("contracts", 0.0))
                if contracts > 0:
                    self.active_positions[symbol] = True
                    active_count += 1
                    logger.info(
                        f"✅ [복구 완료] 진행 중인 기존 포지션 감지: {symbol} (계약 수: {contracts})"
                    )

            # 2. 고립된 진입 대기 주문(Pending Entries) 정리
            # 안전을 위해 봇 재시작 시 포지션이 없는 종목의 미체결 주문은 모두 취소합니다.

            logger.info(
                "내 계좌의 전체 대기 주문을 스캔하여 고립된 찌꺼기 주문을 정리합니다..."
            )
            canceled_count = 0

            try:
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
            except Exception as e:
                logger.error(f"내 계좌 전체 대기 주문(일반) 조회 중 에러: {e}")

            # 2.2 고립된 Algo 주문 (STOP_MARKET 등) 정리 로직 추가
            # 바이낸스 퓨처스 업데이트로 일반 OpenOrders 통신망과 Algo 통신망이 분리됨.
            try:
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
            except Exception as e:
                logger.error(f"내 계좌 전체 대기 주문(Algo) 조회 중 에러: {e}")

            logger.info(
                f"🔄 동기화 완료: 복구된 포지션 {active_count}개, 정리된 찌꺼기 대기 주문 {canceled_count}개."
            )
        except Exception as e:
            logger.error(f"거래소 동기화 중(sync_state_from_exchange) 예외 발생: {e}")

    async def setup_margin_and_leverage(self, symbol: str):
        """
        바이낸스 선물에서 해당 코인의 레버리지를 1배로, 마진 모드를 격리(Isolated)로 설정합니다.
        """
        if settings.DRY_RUN:
            return

        try:
            # 1. 격리 마진(Isolated) 설정
            await self.exchange.set_margin_mode("isolated", symbol)
            logger.info(f"[{symbol}] 마진 모드: 격리(Isolated) 설정 완료.")
        except Exception as e:
            # 이미 격리로 설정되어 있는 경우 Exception 발생 가능 (무시)
            if "No need to change margin type" in str(e):
                pass
            else:
                logger.warning(f"[{symbol}] 마진 모드 설정 중 정보: {e}")

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
    ) -> bool:
        """
        [V16.1] 시장가(Market) 대신 포스트 온리(Maker) 지정가로 호가를 추격(Chasing)하며 진입합니다.
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
        from datetime import datetime as _dt

        cooldown_until = self.loss_cooldown.get(symbol)
        if cooldown_until and _dt.utcnow() < cooldown_until:
            remaining = int((cooldown_until - _dt.utcnow()).total_seconds() / 60)
            logger.info(f"[{symbol}] 손실 쿨다운 중. {remaining}분 후 진입 가능. 스킵.")
            return False

        try:
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
                max_retries = 10
                remaining_amount = amount
                total_cost = 0.0
                filled_amount = 0.0

                for attempt in range(max_retries):
                    try:
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

                        # [OrderEvent] 주문 생성 기록
                        async with AsyncSessionLocal() as session:
                            oe = OrderEvent(
                                timestamp=datetime.utcnow() + timedelta(hours=9),
                                symbol=symbol,
                                order_type="LIMIT_MAKER",
                                event_type="CREATE",
                                price=float(price_str),
                                amount=float(amount_str),
                                attempt_count=attempt + 1,
                            )
                            session.add(oe)
                            await session.commit()

                        # 3. 3.5초 대기
                        await asyncio.sleep(3.5)

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
                                    # [OrderEvent] Canceled 기록
                                    async with AsyncSessionLocal() as session:
                                        oe = OrderEvent(
                                            timestamp=datetime.utcnow()
                                            + timedelta(hours=9),
                                            symbol=symbol,
                                            order_type="LIMIT_MAKER",
                                            event_type="CANCEL_POST_ONLY_EXPIRED",
                                            price=float(price_str),
                                            amount=float(amount_str),
                                            attempt_count=attempt + 1,
                                        )
                                        session.add(oe)
                                        await session.commit()
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
                        # [OrderEvent] Rejected/Error 기록
                        async with AsyncSessionLocal() as session:
                            oe = OrderEvent(
                                timestamp=datetime.utcnow() + timedelta(hours=9),
                                symbol=symbol,
                                order_type="LIMIT_MAKER",
                                event_type="REJECTED_OR_ERROR",
                                price=target_price,
                                amount=remaining_amount,
                                attempt_count=attempt + 1,
                            )
                            session.add(oe)
                            await session.commit()
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
            }

            # --- [HOTFIX] 진입 직후 봇 추적망에 즉각 편입하여 수동진입 감지(오작동) 방지 ---
            self.active_positions[symbol] = True

            # V15: 설정된 시간이 경과하면 제자리에 돌려놓지 않은 포지션을 논리적 시장가 매각 (스캘핑 전용)
            if getattr(settings, "TIME_EXIT_MINUTES", 0) > 0:
                asyncio.create_task(
                    self._time_exit_daemon(
                        symbol, side, amount, settings.TIME_EXIT_MINUTES
                    )
                )

            # 동기적(await)으로 TP/SL 즉시 생성 (대기열 통하지 않음)
            success = await self.place_tp_sl_orders(symbol, entry_info)

            return True

        except Exception as e:
            logger.error(f"[{symbol}] 시장가 진입 로직 처리 중 예외 발생: {e}")
            # TP/SL 생성 도중 에러가 나더라도 포지션은 체결되어 있으므로 봇 루프에서 관리되도록 True 반환
            return True

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

            # DB에 취소 기록 남기기
            try:
                async with AsyncSessionLocal() as session:
                    new_trade = Trade(
                        timestamp=(datetime.utcnow() + timedelta(hours=9)),
                        action="CANCELED",
                        symbol=symbol,
                        price=order_info.get("limit_price", 0.0),
                        quantity=order_info.get("amount", 0.0),
                        reason=f"진입 주문 취소: {reason}",
                        realized_pnl=0.0,
                        dry_run=settings.DRY_RUN,
                    )
                    session.add(new_trade)
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
        amount = entry_info["amount"]
        tp_price = entry_info["tp_price"]
        sl_price = entry_info["sl_price"]
        entry_price = entry_info["limit_price"]

        # Long이면 매도(Sell)로 청산, Short이면 매수(Buy)로 청산
        exit_side = "sell" if signal_type == "LONG" else "buy"

        # SL 설정 시 Taker 수수료(0.05%)가 발생함을 로깅 (V11 Feedback)
        maker_fee = 0.0002
        taker_fee = 0.0005

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
            f"실제 손절률(수수료 차감 후): {real_sl_pct * 100:.2f}% (Taker 수수료 0.05% 포함. R:R={abs(real_tp_pct / real_sl_pct) if real_sl_pct != 0 else 0:.2f})"
        )

        try:
            # DB 기록 (진입) - DRY_RUN 이더라도 테스트 내역을 DB에 기록
            async with AsyncSessionLocal() as session:
                dr_prefix = "[DRY_RUN] " if settings.DRY_RUN else ""
                now_kst = datetime.utcnow() + timedelta(hours=9)

                # 기존 봇 호환용 Trade 모델
                new_trade = Trade(
                    timestamp=now_kst,
                    action=signal_type,
                    symbol=symbol,
                    price=entry_price,
                    quantity=amount,
                    reason=f"{dr_prefix}V16 시장가/추격 진입 완료",
                    dry_run=settings.DRY_RUN,
                    params=self._snapshot_params(),
                )
                session.add(new_trade)

                # [V16.2 ML] ML 파이프라인 전용 TradeLog 모델
                new_tradelog = TradeLog(
                    symbol=symbol,
                    direction=signal_type,
                    qty=amount,
                    entry_time=now_kst,
                    target_price=entry_price,  # 이상적인 슬리피지 계산용으론 추후 보완 필요
                    execution_price=entry_price,
                    slippage=0.0,  # 추격 주문 완료 평균가를 기준으로 설정(0)
                    entry_reason=entry_info.get("reason", "자동 진입"),
                    execution_time_ms=entry_info.get("execution_time_ms", 0),
                    # exit 관련은 NULL 유지
                )
                session.add(new_tradelog)

                await session.commit()

            if settings.DRY_RUN:
                logger.info(f"🧪 [DRY RUN] {symbol} TP/SL 가상 주문 완료 및 DB 기록됨")
                self.active_positions[symbol] = True
                return True

            # 1. Take Profit (LIMIT 방식)
            # -4164(Order's notional) 혹은 Margin insufficient 에러 거절 방지를 위해 예외 감지 기능 탑재
            try:
                await self.exchange.create_order(
                    symbol=symbol,
                    type="limit",
                    side=exit_side,
                    amount=amount,
                    price=tp_price,
                    params={"reduceOnly": True},
                )
            except Exception as tp_err:
                logger.warning(
                    f"[{symbol}] 지정가(Limit) TP 생성 거절됨. 시장가 우회(TAKE_PROFIT_MARKET)로 재시도합니다. 사유: {tp_err}"
                )
                try:
                    await self.exchange.create_order(
                        symbol=symbol,
                        type="take_profit_market",
                        side=exit_side,
                        amount=amount,
                        params={"stopPrice": tp_price, "reduceOnly": True},
                    )
                except Exception as tp_algo_err:
                    err_msg = str(tp_algo_err)
                    if "-4120" in err_msg or "Algo Order API" in err_msg:
                        formatted_amount_tp = self.exchange.amount_to_precision(
                            symbol, amount
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
                        await self.exchange.request(
                            path="algoOrder",
                            api="fapiPrivate",
                            method="POST",
                            params=req_tp,
                        )
                    else:
                        raise tp_algo_err

            # 2. Stop Loss (STOP_MARKET 방식, reduceOnly)
            # 바이낸스 퓨처스 API 업데이트로 인해 일반 엔드포인트에서 예외(-4120)가 발생할 수 있습니다.
            # 이 경우 AlgoOrder 전용 엔드포인트를 우회 호출하는 폴백 로직을 가동합니다.
            try:
                await self.exchange.create_order(
                    symbol=symbol,
                    type="stop_market",
                    side=exit_side,
                    amount=amount,
                    params={"stopPrice": sl_price, "reduceOnly": True},
                )
            except Exception as e:
                err_msg = str(e)
                if "-4120" in err_msg or "Algo Order API endpoints" in err_msg:
                    logger.warning(
                        f"[{symbol}] 일반 Stop Market 거절됨(-4120). 신규 AlgoOrder 전용 엔드포인트로 SL(손절) 전송을 재시도합니다."
                    )

                    # 수량과 호가단위를 거래소 규격에 맞는 문자열 형태로 포맷팅
                    formatted_amount = self.exchange.amount_to_precision(symbol, amount)
                    formatted_price = self.exchange.price_to_precision(symbol, sl_price)
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
                    await self.exchange.request(
                        path="algoOrder",
                        api="fapiPrivate",
                        method="POST",
                        params=req,
                        headers={},
                    )
                else:
                    # 다른 일반적인 에러일 시 상단 try문으로 에러 넘김
                    raise e

            await notifier.send_message(
                f"✅ 포지션 진입 완료\n[{symbol}] {signal_type}\n"
                f"체결가: {entry_price:.4f}\n"
                f"TP 지정가: {tp_price}\n"
                f"SL 시장가: {sl_price}\n"
                f"Real R:R: 1 : {abs(real_tp_pct / real_sl_pct) if real_sl_pct != 0 else 0:.2f}"
            )

            self.active_positions[symbol] = True
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
                    if contracts > 0 and sym not in self.active_positions:
                        self.active_positions[sym] = True
                        entry_price = float(p.get("entryPrice", 0))
                        side = p.get("side", "long").upper()

                        logger.info(
                            f"[{sym}] 수동/외부 진입 감지. 봇 메모리에 편입합니다."
                        )
                        async with AsyncSessionLocal() as session:
                            new_trade = Trade(
                                timestamp=(datetime.utcnow() + timedelta(hours=9)),
                                action="MANUAL",
                                symbol=sym,
                                price=entry_price,
                                quantity=contracts,
                                reason=f"외부/수동 진입 감지 ({side})",
                                realized_pnl=0.0,
                                dry_run=settings.DRY_RUN,
                            )
                            session.add(new_trade)
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
                logger.info(f"🧪 [DRY RUN] {symbol} 포지션 가상 청산 및 DB 기록 완료")
                async with AsyncSessionLocal() as session:
                    new_trade = Trade(
                        timestamp=(datetime.utcnow() + timedelta(hours=9)),
                        action="CLOSED",
                        symbol=symbol,
                        price=0.0,
                        quantity=0.0,
                        reason="[DRY_RUN] 가상 매도 청산",
                        realized_pnl=0.0,
                        dry_run=settings.DRY_RUN,
                        params=self._snapshot_params(),
                    )
                    session.add(new_trade)
                    await session.commit()
                symbols_to_remove.append(symbol)
                continue

            current_contracts = position_map.get(symbol, 0.0)
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

                    trades = await self.exchange.fetch_my_trades(symbol, limit=5)
                    realized_pnl = 0.0
                    close_price = 0.0
                    close_qty = 0.0

                    if trades:
                        last_trade = trades[-1]
                        close_price = float(last_trade.get("price", 0.0))
                        close_qty = float(last_trade.get("amount", 0.0))
                        # 선물의 실현 손익 정보는 info 객체의 필드로 들어옵니다.
                        info = last_trade.get("info", {})
                        realized_pnl = float(info.get("realizedPnl", 0.0))

                    logger.info(
                        f"🏁 [{symbol}] 포지션 자동 청산 확인. DB 기록: PnL {realized_pnl:.4f} USDT"
                    )

                    async with AsyncSessionLocal() as session:
                        now_kst = datetime.utcnow() + timedelta(hours=9)
                        new_trade = Trade(
                            timestamp=now_kst,
                            action="CLOSED",
                            symbol=symbol,
                            price=close_price,
                            quantity=close_qty,
                            reason="포지션 종료 감지 (개입/자동)",
                            realized_pnl=realized_pnl,
                            dry_run=settings.DRY_RUN,
                            params=self._snapshot_params(),
                        )
                        session.add(new_trade)

                        # [V16.2 ML] 기존 TradeLog 찾아 청산/성과 데이터 업데이트
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
                                    trade_log.realized_pnl = realized_pnl
                                    trade_log.exit_reason = (
                                        "자동 청산 감지 (TP/SL/탈출)"
                                    )

                                    # 수익률 펀더멘탈 기록 (레버리지 제외 순수 가격 % 변화)
                                    if (
                                        trade_log.execution_price
                                        and trade_log.execution_price > 0
                                    ):
                                        if trade_log.direction == "LONG":
                                            roi = (
                                                close_price - trade_log.execution_price
                                            ) / trade_log.execution_price
                                        else:
                                            roi = (
                                                trade_log.execution_price - close_price
                                            ) / trade_log.execution_price
                                        trade_log.roi_pct = roi * 100
                            except Exception as ml_err:
                                logger.error(
                                    f"[{symbol}] TradeLog 업데이트 중 예외: {ml_err}"
                                )

                        await session.commit()

                        await notifier.send_message(
                            f"🏁 포지션 청산 자동 감지\n[{symbol}]\n"
                            f"종료가: {close_price:.4f}\n"
                            f"실현손익(PnL): {realized_pnl:.4f} USDT"
                        )

                    symbols_to_remove.append(symbol)

                    # 손실이면 해당 종목 쿨다운 설정 (연속 SL 방지)
                    if realized_pnl < 0:
                        cooldown_min = getattr(settings, "LOSS_COOLDOWN_MINUTES", 15)
                        self.loss_cooldown[symbol] = datetime.utcnow() + timedelta(
                            minutes=cooldown_min
                        )
                        logger.info(
                            f"[{symbol}] 손실 청산 → {cooldown_min}분 쿨다운 적용. "
                            f"PnL: {realized_pnl:.4f} USDT"
                        )

                except Exception as e:
                    logger.error(
                        f"[{symbol}] 포지션 청산 확인 및 DB 기록 중 예외 발생: {e}"
                    )

        # 처리 완료된 포지션은 메모리 감시열에서 제거
        for sym in symbols_to_remove:
            del self.active_positions[sym]

    async def check_state_mismatch(self):
        """
        [Fail-Safe 방어 체계]
        거래소 실잔고와 DB/메모리 기록 사이의 불일치를 감지합니다.
        """
        try:
            # 바이낸스 선물 계좌 조회
            balance_info = await self.exchange.fetch_balance()
            usdt_total = balance_info.get("total", {}).get("USDT", 0.0)

            # 보유 선물 포지션 조회 (CCXT fetch_positions)
            if not settings.DRY_RUN:
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
                # 1. 찌꺼기 펜딩 주문(조건부 SL 포함) 일괄 취소
                await self.exchange.cancel_all_orders(symbol)

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
                        params={"symbol": raw_sym, "algoId": algo.get("algoId")},
                    )

                # 2. 시장가 시장 던지기
                if not settings.DRY_RUN:
                    await self.exchange.create_order(
                        symbol=symbol,
                        type="market",
                        side=exit_side,
                        amount=amount,
                        params={"reduceOnly": True},
                    )
                else:
                    logger.info(f"🧪 [DRY RUN] {symbol} Time Exit 가상 시장가 탈출.")

                # DB 기록
                async with AsyncSessionLocal() as session:
                    new_trade = Trade(
                        timestamp=(datetime.utcnow() + timedelta(hours=9)),
                        action="TIME_EXIT",
                        symbol=symbol,
                        price=0.0,
                        quantity=amount,
                        reason=f"TIME_EXIT ({wait_minutes}분 모멘텀 이탈) 탈출",
                        realized_pnl=0.0,  # 정확한 PNL은 거래소 싱크 통해 보정됨
                        dry_run=settings.DRY_RUN,
                    )
                    session.add(new_trade)
                    await session.commit()

                # 알림 발송
                await notifier.send_message(
                    f"🚨 <b>TIME EXIT 발동</b> 🚨\n[{symbol}] {wait_minutes}분 경과로 포지션 스크래치(강제 시장가 청산) 완료."
                )
                del self.active_positions[symbol]

            except Exception as e:
                logger.error(f"[{symbol}] Time Exit 탈출 로직 중 에러: {e}")
