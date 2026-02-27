from datetime import datetime, timezone, timedelta
from config import settings, logger
from database import Trade, AsyncSessionLocal
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
        self.pending_entries = {}

        # 활성 상태인 포지션 메모리 (TP/SL 등 스레드 충돌 확인용)
        self.active_positions = {}

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
                    symbol = algo.get("symbol")
                    algo_id = algo.get("algoId")

                    is_reduce_only = algo.get("reduceOnly")
                    if str(is_reduce_only).lower() == "true":
                        is_reduce_only = True
                    else:
                        is_reduce_only = False

                    # 포지션이 있으면서 reduce_only 파라미터가 켜진(조건부 청산) 주문은 살림
                    if symbol in self.active_positions and is_reduce_only:
                        continue

                    # 고립된 Algo 주문 정리
                    await self.exchange.request(
                        path="algoOrder",
                        api="fapiPrivate",
                        method="DELETE",
                        params={"symbol": symbol, "algoId": algo_id},
                    )
                    canceled_count += 1
                    logger.info(
                        f"🧹 [Algo 정리 완료] 고립된 조건부(SL 등) 찌꺼기 알고 주문 취소: {symbol} (Algo ID: {algo_id})"
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

    async def place_market_entry_order(
        self,
        symbol: str,
        side: str,  # 'buy' or 'sell'
        amount: float,
        reason: str,
    ) -> bool:
        """
        선물 시장에 신규 포지션을 시장가(Market)로 즉각 진입합니다.
        체결 성공 시, 실제 체결가(average price)를 기반으로 TP/SL 주문을 연이어 등록합니다.
        """
        if self.is_halted:
            logger.warning(
                f"시스템이 일시 중지(Halted) 상태입니다. 신규 진입 요청[{symbol}] 거부."
            )
            return False

        # 포지션이 이미 존재하면 추가 진입 억제
        if symbol in self.active_positions:
            logger.info(f"[{symbol}] 이미 활성 포지션이 존재합니다. 진입 생략.")
            return False

        try:
            logger.info(
                f"[{symbol}] 선물 시장가({side}) 즉각 진입 시도. "
                f"수량: {amount} (DRY_RUN: {settings.DRY_RUN})"
            )

            # 레버리지 및 마진 환경 사전 세팅
            await self.setup_margin_and_leverage(symbol)

            signal_type = "LONG" if side == "buy" else "SHORT"
            average_price = 0.0

            if not settings.DRY_RUN:
                # 시장가 진입
                entry_order = await self.exchange.create_order(
                    symbol=symbol,
                    type="market",
                    side=side,
                    amount=amount,
                )

                # 거래소에서 방금 체결한 주문을 다시 조회하여 정확한 average price를 추출
                order_id = entry_order.get("id")
                filled_order = await self.exchange.fetch_order(order_id, symbol)

                average_price = float(
                    filled_order.get("average", filled_order.get("price", 0.0))
                )
                if average_price == 0.0:
                    trades = await self.exchange.fetch_my_trades(symbol, limit=1)
                    if trades:
                        average_price = float(trades[-1].get("price", 0.0))

            else:
                # DRY RUN 일 경우 현재 시장가(Ticker)를 체결가로 임시 가정
                ticker = await self.exchange.fetch_ticker(symbol)
                average_price = float(ticker.get("last", 0.0))

            if average_price <= 0:
                logger.error(
                    f"[{symbol}] 시장가 체결 단가를 확인할 수 없습니다! TP/SL을 전송할 수 없습니다."
                )
                return False

            # 체결 완료 로깅 및 알림
            logger.info(
                f"🎯 [{symbol}] 시장가 진입 체결 성공! 평균 단가: {average_price:.4f}. TP/SL 즉각 계산 및 전송 개시."
            )

            # strategy 모듈 기준의 TP/SL 비율 (하드코딩된 값 대신 상수 관리 고려)
            long_tp_pct = 0.010
            long_sl_pct = 0.005
            short_tp_pct = 0.012
            short_sl_pct = 0.004

            if signal_type == "LONG":
                raw_tp = average_price * (1 + long_tp_pct)
                raw_sl = average_price * (1 - long_sl_pct)
            else:
                raw_tp = average_price * (1 - short_tp_pct)
                raw_sl = average_price * (1 + short_sl_pct)

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
            entry_info = {
                "signal": signal_type,
                "amount": amount,
                "limit_price": average_price,  # reference name maintained for internal calculation
                "tp_price": tp_price,
                "sl_price": sl_price,
            }

            # 동기적(await)으로 TP/SL 즉시 생성 (대기열 통하지 않음)
            success = await self.place_tp_sl_orders(symbol, entry_info)
            if success:
                self.active_positions[symbol] = True

            return True

        except Exception as e:
            logger.error(f"[{symbol}] 시장가 진입 및 TP/SL 세팅 중 예외 발생: {e}")
            return False

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
                new_trade = Trade(
                    timestamp=(datetime.utcnow() + timedelta(hours=9)),
                    action=signal_type,
                    symbol=symbol,
                    price=entry_price,
                    quantity=amount,
                    reason=f"{dr_prefix}VWAP V11 지정가 체결 후 TP/SL 세팅 완료",
                    dry_run=settings.USE_TESTNET,
                )
                session.add(new_trade)
                await session.commit()

            if settings.DRY_RUN:
                logger.info(f"🧪 [DRY RUN] {symbol} TP/SL 가상 주문 완료 및 DB 기록됨")
                self.active_positions[symbol] = True
                return True

            # 1. Take Profit (LIMIT 방식, reduceOnly)
            # 바이낸스 선물 TAKE_PROFIT_LIMIT 또는 단순 LIMIT + reduceOnly 사용
            await self.exchange.create_order(
                symbol=symbol,
                type="limit",
                side=exit_side,
                amount=amount,
                price=tp_price,
                params={"reduceOnly": True},
            )

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
        """
        if not self.active_positions:
            return

        symbols_to_remove = []

        if not settings.DRY_RUN:
            try:
                positions = await self.exchange.fetch_positions()
                position_map = {
                    p["symbol"]: float(p.get("contracts", 0)) for p in positions
                }
            except Exception as e:
                logger.error(f"활성 포지션 검증 중 거래소 조회 에러: {e}")
                return
        else:
            position_map = {}

        for symbol in self.active_positions.keys():
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
                        algo_orders = await self.exchange.request(
                            path="openAlgoOrders",
                            api="fapiPrivate",
                            method="GET",
                            params={"symbol": symbol},
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
                                params={"symbol": symbol, "algoId": algo.get("algoId")},
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
                        new_trade = Trade(
                            timestamp=(datetime.utcnow() + timedelta(hours=9)),
                            action="SELL",
                            symbol=symbol,
                            price=close_price,
                            quantity=close_qty,
                            reason=f"TP 또는 SL에 의한 자동 청산 처리 완료",
                            realized_pnl=realized_pnl,
                            dry_run=settings.DRY_RUN,
                        )
                        session.add(new_trade)
                        await session.commit()

                        await notifier.send_message(
                            f"🏁 포지션 청산 자동 감지\n[{symbol}]\n"
                            f"종료가: {close_price:.4f}\n"
                            f"실현손익(PnL): {realized_pnl:.4f} USDT"
                        )

                    symbols_to_remove.append(symbol)

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
