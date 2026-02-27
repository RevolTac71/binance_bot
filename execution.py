from datetime import datetime, timezone, timedelta
from config import settings, logger
from database import Trade, AsyncSessionLocal
from data_pipeline import DataPipeline
from notification import notifier


class ExecutionEngine:
    def __init__(self, data_pipeline: DataPipeline):
        self.exchange = data_pipeline.exchange
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
            # 안전을 위해 봇 재시작 시 포지션이 없는 종목의 미체결 주문은 (이전 세션의 진입 대기일 것이므로) 모두 취소
            # Rate Limit(과부하) 방지를 위해 전체 조회가 아닌, 개별 Symbol 단위로 순차적 조회/취소 진행
            try:
                # 거래소에서 현재 거래 가능한 모든 티커를 가져옴
                markets = await self.exchange.load_markets()
                usdt_futures = [
                    s
                    for s in markets.keys()
                    if markets[s].get("linear") and s.endswith("USDT")
                ]
            except Exception as e:
                logger.error(f"거래소 마켓 로드 중 에러: {e}")
                usdt_futures = []

            canceled_count = 0
            # 봇이 모니터링할 Top 티커들을 가져오거나, 전체를 대상으로 빠르게 스캔 (Rate Limit 주의)
            # 선물 시장 전체를 돌면 수백 초가 소요될 수 있으므로, Open Order가 존재하는지 요약 정보만 먼저 확인

            # 봇이 거래하는 대상(활성화 가능한 선물 코인)들만 추림
            target_symbols = [s for s in usdt_futures if "USDT" in s][
                :100
            ]  # 상위 100개로 축약 가능 (선택)

            for symbol in target_symbols:
                # 이미 활성 포지션이 있는 경우는 스킵 (해당 코인의 미체결 주문은 TP/SL로 간주하여 살림)
                if symbol in self.active_positions:
                    continue

                import asyncio

                await asyncio.sleep(0.2)  # API Rate Limit 보호를 위한 0.2초 딜레이

                try:
                    open_orders = await self.exchange.fetch_open_orders(symbol=symbol)
                    for order in open_orders:
                        order_id = order.get("id")
                        await self.exchange.cancel_order(order_id, symbol)
                        canceled_count += 1
                        logger.info(
                            f"🧹 [정리 완료] 포지션이 없는 고립 미체결 주문 취소: {symbol} (Order ID: {order_id})"
                        )
                except Exception as e:
                    # 빈번한 조회 중 특정 코인 에러 발생 시 무시하고 다음 코인 진행
                    pass

            logger.info(
                f"🔄 동기화 완료: 복구된 포지션 {active_count}개, 순차 스캔으로 정리된 대기 주문 {canceled_count}개."
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

    async def place_limit_entry_order(
        self,
        symbol: str,
        side: str,  # 'buy' or 'sell'
        amount: float,
        price: float,
        tp_price: float,
        sl_price: float,
        reason: str,
    ) -> bool:
        """
        선물 시장에 진입 지정가(Limit) 주문을 Post-Only 옵션으로 전송합니다.
        """
        if self.is_halted:
            logger.warning(
                f"시스템이 일시 중지(Halted) 상태입니다. 신규 진입 요청[{symbol}] 거부."
            )
            return False

        # 이미 대기 중인 주문이 있다면 무시
        if symbol in self.pending_entries:
            logger.info(f"[{symbol}] 이미 대기 중인 진입 주문이 존재합니다. 생략.")
            return False

        try:
            logger.info(
                f"[{symbol}] 선물 진입 지정가({side}) 시도. 가격: {price:.4f}, "
                f"수량: {amount} (DRY_RUN: {settings.DRY_RUN})"
            )

            # 레버리지 및 마진 환경 사전 세팅
            await self.setup_margin_and_leverage(symbol)

            order_id = "DRY_RUN_ID"

            # Post-Only (GTX) 지정가 파라미터
            params = {"timeInForce": "GTX", "postOnly": True}

            if not settings.DRY_RUN:
                entry_order = await self.exchange.create_order(
                    symbol=symbol,
                    type="limit",
                    side=side,
                    amount=amount,
                    price=price,
                    params=params,
                )
                order_id = entry_order.get("id")

            # 상태 머신 관리를 위해 대기열 등록
            signal_type = "LONG" if side == "buy" else "SHORT"
            self.pending_entries[symbol] = {
                "order_id": order_id,
                "signal": signal_type,
                "limit_price": price,
                "tp_price": tp_price,
                "sl_price": sl_price,
                "amount": amount,
                "status": "open",
                "created_at": datetime.now(timezone.utc),
            }

            await notifier.send_message(
                f"⏳ 진입 지정가 등록\n[{symbol}] {signal_type}\n수량: {amount}\n"
                f"가격: {price:.4f}\n사유: {reason}"
            )
            return True

        except Exception as e:
            logger.error(f"[{symbol}] 진입 지정가 주문 처리 중 예외 발생: {e}")
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
            # 바이낸스 트리거 주문 설정
            # stopPrice 트리거 시점에 시장가(Market)로 청산됨
            await self.exchange.create_order(
                symbol=symbol,
                type="stop_market",
                side=exit_side,
                amount=amount,
                params={"stopPrice": sl_price, "reduceOnly": True},
            )

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
        상태 머신 (State Machine) 방식의 미체결 지정가 추적루프.
        대기 중인 주문이 체결(Closed)되면 TP/SL을 쏘고,
        만약 체결 이전에 가격이 이미 지나치게 벗어나거나 취소(Canceled)되면 관리망에서 해제합니다.
        """
        if not self.pending_entries:
            return

        symbols_to_remove = []

        for symbol, entry_info in list(self.pending_entries.items()):
            order_id = entry_info["order_id"]

            if settings.DRY_RUN:
                # Dry run 환경에서는 테스트 목적으로 즉시 체결되었다고 가정
                logger.info(f"🧪 [DRY RUN] {symbol} 가상 체결 확인 및 TP/SL 포워딩")
                await self.place_tp_sl_orders(symbol, entry_info)
                symbols_to_remove.append(symbol)
                continue

            # ── 타임아웃(Timeout) 검사: 지정가 매수 후 15분이 지나도 안 잡히면 거래 취소 (시장가 우회 금지) ──
            created_at = entry_info.get("created_at")
            if (
                created_at
                and (datetime.now(timezone.utc) - created_at).total_seconds() > 15 * 60
            ):
                logger.warning(
                    f"⏰ [{symbol}] 지정가 진입 주문 시간 초과(15분). 추세 이탈로 간주하여 주문을 강제 취소합니다."
                )
                await self.cancel_pending_order(
                    symbol, reason="진입 대기 시간 초과(15분)"
                )
                symbols_to_remove.append(symbol)
                continue

            try:
                # 바이낸스 API로 해당 주문 상태 조회
                order_status = await self.exchange.fetch_order(order_id, symbol)
                status = order_status.get("status")

                if status == "closed":
                    # ── 지정가 체결 확인 ──
                    logger.info(
                        f"🎯 [{symbol}] 진입 지정가 체결 성공! TP/SL을 전송합니다."
                    )
                    await self.place_tp_sl_orders(symbol, entry_info)
                    symbols_to_remove.append(symbol)

                elif status in ["canceled", "rejected", "expired"]:
                    # ── 취소 / 거절 ──
                    logger.warning(
                        f"[{symbol}] 진입 지정가 취소/거절 확인 (상태:{status}). 대기열에서 삭제합니다."
                    )
                    symbols_to_remove.append(symbol)

            except Exception as e:
                logger.error(f"[{symbol}] 대기 주문 상태 조회 중 에러: {e}")

        # 완료된/취소된 항목 메모리 해제
        for sym in symbols_to_remove:
            if sym in self.pending_entries:
                del self.pending_entries[sym]

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
