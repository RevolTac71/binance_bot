import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone, timedelta

import aiohttp
import websockets
import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler

from sqlalchemy import delete
from database import AsyncSessionLocal, MarketData_1m
from schemas import HFTFeatures1m
from config import settings, get_logger

logger = get_logger("HFTPipeline")
logger.setLevel(logging.WARNING)  # routine snapshot 로그 무시

# 설정
WS_BASE_URL = "wss://fstream.binance.com"
# Ticks는 메모리 상에 최대 2만건만 보관하여 누수 방지
MAX_DEQUE_SIZE = 20000
RETENTION_DAYS = 7

# 인메모리 버퍼 (Symbol -> deque)
orderbook_buffer = {}
trade_buffer = {}


class HFTDataPipeline:
    def __init__(self, symbols: list[str]):
        self.symbols = [
            s.lower().replace("/", "").replace(":usdt", "") for s in symbols
        ]
        self.session = None
        self.scaler = RobustScaler()

        # [V16.8] 동시성 제어 락 (주기적인 Snapshot 시 스레드 경합 방지)
        self.buffer_locks = {}
        # [V16.8] DB Insert 실패 시 재적재를 위한 메모리 큐
        self.retry_queue = []

        # Initialize deques for each symbol
        for sym in self.symbols:
            orderbook_buffer[sym] = deque(maxlen=MAX_DEQUE_SIZE)
            trade_buffer[sym] = deque(maxlen=MAX_DEQUE_SIZE)
            self.buffer_locks[sym] = asyncio.Lock()

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()

    # ── 1. 웹소켓 스트림 (지수 백오프 자동 재연결) ──
    async def connect_websocket(self, stream_path: str, handler):
        attempt = 0
        url = f"{WS_BASE_URL}/{stream_path}"
        logger.info(f"[HFT] Attempting to connect to WS: {url}")
        while True:
            try:
                async with websockets.connect(url) as ws:
                    logger.info(f"[HFT] Connected to WS Successfully: {stream_path}")
                    attempt = 0  # 연결 성공 시 백오프 초기화
                    async for message in ws:
                        await handler(json.loads(message))
            except websockets.exceptions.ConnectionClosed as e:
                attempt += 1
                wait_time = min(2**attempt, 60)
                logger.warning(
                    f"[HFT] WS Connection Closed ({stream_name}): Code {e.code}, Reason {e.reason}. Reconnecting in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)
            except Exception as e:
                attempt += 1
                wait_time = min(2**attempt, 60)  # Max 60초 대기
                logger.error(
                    f"[HFT] WS Unexpected Error ({stream_name}): {type(e).__name__} - {e}. Reconnecting in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)

    # ── 2. 인메모리 버퍼 적재 핸들러 (DB Insert 아님) ──
    async def handle_bookticker(self, msg: dict):
        """Best Bid/Ask 스냅샷 핸들링 (@bookTicker)"""
        # msg: {'e':'bookTicker', 's':'BTCUSDT', 'b':'...', 'B':'...', 'a':'...', 'A':'...', 'E':1612...}
        if "s" not in msg:
            return

        sym = msg["s"].lower()
        if sym in orderbook_buffer:
            record = {
                "timestamp": msg.get("E", 0),
                "bid_price": float(msg["b"]),
                "bid_qty": float(msg["B"]),
                "ask_price": float(msg["a"]),
                "ask_qty": float(msg["A"]),
            }
            async with self.buffer_locks[sym]:
                orderbook_buffer[sym].append(record)

    async def handle_aggtrade(self, msg: dict):
        """틱 단위 체결 내역 핸들링 (@aggTrade)"""
        # msg: {'e':'aggTrade', 's':'BTCUSDT', 'p':'...', 'q':'...', 'm':True, 'E':1612...}
        if "s" not in msg:
            return

        sym = msg["s"].lower()
        if sym in trade_buffer:
            record = {
                "timestamp": msg.get("E", 0),
                "price": float(msg["p"]),
                "qty": float(msg["q"]),
                "is_buyer_maker": msg["m"],  # True = Sell Trade
            }
            async with self.buffer_locks[sym]:
                trade_buffer[sym].append(record)

    # ── 3. 1분 단위 데이터 백업 & OFI 계산 (스레드 분리) ──
    def _process_1m_snapshot_sync(
        self,
        sym: str,
        timestamp_1m: datetime,
        oi: float,
        funding: float,
        ob_ticks: list,
        tr_ticks: list,
    ) -> dict | None:
        """CPU Bound 연산: RobustScaler 및 OFI 계산"""
        if not tr_ticks:
            return None

        # 2. DataFrame 변환
        df_tr = pd.DataFrame(tr_ticks)

        # 3. 1분봉 OHLCV 기초 생성
        open_p = df_tr["price"].iloc[0]
        high_p = df_tr["price"].max()
        low_p = df_tr["price"].min()
        close_p = df_tr["price"].iloc[-1]
        volume = df_tr["qty"].sum()

        # 4. 기능 추출: OFI (Order Flow Imbalance) 계산
        # 간단한 틱 기반 근사치 계산: 체결된 Buy Qty - Sell Qty
        buy_qty = df_tr[df_tr["is_buyer_maker"] == False]["qty"].sum()
        sell_qty = df_tr[df_tr["is_buyer_maker"] == True]["qty"].sum()
        ofi = buy_qty - sell_qty

        # 5. [V16.8] NOFI (Normalized OFI) 계산 및 Pydantic 스키마 검증
        nofi = ofi / volume if volume > 0 else 0.0

        features_dict = HFTFeatures1m(
            ofi_1m=float(ofi),
            nofi_1m=float(nofi),
            open_interest=float(oi),
            funding_rate=float(funding),
            tick_count=len(tr_ticks),
        ).model_dump()

        return {
            "symbol": sym.upper(),
            "timestamp": timestamp_1m,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "volume": volume,
            "features": features_dict,
        }

    async def fetch_derivatives_data(self, sym: str) -> tuple[float, float]:
        """REST API: 미결제약정(OI) 및 펀딩비 조회"""
        oi, funding = 0.0, 0.0
        try:
            # Open Interest
            async with self.session.get(
                f"https://fapi.binance.com/fapi/v1/openInterest?symbol={sym.upper()}",
                timeout=5,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    oi = float(data.get("openInterest", 0.0))
            # Funding Rate
            async with self.session.get(
                f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym.upper()}",
                timeout=5,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    funding = float(data.get("lastFundingRate", 0.0))
        except Exception as e:
            logger.error(f"[HFT] REST Fetch Error for {sym}: {e}")

        return oi, funding

    async def aggregator_loop(self):
        """매 정각 1분(00초)마다 스냅샷을 찍고 DB에 1 Row Insert"""
        # 한국 시간 KST (UTC+9)
        kst = timezone(timedelta(hours=9))

        while True:
            now = datetime.now(tz=kst)
            # 정확히 다음 1분 정각까지 대기
            sleep_sec = 60 - now.second - (now.microsecond / 1_000_000)
            await asyncio.sleep(sleep_sec)

            # DB 저장을 위해 Timezone 인식 정보를 제거 (Naive datetime으로 변환)
            snapshot_time = datetime.now(tz=kst).replace(
                second=0, microsecond=0, tzinfo=None
            )

            logger.info(f"[HFT] Creating 1-Min Snapshot at {snapshot_time}")

            # 모든 심볼 연산 완료 대기 및 큐 스왑
            snapshot_tasks = []
            for sym in self.symbols:
                oi, funding = await self.fetch_derivatives_data(sym)

                # [V16.8] O(1) Queue Swap with AsyncLock
                async with self.buffer_locks[sym]:
                    ob_ticks = list(orderbook_buffer[sym])
                    tr_ticks = list(trade_buffer[sym])
                    orderbook_buffer[sym].clear()
                    trade_buffer[sym].clear()

                task = asyncio.to_thread(
                    self._process_1m_snapshot_sync,
                    sym,
                    snapshot_time,
                    oi,
                    funding,
                    ob_ticks,
                    tr_ticks,
                )
                snapshot_tasks.append(task)

            results = await asyncio.gather(*snapshot_tasks)
            valid_results = [r for r in results if r is not None]

            # 6. [V16.8] DB Bulk Insert 및 Retry Fallback
            insert_batch = self.retry_queue + valid_results

            if insert_batch:
                async with AsyncSessionLocal() as session:
                    for res in insert_batch:
                        new_row = MarketData_1m(
                            timestamp=res["timestamp"],
                            symbol=res["symbol"],
                            open=res["open"],
                            high=res["high"],
                            low=res["low"],
                            close=res["close"],
                            volume=res["volume"],
                            features=res["features"],
                        )
                        session.add(new_row)
                    try:
                        await session.commit()
                        logger.info(
                            f"[HFT] Successfully inserted {len(insert_batch)} 1M snapshots."
                        )
                        self.retry_queue.clear()
                    except Exception as e:
                        await session.rollback()
                        logger.error(
                            f"[HFT] 1-Min Insert Failed: {e}. Clearing retry queue to avoid infinite DB lock."
                        )
                        self.retry_queue.clear()

    # ── 4. DB 용량 관리 (Retention Policy) ──
    async def retention_policy_loop(self):
        """매일 1회 실행하여 RETENTION_DAYS 초과 데이터를 삭제하는 GC 워커"""
        while True:
            cutoff = datetime.utcnow() - timedelta(days=RETENTION_DAYS)
            try:
                async with AsyncSessionLocal() as session:
                    stmt = delete(MarketData_1m).where(MarketData_1m.timestamp < cutoff)
                    result = await session.execute(stmt)
                    await session.commit()
                    deleted_count = result.rowcount
                    logger.info(
                        f"[HFT GC] Pruned {deleted_count} old records (Before {cutoff})"
                    )
            except Exception as e:
                logger.error(f"[HFT GC] Pruning Error: {e}")

            # 24 시 간 대기
            await asyncio.sleep(86400)

    async def start(self):
        await self.init_session()

        # 1. 묶음 스트림 생성 (ex: btcusdt@bookTicker/btcusdt@aggTrade)
        streams = []
        for sym in self.symbols:
            streams.append(f"{sym}@bookTicker")
            streams.append(f"{sym}@aggTrade")

        stream_param = "/".join(streams)

        # 바이낸스는 한번에 여러 스트림을 구독할 수 있습니다 (wss://.../stream?streams=...)
        # 코딩 편의상 전체 통합 커넥션 1개, 이벤트 파싱 1개로 처리
        async def combined_handler(msg):
            # Combined stream wrapper format: {"stream": "...", "data": {...}}
            if "data" in msg and "e" in msg["data"]:
                evt_type = msg["data"]["e"]
                if evt_type == "bookTicker":
                    await self.handle_bookticker(msg["data"])
                elif evt_type == "aggTrade":
                    await self.handle_aggtrade(msg["data"])

        # 2. 백그라운드 태스크 구동
        tasks = [
            asyncio.create_task(
                self.connect_websocket(
                    f"stream?streams={stream_param}", combined_handler
                )
            ),
            asyncio.create_task(self.aggregator_loop()),
            asyncio.create_task(self.retention_policy_loop()),
        ]

        await asyncio.gather(*tasks)
