import asyncio
import json
import logging
import os
import signal
from collections import deque
from datetime import datetime, timezone, timedelta

import aiohttp
import websockets
import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler

from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError
from database import AsyncSessionLocal, MarketData_1m
from utils import clean_json_data
from schemas import HFTFeatures1m
from config import settings, get_logger

logger = get_logger("HFTPipeline")
logger.setLevel(logging.WARNING)  # routine snapshot 로그 무시

# 설정
WS_BASE_URL = (
    "wss://stream.binancefuture.com"
    if getattr(settings, "USE_TESTNET", False)
    else "wss://fstream.binance.com"
)

# REST API 베이스 (OI/펀딩비 조회용)
REST_BASE_URL = (
    "https://testnet.binancefuture.com"
    if getattr(settings, "USE_TESTNET", False)
    else "https://fapi.binance.com"
)

# Ticks는 메모리 상에 최대 2만건만 보관하여 누수 방지
MAX_DEQUE_SIZE = 20000
RETENTION_DAYS = 7
# V18: OI/펀딩비 조회 주기 (초) — Rate Limit 방어
DERIVATIVES_FETCH_INTERVAL = 300  # 5분


class HFTDataPipeline:
    def __init__(self, symbols: list[str]):
        self.symbols = [
            s.lower().replace("/", "").replace(":usdt", "") for s in symbols
        ]
        self.session = None
        self.scaler = RobustScaler()

        # 인메모리 버퍼
        self.orderbook_buffer = {}
        self.trade_buffer = {}
        self.log_volume_history = {}  # V18: 1분 로그 거래량 히스토리 (Z-Score 산출용)

        # [V18] 동시성 제어 락 (주기적인 Snapshot 시 스레드 경합 방지)
        self.buffer_locks = {}
        # [V19] Batch DB Insert Buffer
        self.db_insert_buffer = []

        # V18: OI/펀딩비 캐시
        self._derivatives_cache = {}  # {sym: (oi, funding)}
        self._derivatives_last_fetch = 0  
        self._nofi_cache = {}  

        for sym in self.symbols:
            self.orderbook_buffer[sym] = deque(maxlen=MAX_DEQUE_SIZE)
            self.trade_buffer[sym] = deque(maxlen=MAX_DEQUE_SIZE)
            self.buffer_locks[sym] = asyncio.Lock()
            self.log_volume_history[sym] = deque(maxlen=100)

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
                async with websockets.connect(
                    url, ping_interval=60, ping_timeout=60
                ) as ws:
                    logger.info(f"[HFT] Connected to WS Successfully: {url}")
                    attempt = 0  
                    async for message in ws:
                        await handler(json.loads(message))
            except websockets.exceptions.ConnectionClosed as e:
                attempt += 1
                wait_time = min(2**attempt, 60)
                logger.warning(f"[HFT] WS Closed Code {e.code}. Reconnecting in {wait_time}s...")
                await asyncio.sleep(wait_time)
            except (asyncio.TimeoutError, TimeoutError):
                attempt += 1
                wait_time = min(2**attempt, 60)
                logger.warning(f"[HFT] WS Timeout. Reconnecting in {wait_time}s...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                attempt += 1
                wait_time = min(2**attempt, 60)
                logger.error(f"[HFT] WS Error: {type(e).__name__} - {e}. Reconnecting in {wait_time}s...")
                await asyncio.sleep(wait_time)

    # ── 2. 인메모리 버퍼 적재 핸들러 ──
    async def handle_bookticker(self, msg: dict):
        if "s" not in msg: return
        sym = msg["s"].lower()
        if sym in self.orderbook_buffer:
            record = {
                "timestamp": msg.get("E", 0),
                "bid_price": float(msg["b"]),
                "bid_qty": float(msg["B"]),
                "ask_price": float(msg["a"]),
                "ask_qty": float(msg["A"]),
            }
            async with self.buffer_locks[sym]:
                self.orderbook_buffer[sym].append(record)

    async def handle_aggtrade(self, msg: dict):
        if "s" not in msg: return
        sym = msg["s"].lower()
        if sym in self.trade_buffer:
            record = {
                "timestamp": msg.get("E", 0),
                "price": float(msg["p"]),
                "qty": float(msg["q"]),
                "is_buyer_maker": msg["m"],
            }
            async with self.buffer_locks[sym]:
                self.trade_buffer[sym].append(record)

    async def handle_kline(self, msg: dict):
        """[V19] 1분봉 캔들 마감 이벤트 핸들링 (@kline_1m) - Race Condition 방어"""
        kdata = msg.get("k")
        if not kdata or not kdata.get("x"):  # 마감 봉인 경우만
            return

        sym = msg["s"].lower()
        k_start = kdata["t"]
        k_end = kdata["T"]

        open_p = float(kdata["o"])
        high_p = float(kdata["h"])
        low_p = float(kdata["l"])
        close_p = float(kdata["c"])
        volume = float(kdata["v"])

        snapshot_time = datetime.fromtimestamp(k_start / 1000)

        # 자체 파생 데이터 (OI 등)
        oi, funding = self.fetch_derivatives_data_cached(sym)

        ob_ticks = []
        tr_ticks = []

        # 타임스탬프 슬라이싱 처리
        if sym in self.buffer_locks:
            async with self.buffer_locks[sym]:
                # --- 체결 틱 ---
                while self.trade_buffer[sym] and self.trade_buffer[sym][0]["timestamp"] < k_start:
                    self.trade_buffer[sym].popleft()
                for t in self.trade_buffer[sym]:
                    if t["timestamp"] <= k_end:
                        tr_ticks.append(t)
                    else: break
                for _ in range(len(tr_ticks)):
                    self.trade_buffer[sym].popleft()

                # --- 호가 틱 ---
                while self.orderbook_buffer[sym] and self.orderbook_buffer[sym][0]["timestamp"] < k_start:
                    self.orderbook_buffer[sym].popleft()
                for o in self.orderbook_buffer[sym]:
                    if o["timestamp"] <= k_end:
                        ob_ticks.append(o)
                    else: break
                for _ in range(len(ob_ticks)):
                    self.orderbook_buffer[sym].popleft()

        # 스레드로 미시구조 연산 밀어내기
        asyncio.create_task(
            self._process_1m_kline_async(
                sym, snapshot_time, open_p, high_p, low_p, close_p, volume,
                oi, funding, ob_ticks, tr_ticks
            )
        )

    # Helper 메서드들
    def get_recent_tick_count(self, symbol: str) -> int:
        sym = symbol.lower().replace("/", "").replace(":usdt", "")
        if sym in self.trade_buffer:
            return len(self.trade_buffer[sym])
        return 0

    def get_recent_nofi(self, symbol: str) -> float:
        sym = symbol.lower().replace("/", "").replace(":usdt", "")
        return self._nofi_cache.get(sym, 0.0)

    # ── 3. V18: OI/펀딩비 5분 캐시 로직 ──
    async def _fetch_single_derivatives(self, sym: str) -> tuple:
        raw_sym = sym.upper()
        oi, funding = 0.0, 0.0
        try:
            async with self.session.get(
                f"{REST_BASE_URL}/fapi/v1/openInterest", params={"symbol": raw_sym},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    oi = float(data.get("openInterest", 0.0))
        except Exception:
            pass
        try:
            async with self.session.get(
                f"{REST_BASE_URL}/fapi/v1/premiumIndex", params={"symbol": raw_sym},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    funding = float(data.get("lastFundingRate", 0.0))
        except Exception:
            pass
        return oi, funding

    async def refresh_derivatives_loop(self):
        while True:
            await self.init_session()
            for sym in self.symbols:
                oi, funding = await self._fetch_single_derivatives(sym)
                self._derivatives_cache[sym] = (oi, funding)
                await asyncio.sleep(0.3)
            logger.info(f"[HFT] OI/펀딩비 캐시 갱신 완료 ({len(self._derivatives_cache)}종목)")
            await asyncio.sleep(DERIVATIVES_FETCH_INTERVAL)

    def fetch_derivatives_data_cached(self, sym: str) -> tuple:
        return self._derivatives_cache.get(sym, (0.0, 0.0))

    # ── 4. 미시구조 피처 동기식 계산 ──
    async def _process_1m_kline_async(self, sym, snapshot_time, open_p, high_p, low_p, close_p, volume, oi, funding, ob_ticks, tr_ticks):
        result = await asyncio.to_thread(
            self._compute_features_sync,
            sym, snapshot_time, open_p, high_p, low_p, close_p, volume, oi, funding, ob_ticks, tr_ticks
        )
        if result:
            self.db_insert_buffer.append(result)

    def _compute_features_sync(
        self, sym, timestamp_1m, open_p, high_p, low_p, close_p, volume, oi, funding, ob_ticks, tr_ticks
    ) -> dict | None:
        buy_qty = 0.0
        sell_qty = 0.0
        if tr_ticks:
            df_tr = pd.DataFrame(tr_ticks)
            buy_qty = df_tr[df_tr["is_buyer_maker"] == False]["qty"].sum()
            sell_qty = df_tr[df_tr["is_buyer_maker"] == True]["qty"].sum()

        ofi = buy_qty - sell_qty
        nofi = ofi / volume if volume > 0 else 0.0
        buy_ratio = float(buy_qty / volume) if volume > 0 else 0.5

        spread_avg = 0.0
        if ob_ticks:
            spreads = [
                t["ask_price"] - t["bid_price"] for t in ob_ticks
                if t.get("ask_price", 0) > 0 and t.get("bid_price", 0) > 0
            ]
            if spreads:
                spread_avg = float(np.mean(spreads))

        log_vol_zscore = 0.0
        vol_hist = self.log_volume_history.get(sym, deque(maxlen=100))
        log_vol = float(np.log1p(volume))
        vol_hist.append(log_vol)
        self.log_volume_history[sym] = vol_hist

        if len(vol_hist) >= 20:
            arr = np.array(vol_hist)
            std_val = arr.std()
            if std_val > 0:
                log_vol_zscore = float((log_vol - arr.mean()) / std_val)

        features_dict = HFTFeatures1m(
            ofi_1m=float(ofi),
            nofi_1m=float(nofi),
            open_interest=float(oi),
            funding_rate=float(funding),
            tick_count=len(tr_ticks),
            buy_ratio=buy_ratio,
            spread_avg=spread_avg,
            log_volume_zscore=log_vol_zscore,
        ).model_dump()

        self._nofi_cache[sym] = float(nofi)

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

    # ── 5. DB Bulk Insert & Fallback ──
    async def batch_insert_loop(self):
        """[V19] 5분 주기로 db_insert_buffer Bulk Insert 워커"""
        while True:
            await asyncio.sleep(300)
            await self.flush_db_buffer()

    async def flush_db_buffer(self):
        if not self.db_insert_buffer:
            return

        insert_batch = self.db_insert_buffer[:]
        self.db_insert_buffer.clear()

        try:
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
                        features=clean_json_data(res["features"]),
                    )
                    session.add(new_row)
                await session.commit()
                logger.info(f"[HFT] Successfully bulk-inserted {len(insert_batch)} 1M snapshots.")
        except IntegrityError:
            logger.warning("[HFT] Bulk Insert 중 중복 데이터 발생 건너뜀.")
        except Exception as e:
            logger.error(f"[HFT] Bulk Insert Failed: {e}. Writing to fallback JSONL.")
            self._write_fallback_jsonl(insert_batch)

    def _write_fallback_jsonl(self, batch: list):
        if not batch: return
        filename = f"fallback_1m_{int(datetime.now().timestamp())}.jsonl"
        try:
            with open(filename, "a", encoding="utf-8") as f:
                for item in batch:
                    item_copy = item.copy()
                    if isinstance(item_copy["timestamp"], datetime):
                        item_copy["timestamp"] = item_copy["timestamp"].isoformat()
                    f.write(json.dumps(item_copy) + "\n")
            logger.info(f"[HFT Fallback] {len(batch)} records backup saved to {filename}.")
        except Exception as e:
            logger.error(f"[HFT Fallback] Failed to write local backup: {e}")

    # ── 6. Retention & Bootstrap ──
    async def retention_policy_loop(self):
        while True:
            cutoff = datetime.utcnow() - timedelta(days=RETENTION_DAYS)
            try:
                async with AsyncSessionLocal() as session:
                    stmt = delete(MarketData_1m).where(MarketData_1m.timestamp < cutoff)
                    result = await session.execute(stmt)
                    await session.commit()
                    logger.info(f"[HFT GC] Pruned {result.rowcount} old records (Before {cutoff})")
            except Exception as e:
                logger.error(f"[HFT GC] Pruning Error: {e}")
            await asyncio.sleep(86400)

    async def start(self):
        await self.init_session()

        streams = []
        for sym in self.symbols:
            streams.append(f"{sym}@bookTicker")
            streams.append(f"{sym}@aggTrade")
            streams.append(f"{sym}@kline_1m")

        stream_param = "/".join(streams)

        async def combined_handler(msg):
            if "data" in msg and "e" in msg["data"]:
                evt = msg["data"]["e"]
                if evt == "bookTicker":
                    await self.handle_bookticker(msg["data"])
                elif evt == "aggTrade":
                    await self.handle_aggtrade(msg["data"])
                elif evt == "kline":
                    await self.handle_kline(msg["data"])

        # [V19] Graceful Shutdown: 시그널 발생 시 백업 동작 (동기적 처리로 루프 문제 차단)
        def handle_exit(signum, frame):
            logger.warning(f"⚠️ [HFT Graceful Shutdown] Exit signal({signum}) received! Force-dumping {len(self.db_insert_buffer)} records to JSONL...")
            if self.db_insert_buffer:
                self._write_fallback_jsonl(self.db_insert_buffer)
                self.db_insert_buffer.clear()
            os._exit(0)

        try:
            signal.signal(signal.SIGINT, handle_exit)
            signal.signal(signal.SIGTERM, handle_exit)
        except Exception as e:
            logger.warning(f"[HFT] Could not bind signal handlers (normal on some Windows env): {e}")

        tasks = [
            asyncio.create_task(self.connect_websocket(f"stream?streams={stream_param}", combined_handler)),
            asyncio.create_task(self.refresh_derivatives_loop()),
            asyncio.create_task(self.batch_insert_loop()),
            asyncio.create_task(self.retention_policy_loop()),
        ]

        await asyncio.gather(*tasks)

