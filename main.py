import asyncio
import sys
import json
import aiohttp
from datetime import datetime, timezone, timedelta
import pandas as pd
from config import logger, settings
import numpy as np
import traceback
import gc
import time

# [V18.5.1] 데이터 부족/정적 데이터로 인한 Numpy 경고(0나누기 등) 전역 억제
np.seterr(divide="ignore", invalid="ignore")
from database import check_db_connection, MarketSnapshot, AsyncSessionLocal
from data_pipeline import DataPipeline
from strategy import StrategyEngine, PortfolioState
from risk_management import RiskManager
from execution import ExecutionEngine
from notification import notifier
from telegram_commands import setup_telegram_bot
from hft_pipeline import HFTDataPipeline


def get_today_0000_utc_timestamp() -> int:
    """
    현재 시각을 기준으로 가장 최근의 당일 00:00 UTC 타임스탬프(ms)를 계산합니다.
    (V18 시간축 혼동 방지를 위해 UTC 자정 기준으로 통일)
    """
    now_utc = datetime.now(timezone.utc)
    target_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(target_utc.timestamp() * 1000)


def calc_next_refresh_seconds() -> float:
    """
    다음 종목 리프레시 시점(UTC 02:15부터 3시간 간격)까지 남은 초(seconds)를 계산합니다.
    (예: 02:15, 05:15, 08:15, 11:15, 14:15, 17:15, 20:15, 23:15)
    """
    now_utc = datetime.now(timezone.utc)

    # 설정된 간격(SYMBOL_REFRESH_INTERVAL)으로 오늘 후보 생성
    interval = settings.SYMBOL_REFRESH_INTERVAL
    candidates = []
    # 02:15부터 시작하여 24시간을 해당 간격으로 나눈 후보 생성
    for h in range(2, 26, interval):
        if h < 24:
            candidates.append(
                now_utc.replace(hour=h, minute=15, second=0, microsecond=0)
            )
        else:
            # 다음날 첫 후보
            candidates.append(
                now_utc.replace(hour=2, minute=15, second=0, microsecond=0)
                + timedelta(days=1)
            )

    # 현재 시각 이후의 가장 빠른 후보를 찾음
    for target in candidates:
        if target > now_utc:
            return (target - now_utc).total_seconds()

    return interval * 3600  # Fallback


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


async def warm_up_differential_data(new_symbols: set, pipeline: DataPipeline):
    """
    동적 리프레시 시 새로 추가된 종목(New Tickers)들에 대해서만 데이터를 웜업합니다.
    """
    if not new_symbols:
        return

    global df_map, htf_df_1h, htf_df_15m
    logger.info(f"🆕 신규 편입 종목 웜업 시작: {new_symbols}")

    # [V18] TIMEFRAME 동적 파싱을 통한 최소 1000봉 확보
    tf_str = getattr(settings, "TIMEFRAME", "3m")
    tf_min = int(tf_str[:-1]) if tf_str.endswith("m") else (int(tf_str[:-1]) * 60 if tf_str.endswith("h") else 3)
    since_ts = get_today_0000_utc_timestamp() - (tf_min * 1000 * 60 * 1000)

    tasks_3m = [
        pipeline.fetch_ohlcv_since(sym, timeframe=settings.TIMEFRAME, since=since_ts)
        for sym in new_symbols
    ]
    tasks_1h = [
        pipeline.fetch_ohlcv_htf(sym, timeframe=settings.HTF_TIMEFRAME_1H, limit=300)
        for sym in new_symbols
    ]
    tasks_15m = [
        pipeline.fetch_ohlcv_htf(sym, timeframe=settings.HTF_TIMEFRAME_15M, limit=200)
        for sym in new_symbols
    ]

    results_3m, results_1h, results_15m = await asyncio.gather(
        asyncio.gather(*tasks_3m, return_exceptions=True),
        asyncio.gather(*tasks_1h, return_exceptions=True),
        asyncio.gather(*tasks_15m, return_exceptions=True),
    )

    for sym, res in zip(new_symbols, results_3m):
        if isinstance(res, Exception):
            logger.error(f"[{sym}] 웜업 3m 데이터 로딩 실패: {res}")
            continue
        df_map[sym] = res

    for sym, res_1h, res_15m in zip(new_symbols, results_1h, results_15m):
        if isinstance(res_1h, Exception):
            htf_df_1h[sym] = None
        else:
            htf_df_1h[sym] = res_1h

        if isinstance(res_15m, Exception):
            htf_df_15m[sym] = None
        else:
            htf_df_15m[sym] = res_15m

        if htf_df_1h.get(sym) is not None and htf_df_15m.get(sym) is not None:
            htf_df_1h[sym], htf_df_15m[sym] = pipeline.calculate_htf_indicators(
                htf_df_1h[sym], htf_df_15m[sym]
            )


# ── Background Loops ─────────────────────────────────────────────────────────
# 3분봉 데이터 (15종목 × 1500개)
df_map: dict[str, pd.DataFrame] = {}

# [V18] 상위 타임프레임 데이터 (종목별 1H / 15m)
htf_df_1h: dict[str, pd.DataFrame] = {}
htf_df_15m: dict[str, pd.DataFrame] = {}

# V18 포트폴리오 전역 상태 (단일 인스턴스 공유)
portfolio = PortfolioState()

# [V18] CVD 실시간 틱 누적 공간
cvd_data: dict[str, float] = {}
# 캔들 마감 시점의 CVD 스냅샷 저장 (추세 판단용)
cvd_history: dict[str, list] = {}

# [V18 ML] 호가창 불균형(Imbalance) TWAP 내역 및 스냅샷 큐
imbalance_history: dict[str, list] = {}
snapshot_queue: list[dict] = []

# [V18.6] 동시 진입 시그널 우선순위 큐
entry_signal_queue: list[dict] = []
entry_signal_lock = asyncio.Lock()


async def warm_up_data(symbols: list, pipeline: DataPipeline):
    """
    최초 접속 혹은 재접속 시 이전 데이터를 로드하여 지표 연속성을 확보합니다.
    V18 3분봉에 더해 1H / 15m 상위 타임프레임 데이터도 함께 웜업합니다.
    """
    global df_map, htf_df_1h, htf_df_15m

    # [V18] TIMEFRAME 동적 파싱을 통한 최소 1000봉 확보
    tf_str = getattr(settings, "TIMEFRAME", "3m")
    tf_min = int(tf_str[:-1]) if tf_str.endswith("m") else (int(tf_str[:-1]) * 60 if tf_str.endswith("h") else 3)
    since_ts = get_today_0000_utc_timestamp() - (tf_min * 1000 * 60 * 1000)

    logger.info(
        f"[WarmUp] TIMEFRAME='{settings.TIMEFRAME}', HTF_1H='{settings.HTF_TIMEFRAME_1H}', HTF_15M='{settings.HTF_TIMEFRAME_15M}'"
    )

    # 3분봉 로드 태스크
    tasks_3m = [
        pipeline.fetch_ohlcv_since(sym, timeframe=settings.TIMEFRAME, since=since_ts)
        for sym in symbols
    ]

    # [V18] 1H·15m 로드 태스크 (동시 병렬 처리)
    # 장기 지표(EMA200, ATR200 등) 수렴(Smoothing)을 위해 최소 1000개 로드
    tasks_1h = [
        pipeline.fetch_ohlcv_htf(sym, timeframe=settings.HTF_TIMEFRAME_1H, limit=1000)
        for sym in symbols
    ]
    tasks_15m = [
        pipeline.fetch_ohlcv_htf(sym, timeframe=settings.HTF_TIMEFRAME_15M, limit=1000)
        for sym in symbols
    ]

    results_3m = await asyncio.gather(*tasks_3m, return_exceptions=True)
    results_1h, results_15m = await asyncio.gather(
        asyncio.gather(*tasks_1h, return_exceptions=True),
        asyncio.gather(*tasks_15m, return_exceptions=True),
    )

    for sym, res in zip(symbols, results_3m):
        if isinstance(res, Exception):
            logger.error(f"[{sym}] 웜업 데이터 로딩 실패: {res}")
            continue
        # [V18] float32 다운캐스팅으로 메모리 50% 절감
        df_map[sym] = res.astype(
            {col: "float32" for col in res.select_dtypes(include=["float64"]).columns}
        )
        logger.info(
            f"[{sym}] {settings.TIMEFRAME} 캔들 초기 데이터 {len(res)}개 장전 완료."
        )

    gc.collect()  # 웜업 후 파편화된 메모리 정리

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
            try:
                htf_df_1h[sym], htf_df_15m[sym] = pipeline.calculate_htf_indicators(
                    htf_df_1h[sym], htf_df_15m[sym]
                )
                logger.info(f"[{sym}] HTF(1H/15m) 지표 웜업 완료.")
            except Exception as e:
                logger.error(f"[{sym}] HTF(1H/15m) 지표 웜업 중 치명적 예외 발생: {e}")
                logger.error(traceback.format_exc())
                continue


async def process_closed_kline(
    symbol: str,
    kline: dict,
    pipeline: DataPipeline,
    strategy: StrategyEngine,
    risk: RiskManager,
    execution: ExecutionEngine,
    interval: str = "3m",
):
    """
    웹소켓으로 수신된 '마감된(x: True)' 캔들을 기존 df에 병합하고 판단을 내립니다.
    V18 HTF 데이터(df_1h, df_15m)와 PortfolioState를 strategy에 함께 전달합니다.
    """
    if symbol not in df_map:
        return

    # [V18.2] 데이터 업데이트는 포지션 유무와 상관없이 지속 (지표 연속성 확보)
    # 진입 체크만 하단에서 필터링합니다.

    try:
        new_ts = int(kline["t"])
        new_dt = pd.to_datetime(new_ts, unit="ms") + pd.Timedelta(
            hours=9
        )  # UTC → KST 통일

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
        )
        # [V18] float32 타겟팅: LossySetitemError(float64->float32) 방지
        new_row = new_row.astype(
            {col: "float32" for col in ["open", "high", "low", "close", "volume"]}
        )
        new_row.set_index("datetime", inplace=True)

        df = df_map[symbol]

        # 캔들 병합 (웹소켓 중복 수신 방어)
        if new_dt in df.index:
            df.loc[new_dt] = new_row.iloc[0]
        else:
            df.loc[new_dt] = new_row.iloc[0]

        # [V18] 메모리 점유 방지를 위해 1000개만 유지 및 다운캐스팅
        df = df.astype(
            {col: "float32" for col in df.select_dtypes(include=["float64"]).columns}
        ).tail(1000)
        df_map[symbol] = df
        gc.collect()  # 매 캔들 마감 연산 후 GC 호출 (선택적)

        curr_df = df_map[symbol]

        if is_funding_fee_cutoff():
            # 펀딩비 시간대면 캔들 저장만 하고 진입은 하지 않음
            return

        # 1. 3분봉 지표 연산 (비동기 블로킹 방지를 위해 스레드 위임)
        df_ind = await asyncio.to_thread(
            pipeline.calculate_vwap_indicators, curr_df.copy()
        )

        # [V18] 상위 타임프레임 데이터 참조 (htf_refresh_loop가 주기적으로 갱신)
        df_1h = htf_df_1h.get(symbol)
        df_15m = htf_df_15m.get(symbol)

        # [V18 ML] Snapshot Feature 수집 및 Queue 적재
        imbal_list = imbalance_history.get(symbol, [])
        twap_imbalance = sum(imbal_list) / len(imbal_list) if imbal_list else 0.5

        funding_rate = await pipeline.fetch_funding_rate(symbol)
        fr_match = 1 if funding_rate > 0 else (-1 if funding_rate < 0 else 0)

        # 최신 지표 파싱 (안전하게 get 사용, 없을 시 0.0)
        curr_atr_14 = float(df_ind.iloc[-1].get("ATR_14", 0))
        curr_atr_200 = float(df_ind.iloc[-1].get("ATR_200", 0))
        curr_rsi = float(df_ind.iloc[-1].get("RSI_14", 50))

        macd_h = (
            float(df_15m.iloc[-1].get("MACD_H", 0))
            if df_15m is not None and not df_15m.empty
            else 0.0
        )
        adx_14 = (
            float(df_15m.iloc[-1].get("ADX_14", 0))
            if df_15m is not None and not df_15m.empty
            else 0.0
        )

        curr_price = float(df_ind.iloc[-1]["close"])
        ema_1h_dist = (
            float(
                (curr_price - df_1h.iloc[-1].get("EMA_50", curr_price))
                / df_1h.iloc[-1].get("EMA_50", curr_price)
            )
            if df_1h is not None and not df_1h.empty
            else 0.0
        )
        ema_15m_dist = (
            float(
                (curr_price - df_15m.iloc[-1].get("EMA_50", curr_price))
                / df_15m.iloc[-1].get("EMA_50", curr_price)
            )
            if df_15m is not None and not df_15m.empty
            else 0.0
        )

        current_cvd = cvd_data.get(symbol, 0.0)
        hist = cvd_history.setdefault(symbol, [])

        # CVD 5m / 15m 델타 (누적값 차이) 계산
        # hist에는 이전 캔들들의 마감 시점 CVD 누적값이 들어있음
        cvd_5m_delta = (current_cvd - hist[-2]) if len(hist) >= 2 else 0.0
        cvd_15m_delta = (current_cvd - hist[-5]) if len(hist) >= 5 else cvd_5m_delta
        cvd_slope = (current_cvd - hist[-1]) if len(hist) > 0 else 0.0

        hist.append(current_cvd)
        if len(hist) > 20:
            hist.pop(0)

        cvd_trend = (
            "BUY_PRESSURE"
            if cvd_slope > 0
            else ("SELL_PRESSURE" if cvd_slope < 0 else None)
        )

        # V18: 현재 종목과 활성 포지션 간 최대 상관계수 산출
        max_corr = 0.0
        if df_15m is not None and not df_15m.empty:
            target_returns = df_15m["close"].pct_change().dropna().tail(100)
            if len(target_returns) >= 50:
                for active_sym, pos_info in portfolio.positions.items():
                    active_df = htf_df_15m.get(active_sym)
                    if active_df is not None and not active_df.empty:
                        active_returns = (
                            active_df["close"].pct_change().dropna().tail(100)
                        )
                        aligned = pd.concat(
                            [target_returns, active_returns], axis=1, join="inner"
                        ).dropna()
                        if len(aligned) >= 50:
                            # [V18.5] 0으로 나누기 방지 및 상관관계 산출
                            if (
                                aligned.iloc[:, 0].std() > 1e-9
                                and aligned.iloc[:, 1].std() > 1e-9
                            ):
                                corr = aligned.iloc[:, 0].corr(aligned.iloc[:, 1])
                                if not pd.isna(corr):
                                    max_corr = max(max_corr, abs(corr))

        # [V18] HFT 파이프라인에서 최신 미시구조 피처(OI, Tick Count 등) 조회
        hft_feats = {"open_interest": 0.0, "funding_rate": 0.0, "tick_count": 0}
        if "hft_pipeline" in globals():
            hft_pipe = globals()["hft_pipeline"]
            try:
                raw_sym = symbol.lower().replace("/", "").replace(":usdt", "")
                oi, funding = hft_pipe.fetch_derivatives_data_cached(raw_sym)
                hft_feats = {
                    "open_interest": oi,
                    "funding_rate": funding,
                    "tick_count": hft_pipe.get_recent_tick_count(raw_sym),
                    "nofi_1m": hft_pipe.get_recent_nofi(raw_sym),  # 신규 추가
                }
            except Exception as hft_err:
                logger.warning(f"[{symbol}] HFT 피처 조회 중 일시적 오류: {hft_err}")

        snapshot = {
            "timestamp": new_dt.to_pydatetime()
            if hasattr(new_dt, "to_pydatetime")
            else new_dt,
            "symbol": symbol,
            "rsi": curr_rsi,
            "macd_hist": macd_h,
            "adx": adx_14,
            "atr_14": curr_atr_14,
            "atr_200": curr_atr_200,
            "ema_1h_dist": ema_1h_dist,
            "ema_15m_dist": ema_15m_dist,
            "cvd_5m_sum": float(cvd_5m_delta),
            "cvd_15m_sum": float(cvd_15m_delta),
            "cvd_delta_slope": float(cvd_slope),
            "bid_ask_imbalance": float(twap_imbalance),
            "funding_rate_match": int(fr_match),  # 필드명 및 타입 수정
            "log_vol_zscore": float(
                df_ind.iloc[-1].get("Log_Vol_ZScore", 0.0)
            ),  # 명칭 단축
            "correlation_max": float(max_corr),
            "nofi_1m": float(hft_feats.get("nofi_1m", 0.0)),  # 필드 추가
        }

        # 3. [V18.1] 신규 진입 필터 (데이터 업데이트 이후 수행)
        if symbol in execution.active_positions or symbol in execution.pending_entries:
            # 포지션이 있더라도 마감 스냅샷 기록은 진행
            snapshot["long_score"] = 0
            snapshot["short_score"] = 0
            snapshot_queue.append(snapshot)
            return

        decision = strategy.check_entry(
            symbol=symbol,
            df=df_ind,
            portfolio=portfolio,
            df_1h=df_1h,
            df_15m=df_15m,
            cvd_trend=cvd_trend,
            bid_ask_imbalance=twap_imbalance,
            all_htf_15m=htf_df_15m,
            hft_features=hft_feats,
            interval=interval,
        )

        # 3. [V18] 스코어 및 확장 피처를 MarketSnapshot에 기록 (DB 적재용)
        snapshot["buy_ratio"] = decision.get("buy_ratio", 0.5)
        snapshot["long_score"] = decision.get("long_score")
        snapshot["short_score"] = decision.get("short_score")

        snapshot_queue.append(snapshot)

        if decision["signal"]:
            # [V19] 명시적으로 선물(future) 계좌만 조회하여 Margin API 호출 및 타임아웃 방지
            balance_info = await pipeline.exchange.fetch_balance({"type": "future"})
            capital = balance_info.get("total", {}).get("USDT", 0.0)

            if settings.DRY_RUN:
                capital = 1000.0

            if capital < risk.min_order_usdt:
                logger.warning(f"⚠️ 전체 선물 잔고 부족({capital:.2f} USDT). 패스.")
                return

            market_price = decision["market_price"]
            reason = decision["reason"]
            atr_val = decision.get("atr_val", market_price * 0.005)

            # 3. 투입 사이즈 산출 (V18.4: 진입 방향별 차등 TP/SL 반영)
            sizing = await risk.calculate_position_size(
                symbol,
                capital,
                market_price,
                atr_val,
                entry_type=decision.get("entry_type", "TREND_MACD"),
                direction=(1 if decision["signal"] == "LONG" else -1),
            )

            if sizing["size"] <= 0:
                return

            qty = sizing["size"]
            side = "buy" if decision["signal"] == "LONG" else "sell"

            signal_score = (
                decision["long_score"]
                if decision["signal"] == "LONG"
                else decision["short_score"]
            )
            logger.info(
                f"[Signal Queue] 🎯 {symbol} 진입 시그널 큐에 대기 등록 (방향={side.upper()}, 점수={signal_score})"
            )

            async with entry_signal_lock:
                entry_signal_queue.append(
                    {
                        "symbol": symbol,
                        "side": side,
                        "direction": decision["signal"],
                        "market_price": market_price,
                        "atr_val": atr_val,
                        "amount": qty,
                        "reason": reason,
                        "tp_dist": sizing["tp_dist"],
                        "sl_dist": sizing["sl_dist"],
                        "market_data": decision.get("market_data"),
                        "signal_score": signal_score,
                        "volume_usd": float(curr_df["volume"].iloc[-1] * market_price),
                        "kline_close_time": int(kline.get("T", time.time() * 1000)),
                    }
                )

    except Exception as e:
        logger.error(f"[{symbol}] KLINE 마감 처리 중 에러: {e}")
        logger.error(traceback.format_exc())


async def htf_refresh_loop(pipeline: DataPipeline):
    """
    [V18] 15분마다 1H·15m 상위 타임프레임 데이터를 갱신하는 독립 루프.
    WebSocket 루프와 별도로 asyncio.create_task()로 병렬 가동됩니다.

    갱신 주기: 15분 (15m 봉 마감 주기와 동일하게 설정)
    실패 시:   경고 로그만 남기고 계속 실행 (봇 전체 다운 방지)
    """
    global htf_df_1h, htf_df_15m

    while True:
        # 15분 대기 후 갱신 (첫 실행은 warm_up에서 이미 로드되었으므로 대기 먼저)
        await asyncio.sleep(15 * 60)

        symbols = getattr(settings, "CURRENT_TARGET_SYMBOLS", [])
        if not symbols:
            continue

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


async def orderbook_twap_loop(pipeline: DataPipeline):
    """
    [V18 ML] 매 5초 단위로 15개 종목의 오더북 Imbalance를 폴링하여
    지속적으로 기록해 두고, 최근 6회(30초)의 TWAP을 산출하기 위한 메인 루프.
    """
    global imbalance_history
    while True:
        try:
            symbols = getattr(settings, "CURRENT_TARGET_SYMBOLS", [])
            if not symbols:
                await asyncio.sleep(5)
                continue

            tasks = [pipeline.fetch_orderbook_imbalance(sym) for sym in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for sym, res in zip(symbols, results):
                if isinstance(res, Exception):
                    continue
                hist = imbalance_history.setdefault(sym, [])
                hist.append(res)
                # 최근 30초 분량(폴링 주기 5초 => 6개)만 남기고 롤링
                if len(hist) > 6:
                    hist.pop(0)
        except Exception as e:
            logger.error(f"[Orderbook TWAP] 오류: {e}")
        await asyncio.sleep(5)


async def snapshot_flush_loop():
    """
    [V18 ML] DB 쓰기 병목(I/O 부하) 방지를 위해 큐에 쌓인
    전 종목의 MarketSnapshot 데이터를 단일 트랜잭션으로 bulk_insert 합니다.
    """
    global snapshot_queue
    while True:
        await asyncio.sleep(10)  # 10초마다 큐 점검
        if len(snapshot_queue) > 0:
            # 캔들 마감이 종목별로 동시 다발적으로 이뤄지므로 전부 찰 때까지 잠시(3초) 대기
            await asyncio.sleep(3)

            items_to_insert = snapshot_queue[:]
            snapshot_queue.clear()

            try:
                # SQLAlchemy ORM add_all을 활용한 Batch Insert
                async with AsyncSessionLocal() as session:
                    records = [MarketSnapshot(**item) for item in items_to_insert]
                    session.add_all(records)
                    await session.commit()
                # 불필요한 로그 생략 (정상 작동 시 조용히)
            except Exception as e:
                logger.error(f"[Snapshot Bulk Insert] 처리 실패: {e}")


async def signal_processor_loop(execution: ExecutionEngine):
    """
    [V18.6] 여러 종목의 캔들이 동시에 마감될 때 발생하는 복수의 진입 시그널을 수집하여,
    1. 초과 점수(excess_score)
    2. 거래대금(volume_usd)
    순으로 우선순위를 매겨 순차적으로 체결을 요청합니다.
    """
    global entry_signal_queue
    while True:
        await asyncio.sleep(1.5)  # 캔들 마감 시 1.5초간 시그널을 모음

        async with entry_signal_lock:
            if not entry_signal_queue:
                continue

            # 큐 복사 및 초기화
            signals_to_process = entry_signal_queue[:]
            entry_signal_queue.clear()

        if not signals_to_process:
            continue

        # 정렬:
        # 1. kline_close_time (오름차순): 물리적으로 먼저 마감된 캔들 우선
        # 2. signal_score (내림차순): 같은 시간대라면 점수가 더 높은 종목 우선
        # 3. volume_usd (내림차순): 점수까지 같다면 거래대금이 큰 종목 우선 (슬리피지 방어)
        # 4. symbol (오름차순): 위 3개가 모두 같다면 마지막 타이브레이커
        signals_to_process.sort(
            key=lambda x: (
                x["kline_close_time"],
                -x["signal_score"],
                -x["volume_usd"],
                x["symbol"],
            )
        )

        logger.info(
            f"[Signal Processor] {len(signals_to_process)}개의 동시 시그널을 정렬하여 진입을 시도합니다."
        )
        for idx, sig in enumerate(signals_to_process):
            logger.info(
                f"  -> {idx + 1}위: [{sig['symbol']}] (시간: {sig['kline_close_time']}, 점수: {sig['signal_score']}, 대금: $ {sig['volume_usd']:,.0f})"
            )

        # 순차적으로 진입 로직 수행 (포트폴리오 한도/중복진입 등은 execution 내부에서 방어됨)
        for sig in signals_to_process:
            try:
                entry_success = await execution.place_chasing_entry_order(
                    symbol=sig["symbol"],
                    side=sig["side"],
                    amount=sig["amount"],
                    reason=sig["reason"],
                    tp_dist=sig["tp_dist"],
                    sl_dist=sig["sl_dist"],
                    market_data=sig["market_data"],
                )

                # 진입이 실제로 시작된 경우에만 등록
                if entry_success:
                    portfolio.register_position(
                        symbol=sig["symbol"],
                        direction=sig["direction"],
                        entry_price=sig["market_price"],
                        atr=sig["atr_val"],
                    )
            except Exception as e:
                logger.error(
                    f"[Signal Processor] {sig['symbol']} 체결 시도 중 내부 에러: {e}"
                )


async def chandelier_monitoring_loop(
    strategy: StrategyEngine, execution: ExecutionEngine, pipeline: DataPipeline
):
    """
    [V18] 매 캔들 주기(~30초)마다 활성 포지션의 샹들리에 손절선을 점검합니다.
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

        if not tracked_symbols:
            continue

        try:
            # [Fix] 실시간 현재가(Tickers) 조회 (과거 3분봉 꼬리로 인한 오작동 방지)
            tickers = await execution.exchange.fetch_tickers(tracked_symbols)
        except Exception as e:
            logger.error(f"[Chandelier Loop] Tickers Fetch Error: {e}")
            continue

        for symbol in tracked_symbols:
            pos = portfolio.positions.get(symbol)
            if pos is None:
                continue

            ticker = tickers.get(symbol)
            if ticker is None or ticker.get("last") is None:
                continue

            # 실시간 현재가
            curr_price = float(ticker["last"])
            # [V18.2] 실시간 ATR 추출 (df_map에서 최신값 가져오기)
            # 3분봉 데이터프레임에서 최신 ATR_14 컬럼 확보
            df_curr = df_map.get(symbol)
            if df_curr is not None and "ATR_14" in df_curr.columns:
                curr_atr = float(df_curr["ATR_14"].iloc[-1])
            else:
                # 백업용으로 등록 시의 ATR 사용
                curr_atr = pos.get("atr", curr_price * 0.005)

            # 샹들리에 손절선 갱신 + 돌파 여부 확인
            ce_result = strategy.check_chandelier_exit(
                symbol=symbol,
                portfolio=portfolio,
                current_price=curr_price,
                current_high=curr_price,
                current_low=curr_price,
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

                        # [V18.2] 통합 청산 메서드 호출 (REAL/DRY 및 주문취소/DB기록 일괄 처리)
                        await execution.close_position_market(
                            symbol, reason="Chandelier Exit"
                        )

                        # 포지션 트래킹 삭제 로직 제거 (V18)
                        # - 여기서 수동으로 삭제해버리면 state_machine_loop가 체결(청산)을 감지하지 못해
                        #   DB 기록(Trade, TradeLog) 로직이 통째로 씹히는 치명적 버그가 발생합니다.
                        # - 따라서 봇은 오직 거래소 청산 호출만 날리고, 추적망 삭제와 DB 기록은
                        #   execution.check_active_positions_state() 폴링 루프에 전적으로 위임합니다.

                except Exception as e:
                    logger.error(f"[Chandelier Exit] {symbol} 청산 중 에러: {e}")


# [V18] 동적 심볼 갱신을 위한 웹소켓 재연결 플래그 및 즉시 새로고침 이벤트
ws_reconnect_flag = False
refresh_event = asyncio.Event()


async def target_refresh_loop(pipeline: DataPipeline, execution: ExecutionEngine):
    """
    12시간(오프셋 기준)마다 Top Volume 15종목을 갱신하고 WebSocket을 재연결합니다.
    사용자의 수동 요청(refresh_event)이 있을 경우 즉시 수행합니다.
    """
    global \
        ws_reconnect_flag, \
        refresh_event, \
        df_map, \
        htf_df_1h, \
        htf_df_15m, \
        cvd_data, \
        imbalance_history

    while True:
        wait_sec = calc_next_refresh_seconds()
        logger.info(
            f"⏳ [Target Refresh] 다음 정기 심볼 갱신까지 {wait_sec / 3600:.1f} 시간 대기합니다. (수동 요청 대기 중)"
        )

        try:
            # 정기 스케줄까지 대기하되, 수동 이벤트 발생 시 즉시 깨어남
            await asyncio.wait_for(refresh_event.wait(), timeout=wait_sec)
            logger.info("⚡ [Target Refresh] 수동 즉시 새로고침 요청 감지!")
            refresh_event.clear()
        except asyncio.TimeoutError:
            logger.info("🔄 [Target Refresh] 정기 심볼 갱신 타이머 작동!")

        # 1. 새 종목 리스트 추출
        base_symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
        try:
            # 블랙리스트에 기본 종목이 있으면 제외
            base_symbols = [
                s for s in base_symbols if s not in settings.BLACKLIST_SYMBOLS
            ]

            alts = await pipeline.fetch_top_altcoins_by_volume(
                limit=13
                + len(settings.BLACKLIST_SYMBOLS),  # 블랙리스트 대비 넉넉하게 추출
                exclude_symbols=base_symbols + settings.BLACKLIST_SYMBOLS,
            )
            # 최종적으로 다시 한번 블랙리스트 필터링 (fetch_top_altcoins_by_volume 내부 혹은 외부)
            alts = [s for s in alts if s not in settings.BLACKLIST_SYMBOLS][:13]
        except Exception as e:
            logger.error(f"심볼 갱신 중 에러 발생: {e}. 다음 주기로 연기합니다.")
            continue

        new_target_symbols = base_symbols + alts

        # 2. 보유 포지션 보호 (Retention)
        active_coins = list(execution.active_positions.keys())
        for coin in active_coins:
            if coin not in new_target_symbols:
                logger.warning(
                    f"🛡️ [Target Refresh] {coin} 종목은 포지션이 있어 감시 리스트에 강제 유지됩니다."
                )
                new_target_symbols.append(coin)

        # 3. 변경사항이 있는지 확인
        global_target_names = getattr(settings, "CURRENT_TARGET_SYMBOLS", [])
        if set(new_target_symbols) == set(global_target_names):
            logger.info(
                "✅ [Target Refresh] 감시 종목에 변화가 없습니다. 연결을 유지합니다."
            )
            continue

        logger.info(
            f"📈 [Target Refresh] 감시 종목이 변경되었습니다. (기존 {len(global_target_names)} -> 신규 {len(new_target_symbols)})"
        )

        # 4. 차집합 웜업 (Differential Warm-up)
        # 새로 추가된 종목만 REST API 호출
        added_symbols = set(new_target_symbols) - set(global_target_names)
        await warm_up_differential_data(added_symbols, pipeline)

        # 5. 가비지 컬렉션 (더 이상 감시하지 않는 Old Tickers 메모리 정리)
        removed_symbols = set(global_target_names) - set(new_target_symbols)
        if removed_symbols:
            logger.info(
                f"🧹 [Target Refresh] 감시 제외 종목 메모리 정리: {removed_symbols}"
            )
            for rm_sym in removed_symbols:
                df_map.pop(rm_sym, None)
                htf_df_1h.pop(rm_sym, None)
                htf_df_15m.pop(rm_sym, None)
                cvd_data.pop(rm_sym, None)
                imbalance_history.pop(rm_sym, None)
                portfolio.close_position(
                    rm_sym
                )  # 혹시 남아있는 포트폴리오 가상 상태도 정리

        # 6. Global 상태 업데이트 및 WebSocket 재시작 신호 발송
        settings.CURRENT_TARGET_SYMBOLS = new_target_symbols
        ws_reconnect_flag = True


async def websocket_loop(
    pipeline: DataPipeline,
    strategy: StrategyEngine,
    risk: RiskManager,
    execution: ExecutionEngine,
):
    """
    V18 Aiohttp를 활용한 동적 타임프레임 무지연 이벤트 루프
    """
    while True:
        try:
            target_symbols = getattr(settings, "CURRENT_TARGET_SYMBOLS", [])
            # CCXT 심볼 포맷('BTC/USDT:USDT') <-> 바이낸스 소켓 포맷('btcusdt') 상호 변환기
            ccxt_to_binance = {
                sym: sym.split("/")[0].lower() + "usdt" for sym in target_symbols
            }
            binance_to_ccxt = {v: k for k, v in ccxt_to_binance.items()}

            # [V19.5 Dual-TF] 롱/숏 타임프레임 동적 구독 (L/S 개별 주기 지원)
            l_tf = getattr(settings, "L_TIMEFRAME", "15m")
            s_tf = getattr(settings, "S_TIMEFRAME", "3m")
            unique_tfs = list(set([l_tf, s_tf]))
            streams = []
            for tf_item in unique_tfs:
                streams.extend([f"{v}@kline_{tf_item}" for v in ccxt_to_binance.values()])

            agg_streams = [f"{v}@aggTrade" for v in ccxt_to_binance.values()]
            streams.extend(agg_streams)

            ws_url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
            global ws_reconnect_flag
            ws_reconnect_flag = False

            logger.info(
                f"⚡ 무지연 WebSocket 스트림({tf} {len(target_symbols)}종목) 접속 시도 중..."
            )
            async with aiohttp.ClientSession() as session:
                # Binance 푸시핑에 응답하기 위한 heartbeat
                async with session.ws_connect(ws_url, heartbeat=20.0) as ws:
                    logger.info("🟢 웹소켓 연결 완료! 실시간 트레이딩 봇 가동 시작.")

                    async for msg in ws:
                        if getattr(settings, "PAUSED", False):
                            continue

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            stream_name = data.get("stream", "")

                            # [V18] CVD 실시간 틱 처리 (@aggTrade)
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
                                                interval=kline["i"],
                                            )
                                        )

                        if ws_reconnect_flag:
                            logger.info(
                                "🔄 타겟 종목 갱신 플래그가 수신되어 기존 연결을 리셋합니다."
                            )
                            break

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
    V18 PortfolioState 동기화: execution에서 포지션이 청산되면 portfolio에서도 제거
    """
    while True:
        try:
            await execution.check_pending_orders_state()
            await execution.check_active_positions_state()
            await execution.check_state_mismatch()

            # V18 execution과 portfolio 상태 동기화
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
    logger.info("============== BINANCE V18 MTF SCALPING BOT START ==============")

    is_db_connected = await check_db_connection()
    if not is_db_connected:
        logger.error(
            "🛑 Supabase 접속 불가 또는 테이블 오류 발생으로 구동을 강제 종료합니다."
        )
        return

    # [V19] 텔레그램 알림이 시작 절차를 블로킹하지 않도록 비동기 태스크로 실행
    asyncio.create_task(
        notifier.send_message(
            f"🚀 [시작] 바이낸스 V18 MTF {settings.TIMEFRAME} 스캘핑 봇 웹소켓 대기열 접속 중..."
        )
    )

    pipeline = DataPipeline()
    strategy = StrategyEngine()
    risk = RiskManager(pipeline)
    execution = ExecutionEngine(pipeline, portfolio)

    try:
        await execution.sync_state_from_exchange()

        app = setup_telegram_bot(execution, refresh_event)
        if app:
            await app.initialize()
            await app.start()
            await app.updater.start_polling()

        # [V18] 포트폴리오 최초 종목 15개 선정 및 웜업 (HFT Pipeline 가동 전)
        if not getattr(settings, "CURRENT_TARGET_SYMBOLS", None):
            base_symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
            # 블랙리스트 필터링
            base_symbols = [
                s for s in base_symbols if s not in settings.BLACKLIST_SYMBOLS
            ]

            alts = await pipeline.fetch_top_altcoins_by_volume(
                limit=13 + len(settings.BLACKLIST_SYMBOLS),
                exclude_symbols=base_symbols + settings.BLACKLIST_SYMBOLS,
            )
            alts = [s for s in alts if s not in settings.BLACKLIST_SYMBOLS][:13]
            settings.CURRENT_TARGET_SYMBOLS = base_symbols + alts

            logger.info(
                f"📡 V18 최초 포트폴리오 15종목 동적 선정 결과: {settings.CURRENT_TARGET_SYMBOLS}"
            )
            await warm_up_data(settings.CURRENT_TARGET_SYMBOLS, pipeline)

        global hft_pipeline
        hft_pipeline = HFTDataPipeline(settings.CURRENT_TARGET_SYMBOLS)
        asyncio.create_task(hft_pipeline.start())

        # [V18] 백그라운드 태스크는 최초 진입 시 한 번만 가동
        asyncio.create_task(htf_refresh_loop(pipeline))
        asyncio.create_task(orderbook_twap_loop(pipeline))
        asyncio.create_task(snapshot_flush_loop())
        asyncio.create_task(signal_processor_loop(execution))
        # [V18.3] 12시간 주기 동적 타임프레임 갱신 루프 가동
        asyncio.create_task(target_refresh_loop(pipeline, execution))
        logger.info(
            "[V18] 백그라운드 태스크(HTF / TWAP / Snapshot / Signal Processor / Refresher) 가동 완료."
        )

        # V18 메인 웹소켓 루프 / 스테이트 머신 / 샹들리에 모니터링 병렬 가동
        async def guarded(coro, name):
            try:
                await coro
            except asyncio.CancelledError:
                logger.info(f"[{name}] 태스크가 취소되어 종료됩니다.")
            except Exception as e:
                logger.error(f"[{name}] 태스크 비정상 종료: {e}")
                # 핵심 태스크 종료 시 전체 봇 종료 유도
                if not shutdown_event.is_set():
                    shutdown_event.set()

        # 텔레그램에서 넘겨받은 이벤트 사용 (혹은 새로 생성)
        shutdown_event = app.bot_data.get("shutdown_event", asyncio.Event())

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

        # shutdown_event가 set될 때까지 대기 (혹은 태스크들이 종료될 때까지)
        wait_shutdown = asyncio.create_task(shutdown_event.wait())

        done, pending = await asyncio.wait(
            [task_state, task_trade, task_chandelier, wait_shutdown],
            return_when=asyncio.FIRST_COMPLETED,
        )

        logger.info(f"봇 종료 신호 감지됨. (활성 태스크 수: {len(done)})")
        if not shutdown_event.is_set():
            shutdown_event.set()

        # 모든 펜딩 태스크 정리
        for task in [task_state, task_trade, task_chandelier, wait_shutdown]:
            if not task.done():
                task.cancel()

        await asyncio.gather(
            task_state, task_trade, task_chandelier, return_exceptions=True
        )

    except KeyboardInterrupt:
        logger.warning("CTRL+C(키보드 인터럽트)로 시스템이 정지되었습니다.")
    except Exception as e:
        logger.error(f"Main Loop unexpected Error: {e}")
    finally:
        exit_code = 0
        if "app" in locals() and app:
            exit_code = app.bot_data.get("exit_code", 0)
            logger.info(
                f"텔레그램 인터랙티브 커맨더를 안전하게 종료합니다... (Exit Code: {exit_code})"
            )
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
            # [V19] Telegram Notifier 세션 종료
            await notifier.close()

            # [V18] HFT 파이프라인 세션 종료
            if "hft_pipeline" in globals() and hft_pipeline:
                await hft_pipeline.close_session()

            await pipeline.close()
            # [V19] aiohttp 세션이 완전히 닫힐 시간을 주기 위해 아주 짧은 대기 추가
            await asyncio.sleep(0.25)
        except Exception as e:
            logger.warning(f"연결 및 세션 종료 중 예외 발생: {e}")

        logger.info("거래소 API 객체 릴리즈 및 시스템 종료 절차 통과 완료.")
        sys.exit(exit_code)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except SystemExit as e:
        # sys.exit()의 상태 코드를 상위 프로세스로 전달
        sys.exit(e.code)
    except Exception as e:
        logger.critical(f"봇이 치명적 오류로 종료되었습니다: {e}")
        sys.exit(1)
