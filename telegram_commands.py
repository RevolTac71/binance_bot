import os
import sys
import asyncio
import psutil
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from config import settings, logger, update_env_variable

START_TIME = datetime.utcnow() + timedelta(hours=9)


async def check_admin(update: Update) -> bool:
    chat_id = str(update.effective_chat.id)
    if chat_id != settings.TELEGRAM_CHAT_ID:
        await update.message.reply_text("🚨 권한이 없는 사용자입니다.")
        return False
    return True


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    msg = (
        "🤖 V15.2 자동매매 봇 컨트롤 패널\n\n"
        "📌 기본 명령어\n"
        "/help — 전체 명령어 도움말\n"
        "/status — 봇 상태 및 포지션 요약\n"
        "/pause / /resume — 신규 진입 일시정지 / 재개\n"
        "/panic — 비상! 전량 시장가 청산 후 정지\n"
        "/restart — 봇 재부팅\n\n"
        "⚙️ 파라미터 변경 (재시작 불필요)\n"
        "/setparam [키] [값] — 파라미터 한 번에 변경\n"
        "예: /setparam risk 0.02\n"
        "예: /setparam sl 3.0\n"
        "자세한 파라미터 목록은 /help 참조"
    )
    await update.message.reply_text(msg)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return

    args = context.args
    category = args[0].lower() if args else None

    if not category:
        msg = (
            "📖 [V18 텔레그램 도움말 센터]\n"
            "원하시는 카테고리를 선택해 상세 정보를 확인하세요.\n\n"
            "── 기본 명령어 ──\n"
            "👉 `/help cmd` : 봇 제어 및 상태 확인 명령어\n"
            "👉 `/help score` : V18 스코어링 및 임계값 설정\n"
            "👉 `/help risk` : 손절/익절 및 리스크 관리 설정\n"
            "👉 `/help trade` : 체결, 사이징, 레버리지 설정\n\n"
            "💡 팁: `/setparam [키] [값]` 으로 즉시 수정 가능합니다."
        )
    elif category == "cmd":
        msg = (
            "🤖 [봇 제어 명령어 목록]\n\n"
            "/status — 현재 상태, 포지션, 잔고 요약\n"
            "/params — 현재 봇에 설정된 모든 파라미터 값 조회\n"
            "/pause  — 새로운 진입을 일시 중단\n"
            "/resume — 일시 중단된 진입을 다시 시작\n"
            "/panic  — 모든 포지션 시장가 정리 후 봇 정지\n"
            "/restart — 봇 프로세스 강제 재시작 (업데이트 적용 등)\n"
            "/refresh — 즉시 상위 거래량 종목 새로고침 수행\n"
            "/ignore [코인] — 해당 종목 진입 타겟에서 제외\n"
            "/allow  [코인] — 블랙리스트에서 종목 제거\n"
            "/close  [코인] — 해당 종목만 시장가 즉시 청산"
        )
    elif category == "score":
        msg = (
            "📈 [V18 진입 스코어링 파라미터]\n"
            "진입 결정을 위한 주요 지표의 임계값과 가중치입니다.\n\n"
            "── 통합 설정 ──\n"
            "long_score  롱 진입 임계 점수 (Min Score)\n"
            "short_score 숏 진입 임계 점수 (Min Score)\n"
            "adx_boost   추세 가점용 ADX 백분위 (70)\n"
            "adx_window  백분위 계산용 윈도우 (100)\n\n"
            "── 상세 임계값 기준 (Thresholds) ──\n"
            "macd_1/2/4, cvd_1/2, rsi_1/2, buy_1/2,\n"
            "vol_1/2, imbalance_1/2, nofi_1/2, oi_1/2\n\n"
            "── 지표별 가중치 (Weights) ──\n"
            "w_macd_1/2/4, w_cvd_1/2, w_rsi_1/2, w_atr_2\n"
            "w_htf_2, w_mtm_2, w_reg_1, w_vwap_2\n\n"
            "💡 수정 예: `/setparam w_macd_4 10`"
        )
    elif category == "trade":
        msg = (
            "💰 [체결 및 사이징 설정]\n"
            "베팅 비중과 체결 방식에 관한 설정입니다.\n\n"
            "risk        베팅 비중 (계좌 대비 %, 0.01 = 1%)\n"
            "leverage    레버리지 배수 (int)\n"
            "mode        dry(모의) 또는 real(실전)\n"
            "kelly       켈리 사이징 사용 (on/off)\n"
            "kelly_min   켈리 계산용 최소 거래수 (20)\n"
            "kelly_max   켈리 최대 베팅 상한 (0.05)\n"
            "chasing     지정가 체결 대기 시간 (초)\n"
            "refresh     종목 리프레시 주기 (시간 단위, 3)\n"
            "timeframe   메인 분석 봉 (예: 3m)\n"
            "htf_1h      장기 분석 봉 (예: 1h)\n"
            "htf_15m     중기 분석 봉 (예: 15m)"
        )
    elif category == "risk":
        msg = (
            "🛡️ [청산 및 리스크 관리]\n"
            "손실 제한 및 수익 확정 관련 설정입니다.\n\n"
            "sl          Stop Loss 배수 (ATR 대비)\n"
            "tp          Take Profit 배수 (ATR 대비)\n"
            "partial_tp  분할 익절 수량 비율 (0.5)\n"
            "chandelier  추적 손절(Chandelier) 배수\n"
            "chan_atr    Chandelier 계산 ATR 기간 (22)\n"
            "be_trigger  본절가(BE) 전환 트리거 배수\n"
            "be_profit   BE 전환 시 보존할 수익 배수\n"
            "cooldown    진입 실패 후 대기 분 (15)\n"
            "max_trades  최대 동시 포지션 개수 (3)\n"
            "max_dir     동일 방향 최대 개수 (2)\n"
            "time_exit   자동 시간 청산 기준 (분)"
        )
    else:
        msg = "❌ 알 수 없는 카테고리입니다. `/help`를 입력해 목록을 확인하세요."

    await update.message.reply_text(msg, parse_mode="Markdown")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    execution = context.bot_data["execution"]

    now = datetime.utcnow() + timedelta(hours=9)
    uptime = now - START_TIME
    days, seconds = uptime.days, uptime.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60

    try:
        balance_info = await execution.exchange.fetch_balance()
        capital = balance_info.get("total", {}).get("USDT", 0.0)
    except Exception as e:
        capital = "조회 실패"

    position_details = ""
    try:
        positions = await execution.exchange.fetch_positions()
        active_pos_list = []
        for p in positions:
            amt = float(p.get("contracts", 0))
            if amt > 0:
                sym = p.get("symbol", "Unknown")
                side = p.get("side", "long")
                entry_price = float(p.get("entryPrice", 0))
                mark_price = float(p.get("markPrice", 0))
                leverage = p.get("leverage", 1)
                unrealized_pnl = float(p.get("unrealizedPnl", 0))
                percentage = p.get("percentage")

                # ccxt percentage가 제공되지 않을 경우 수동 계산: (미실현 손익 / (포지션 규모 / 레버리지)) * 100
                if percentage is None or percentage == 0:
                    cost = (amt * entry_price) / float(leverage) if leverage else 0
                    percentage = (unrealized_pnl / cost * 100) if cost > 0 else 0

                side_str = "🟢LONG" if side == "long" else "🔴SHORT"

                detail = (
                    f"[{sym}] {side_str} ({leverage}x)\n"
                    f" ├ 진입가: {entry_price:.4f}\n"
                    f" ├ 현재가: {mark_price:.4f}\n"
                    f" └ 수익률: {unrealized_pnl:.2f} USDT ({percentage:.2f}%)"
                )
                active_pos_list.append(detail)

        if active_pos_list:
            position_details = "\n\n".join(active_pos_list)
        else:
            position_details = "활성 포지션 없음"
    except Exception as e:
        position_details = f"포지션 상세 조회 실패: {e}"

    mode = "DRY_RUN (모의투자)" if settings.DRY_RUN else "REAL (실전 매매)"
    status_str = "일시정지됨 ⏸️" if settings.IS_PAUSED else "가동 중 🟢"

    msg = (
        f"📊 [봇 상태 요약]\n"
        f"── 시스템 ──\n"
        f"매매 모드 : {mode}\n"
        f"봇 동작  : {status_str}\n"
        f"생존 시간 : {days}일 {hours}시간 {minutes}분\n"
        f"전체 잔고  : {capital} USDT\n"
        f"기동 포지션: {len(execution.active_positions)}개 | "
        f"대기 주문: {len(execution.pending_entries)}개\n\n"
        f"── 서버 상태 (System) ──\n"
        f"CPU 사용률: {psutil.cpu_percent(interval=0.1)}%\n"
        f"메모리 사용: {psutil.virtual_memory().percent}%\n"
        f"감시 종목 수: {len(getattr(settings, 'CURRENT_TARGET_SYMBOLS', []))}개\n\n"
        f"── 현재 감시 중인 종목 목록 ──\n"
        f"{', '.join(getattr(settings, 'CURRENT_TARGET_SYMBOLS', ['아직 결정되지 않음']))[:200]}...\n\n"
        f"── 블랙리스트 (차단됨) ──\n"
        f"{', '.join(execution.blacklist) if execution.blacklist else '없음'}\n\n"
        f"── 현재 포지션 (실제 거래소) ──\n"
        f"{position_details}"
    )
    await update.message.reply_text(msg)


async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    settings.IS_PAUSED = True
    await update.message.reply_text(
        "⏸️ 봇이 [일시정지] 되었습니다. 신규 진입을 중단하지만 기존 포지션 청산(TP/SL) 감시는 계속 작동합니다."
    )


async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    settings.IS_PAUSED = False
    await update.message.reply_text(
        "▶️ 봇이 [재개] 되었습니다. 신규 진입 스캔을 정상적으로 다시 탐색합니다."
    )


async def restart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    await update.message.reply_text(
        "🔄 봇 프로세스를 완전히 재부팅합니다... (Watchdog에 의해 3초 후 깔끔하게 새 창으로 켜집니다)"
    )

    logger.info(
        "텔레그램 /restart 커맨드 수신. Exit Code 42로 프로세스를 자발적 종료합니다."
    )

    # asyncio loop를 지연 후 정지 및 종료코드 42 반환
    loop = asyncio.get_running_loop()
    loop.call_later(1.0, lambda: sys.exit(42))


async def panic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    await update.message.reply_text(
        "🚨 [비상 정지] 패닉 모드를 가동합니다! 모든 포지션을 시장가로 방어하고 봇을 전면 정지합니다."
    )

    settings.IS_PAUSED = True
    execution = context.bot_data["execution"]

    target_symbols = list(execution.active_positions.keys()) + list(
        execution.pending_entries.keys()
    )
    target_symbols = list(set(target_symbols))  # 중복제거

    closed_count = 0
    # 1. 모든 대기주문 (일반 + Algo) 삭제
    for sym in target_symbols:
        try:
            raw_sym = execution.exchange.market(sym)["id"]
            await execution.exchange.cancel_all_orders(sym)
            algo_orders = await execution.exchange.request(
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
                await execution.exchange.request(
                    path="algoOrder",
                    api="fapiPrivate",
                    method="DELETE",
                    params={"symbol": raw_sym, "algoId": algo.get("algoId")},
                )
        except Exception as e:
            logger.error(f"Panic Cancel Error [{sym}]: {e}")

    # 2. 모든 포지션 시장가 청산
    try:
        positions = await execution.exchange.fetch_positions()
        for p in positions:
            amt = float(p.get("contracts", 0))
            if amt > 0:
                sym = p["symbol"]
                side = "sell" if p["side"] == "long" else "buy"
                if not settings.DRY_RUN:
                    await execution.exchange.create_order(
                        sym, "market", side, amt, params={"reduceOnly": True}
                    )
                closed_count += 1
    except Exception as e:
        logger.error(f"Panic Market Close Error: {e}")

    await update.message.reply_text(
        f"💥 패닉 프로토콜 처리 완료. (정리된 포지션: {closed_count}개)\n모든 잔여 주문 상태가 초기화되었고 신규 진입이 잠겼습니다."
    )


async def setparam_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setparam [key] [value] — 전략 파라미터를 키윗-밸류 방식으로 일괄 변경합니다.
    재시작 없이 즉시 적용되며 .env에 영구 저장됩니다.
    """
    if not await check_admin(update):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "💡 사용법: /setparam [키] [값]\n"
            "예) /setparam risk 0.02\n"
            "예) /setparam sl 3.0\n"
            "예) /setparam cooldown 15\n"
            "\n전체 파라미터 목록은 /help 참조"
        )
        return

    key = args[0].lower()
    raw_val = args[1]

    try:
        mapping = {
            "risk": ("RISK_PERCENTAGE", float, "RISK_PERCENTAGE"),
            "leverage": ("LEVERAGE", int, "LEVERAGE"),
            "timeframe": ("TIMEFRAME", str, "TIMEFRAME"),
            "time_exit": ("TIME_EXIT_MINUTES", int, "TIME_EXIT_MINUTES"),
            "refresh": ("SYMBOL_REFRESH_INTERVAL", int, "SYMBOL_REFRESH_INTERVAL"),
            "htf_1h": ("HTF_TIMEFRAME_1H", str, "HTF_TIMEFRAME_1H"),
            "htf_15m": ("HTF_TIMEFRAME_15M", str, "HTF_TIMEFRAME_15M"),
            "atr_ratio": ("ATR_RATIO_MULT", float, "ATR_RATIO_MULT"),
            "atr_long": ("ATR_LONG_LEN", int, "ATR_LONG_LEN"),
            "chandelier": ("CHANDELIER_MULT", float, "CHANDELIER_MULT"),
            "chan_atr": ("CHANDELIER_ATR_LEN", int, "CHANDELIER_ATR_LEN"),
            "sl": ("SL_MULT", float, "SL_MULT"),
            "tp": ("TP_MULT", float, "TP_MULT"),
            "cooldown": ("LOSS_COOLDOWN_MINUTES", int, "LOSS_COOLDOWN_MINUTES"),
            "max_trades": ("MAX_TRADES", int, "MAX_TRADES"),
            "max": ("MAX_TRADES", int, "MAX_TRADES"),
            "max_dir": ("MAX_CONCURRENT_SAME_DIR", int, "MAX_CONCURRENT_SAME_DIR"),
            "be_trigger": ("BREAKEVEN_TRIGGER_MULT", float, "BREAKEVEN_TRIGGER_MULT"),
            "be_profit": ("BREAKEVEN_PROFIT_MULT", float, "BREAKEVEN_PROFIT_MULT"),
            "mode": (None, None, None),
            # V18
            "long_score": ("MIN_SCORE_LONG", int, "MIN_SCORE_LONG"),
            "short_score": ("MIN_SCORE_SHORT", int, "MIN_SCORE_SHORT"),
            "adx_boost": ("ADX_BOOST_PCTL", float, "ADX_BOOST_PCTL"),
            "adx_window": ("PCTL_WINDOW", int, "PCTL_WINDOW"),
            "partial_tp": ("PARTIAL_TP_RATIO", float, "PARTIAL_TP_RATIO"),
            "chasing": ("CHASING_WAIT_SEC", float, "CHASING_WAIT_SEC"),
            "kelly": (None, None, None),
            "kelly_min": ("KELLY_MIN_TRADES", int, "KELLY_MIN_TRADES"),
            "kelly_max": ("KELLY_MAX_FRACTION", float, "KELLY_MAX_FRACTION"),
            # Scoring Thresholds (Custom 키)
            "macd_1": ("macd_pctl", int, "SCORE_MACD_1"),
            "macd_2": ("macd_pctl", int, "SCORE_MACD_2"),
            "macd_4": ("macd_pctl", int, "SCORE_MACD_4"),
            "cvd_1": ("cvd_pctl", int, "SCORE_CVD_1"),
            "cvd_2": ("cvd_pctl", int, "SCORE_CVD_2"),
            "imbal_1": ("imbalance", int, "SCORE_IMBAL_1"),
            "imbal_2": ("imbalance", int, "SCORE_IMBAL_2"),
            "nofi_1": ("nofi_pctl", int, "SCORE_NOFI_1"),
            "nofi_2": ("nofi_pctl", int, "SCORE_NOFI_2"),
            "rsi_1": ("rsi", int, "SCORE_RSI_1"),
            "rsi_2": ("rsi", int, "SCORE_RSI_2"),
            "buy_1": ("buy_ratio", int, "SCORE_BUY_1"),
            "buy_2": ("buy_ratio", int, "SCORE_BUY_2"),
            "vol_1": ("vol_zscore", float, "SCORE_VOL_1"),
            "vol_2": ("vol_zscore", float, "SCORE_VOL_2"),
            "oi_1": ("oi_pctl", int, "SCORE_OI_1"),
            "oi_2": ("oi_pctl", int, "SCORE_OI_2"),
            "tick_1": ("tick_pctl", int, "SCORE_TICK_1"),
            "tick_2": ("tick_pctl", int, "SCORE_TICK_2"),
            "atr_boost": (
                "atr_boost",
                int,
                "SCORE_ATR_BOOST_2",
            ),  # ATR Boost is a threshold
            # Scoring Weights (Custom 키)
            "w_macd_1": ("macd", int, "WEIGHT_MACD_1"),
            "w_macd_2": ("macd", int, "WEIGHT_MACD_2"),
            "w_macd_4": ("macd", int, "WEIGHT_MACD_4"),
            "w_cvd_1": ("cvd", int, "WEIGHT_CVD_1"),
            "w_cvd_2": ("cvd", int, "WEIGHT_CVD_2"),
            "w_imbal_1": ("imbalance", int, "WEIGHT_IMBAL_1"),
            "w_imbal_2": ("imbalance", int, "WEIGHT_IMBAL_2"),
            "w_nofi_1": ("nofi", int, "WEIGHT_NOFI_1"),
            "w_nofi_2": ("nofi", int, "WEIGHT_NOFI_2"),
            "w_rsi_1": ("rsi", int, "WEIGHT_RSI_1"),
            "w_rsi_2": ("rsi", int, "WEIGHT_RSI_2"),
            "w_buy_1": ("buy_ratio", int, "WEIGHT_BUY_1"),
            "w_buy_2": ("buy_ratio", int, "WEIGHT_BUY_2"),
            "w_vol_1": ("vol_z", int, "WEIGHT_VOL_1"),
            "w_vol_2": ("vol_z", int, "WEIGHT_VOL_2"),
            "w_oi_1": ("oi", int, "WEIGHT_OI_1"),
            "w_oi_2": ("oi", int, "WEIGHT_OI_2"),
            "w_tick_1": ("tick", int, "WEIGHT_TICK_1"),
            "w_tick_2": ("tick", int, "WEIGHT_TICK_2"),
            "w_atr_2": ("atr", int, "WEIGHT_ATR_2"),
            "w_adx_1": ("adx_boost", int, "WEIGHT_ADX_1"),
            "w_fr_2": ("fr_boost", int, "WEIGHT_FR_2"),
            "w_htf_2": ("htf_bias", int, "WEIGHT_HTF_BIAS"),
            "w_mtm_2": ("mtf_moment", int, "WEIGHT_MTF_MOMENT"),
            "w_reg_1": ("mtf_regime", int, "WEIGHT_MTF_REGIME"),
            "w_vwap_2": ("vwap_dist", int, "WEIGHT_VWAP_DIST"),
        }

        if key not in mapping:
            await update.message.reply_text(
                f"❌ 알 수 없는 파라미터: '{key}'\n/help로 파라미터 목록을 확인하세요."
            )
            return

        attr_name, cast_fn, env_key = mapping[key]

        # mode 특별 처리
        if key == "mode":
            is_dry = raw_val.lower() in ("dry", "dry_run", "true", "1")
            settings.DRY_RUN = is_dry
            update_env_variable("DRY_RUN", str(is_dry).capitalize())
            label = "모의투자(DRY_RUN)" if is_dry else "실전매매(REAL)"
            await update.message.reply_text(f"✅ 매매 모드 → {label} 설정 완료")
            return

        # V17: Kelly 사이징 on/off 특별 처리
        elif key == "kelly":
            is_on = raw_val.lower() in ("true", "1", "yes", "on")
            settings.KELLY_SIZING = is_on
            update_env_variable("KELLY_SIZING", str(is_on).capitalize())
            label = (
                "활성화 (Half-Kelly 동적 사이징)" if is_on else "비활성화 (고정 비율)"
            )
            await update.message.reply_text(f"✅ Kelly 사이징 → {label} 설정 완료")
            return

        # 스코어링 임계값 딕셔너리 업데이트 처리
        threshold_keys = (
            "macd_1",
            "macd_2",
            "macd_4",
            "cvd_1",
            "cvd_2",
            "imbal_1",
            "imbal_2",
            "nofi_1",
            "nofi_2",
            "rsi_1",
            "rsi_2",
            "buy_1",
            "buy_2",
            "vol_1",
            "vol_2",
            "oi_1",
            "oi_2",
            "tick_1",
            "tick_2",
            "atr_boost",
        )
        if key in threshold_keys:
            # atr_boost는 sub_key가 +2로 고정
            if key == "atr_boost":
                sub_key = "+2"
            else:
                # sub_key 추출 (+1, +2 등)
                sub_key = "+" + key.split("_")[1]

            new_val = cast_fn(raw_val)

            # settings.SCORING_THRESHOLDS 직접 업데이트
            settings.SCORING_THRESHOLDS[attr_name][sub_key] = new_val

            # .env 파일에도 반영 (영구 저장)
            update_env_variable(env_key, str(new_val))

            await update.message.reply_text(
                f"✅ [스코어 기준] {key}({env_key}) → {new_val} 설정 및 저장 완료"
            )
            return

        # 스코어링 가중치(Weights) 업데이트 처리
        if key.startswith("w_"):
            # 'w_macd_1' -> 'macd', '1'
            # 'w_atr_2' -> 'atr', '2'
            # 'w_adx_1' -> 'adx_boost', '1'
            # 'w_fr_2' -> 'fr_boost', '2'

            # attr_name은 mapping에서 이미 가져옴 (예: 'macd', 'cvd', 'adx_boost')
            # sub_key는 마지막 숫자 부분
            sub_key = key.split("_")[-1]

            new_val = cast_fn(raw_val)
            settings.SCORING_WEIGHTS[attr_name][sub_key] = new_val
            update_env_variable(env_key, str(new_val))
            await update.message.reply_text(
                f"✅ [가중치] {key}({env_key}) → {new_val} 설정 및 저장 완료"
            )
            return

        # 일반 키 처리 (변경 전 값을 먼저 읽어둠)
        old_val = getattr(settings, attr_name, "(없음)")
        new_val = cast_fn(raw_val)

        # 유효성 검사 등...
        if key in ("be_trigger", "be_profit"):
            if new_val < 0.0 or new_val > 3.0:
                await update.message.reply_text(
                    "❌ 지정 가능한 범위를 벗어났습니다. (0.0~3.0)"
                )
                return

        setattr(settings, attr_name, new_val)
        update_env_variable(env_key, str(new_val))

        restart_notice = ""
        if key in ("timeframe", "htf_1h", "htf_15m", "mode"):
            restart_notice = f"\n⚠️ {key} 변경 시 /restart 권장!"

        await update.message.reply_text(
            f"✅ [{key.upper()}] 변경 완료\n이전: {old_val} → 새: {new_val}{restart_notice}"
        )

    except Exception as e:
        logger.error(f"Error in setparam: {e}")
        await update.message.reply_text(f"❌ 설정 변경 중 오류 발생: {e}")


async def ignore_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    execution = context.bot_data["execution"]
    if not context.args:
        await update.message.reply_text(
            "💡 사용법: /ignore [코인명]\n예) /ignore LINK/USDT"
        )
        return
    coin = context.args[0].upper()
    if not coin.endswith("USDT"):
        coin += "USDT"  # 편의성을 위해 USDT 붙여줌, ccxt 포맷상 /USDT나 단순 문자열 처리에 주의. 바이낸스는 일단 매핑 필요.
        # 실제 Binance ccxt symbol 형식은 "BTC/USDT" 등. 사용자가 'LINK'라고 치면 'LINK/USDT'로.
        if "/" not in coin:
            coin = coin.replace("USDT", "/USDT")
    execution.blacklist.add(coin)
    await update.message.reply_text(
        f"✅ {coin} 코인이 블랙리스트에 추가되어 신규 진입이 차단됩니다."
    )


async def allow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    execution = context.bot_data["execution"]
    if not context.args:
        await update.message.reply_text(
            "💡 사용법: /allow [코인명]\n예) /allow LINK/USDT"
        )
        return
    coin = context.args[0].upper()
    if not coin.endswith("USDT"):
        coin += "USDT"
        if "/" not in coin:
            coin = coin.replace("USDT", "/USDT")
    if coin in execution.blacklist:
        execution.blacklist.remove(coin)
        await update.message.reply_text(
            f"✅ {coin} 코인이 블랙리스트에서 제거되었습니다. 진입이 허용됩니다."
        )
    else:
        await update.message.reply_text(f"❌ {coin} 코인은 블랙리스트에 없습니다.")


async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    execution = context.bot_data["execution"]
    if not context.args:
        await update.message.reply_text(
            "💡 사용법: /close [코인명]\n예) /close LINK/USDT"
        )
        return

    coin = context.args[0].upper()
    if not coin.endswith("USDT"):
        coin += "USDT"
        if "/" not in coin:
            coin = coin.replace("USDT", "/USDT")

    await update.message.reply_text(f"🗑️ [{coin}] 코인의 시장가 청산을 시도합니다...")
    try:
        # 1. 펜딩 주문(조건부 SL 포함) 취소
        try:
            raw_sym = execution.exchange.market(coin)["id"]
            await execution.exchange.cancel_all_orders(coin)
            # Algo 주문 취소 로직도 포함하면 좋지만 심플하게
        except Exception as cancel_e:
            logger.warning(
                f"/{coin} 미체결 주문 취소 실패 (존재하지 않거나 에러): {cancel_e}"
            )

        # 2. 시장가 청산
        positions = await execution.exchange.fetch_positions()
        closed = False
        for p in positions:
            if p["symbol"] == coin:
                amt = float(p.get("contracts", 0))
                if amt > 0:
                    side = "sell" if p["side"] == "long" else "buy"
                    if not settings.DRY_RUN:
                        await execution.exchange.create_order(
                            coin, "market", side, amt, params={"reduceOnly": True}
                        )
                    closed = True
                    break

        if closed:
            await update.message.reply_text(f"✅ [{coin}] 전량 시장가 청산 완료")
        else:
            await update.message.reply_text(
                f"❌ [{coin}] 활성 포지션을 찾을 수 없습니다."
            )
    except Exception as e:
        logger.error(f"개별 종목 {coin} 청산 중 에러: {e}")
        await update.message.reply_text(f"❌ 청산 중 에러 발생: {e}")


async def params_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return

    mode = "모의투자(DRY_RUN)" if settings.DRY_RUN else "실전매매(REAL)"

    msg = (
        "⚙️ [V18 현재 설정 파라미터]\n"
        f"모드      : {mode}\n"
        f"레버리지  : {settings.LEVERAGE}x\n"
        f"리스크    : {settings.RISK_PERCENTAGE * 100:.1f}%\n"
        f"MaxTrades : {getattr(settings, 'MAX_TRADES', 3)} (동일방향: {getattr(settings, 'MAX_CONCURRENT_SAME_DIR', 2)})\n"
        f"캔들/보유 : {getattr(settings, 'TIMEFRAME', '3m')} / {getattr(settings, 'TIME_EXIT_MINUTES', 0)}분\n\n"
        f"── 진입 조건 (V18 스코어링) ──\n"
        f"임계점수  : LONG={settings.MIN_SCORE_LONG} / SHORT={settings.MIN_SCORE_SHORT}\n"
        f"ADX 부스트: {settings.ADX_BOOST_PCTL}%tile (윈도우: {settings.PCTL_WINDOW})\n"
        f"ATR 필터  : {settings.ATR_RATIO_MULT}x\n"
    )

    t = getattr(settings, "SCORING_THRESHOLDS", {})
    if t:
        msg += (
            f"\n[V18 세부 지표 점수(+1/+2/+4) 기준]\n"
            f"- MACD 히스토 : {t.get('macd_pctl', {}).get('+1')}/{t.get('macd_pctl', {}).get('+2')}/{t.get('macd_pctl', {}).get('+4')}%\n"
            f"- CVD 기울기  : {t.get('cvd_pctl', {}).get('+1')}/{t.get('cvd_pctl', {}).get('+2')}%\n"
            f"- 호가 불균형 : {t.get('imbalance', {}).get('+1')}/{t.get('imbalance', {}).get('+2')}%\n"
            f"- 정규화 OFI  : {t.get('nofi_pctl', {}).get('+1')}/{t.get('nofi_pctl', {}).get('+2')}%\n"
            f"- RSI 과매도  : {t.get('rsi', {}).get('+1')}/{t.get('rsi', {}).get('+2')}\n"
            f"- 미결제약정  : {t.get('oi_pctl', {}).get('+1')}/{t.get('oi_pctl', {}).get('+2')}%\n"
            f"- 체결 횟수   : {t.get('tick_pctl', {}).get('+1')}/{t.get('tick_pctl', {}).get('+2')}%\n"
            f"- 역발상(Buy) : {t.get('buy_ratio', {}).get('+1')}/{t.get('buy_ratio', {}).get('+2')}%\n"
            f"- 볼륨 Z-스코어: {t.get('vol_zscore', {}).get('+1')}/{t.get('vol_zscore', {}).get('+2')}σ\n\n"
            f"[V18 지표별 가중치(Weights)]\n"
            f"- MACD(+1/+2/+4): {settings.SCORING_WEIGHTS['macd']['1']}/{settings.SCORING_WEIGHTS['macd']['2']}/{settings.SCORING_WEIGHTS['macd']['4']}점\n"
            f"- CVD(+1/+2)     : {settings.SCORING_WEIGHTS['cvd']['1']}/{settings.SCORING_WEIGHTS['cvd']['2']}점\n"
            f"- RSI(+1/+2)     : {settings.SCORING_WEIGHTS['rsi']['1']}/{settings.SCORING_WEIGHTS['rsi']['2']}점\n"
            f"- ATR/ADX/FR     : {settings.SCORING_WEIGHTS['atr']['2']}/{settings.SCORING_WEIGHTS['adx_boost']['1']}/{settings.SCORING_WEIGHTS['fr_boost']['2']}점\n"
            f"- HTF/MTM/Reg/VW : {settings.SCORING_WEIGHTS['htf_bias']['2']}/{settings.SCORING_WEIGHTS['mtf_moment']['2']}/{settings.SCORING_WEIGHTS['mtf_regime']['1']}/{settings.SCORING_WEIGHTS['vwap_dist']['2']}점\n\n"
        )

    msg += (
        f"── 체결 & 사이징 ──\n"
        f"Kelly    : {'ON' if getattr(settings, 'KELLY_SIZING', False) else 'OFF'} (Min:{getattr(settings, 'KELLY_MIN_TRADES', 20)}, Max:{getattr(settings, 'KELLY_MAX_FRACTION', 0.05)})\n"
        f"Chasing  : {getattr(settings, 'CHASING_WAIT_SEC', 5.0)}초\n\n"
        f"── 청산 & 리스크 ──\n"
        f"SL / TP   : {getattr(settings, 'SL_MULT', 1.5)}x / {getattr(settings, 'TP_MULT', 5.0)}x\n"
        f"분할익절  : {getattr(settings, 'PARTIAL_TP_RATIO', 0.5) * 100:.0f}%\n"
        f"본절발동  : {getattr(settings, 'BREAKEVEN_TRIGGER_MULT', 1.5)}x (보존: {getattr(settings, 'BREAKEVEN_PROFIT_MULT', 0.2)}x)\n"
        f"재진입대기: {getattr(settings, 'LOSS_COOLDOWN_MINUTES', 15)}분\n\n"
        "💡 변경은 /setparam [옵션] [값] 이용"
    )
    await update.message.reply_text(msg)


async def refresh_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return

    refresh_event = context.bot_data.get("refresh_event")
    if refresh_event:
        refresh_event.set()
        await update.message.reply_text(
            "🔄 즉시 종목 새로고침 신호를 보냈습니다. 곧 반영됩니다."
        )
    else:
        await update.message.reply_text("❌ 새로고침 이벤트를 찾을 수 없습니다.")


def setup_telegram_bot(execution_engine, refresh_event=None):
    """
    python-telegram-bot Application 인스턴스를 빌드하고 핸들러를 붙여 반환합니다.
    """
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID

    if not token or not chat_id:
        logger.warning(
            "텔레그램 토큰 또는 Chat ID가 설정되지 않아 Interactive 커맨더를 시작할 수 없습니다."
        )
        return None

    application = ApplicationBuilder().token(token).build()
    application.bot_data["execution"] = execution_engine
    application.bot_data["refresh_event"] = refresh_event

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("params", params_cmd))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CommandHandler("pause", pause_cmd))
    application.add_handler(CommandHandler("resume", resume_cmd))
    application.add_handler(CommandHandler("restart", restart_cmd))
    application.add_handler(CommandHandler("panic", panic_cmd))
    application.add_handler(CommandHandler("setparam", setparam_cmd))
    application.add_handler(CommandHandler("ignore", ignore_cmd))
    application.add_handler(CommandHandler("allow", allow_cmd))
    application.add_handler(CommandHandler("close", close_cmd))
    application.add_handler(CommandHandler("refresh", refresh_cmd))

    logger.info("텔레그램 Interactive 커맨더(Poller) 세팅이 완료되었습니다.")
    return application
