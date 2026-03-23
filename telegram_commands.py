import asyncio
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest
from config import settings, logger, update_env_variable

START_TIME = datetime.utcnow() + timedelta(hours=9)


async def reply(update: Update, text: str, **kwargs):
    msg = update.effective_message
    if msg is None:
        logger.warning("Telegram update has no message; reply skipped.")
        return
    await msg.reply_text(text, **kwargs)


async def check_admin(update: Update) -> bool:
    chat = update.effective_chat
    if chat is None:
        logger.warning("Telegram update has no chat; admin check failed.")
        return False
    chat_id = str(chat.id)
    if chat_id != settings.TELEGRAM_CHAT_ID:
        await reply(update, "🚨 권한이 없는 사용자입니다.")
        return False
    return True


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    msg = (
        "🤖 V18 자동매매 봇 컨트롤 패널\n\n"
        "📌 기본 명령어\n"
        "/help — 전체 명령어 도움말\n"
        "/status — 봇 상태 및 포지션 요약\n"
        "/pause / /resume — 신규 진입 일시정지 / 재개\n"
        "/real / /dryrun — 실전매매 / 모의투자 전환\n"
        "/panic — 비상! 전량 시장가 청산 후 정지\n"
        "/restart — 봇 재부팅\n\n"
        "⚙️ 파라미터 변경 (재시작 불필요)\n"
        "/setparam [키] [값] — 파라미터 한 번에 변경\n"
        "예: /setparam risk 0.02\n"
        "예: /setparam sl 3.0\n"
        "자세한 파라미터 목록은 /help 참조"
    )
    await reply(update, msg)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return

    args = context.args
    category = args[0].lower() if args else None

    if not category:
        msg = (
            "📖 <b>[V18 텔레그램 도움말 센터]</b>\n"
            "원하시는 카테고리를 선택해 상세 정보를 확인하세요.\n\n"
            "── <b>명령어 카테고리</b> ──\n"
            "👉 `/help cmd` : 봇 제어 및 상태 확인 관련\n"
            "👉 `/help score` : V18 스코어링 규칙 및 임계치 설정\n"
            "👉 `/help risk` : 손절/익절 및 리스크 관리 설정\n"
            "👉 `/help trade` : 체결, 사이징, 레버리지 설정\n\n"
            "💡 <b>사용법:</b> `/setparam [키] [값]` 으로 즉시 수정 가능"
        )
    elif category == "cmd":
        msg = (
            "🤖 <b>[봇 제어 명령어 목록]</b>\n\n"
            "▫ `/status` — 현재 상태, 포지션, 잔고 요약\n"
            "▫ `/params` — 현재 설정된 모든 파라미터 조회\n"
            "▫ `/pause`  — 새로운 진입 일시 중단\n"
            "▫ `/resume` — 중단된 진입 다시 시작\n"
            "▫ `/real`   — 실전매매(REAL) 모드로 변경\n"
            "▫ `/dryrun` — 모의투자(DRY_RUN) 모드로 변경\n"
            "▫ `/refresh` — 즉시 상위 거래량 종목 갱신\n"
            "▫ `/ignore [코인]` — 해당 종목 감시 제외\n"
            "▫ `/allow [코인]` — 종목 제외 해제\n"
            "▫ `/close [코인]` — 특정 종목 시장가 청산\n"
            "▫ `/panic`  — 전량 청산 후 봇 종료\n"
            "▫ `/restart` — 봇 강제 재시작"
        )
    elif category == "score":
        msg = (
            "📈 <b>[스코어링 규칙 설정 가이드]</b>\n"
            "롱(L)과 숏(S) 규칙을 개별 설정할 수 있습니다.\n\n"
            "<b>1. 기본 형식:</b> <code>[L/S]_[지표]_[T/W][단계]</code>\n"
            "▫ <code>T</code>: 임계값(Threshold), <code>W</code>: 점수(Weight)\n"
            "▫ <b>지표 키워드:</b> CVD, MACD, IMBAL, NOFI, OI, TICK, VOL, BUY, RSI\n\n"
            "<b>2. 주요 파라미터 변수명 (Key):</b>\n"
            "▫ <code>min_score_long</code>, <code>min_score_short</code>\n"
            "▫ <code>macd_filter</code> (on/off)\n"
            "▫ <code>l_cvd_t1</code>, <code>l_cvd_w1</code>, <code>s_macd_t2</code> 등\n\n"
            "<b>3. 가중치(Weight) 변수명:</b>\n"
            "▫ <code>weight_adx</code>, <code>weight_mtf_moment</code>\n"
            "▫ <code>weight_atr</code>, <code>weight_fr</code>, <code>weight_htf</code>\n"
            "▫ <code>weight_mtf_regime</code>, <code>weight_vwap</code>"
        )
    elif category == "trade":
        msg = (
            "💰 <b>[체결 및 사이징 설정]</b>\n\n"
            "<b>주요 파라미터 변수명 (Key):</b>\n"
            "▫ <code>risk</code> : 베팅 비중 (0.01 = 1%)\n"
            "▫ <code>leverage</code> : 레버리지 배수 (int)\n"
            "▫ <code>kelly</code> : 켈리 사이징 (on/off)\n"
            "▫ <code>chasing</code> : 지정가 대기 시간 (초)\n"
            "▫ <code>refresh</code> : 종목 갱신 주기 (시간)\n"
            "▫ <code>timeframe</code> : 기준 봉 (예: 3m)"
        )
    elif category == "risk":
        msg = (
            "🛡️ <b>[청산 및 리스크 관리]</b>\n\n"
            "<b>주요 파라미터 변수명 (Key):</b>\n"
            "▫ <code>l_tp_mode</code> / <code>s_tp_mode</code> : 익절 방식 (ATR/PERCENT)\n"
            "▫ <code>l_sl_mode</code> / <code>s_sl_mode</code> : 손절 방식 (ATR/PERCENT)\n"
            "▫ <code>l_tp</code> / <code>s_tp</code> : ATR 기반 익절 배수\n"
            "▫ <code>l_sl</code> / <code>s_sl</code> : ATR 기반 손절 배수\n"
            "▫ <code>l_tp_pct</code> / <code>s_tp_pct</code> : 비율 기반 (0.05 = 5%)\n"
            "▫ <code>l_sl_pct</code> / <code>s_sl_pct</code> : 비율 기반 (0.02 = 2%)\n"
            "▫ <code>chandelier</code> : 샹들리에 추적 손절 배수\n"
            "▫ <code>cooldown</code> : 손절 후 재진입 제한(분)\n"
            "▫ <code>partial_tp</code> : 1차 익절 비중 (0.5 = 50%)\n"
            "▫ <code>be_trigger</code> : 본절 추적 트리거 (ATR배수)\n"
            "▫ <code>be_profit</code> : 본절 확보 수익 (ATR배수)"
        )
    else:
        msg = "❌ 알 수 없는 카테고리입니다. `/help`를 입력해 목록을 확인하세요."

    await reply(update, msg, parse_mode="HTML")


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
        # [V19] 명시적으로 선물(future) 계좌만 조회하여 Margin API 호출 및 타임아웃 방지
        balance_info = await execution.exchange.fetch_balance({"type": "future"})
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
                pnl = float(p.get("unrealizedPnl", 0))
                roe = (pnl / (entry_price * amt / leverage)) * 100 if entry_price > 0 else 0
                active_pos_list.append(
                    f"▫<b>{sym}</b> ({side.upper()} {leverage}x)\n"
                    f"  진입: {entry_price:.4f} / 수익: {pnl:+.2f} USDT ({roe:+.2f}%)"
                )
        position_details = "\n".join(active_pos_list) if active_pos_list else "현재 보유 포지션 없음"
    except Exception as e:
        position_details = f"조회 오류: {e}"

    is_paused = getattr(settings, "PAUSED", False)
    capital_text = f"{capital:,.2f} USDT" if isinstance(capital, (int, float)) else str(capital)
    msg = (
        f"📊 <b>실시간 봇 상태 리포트</b>\n"
        f"──────────────────\n"
        f"🕒 <b>Uptime</b>: {days}일 {hours}시간 {minutes}분\n"
        f"💰 <b>USDT 잔고</b>: {capital_text}\n"
        f"🚦 <b>신규 진입</b>: {'✅ ACTIVE' if not is_paused else '⚠️ PAUSED'}\n"
        f"──────────────────\n"
        f"📦 <b>보유 포지션</b>:\n{position_details}"
    )
    await reply(update, msg, parse_mode="HTML")


async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    setattr(settings, "PAUSED", True)
    update_env_variable("PAUSED", "True")
    await reply(update, "⚠️ 신규 진입이 일시 중지되었습니다.")


async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    setattr(settings, "PAUSED", False)
    update_env_variable("PAUSED", "False")
    await reply(update, "✅ 신규 진입이 재개되었습니다.")


async def restart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    await reply(update, "🔄 봇을 재시작합니다 (감시 프로세스에 의해 42번 코드로 재기동)...")
    # shutdown_event를 호출하여 main 루프에서 안전하게 종료하도록 유도
    context.bot_data["exit_code"] = 42
    shutdown_event = context.bot_data.get("shutdown_event")
    if shutdown_event:
        shutdown_event.set()


async def real_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    setattr(settings, "DRY_RUN", False)
    update_env_variable("DRY_RUN", "False")
    await reply(update, "⚠️ 봇이 <b>실전매매(REAL)</b> 모드로 전환되었습니다!\n(포지션 충돌을 방지하기 위해 봇 재시작을 권장합니다: /restart)", parse_mode="HTML")


async def dryrun_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    setattr(settings, "DRY_RUN", True)
    update_env_variable("DRY_RUN", "True")
    await reply(update, "✅ 봇이 <b>모의투자(DRY_RUN)</b> 모드로 전환되었습니다.\n(가상 체결로 동작합니다. 재시작 권장: /restart)", parse_mode="HTML")


async def panic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    execution = context.bot_data["execution"]
    await reply(update, "🚨 PANIC! 모든 포지션 시장가 정리 및 봇 정지 시도...")
    
    # 1. 진입 중지
    setattr(settings, "PAUSED", True)
    update_env_variable("PAUSED", "True")

    # 2. 모든 포지션 시장가 종료
    try:
        positions = await execution.exchange.fetch_positions()
        for p in positions:
            amt = float(p.get("contracts", 0))
            if amt > 0:
                side = "sell" if p["side"] == "long" else "buy"
                symbol = p["symbol"]
                await execution.exchange.create_order(symbol, "market", side, amt, params={"reduceOnly": True})
        await reply(update, "✅ 모든 포지션이 정리되었습니다. 봇을 종료합니다.")
        
        context.bot_data["exit_code"] = 0
        shutdown_event = context.bot_data.get("shutdown_event")
        if shutdown_event:
            shutdown_event.set()
    except Exception as e:
        await reply(update, f"❌ 패닉 셀 중 오류 발생: {e}")


async def setparam_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    
    args = context.args or []
    if len(args) < 2:
        await reply(update, "사용법: /setparam [키] [값]\n예: /setparam risk 0.03")
        return

    key = args[0].lower()
    value_str = args[1]

    # 설정 가능 파라미터 맵핑
    param_map = {
        "risk": ("RISK_PERCENTAGE", float),
        "leverage": ("LEVERAGE", int),
        "l_tp": ("L_TP_MULT", float),
        "l_sl": ("L_SL_MULT", float),
        "s_tp": ("S_TP_MULT", float),
        "s_sl": ("S_SL_MULT", float),
        "l_tp_pct": ("L_TP_PCT", float),
        "l_sl_pct": ("L_SL_PCT", float),
        "s_tp_pct": ("S_TP_PCT", float),
        "s_sl_pct": ("S_SL_PCT", float),
        "l_tp_mode": ("LONG_TP_MODE", str),
        "l_sl_mode": ("LONG_SL_MODE", str),
        "s_tp_mode": ("SHORT_TP_MODE", str),
        "s_sl_mode": ("SHORT_SL_MODE", str),
        "partial_tp": ("PARTIAL_TP_RATIO", float),
        "fee_rate": ("FEE_RATE", float),
        "chandelier": ("CHANDELIER_MULT", float),
        "cooldown": ("LOSS_COOLDOWN_MINUTES", int),
        "be_trigger": ("BREAKEVEN_TRIGGER_MULT", float),
        "be_profit": ("BREAKEVEN_PROFIT_MULT", float),
        "min_score_long": ("MIN_SCORE_LONG", int),
        "min_score_short": ("MIN_SCORE_SHORT", int),
        "macd_filter": ("MACD_FILTER_ENABLED", lambda v: v.lower() == "on"),
        "kelly": ("KELLY_SIZING", lambda v: v.lower() == "on"),
        "chasing": ("CHASING_WAIT_SEC", float),
        "max_trades": ("MAX_TRADES", int),
        "max_same_dir": ("MAX_CONCURRENT_SAME_DIR", int),
        "refresh": ("SYMBOL_REFRESH_INTERVAL", int),
        "timeframe": ("TIMEFRAME", str),
        "weight_adx": ("WEIGHT_ADX_1", int),
        "weight_mtf_moment": ("WEIGHT_MTF_MOMENT", int),
        "weight_atr": ("WEIGHT_ATR_2", int),
        "weight_fr": ("WEIGHT_FR_2", int),
        "weight_htf": ("WEIGHT_HTF_BIAS", int),
        "weight_mtf_regime": ("WEIGHT_MTF_REGIME", int),
        "weight_vwap": ("WEIGHT_VWAP_DIST", int)
    }

    # [V18.4] 스코어링 세부 규칙 동적 지원 (자동 생성)
    scoring_keys = [
        "L_MACD_T1", "L_MACD_W1", "L_MACD_T2", "L_MACD_W2", "L_MACD_T4", "L_MACD_W4",
        "L_CVD_T1", "L_CVD_W1", "L_CVD_T2", "L_CVD_W2",
        "L_IMBAL_T1", "L_IMBAL_W1", "L_NOFI_T1", "L_NOFI_W1",
        "L_OI_T1", "L_OI_W1", "L_OI_T2", "L_OI_W2",
        "L_TICK_T1", "L_TICK_W1", "L_VOL_T1", "L_VOL_W1",
        "L_BUY_T1", "L_BUY_W1",
        "S_MACD_T1", "S_MACD_W1", "S_MACD_T2", "S_MACD_W2", "S_MACD_T4", "S_MACD_W4",
        "S_CVD_T1", "S_CVD_W1", "S_CVD_T2", "S_CVD_W2",
        "S_IMBAL_T1", "S_IMBAL_W1", "S_IMBAL_T2", "S_IMBAL_W2",
        "S_NOFI_T1", "S_NOFI_W1", "S_NOFI_T2", "S_NOFI_W2",
        "S_OI_T1", "S_OI_W1", "S_OI_T2", "S_OI_W2",
        "S_TICK_T1", "S_TICK_W1", "S_TICK_T2", "S_TICK_W2",
        "S_VOL_T1", "S_VOL_W1", "S_VOL_T2", "S_VOL_W2",
        "S_RSI_T1", "S_RSI_W1", "S_RSI_T2", "S_RSI_W2",
        "S_BUY_T1", "S_BUY_W1", "S_BUY_T2", "S_BUY_W2",
        "WEIGHT_ADX_1", "WEIGHT_MTF_MOMENT", "WEIGHT_ATR_2", "WEIGHT_FR_2",
        "WEIGHT_HTF_BIAS", "WEIGHT_MTF_REGIME", "WEIGHT_VWAP_DIST"
    ]

    for skey in scoring_keys:
        k_lower = skey.lower()
        # 점수(W)는 int, 임계치(T) 중 VOL은 float, 나머지는 int 가정
        if "_W" in skey or "_T" in skey:
            t_func = float if "_VOL_T" in skey else int
            param_map[k_lower] = (skey, t_func)

    if key not in param_map:
        await reply(update, f"❌ 설정 불가능한 키입니다: {key}")
        return

    env_key, type_func = param_map[key]
    try:
        typed_val = type_func(value_str)
        setattr(settings, env_key, typed_val)
        
        # [V18.4] 스코어링 규칙 관련이면 엔진 리빌드
        is_scoring_rule = env_key in scoring_keys
        update_env_variable(env_key, str(typed_val), silent=is_scoring_rule)
        
        if is_scoring_rule:
            settings.rebuild_scoring_rules()
            await reply(update, f"🎯 [Engine Rebuild] 스코어링 규칙 '{key}'가 {typed_val}로 즉시 업데이트되었습니다.")
        else:
            await reply(update, f"✅ 설정 변경 완료: {key} -> {typed_val}")
    except Exception as e:
        await reply(update, f"❌ 변환 오류: {e}")


async def ignore_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    if not context.args:
        await reply(update, "사용법: /ignore [BTC]")
        return
    
    coin = context.args[0].upper()
    if coin not in settings.BLACKLIST_SYMBOLS:
        settings.BLACKLIST_SYMBOLS.append(coin)
        update_env_variable("BLACKLIST_SYMBOLS", ",".join(settings.BLACKLIST_SYMBOLS))
        await reply(update, f"🚫 {coin} 종목을 블랙리스트에 추가했습니다.")
    else:
        await reply(update, f"이미 {coin} 종목이 제외되어 있습니다.")


async def allow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    if not context.args:
        await reply(update, "사용법: /allow [BTC]")
        return
    
    coin = context.args[0].upper()
    if coin in settings.BLACKLIST_SYMBOLS:
        settings.BLACKLIST_SYMBOLS.remove(coin)
        update_env_variable("BLACKLIST_SYMBOLS", ",".join(settings.BLACKLIST_SYMBOLS))
        await reply(update, f"✅ {coin} 종목을 다시 허용했습니다.")
    else:
        await reply(update, f"{coin} 종목은 이미 허용되어 있습니다.")


async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    if not context.args:
        await reply(update, "사용법: /close [DOGE]")
        return
    
    coin = context.args[0].upper()
    execution = context.bot_data["execution"]
    try:
        positions = await execution.exchange.fetch_positions()
        closed = False
        for p in positions:
            sym = p["symbol"]
            if coin in sym:
                amt = float(p.get("contracts", 0))
                if amt > 0:
                    side = "sell" if p["side"] == "long" else "buy"
                    if not settings.DRY_RUN:
                        await execution.exchange.create_order(sym, "market", side, amt, params={"reduceOnly": True})
                    closed = True
                    break
        if closed:
            await reply(update, f"✅ [{coin}] 전량 시장가 청산 완료")
        else:
            await reply(update, f"❌ [{coin}] 활성 포지션을 찾을 수 없습니다.")
    except Exception as e:
        await reply(update, f"❌ 청산 중 오류 발생: {e}")


async def params_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    
    try:
        args = context.args
        category = args[0].lower() if args else "main"

        if category == "main":
            mode = "모의투자(DRY_RUN)" if settings.DRY_RUN else "실전매매(REAL)"
            msg = (
                "⚙️ <b>[V18 시스템 주요 설정 요약]</b>\n"
                f"▪ <b>운영 모드</b>: {mode}\n"
                f"▪ <b>베팅 비중</b>: {settings.RISK_PERCENTAGE*100:.1f}% (Leverage: {settings.LEVERAGE}x)\n"
                f"▪ <b>기준 봉</b>: {settings.TIMEFRAME} / <b>갱신 주기</b>: {settings.SYMBOL_REFRESH_INTERVAL}시간\n"
                f"▪ <b>포지션 한도</b>: 총 {settings.MAX_TRADES}개 (단방향 {settings.MAX_CONCURRENT_SAME_DIR}개)\n"
                f"▪ <b>최소 스코어</b>: 🟢L:{settings.MIN_SCORE_LONG} / 🔴S:{settings.MIN_SCORE_SHORT}\n"
                f"▪ <b>MACD 필터</b>: {'✅ ON' if getattr(settings, 'MACD_FILTER_ENABLED', False) else '❌ OFF'}\n"
                f"▪ <b>블랙리스트</b>: {', '.join(settings.BLACKLIST_SYMBOLS) if settings.BLACKLIST_SYMBOLS else '없음'}\n\n"
                "💡 <b>상세 조회:</b> <code>/params [risk|trade|score|weight]</code>"
            )
        elif category == "risk":
            chandelier_mult = settings.CHANDELIER_MULT
            chandelier_atr_len = settings.CHANDELIER_ATR_LEN
            msg = (
                "🛡️ <b>[청산 및 리스크 상세 설정]</b>\n\n"
                "🟢 <b>LONG Positions</b>:\n"
                f" ▫ <b>TP 모드</b>: {settings.LONG_TP_MODE}\n"
                f"   - ATR배수: {settings.L_TP_MULT}x\n"
                f"   - 고정비율: {settings.L_TP_PCT*100:.1f}%\n"
                f" ▫ <b>SL 모드</b>: {settings.LONG_SL_MODE}\n"
                f"   - ATR배수: {settings.L_SL_MULT}x\n"
                f"   - 고정비율: {settings.L_SL_PCT*100:.1f}%\n\n"
                "🔴 <b>SHORT Positions</b>:\n"
                f" ▫ <b>TP 모드</b>: {settings.SHORT_TP_MODE}\n"
                f"   - ATR배수: {settings.S_TP_MULT}x\n"
                f"   - 고정비율: {settings.S_TP_PCT*100:.1f}%\n"
                f" ▫ <b>SL 모드</b>: {settings.SHORT_SL_MODE}\n"
                f"   - ATR배수: {settings.S_SL_MULT}x\n"
                f"   - 고정비율: {settings.S_SL_PCT*100:.1f}%\n\n"
                "⚙️ <b>공통 리스크 제어</b>:\n"
                f" ▪ <b>Chandelier</b>: {chandelier_mult}x (ATR:{chandelier_atr_len})\n"
                f" ▪ <b>Partial TP</b>: {settings.PARTIAL_TP_RATIO*100:.0f}% 물량 청산\n"
                f" ▪ <b>Breakeven</b>: 트리거 {settings.BREAKEVEN_TRIGGER_MULT}x / 확보 {settings.BREAKEVEN_PROFIT_MULT}x\n"
                f" ▪ <b>Exit Timeout</b>: {settings.TIME_EXIT_MINUTES}분 강제청산\n"
                f" ▪ <b>Cooldown</b>: 손절 후 {settings.LOSS_COOLDOWN_MINUTES}분 입구컷"
            )
        elif category == "trade":
            kelly_status = "✅ ACTIVE" if settings.KELLY_SIZING else "❌ DISABLED"
            msg = (
                "💰 <b>[체결 및 사이징 상세 설정]</b>\n\n"
                f"▪ <b>Kelly Sizing</b>: {kelly_status}\n"
                f"  - 최소표본: {settings.KELLY_MIN_TRADES} / 최대캡: {settings.KELLY_MAX_FRACTION*100:.1f}%\n"
                f"▪ <b>Chasing (Limit)</b>: {settings.CHASING_WAIT_SEC}초 대기\n"
                f"  - 최대재시도: {settings.CHASING_MAX_RETRY}회\n"
                f"  - 시장가전환점: {settings.CHASING_MARKET_THRESHOLD}회\n"
                f"▪ <b>Fee Rate</b>: {settings.FEE_RATE*100:.3f}% (계산용)\n"
                f"▪ <b>ATR Length</b>: Long {settings.ATR_LONG_LEN} / Multiplier {settings.ATR_RATIO_MULT}x"
            )
        elif category == "score":
            msg = (
                "📊 <b>[세부 스코어링 규칙 (Threshold/Weight)]</b>\n\n"
                "🟢 <b>LONG Rules</b>:\n"
                f" ▫ CVD: {settings.L_CVD_T1}/{settings.L_CVD_W1}, {settings.L_CVD_T2}/{settings.L_CVD_W2}\n"
                f" ▫ MACD: {settings.L_MACD_T1}/{settings.L_MACD_W1}, {settings.L_MACD_T2}/{settings.L_MACD_W2}, {settings.L_MACD_T4}/{settings.L_MACD_W4}\n"
                f" ▫ OI: {settings.L_OI_T1}/{settings.L_OI_W1}, {settings.L_OI_T2}/{settings.L_OI_W2}\n"
                f" ▫ IMBAL/NOFI: {settings.L_IMBAL_T1}/{settings.L_IMBAL_W1} | {settings.L_NOFI_T1}/{settings.L_NOFI_W1}\n"
                f" ▫ VOL/TICK: {settings.L_VOL_T1}/{settings.L_VOL_W1} | {settings.L_TICK_T1}/{settings.L_TICK_W1}\n"
                f" ▫ BUY Ratio: {settings.L_BUY_T1}/{settings.L_BUY_W1}\n\n"
                "🔴 <b>SHORT Rules</b>:\n"
                f" ▫ CVD: {settings.S_CVD_T1}/{settings.S_CVD_W1}, {settings.S_CVD_T2}/{settings.S_CVD_W2}\n"
                f" ▫ MACD: {settings.S_MACD_T1}/{settings.S_MACD_W1}, {settings.S_MACD_T2}/{settings.S_MACD_W2}, {settings.S_MACD_T4}/{settings.S_MACD_W4}\n"
                f" ▫ OI: {settings.S_OI_T1}/{settings.S_OI_W1}, {settings.S_OI_T2}/{settings.S_OI_W2}\n"
                f" ▫ IMBAL: {settings.S_IMBAL_T1}/{settings.S_IMBAL_W1}, {settings.S_IMBAL_T2}/{settings.S_IMBAL_W2}\n"
                f" ▫ NOFI: {settings.S_NOFI_T1}/{settings.S_NOFI_W1}, {settings.S_NOFI_T2}/{settings.S_NOFI_W2}\n"
                f" ▫ VOL: {settings.S_VOL_T1}/{settings.S_VOL_W1}, {settings.S_VOL_T2}/{settings.S_VOL_W2}\n"
                f" ▫ TICK: {settings.S_TICK_T1}/{settings.S_TICK_W1}, {settings.S_TICK_T2}/{settings.S_TICK_W2}\n"
                f" ▫ RSI: {settings.S_RSI_T1}/{settings.S_RSI_W1}, {settings.S_RSI_T2}/{settings.S_RSI_W2}\n"
                f" ▫ BUY Ratio: {settings.S_BUY_T1}/{settings.S_BUY_W1}, {settings.S_BUY_T2}/{settings.S_BUY_W2}"
            )
        elif category == "weight":
            w = settings.SCORING_WEIGHTS
            msg = (
                "⚖️ <b>[글로벌 가중치 및 필터 설정]</b>\n\n"
                f"▫ <b>ADX Boost</b>: {w.get('adx_boost', {}).get('1', 'N/A')} (Pctl: {settings.ADX_BOOST_PCTL}%)\n"
                f"▫ <b>HTF Bias</b>: {w.get('htf_bias', {}).get('2', 'N/A')} ({settings.HTF_TIMEFRAME_1H})\n"
                f"▫ <b>MTF Regime</b>: {w.get('mtf_regime', {}).get('1', 'N/A')}\n"
                f"▫ <b>MTF Moment</b>: {w.get('mtf_moment', {}).get('2', 'N/A')}\n"
                f"▫ <b>ATR Vol</b>: {w.get('atr', {}).get('2', 'N/A')}\n"
                f"▫ <b>Funding Rate</b>: {w.get('fr_boost', {}).get('2', 'N/A')}\n"
                f"▫ <b>VWAP Distance</b>: {w.get('vwap_dist', {}).get('2', 'N/A')}\n"
                f"▫ <b>PCTL Window</b>: {settings.PCTL_WINDOW}봉"
            )
        else:
            msg = "❌ 알 수 없는 카테고리입니다. <code>/params [risk|trade|score|weight]</code>를 확인하세요."

        await reply(update, msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error in params_cmd: {e}", exc_info=True)
        await reply(update, f"❌ 설정 조회 중 오류가 발생했습니다: {e}")


async def refresh_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return

    refresh_event = context.bot_data.get("refresh_event")
    if refresh_event:
        refresh_event.set()
        await reply(update, "🔄 즉시 종목 새로고침 신호를 보냈습니다. 곧 반영됩니다.")
    else:
        await reply(update, "❌ 새로고침 이벤트를 찾을 수 없습니다.")


def setup_telegram_bot(execution_engine, refresh_event=None):
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID

    if not token or not chat_id:
        logger.warning("텔레그램 토큰/Chat ID가 없어 컨트롤러를 시작할 수 없습니다.")
        return None

    request = HTTPXRequest(connect_timeout=20, read_timeout=20)
    application = ApplicationBuilder().token(token).request(request).build()
    application.bot_data["execution"] = execution_engine
    application.bot_data["refresh_event"] = refresh_event
    application.bot_data["shutdown_event"] = asyncio.Event()  # 기본 이벤트 생성
    application.bot_data["exit_code"] = 0  # 기본 종료 코드

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("params", params_cmd))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CommandHandler("pause", pause_cmd))
    application.add_handler(CommandHandler("resume", resume_cmd))
    application.add_handler(CommandHandler("real", real_cmd))
    application.add_handler(CommandHandler("dryrun", dryrun_cmd))
    application.add_handler(CommandHandler("restart", restart_cmd))
    application.add_handler(CommandHandler("panic", panic_cmd))
    application.add_handler(CommandHandler("setparam", setparam_cmd))
    application.add_handler(CommandHandler("ignore", ignore_cmd))
    application.add_handler(CommandHandler("allow", allow_cmd))
    application.add_handler(CommandHandler("close", close_cmd))
    application.add_handler(CommandHandler("refresh", refresh_cmd))

    logger.info("텔레그램 Interactive 커맨더 세팅 완료.")
    return application
