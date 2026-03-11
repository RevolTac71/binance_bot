import os
import sys
import asyncio
import psutil
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest
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
        "🤖 V18 자동매매 봇 컨트롤 패널\n\n"
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
            "📈 <b>[V18 진입 스코어링 시스템 안내]</b>\n"
            "롱/숏이 완전히 분리된 규칙 기반 엔진을 사용합니다.\n\n"
            "<b>1. 진입 합격점 (Min Score)</b>\n"
            "▫ <code>long_score</code> : 롱 최소 합계 점수\n"
            "▫ <code>short_score</code>: 숏 최소 합계 점수\n\n"
            "<b>2. 규칙 설정 형식 (Rules)</b>\n"
            "기본 형식: <code>{l/s}_{지표}_{t/w}{단계}</code>\n"
            "▫ <code>t</code>: 임계값 (상위 %, σ 등)\n"
            "▫ <code>w</code>: 부여할 점수 (배점)\n"
            "예: <code>s_rsi_w2 2</code> (숏 RSI 2단계 달성 시 2점 부여)\n\n"
            "▫ <code>macd_filter</code>: ON 설정 시 MACD 방향 불일치 시 스코어 무관 진입 차단\n\n"
            "💡 <b>팁:</b> 특정 방향 진입을 막으려면 <code>min_score</code>를 매우 높게 설정하세요."
        )
    elif category == "trade":
        msg = (
            "💰 <b>[체결 및 사이징 설정]</b>\n"
            "베팅 비중과 체결 방식에 관한 설정입니다.\n\n"
            "▫ <code>risk</code> : 베팅 비중 (계좌 대비 %, 0.01 = 1%)\n"
            "▫ <code>leverage</code> : 레버리지 배수\n"
            "▫ <code>mode</code> : dry(모의) 또는 real(실전)\n"
            "▫ <code>kelly</code> : 켈리 사이징 사용 (on/off)\n"
            "▫ <code>chasing</code> : 지정가 체결 대기 시간 (초)\n"
            "▫ <code>refresh</code> : 종목 리프레시 주기 (시간 단위)\n"
            "▫ <code>timeframe</code> : 메인 분석 봉 (예: 3m)\n"
            "▫ <code>htf_1h</code> / <code>htf_15m</code> : 중장기 분석 봉"
        )
    elif category == "risk":
        msg = (
            "🛡️ <b>[청산 및 리스크 관리 설정]</b>\n"
            "포지션 종료(Exit) 전략을 세밀하게 제어합니다.\n\n"
            "<b>1. 익절/손절 모드 (Exit Mode)</b>\n"
            "▫ <code>l_tp_mode</code> / <code>l_sl_mode</code> : 롱 익절/손절 방식\n"
            "▫ <code>s_tp_mode</code> / <code>s_sl_mode</code> : 숏 익절/손절 방식\n"
            "   (값: <code>ATR</code> 또는 <code>PERCENT</code>)\n\n"
            "<b>2. 목표가 설정 파라미터</b>\n"
            "▫ <b>ATR 기반 (Mult)</b>\n"
            "   - <code>l_tp</code>, <code>l_sl</code>, <code>s_tp</code>, <code>s_sl</code> (배수)\n"
            "▫ <b>비율 기반 (Pct)</b>\n"
            "   - <code>l_tp_pct</code>, <code>l_sl_pct</code>\n"
            "   - <code>s_tp_pct</code>, <code>s_sl_pct</code> (예: 0.03 = 3%)\n\n"
            "<b>3. 기타 안전장치</b>\n"
            "▫ <code>fee_rate</code>   : 기본 수수료율 (0.00045)\n"
            "▫ <code>partial_tp</code> : 분할 익절 비율 (0.5)\n"
            "▫ <code>chandelier</code> : 추적 손절 배수\n"
            "▫ <code>cooldown</code>   : 손절 후 진입 제한 (분)\n\n"
            "💡 <b>예시:</b> 숏 익절만 퍼센트로 바꾸려면?\n"
            "<code>/setparam s_tp_mode PERCENT</code>"
        )
    else:
        msg = "❌ 알 수 없는 카테고리입니다. `/help`를 입력해 목록을 확인하세요."

    await update.message.reply_text(msg, parse_mode="HTML")


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

    msg = (
        f"📊 <b>실시간 봇 상태 리포트</b>\n"
        f"──────────────────\n"
        f"🕒 <b>Uptime</b>: {days}일 {hours}시간 {minutes}분\n"
        f"💰 <b>USDT 잔고</b>: {capital:,.2f} USDT\n"
        f"🚦 <b>신규 진입</b>: {'✅ ACTIVE' if not settings.PAUSED else '⚠️ PAUSED'}\n"
        f"──────────────────\n"
        f"📦 <b>보유 포지션</b>:\n{position_details}"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    settings.PAUSED = True
    update_env_variable("PAUSED", "True")
    await update.message.reply_text("⚠️ 신규 진입이 일시 중지되었습니다.")


async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    settings.PAUSED = False
    update_env_variable("PAUSED", "False")
    await update.message.reply_text("✅ 신규 진입이 재개되었습니다.")


async def restart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    await update.message.reply_text("🔄 봇을 재시작합니다 (프로세스 재실행)...")
    # watchdog이 다시 띄워줄 수 있도록 프로세스 종료
    os._exit(0)


async def panic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    execution = context.bot_data["execution"]
    await update.message.reply_text("🚨 PANIC! 모든 포지션 시장가 정리 및 봇 정지 시도...")
    
    # 1. 진입 중지
    settings.PAUSED = True
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
        await update.message.reply_text("✅ 모든 포지션이 정리되었습니다. 봇을 종료합니다.")
        os._exit(0)
    except Exception as e:
        await update.message.reply_text(f"❌ 패닉 셀 중 오류 발생: {e}")


async def setparam_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("사용법: /setparam [키] [값]\n예: /setparam risk 0.03")
        return

    key = context.args[0].lower()
    value_str = context.args[1]

    # 설정 가능 파라미터 맵핑
    param_map = {
        "risk": ("RISK_PERCENTAGE", float),
        "leverage": ("LEVERAGE", int),
        "sl": ("STOP_LOSS_MULT", float),
        "tp": ("TAKE_PROFIT_MULT", float),
        "l_tp": ("LONG_TP_MULT", float),
        "l_sl": ("LONG_SL_MULT", float),
        "s_tp": ("SHORT_TP_MULT", float),
        "s_sl": ("SHORT_SL_MULT", float),
        "l_tp_pct": ("LONG_TP_PCT", float),
        "l_sl_pct": ("LONG_SL_PCT", float),
        "s_tp_pct": ("SHORT_TP_PCT", float),
        "s_sl_pct": ("SHORT_SL_PCT", float),
        "l_tp_mode": ("LONG_TP_MODE", str),
        "l_sl_mode": ("LONG_SL_MODE", str),
        "s_tp_mode": ("SHORT_TP_MODE", str),
        "s_sl_mode": ("SHORT_SL_MODE", str),
        "partial_tp": ("PARTIAL_TP_RATIO", float),
        "fee_rate": ("FEE_RATE", float),
        "chandelier": ("CHANDELIER_MULT", float),
        "cooldown": ("LOSS_COOLDOWN_MINUTES", int),
        "min_score_long": ("MIN_SCORE_LONG", int),
        "min_score_short": ("MIN_SCORE_SHORT", int),
        "macd_filter": ("MACD_FILTER_ENABLED", lambda v: v.lower() == "on")
    }

    if key not in param_map:
        await update.message.reply_text(f"❌ 설정 불가능한 키입니다: {key}")
        return

    env_key, type_func = param_map[key]
    try:
        typed_val = type_func(value_str)
        setattr(settings, env_key, typed_val)
        update_env_variable(env_key, str(typed_val))
        await update.message.reply_text(f"✅ 설정 변경 완료: {key} -> {typed_val}")
    except Exception as e:
        await update.message.reply_text(f"❌ 변환 오류: {e}")


async def ignore_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    if not context.args:
        await update.message.reply_text("사용법: /ignore [BTC]")
        return
    
    coin = context.args[0].upper()
    if coin not in settings.BLACKLIST_SYMBOLS:
        settings.BLACKLIST_SYMBOLS.append(coin)
        update_env_variable("BLACKLIST_SYMBOLS", ",".join(settings.BLACKLIST_SYMBOLS))
        await update.message.reply_text(f"🚫 {coin} 종목을 블랙리스트에 추가했습니다.")
    else:
        await update.message.reply_text(f"이미 {coin} 종목이 제외되어 있습니다.")


async def allow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    if not context.args:
        await update.message.reply_text("사용법: /allow [BTC]")
        return
    
    coin = context.args[0].upper()
    if coin in settings.BLACKLIST_SYMBOLS:
        settings.BLACKLIST_SYMBOLS.remove(coin)
        update_env_variable("BLACKLIST_SYMBOLS", ",".join(settings.BLACKLIST_SYMBOLS))
        await update.message.reply_text(f"✅ {coin} 종목을 다시 허용했습니다.")
    else:
        await update.message.reply_text(f"{coin} 종목은 이미 허용되어 있습니다.")


async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    if not context.args:
        await update.message.reply_text("사용법: /close [DOGE]")
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
            await update.message.reply_text(f"✅ [{coin}] 전량 시장가 청산 완료")
        else:
            await update.message.reply_text(f"❌ [{coin}] 활성 포지션을 찾을 수 없습니다.")
    except Exception as e:
        await update.message.reply_text(f"❌ 청산 중 오류 발생: {e}")


async def params_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    
    mode = "모의투자(DRY_RUN)" if settings.DRY_RUN else "실전매매(REAL)"
    msg = (
        "⚙️ <b>[현재 전략 파라미터 요약]</b>\n"
        f"▪ <b>Mode</b>: {mode}\n"
        f"▪ <b>Risk</b>: {settings.RISK_PERCENTAGE*100:.1f}%\n"
        f"▪ <b>Leverage</b>: {settings.LEVERAGE}x\n"
        f"▪ <b>Candle</b>: {settings.TIMEFRAME} / Exit={settings.TIME_EXIT_MINUTES}min\n"
        f"▪ <b>Score</b>: L={settings.MIN_SCORE_LONG} / S={settings.MIN_SCORE_SHORT}\n"
        f"▪ <b>MACD Filter</b>: {'ON' if getattr(settings, 'MACD_FILTER_ENABLED', False) else 'OFF'}\n\n"
        "── <b>방향별 청산 전략</b> ──\n"
        f"<b>LONG</b>: {settings.LONG_TP_MODE}({settings.LONG_TP_MULT}x / {settings.LONG_TP_PCT*100}%) | {settings.LONG_SL_MODE}({settings.LONG_SL_MULT}x / {settings.LONG_SL_PCT*100}%)\n"
        f"<b>SHORT</b>: {settings.SHORT_TP_MODE}({settings.SHORT_TP_MULT}x / {settings.SHORT_TP_PCT*100}%) | {settings.SHORT_SL_MODE}({settings.SHORT_SL_MULT}x / {settings.SHORT_SL_PCT*100}%)\n"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def refresh_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return

    refresh_event = context.bot_data.get("refresh_event")
    if refresh_event:
        refresh_event.set()
        await update.message.reply_text("🔄 즉시 종목 새로고침 신호를 보냈습니다. 곧 반영됩니다.")
    else:
        await update.message.reply_text("❌ 새로고침 이벤트를 찾을 수 없습니다.")


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

    logger.info("텔레그램 Interactive 커맨더 세팅 완료.")
    return application
