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
        await reply(update, "❌ 권한이 없는 사용자입니다.")
        return False
    return True


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    msg = (
        "🤖 V18.6 자동매매 봇 컨트롤 패널\n\n"
        "ℹ️ 기본 명령어\n"
        "/help : 전체 명령어 가이드\n"
        "/status : 봇 운영 상태 및 포지션 요약\n"
        "/pause / /resume : 신규 진입 일시정지 / 재개\n"
        "/real / /dryrun : 실전매매 / 모의투자 전환\n"
        "/panic : 비상! 전 포지션 시장가 정리 후 종료\n"
        "/restart : 봇 재시작\n\n"
        "⚙️ 파라미터 변경\n"
        "/setparam [변수명] [값] : 파라미터 즉시 변경\n"
        "  예: /setparam risk 0.02\n"
        "  예: /setparam leverage 5\n"
        "상세한 파라미터 목록은 /help 참조"
    )
    await reply(update, msg)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return

    args = context.args
    category = args[0].lower() if args else None

    if not category:
        msg = (
            "🤖 <b>[Binance Bot 텔레그램 관리 센터]</b>\n"
            "원하는 카테고리를 선택하여 상세 정보를 확인하세요.\n\n"
            "ℹ️ <b>명령어 카테고리</b> 안내\n"
            " • `/help cmd` : 제어 명령어 목록 및 봇 상태 제어\n"
            " • `/help strategy` : 볼린저 밴드 스퀴즈 전략 파라미터 설정\n"
            " • `/help risk` : 익절/손절 및 리스크 관리 설정\n"
            " • `/help trade` : 주문 속성 및 체결 틱 설정\n\n"
            "💡 <b>사용법:</b> `/setparam [변수명] [설정값]` 으로 즉시 설정 변경 가능"
        )
    elif category == "cmd":
        msg = (
            "🤖 <b>[제어 명령어 목록]</b>\n\n"
            " • `/status` : 현재 상태, 포지션 현황 요약\n"
            " • `/params` : 현재 설정된 모든 파라미터 조회\n"
            " • `/pause`  : 신규 진입 일시 중단\n"
            " • `/resume` : 중단된 진입 다시 시작\n"
            " • `/real`   : 실전매매(REAL) 모드로 변경\n"
            " • `/dryrun` : 모의투자(DRY_RUN) 모드로 변경\n"
            " • `/refresh` : 즉시 상위 거래대금 종목 갱신\n"
            " • `/ignore [코인]` : 해당 종목 감시 제외\n"
            " • `/allow [코인]` : 종목 제외 해제\n"
            " • `/close [코인]` : 특정 종목 시장가 강제 청산\n"
            " • `/panic`  : 모든 포지션 강제 청산 및 봇 종료\n"
            " • `/restart` : 봇 강제 재시작"
        )
    elif category == "strategy":
        msg = (
            "⚙️ <b>[BB Squeeze Breakout 전략 설정 가이드]</b>\n"
            "볼린저 밴드 스퀴즈 수축 및 돌파 전략의 주요 변수 설정 가이드입니다.\n\n"
            "<b>주요 파라미터 변수명 (Key):</b>\n"
            " • <code>squeeze_threshold</code> : 밴드폭 수축 임계 비율 (기본 0.8 => 100봉 평균 대비 80%)\n"
            " • <code>volume_mult</code> : 거래량 돌파 임계 비율 (기본 2.0 => 20봉 평균 대비 2배)\n"
            " • <code>atr_mult</code> : 진입 시 TP/SL을 결정하는 ATR 승수 (기본 1.5)\n"
            " • <code>max_squeeze_dur</code> : 스퀴즈 상태 유지 최대 봉 수 (기본 12)"
        )
    elif category == "trade":
        msg = (
            "💸 <b>[체결 및 파이프라인 설정]</b>\n\n"
            "<b>주요 파라미터 변수명 (Key):</b>\n"
            " • <code>risk</code> : 베팅 비중 (0.01 = 1%)\n"
            " • <code>leverage</code> : 레버리지 배수 (int)\n"
            " • <code>chasing</code> : 지정가 대기 시간 (초)\n"
            " • <code>timeframe</code> : 기준 분봉 (예: 3m)"
        )
    elif category == "risk":
        msg = (
            "🛡️ <b>[익절 및 리스크 관리]</b>\n\n"
            "<b>주요 파라미터 변수명 (Key):</b>\n"
            " • <code>l_tp_mode</code> / <code>s_tp_mode</code> : 익절 방식 (ATR/PERCENT)\n"
            " • <code>l_sl_mode</code> / <code>s_sl_mode</code> : 손절 방식 (ATR/PERCENT)\n"
            " • <code>l_tp</code> / <code>s_tp</code> : LONG/SHORT ATR 익절 배수\n"
            " • <code>l_sl</code> / <code>s_sl</code> : LONG/SHORT ATR 손절 배수\n"
            " • <code>l_tp_pct</code> / <code>s_tp_pct</code> : 비율 기반 익절 (0.05 = 5%)\n"
            " • <code>l_sl_pct</code> / <code>s_sl_pct</code> : 비율 기반 손절 (0.02 = 2%)\n"
            " • <code>chandelier</code> : 샹들리에 추적 손절 배수\n"
            " • <code>cooldown</code> : 손절 후 재진입 제한(분)\n"
            " • <code>partial_tp</code> : 분할 익절 비중 (0.5 = 50%)"
        )
    else:
        msg = "❌ 존재하지 않는 카테고리입니다. `/help`을 입력해 목록을 확인하세요."

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
        balance_info = await execution.exchange.fetch_balance({"type": "future"})
        capital = balance_info.get("total", {}).get("USDT", 0.0)
    except Exception as e:
        capital = "조회 실패"

    position_details = ""
    try:
        active_pos_list = []
        for sym, pos in execution.active_positions.items():
            side = pos.get("signal", "Unknown")
            entry_price = pos.get("limit_price", 0.0)
            amount = pos.get("amount", 0.0)
            tp_price = pos.get("tp_price", 0.0)
            sl_price = pos.get("sl_price", 0.0)
            active_pos_list.append(
                f"  • <b>{sym}</b> ({side} {amount}개)\n"
                f"    평단: {entry_price:.4f} / TP: {tp_price:.4f} / SL: {sl_price:.4f}"
            )
        position_details = "\n".join(active_pos_list) if active_pos_list else "현재 보유 포지션 없음"
    except Exception as e:
        position_details = f"조회 오류: {e}"

    pending_details = ""
    try:
        pending_list = []
        for sym, order in execution.pending_entries.items():
            side = order.get("side", "Unknown")
            price = order.get("price", 0.0)
            qty = order.get("amount", 0.0)
            elapsed = order.get("bars_elapsed", 0)
            pending_list.append(
                f"  • <b>{sym}</b> ({side.upper()} {qty}개) - 대기단가: {price:.4f} ({elapsed}봉 경과)"
            )
        pending_details = "\n".join(pending_list) if pending_list else "대기 진입 주문 없음"
    except Exception as e:
        pending_details = f"조회 오류: {e}"

    is_paused = getattr(settings, "PAUSED", False)
    capital_text = f"{capital:,.2f} USDT" if isinstance(capital, (int, float)) else str(capital)
    msg = (
        f"📊 <b>퀀트 봇 상태 리포트</b>\n"
        f"──────────────────────\n"
        f" • <b>Uptime</b>: {days}일 {hours}시간 {minutes}분\n"
        f" • <b>USDT 잔고</b>: {capital_text}\n"
        f" • <b>신규 진입</b>: {'🟢 ACTIVE' if not is_paused else '🔴 PAUSED'}\n"
        f"──────────────────────\n"
        f"📈 <b>활성 포지션</b>:\n{position_details}\n"
        f"──────────────────────\n"
        f"⏳ <b>진입 대기 주문</b>:\n{pending_details}"
    )
    await reply(update, msg, parse_mode="HTML")


async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    setattr(settings, "PAUSED", True)
    update_env_variable("PAUSED", "True")
    await reply(update, "⏸️ 신규 진입이 일시 중단되었습니다.")


async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    setattr(settings, "PAUSED", False)
    update_env_variable("PAUSED", "False")
    await reply(update, "▶️ 신규 진입이 재개되었습니다.")


async def restart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    await reply(update, "🔄 봇을 재시작합니다 (프로세스 자동 복구 가동)...")
    context.bot_data["exit_code"] = 42
    shutdown_event = context.bot_data.get("shutdown_event")
    if shutdown_event:
        shutdown_event.set()


async def real_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    setattr(settings, "DRY_RUN", False)
    update_env_variable("DRY_RUN", "False")
    await reply(update, "⚠️ 봇이 <b>실전매매(REAL)</b> 모드로 전환되었습니다.\n(/restart 로 프로세스 재부팅 권장)", parse_mode="HTML")


async def dryrun_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    setattr(settings, "DRY_RUN", True)
    update_env_variable("DRY_RUN", "True")
    await reply(update, "🧪 봇이 <b>모의투자(DRY_RUN)</b> 모드로 전환되었습니다.", parse_mode="HTML")


async def panic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    execution = context.bot_data["execution"]
    await reply(update, "🚨 PANIC! 모든 활성 포지션을 시장가로 즉시 청산하고 봇을 중지합니다...")
    
    setattr(settings, "PAUSED", True)
    update_env_variable("PAUSED", "True")

    try:
        positions = await execution.exchange.fetch_positions()
        for p in positions:
            amt = float(p.get("contracts", 0))
            if amt > 0:
                side = "sell" if p["side"] == "long" else "buy"
                symbol = p["symbol"]
                await execution.exchange.create_order(symbol, "market", side, amt, params={"reduceOnly": True})
        await reply(update, "✅ 모든 포지션 청산 완료. 봇을 종료합니다.")
        
        context.bot_data["exit_code"] = 0
        shutdown_event = context.bot_data.get("shutdown_event")
        if shutdown_event:
            shutdown_event.set()
    except Exception as e:
        await reply(update, f"❌ 긴급 청산 중 오류 발생: {e}")


async def setparam_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    
    args = context.args or []
    if len(args) < 2:
        await reply(update, "💡 사용법: /setparam [변수명] [설정값]\n예: /setparam risk 0.03")
        return

    key = args[0]
    value_str = args[1]

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
        "chasing": ("CHASING_WAIT_SEC", float),
        "max_trades": ("MAX_TRADES", int),
        "max_same_dir": ("MAX_CONCURRENT_SAME_DIR", int),
        "timeframe": ("TIMEFRAME", str),
        "squeeze_threshold": ("SQUEEZE_THRESHOLD", float),
        "volume_mult": ("VOLUME_MULTIPLIER", float),
        "atr_mult": ("ATR_MULTIPLIER", float),
        "max_squeeze_dur": ("MAX_SQUEEZE_DURATION", int),
    }

    if key not in param_map:
        await reply(update, f"❌ 설정할 수 없는 변수명입니다: {key}")
        return

    env_key, type_func = param_map[key]
    try:
        typed_val = type_func(value_str)
        setattr(settings, env_key, typed_val)
        update_env_variable(env_key, str(typed_val), silent=False)
        await reply(update, f"✅ 설정 완료: {key} -> {typed_val}")
    except Exception as e:
        await reply(update, f"❌ 값 변경 실패: {e}")


async def ignore_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    args = context.args or []
    if not args:
        await reply(update, "💡 사용법: /ignore [코인명]\n예: /ignore SOL/USDT:USDT")
        return
    coin = args[0].upper()
    execution = context.bot_data["execution"]
    execution.blacklist.add(coin)
    await reply(update, f"🚫 {coin} 종목을 감시 제외 블랙리스트에 등록했습니다.")


async def allow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    args = context.args or []
    if not args:
        await reply(update, "💡 사용법: /allow [코인명]\n예: /allow SOL/USDT:USDT")
        return
    coin = args[0].upper()
    execution = context.bot_data["execution"]
    execution.blacklist.discard(coin)
    await reply(update, f"🟢 {coin} 종목을 감시 제외 리스트에서 제거했습니다.")


async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    args = context.args or []
    if not args:
        await reply(update, "💡 사용법: /close [코인명]\n예: /close SOL/USDT:USDT")
        return
    coin = args[0].upper()
    execution = context.bot_data["execution"]
    if coin in execution.active_positions:
        await reply(update, f"⚠️ {coin} 종목 시장가 강제 청산 명령을 송신합니다...")
        await execution.close_position_market(coin, reason="Telegram Force Close")
        await reply(update, f"✅ {coin} 포지션 수동 청산 완료.")
    else:
        await reply(update, f"❌ {coin} 종목의 활성 포지션이 존재하지 않습니다.")


async def params_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return

    try:
        args = context.args
        category = args[0].lower() if args else "main"

        if category == "main":
            mode = "모의투자(DRY_RUN)" if settings.DRY_RUN else "실전매매(REAL)"
            msg = (
                "🤖 <b>[Scalper 봇 주요 설정 요약]</b>\n"
                f" • <b>운영 모드</b>: {mode}\n"
                f" • <b>베팅 비중</b>: {settings.RISK_PERCENTAGE*100:.1f}% (레버리지: {settings.LEVERAGE}x)\n"
                f" • <b>기준 분봉</b>: {settings.TIMEFRAME} / <b>스퀴즈 한도</b>: {getattr(settings, 'SQUEEZE_THRESHOLD', 0.8):.2f}\n"
                f" • <b>최대 포지션</b>: 총 {settings.MAX_TRADES}개 (동일방향 {settings.MAX_CONCURRENT_SAME_DIR}개)\n"
                f" • <b>블랙리스트</b>: {', '.join(settings.BLACKLIST_SYMBOLS) if settings.BLACKLIST_SYMBOLS else '없음'}\n\n"
                "💡 <b>상세 조회:</b> <code>/params [risk|trade|strategy]</code>"
            )
        elif category == "risk":
            msg = (
                "🛡️ <b>[익절 및 리스크 상세 설정]</b>\n\n"
                f" • <b>LONG Positions</b>:\n"
                f"   - TP 모드: {settings.LONG_TP_MODE} / ATR배수: {settings.L_TP_MULT}x / 고정비율: {settings.L_TP_PCT*100:.1f}%\n"
                f"   - SL 모드: {settings.LONG_SL_MODE} / ATR배수: {settings.L_SL_MULT}x / 고정비율: {settings.L_SL_PCT*100:.1f}%\n\n"
                f" • <b>SHORT Positions</b>:\n"
                f"   - TP 모드: {settings.SHORT_TP_MODE} / ATR배수: {settings.S_TP_MULT}x / 고정비율: {settings.S_TP_PCT*100:.1f}%\n"
                f"   - SL 모드: {settings.SHORT_SL_MODE} / ATR배수: {settings.S_SL_MULT}x / 고정비율: {settings.S_SL_PCT*100:.1f}%\n\n"
                f" • <b>공통 세부 제어</b>:\n"
                f"   - Chandelier Exit: {settings.CHANDELIER_MULT}x (ATR: {settings.CHANDELIER_ATR_LEN})\n"
                f"   - 분할 TP 비율: {settings.PARTIAL_TP_RATIO*100:.0f}% 물량\n"
                f"   - 본절 확보(Breakeven): 트리거 {settings.BREAKEVEN_TRIGGER_MULT}x / 확보 {settings.BREAKEVEN_PROFIT_MULT}x\n"
                f"   - 손절 후 쿨다운: {settings.LOSS_COOLDOWN_MINUTES}분"
            )
        elif category == "trade":
            msg = (
                "💸 <b>[주문 체결 상세 설정]</b>\n\n"
                f" • <b>Chasing (지정가 대기)</b>: {settings.CHASING_WAIT_SEC}초\n"
                f" • <b>Fee Rate (수수료율)</b>: {settings.FEE_RATE*100:.3f}%\n"
                f" • <b>ATR Length</b>: Long {settings.ATR_LONG_LEN} / Multiplier {settings.ATR_RATIO_MULT}x"
            )
        elif category == "strategy":
            msg = (
                "⚙️ <b>[BB Squeeze Breakout 전략 매개변수]</b>\n\n"
                f" • <b>Squeeze Threshold</b>: {getattr(settings, 'SQUEEZE_THRESHOLD', 0.8):.2f} (수축 기준 비율)\n"
                f" • <b>Volume Multiplier</b>: {getattr(settings, 'VOLUME_MULTIPLIER', 2.0):.1f}x (평균 대비 거래량 폭발 배수)\n"
                f" • <b>ATR Multiplier</b>: {getattr(settings, 'ATR_MULTIPLIER', 1.5):.1f}x (TP/SL ATR 거리)\n"
                f" • <b>Max Squeeze Duration</b>: {getattr(settings, 'MAX_SQUEEZE_DURATION', 12)}봉 (스퀴즈 유지 봉 수)"
            )
        else:
            msg = "❌ 존재하지 않는 카테고리입니다. <code>/params [risk|trade|strategy]</code>을 입력하세요."

        await reply(update, msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error in params_cmd: {e}", exc_info=True)
        await reply(update, f"❌ 설정값 조회 중 오류 발생: {e}")


async def refresh_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return

    refresh_event = context.bot_data.get("refresh_event")
    if refresh_event:
        refresh_event.set()
        await reply(update, "🔄 즉시 상위 15개 거래대금 종목 갱신 신호를 보냈습니다.")
    else:
        await reply(update, "❌ 갱신 이벤트가 설정되지 않았습니다.")


def setup_telegram_bot(execution_engine, refresh_event=None):
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID

    if not token or not chat_id:
        logger.warning("텔레그램 토큰/Chat ID가 없어 컨트롤러를 시작할 수 없습니다.")
        return None

    request = HTTPXRequest(connect_timeout=30, read_timeout=30)
    application = ApplicationBuilder().token(token).request(request).build()
    application.bot_data["execution"] = execution_engine
    application.bot_data["refresh_event"] = refresh_event
    application.bot_data["shutdown_event"] = asyncio.Event()  # 기본 이벤트 활성화
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
