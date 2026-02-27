import os
import sys
import asyncio
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
        "🤖 V11.2 자동매매 봇 컨트롤 패널\n\n"
        "💡 가능한 명령어:\n"
        "/help - 이 도움말 메뉴 표시\n"
        "/status - 봇 상태 및 수익 요약\n"
        "/pause - 매매 신규 진입 일시정지\n"
        "/resume - 매매 재개\n"
        "/leverage [N] - 레버리지 N배로 변경 (영구)\n"
        "/mode [dry_run|real] - 매매 모드 변경 (영구)\n"
        "/panic - 비상! 모든 주문 취소 및 시장가 전량 청산 후 정지\n"
        "/restart - 봇 재부팅 (nohup 효과)"
    )
    await update.message.reply_text(msg)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    msg = (
        "📖 [자동매매 봇 명령어 도움말]\n\n"
        "🔹 /help : 현재 보여지는 명령어 목록과 설명을 확인합니다.\n"
        "🔹 /status : 봇의 현재 상태(모드, 가동 여부, 수익 요약, 활성 포지션)를 요약해서 보여줍니다.\n"
        "🔹 /pause : 새로운 매매 진입을 일시정지합니다 (기존 포지션의 수익실현/손절 감시는 유지됨).\n"
        "🔹 /resume : 일시정지된 봇의 매매 진입을 다시 재개합니다.\n"
        "🔹 /leverage [숫자] : 거래 레버리지를 주어진 숫자로 영구 변경합니다 (예: /leverage 5).\n"
        "🔹 /mode [dry_run|real] : 모의투자(dry_run) 또는 실전매매(real) 모드로 영구 전환합니다.\n"
        "🔹 /panic : [위급상황] 모든 미체결 주문을 취소하고, 보유 포지션을 전부 시장가로 전량 청산한 후 봇을 일시정지(pause) 상태로 만듭니다.\n"
        "🔹 /restart : 봇 애플리케이션 프로세스를 강제 재부팅합니다."
    )
    await update.message.reply_text(msg)


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

    mode = "DRY_RUN (모의투자)" if settings.DRY_RUN else "REAL (실전 매매)"
    status_str = "일시정지됨 ⏸️" if settings.IS_PAUSED else "가동 중 🟢"

    msg = (
        f"📊 [봇 상태 요약]\n"
        f"- 매매 모드: {mode}\n"
        f"- 봇 동작: {status_str}\n"
        f"- 레버리지: {settings.LEVERAGE}x\n"
        f"- 생존 시간: {days}일 {hours}시간 {minutes}분\n"
        f"- 총 잔고: {capital} USDT\n\n"
        f"✅ 활성 포지션: {len(execution.active_positions)} 개\n"
        f"⏳ 대기중 진입: {len(execution.pending_entries)} 개\n"
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


async def leverage_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text(
            "💡 사용법: /leverage [숫자]\n예시: /leverage 5"
        )
        return

    new_lev = int(args[0])
    settings.LEVERAGE = new_lev
    update_env_variable("LEVERAGE", str(new_lev))

    await update.message.reply_text(
        f"✅ 레버리지가 {new_lev}x 로 변경되었습니다. (DB 환경변수 영구 반영 완료)"
    )


async def mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    args = context.args
    if not args or args[0].lower() not in ["dry_run", "real"]:
        await update.message.reply_text(
            "💡 사용법: /mode [dry_run|real]\n예시: /mode real"
        )
        return

    mode_str = args[0].lower()
    is_dry = "true" if mode_str == "dry_run" else "false"

    settings.DRY_RUN = mode_str == "dry_run"
    update_env_variable("DRY_RUN", is_dry.capitalize())

    res_str = "모의투자(DRY_RUN)" if settings.DRY_RUN else "실전 매매(REAL)"
    await update.message.reply_text(
        f"🔄 매매 모드가 [{res_str}] 상태로 전환되었습니다. (DB 환경변수 영구 반영 완료)"
    )


async def restart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    await update.message.reply_text(
        "🔄 봇 프로세스를 완전히 재부팅합니다... (잠시 후 상태를 확인하세요!)"
    )

    # 2초 뒤에 파이썬 프로세스 자체를 시스템 적으로 재가동합니다.
    loop = asyncio.get_running_loop()
    loop.call_later(2, lambda: os.execv(sys.executable, ["python"] + sys.argv))


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
            await execution.exchange.cancel_all_orders(sym)
            algo_orders = await execution.exchange.request(
                path="openAlgoOrders",
                api="fapiPrivate",
                method="GET",
                params={"symbol": sym},
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
                    params={"symbol": sym, "algoId": algo.get("algoId")},
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


def setup_telegram_bot(execution_engine):
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

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CommandHandler("pause", pause_cmd))
    application.add_handler(CommandHandler("resume", resume_cmd))
    application.add_handler(CommandHandler("leverage", leverage_cmd))
    application.add_handler(CommandHandler("mode", mode_cmd))
    application.add_handler(CommandHandler("restart", restart_cmd))
    application.add_handler(CommandHandler("panic", panic_cmd))

    logger.info("텔레그램 Interactive 커맨더(Poller) 세팅이 완료되었습니다.")
    return application
