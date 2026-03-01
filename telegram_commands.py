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
        "예: /setparam k 2.5\n"
        "예: /setparam sl 3.0\n"
        "자세한 파라미터 목록은 /help 참조"
    )
    await update.message.reply_text(msg)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update):
        return
    msg = (
        "📖 [V15.2 자동매매 봇 전체 명령어]\n\n"
        "── 봇 제어 ──\n"
        "/status — 봇 상태·포지션·잔고 요약\n"
        "/pause — 신규 진입 일시정지 (기존 포지션 감시 유지)\n"
        "/resume — 일시정지 해제\n"
        "/panic — 비상! 전량 시장가 청산 후 정지\n"
        "/restart — 봇 프로세스 강제 재부팅\n\n"
        "── 파라미터 변경 (/setparam 키 값) ──\n"
        "/setparam k [숫자] — K-Value (VWAP 밴드 너비, 기본 2.0)\n"
        "/setparam risk [숫자] — 1회 증거금 비율 (예: 0.1 = 10%)\n"
        "/setparam leverage [정수] — 레버리지 배수\n"
        "/setparam timeframe [값] — 캔들봉 (1m/3m/5m/15m, 변경 후 /restart!)\n"
        "/setparam time_exit [분] — 최대 포지션 보유 시간 (0=비활성)\n"
        "/setparam vol_mult [숫자] — 거래량 스파이크 배수 (기본 1.5)\n"
        "/setparam atr_ratio [숫자] — 단/장기 ATR 비율 필터 (기본 1.2)\n"
        "/setparam sl [숫자] — SL 배율 × ATR (기본 3.0, 클수록 넓은 손절)\n"
        "/setparam tp [숫자] — TP 배율 × ATR (기본 6.0, R:R = tp/sl)\n"
        "/setparam cooldown [분] — 손실 후 동일종목 쿨다운 (기본 15분)\n"
        "/setparam mode [dry|real] — 모의/실전 모드 전환\n\n"
        "── 레거시 명령어 (동일 기능) ──\n"
        "/leverage [N] / /k_value [N] / /risk [N]\n"
        "/timeframe [N] / /time_exit [N] / /mode [N]\n"
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
        f"── 현재 파라미터 ──\n"
        f"K-Value   : {settings.K_VALUE}\n"
        f"SL 배율   : {getattr(settings, 'SL_MULT', 3.0)} × ATR\n"
        f"TP 배율   : {getattr(settings, 'TP_MULT', 6.0)} × ATR\n"
        f"레버리지  : {settings.LEVERAGE}x\n"
        f"캔들봉    : {getattr(settings, 'TIMEFRAME', '3m')}\n"
        f"증거금 %  : {settings.RISK_PERCENTAGE * 100:.1f}%\n"
        f"Time Exit : {getattr(settings, 'TIME_EXIT_MINUTES', 0)}분\n"
        f"Vol Mult  : {getattr(settings, 'VOL_MULT', 1.5)}\n"
        f"ATR Ratio : {getattr(settings, 'ATR_RATIO_MULT', 1.2)}\n"
        f"Cooldown  : {getattr(settings, 'LOSS_COOLDOWN_MINUTES', 15)}분\n\n"
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
        "🔄 봇 프로세스를 완전히 재부팅합니다... 여러 개가 켜져 있다면 모두 종료한 뒤 하나만 새로 기동합니다!"
    )

    current_pid = os.getpid()
    killed_count = 0

    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline")
            if (
                cmdline
                and len(cmdline) > 0
                and "python" in proc.info.get("name", "").lower()
            ):
                cmd_str = " ".join(cmdline)
                if "main.py" in cmd_str and proc.info["pid"] != current_pid:
                    proc.kill()
                    killed_count += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    if killed_count > 0:
        logger.info(f"동일한 main.py 프로세스 {killed_count}개를 강제 종료했습니다.")

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
            "예) /setparam k 2.5\n"
            "예) /setparam sl 3.0\n"
            "예) /setparam cooldown 15\n"
            "\n전체 파라미터 목록은 /help 참조"
        )
        return

    key = args[0].lower()
    raw_val = args[1]

    try:
        # 키 매핑 테이블
        mapping = {
            "k": ("K_VALUE", float, "K_VALUE"),
            "k_value": ("K_VALUE", float, "K_VALUE"),
            "risk": ("RISK_PERCENTAGE", float, "RISK_PERCENTAGE"),
            "leverage": ("LEVERAGE", int, "LEVERAGE"),
            "timeframe": ("TIMEFRAME", str, "TIMEFRAME"),
            "time_exit": ("TIME_EXIT_MINUTES", int, "TIME_EXIT_MINUTES"),
            "vol_mult": ("VOL_MULT", float, "VOL_MULT"),
            "atr_ratio": ("ATR_RATIO_MULT", float, "ATR_RATIO_MULT"),
            "sl": ("SL_MULT", float, "SL_MULT"),
            "sl_mult": ("SL_MULT", float, "SL_MULT"),
            "tp": ("TP_MULT", float, "TP_MULT"),
            "tp_mult": ("TP_MULT", float, "TP_MULT"),
            "cooldown": ("LOSS_COOLDOWN_MINUTES", int, "LOSS_COOLDOWN_MINUTES"),
            "mode": ("DRY_RUN", str, "DRY_RUN"),  # dry 또는 real
        }

        if key not in mapping:
            await update.message.reply_text(
                f"❌ 알 수 없는 파라미터: '{key}'\n/help로 파라미터 목록을 확인하세요."
            )
            return

        attr_name, cast_fn, env_key = mapping[key]

        # mode 키는 특별 처리
        if key == "mode":
            is_dry = raw_val.lower() in ("dry", "dry_run", "true")
            settings.DRY_RUN = is_dry
            update_env_variable("DRY_RUN", str(is_dry).capitalize())
            label = "모의투자(DRY_RUN)" if is_dry else "실전매매(REAL)"
            await update.message.reply_text(f"✅ 매매 모드 → {label} 전환 완료")
            return

        # 일반 키 처리 (변경 전 값을 먼저 읽어둠)
        old_val = getattr(settings, attr_name, "(없음)")
        new_val = cast_fn(raw_val)
        setattr(settings, attr_name, new_val)
        update_env_variable(env_key, str(new_val))

        restart_notice = (
            "\n⚠️ timeframe 변경 시 /restart 필요!" if key == "timeframe" else ""
        )
        await update.message.reply_text(
            f"✅ [{key.upper()}] 변경 완료\n"
            f"이전 값: {old_val}\n"
            f"새로운 값: {new_val}\n"
            f"(영구 저장 완료){restart_notice}"
        )

    except ValueError:
        await update.message.reply_text(f"❌ [{key}]에 올바른 형식의 값을 입력하세요.")


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
    application.add_handler(CommandHandler("restart", restart_cmd))
    application.add_handler(CommandHandler("panic", panic_cmd))
    application.add_handler(CommandHandler("setparam", setparam_cmd))

    logger.info("텔레그램 Interactive 커맨더(Poller) 세팅이 완료되었습니다.")
    return application
