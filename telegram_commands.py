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
        "/params — 현재 설정된 봇의 모든 파라미터(설정값) 조회\n"
        "/panic — 비상! 전량 시장가 청산 후 정지\n"
        "/restart — 봇 프로세스 강제 재부팅\n\n"
        "── 파라미터 변경: /setparam [키] [값] ──\n"
        "k          — K-Value VWAP 밴드 너비 (float, 기본 2.0)\n"
        "risk       — 1회 증거금 비율 (float, 예: 0.1 = 10%)\n"
        "leverage   — 레버리지 배수 (int)\n"
        "timeframe  — 캔들봉 (1m/3m/5m/15m, 변경 후 /restart!)\n"
        "time_exit  — 최대 보유 시간 분 (int, 0=비활성)\n"
        "max_trades — (NEW) 동시 진입 한도 (int, 기본 3)\n"
        "mtf_filter — (NEW) 상위 프레임 필터 (on/off, 기본 on)\n"
        "rsi_period — RSI 계산 기간 (int, 기본 14)\n"
        "rsi_ob     — 과매수/숏 기준치 (int, 기본 70)\n"
        "rsi_os     — 과매도/롱 기준치 (int, 기본 30)\n"
        "vol_mult   — 거래량 스파이크 배수 (float, 기본 1.5)\n"
        "atr_ratio  — 단/장기 ATR 비율 필터 (float, 기본 1.2)\n"
        "sl         — SL 배율 ×ATR (float, 기본 3.0 = 넓은 손절)\n"
        "tp         — TP 배율 ×ATR (float, 기본 6.0, R:R=tp/sl)\n"
        "cooldown   — 손실 후 동일종목 쿨다운 분 (int, 기본 15)\n"
        "htf_1h     — 1시간봉 상위 프레임 (str, 예: 1h, 2h, 4h, 변경 후 /restart 권장)\n"
        "htf_15m    — 15분봉 상위 프레임 (str, 예: 15m, 30m, 변경 후 /restart 권장)\n"
        "mode       — dry 또는 real\n\n"
        "── (NEW) 개별 종목 제어 ──\n"
        "/ignore [코인] — 블랙리스트 추가 (진입 차단, 예: /ignore LINK/USDT)\n"
        "/allow [코인]  — 블랙리스트 제거 (진입 허용, 예: /allow LINK/USDT)\n"
        "/close [코인]  — 특정 코인 시장가 즉시 청산 (예: /close BTC/USDT)\n"
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
        f"HTF 1H: {getattr(settings, 'HTF_TIMEFRAME_1H', '1h')}\n"
        f"HTF 15m: {getattr(settings, 'HTF_TIMEFRAME_15M', '15m')}\n"
        f"RSI Period: {getattr(settings, 'RSI_PERIOD', 14)}\n"
        f"RSI OB: {getattr(settings, 'RSI_OB', 70)}\n"
        f"RSI OS: {getattr(settings, 'RSI_OS', 30)}\n"
        f"Vol Mult  : {getattr(settings, 'VOL_MULT', 1.5)}\n"
        f"ATR Ratio : {getattr(settings, 'ATR_RATIO_MULT', 1.2)}\n"
        f"Cooldown  : {getattr(settings, 'LOSS_COOLDOWN_MINUTES', 15)}분\n"
        f"Max Trades: {getattr(settings, 'MAX_TRADES', 3)}개\n"
        f"MTF Filter: {'ON' if getattr(settings, 'MTF_FILTER', True) else 'OFF'}\n\n"
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
            "adx": ("ADX_THRESHOLD", float, "ADX_THRESHOLD"),
            "chandelier": ("CHANDELIER_MULT", float, "CHANDELIER_MULT"),
            "chandel": ("CHANDELIER_MULT", float, "CHANDELIER_MULT"),
            "sl": ("SL_MULT", float, "SL_MULT"),
            "sl_mult": ("SL_MULT", float, "SL_MULT"),
            "tp": ("TP_MULT", float, "TP_MULT"),
            "tp_mult": ("TP_MULT", float, "TP_MULT"),
            "cooldown": ("LOSS_COOLDOWN_MINUTES", int, "LOSS_COOLDOWN_MINUTES"),
            "max": ("MAX_TRADES", int, "MAX_TRADES"),
            "max_trades": ("MAX_TRADES", int, "MAX_TRADES"),
            "htf_1h": ("HTF_TIMEFRAME_1H", str, "HTF_TIMEFRAME_1H"),
            "htf_15m": ("HTF_TIMEFRAME_15M", str, "HTF_TIMEFRAME_15M"),
            "rsi_period": ("RSI_PERIOD", int, "RSI_PERIOD"),
            "rsi_ob": ("RSI_OB", int, "RSI_OB"),
            "rsi_os": ("RSI_OS", int, "RSI_OS"),
            "mtf_filter": (None, None, None),  # Custom logic
            "mode": (None, None, None),  # dry 또는 real
        }

        if key not in mapping:
            await update.message.reply_text(
                f"❌ 알 수 없는 파라미터: '{key}'\n/help로 파라미터 목록을 확인하세요."
            )
            return

        attr_name, cast_fn, env_key = mapping[key]

        # mode 및 mtf_filter 키는 특별 처리
        if key == "mode":
            is_dry = raw_val.lower() in ("dry", "dry_run", "true", "1")
            settings.DRY_RUN = is_dry
            update_env_variable("DRY_RUN", str(is_dry).capitalize())
            label = "모의투자(DRY_RUN)" if is_dry else "실전매매(REAL)"
            await update.message.reply_text(f"✅ 매매 모드 → {label} 설정 완료")
            return

        elif key in ("mtf_filter", "mtf"):
            is_on = raw_val.lower() in ("on", "true", "1", "yes")
            settings.MTF_FILTER = is_on
            update_env_variable("MTF_FILTER", str(is_on).capitalize())
            label = "ON (필터 켜짐)" if is_on else "OFF (필터 꺼짐 - 횡보장 모드)"
            await update.message.reply_text(f"✅ MTF 필터 → {label} 설정 완료")
            return

        # 일반 키 처리 (변경 전 값을 먼저 읽어둠)
        old_val = getattr(settings, attr_name, "(없음)")
        new_val = cast_fn(raw_val)
        setattr(settings, attr_name, new_val)
        update_env_variable(env_key, str(new_val))

        restart_notice = ""
        if key in ("timeframe", "htf_1h", "htf_15m"):
            restart_notice = f"\n⚠️ {key} 변경 시 /restart 필요!"

        await update.message.reply_text(
            f"✅ [{key.upper()}] 변경 완료\n"
            f"이전 값: {old_val}\n"
            f"새로운 값: {new_val}\n"
            f"(영구 저장 완료){restart_notice}"
        )

    except ValueError:
        await update.message.reply_text(f"❌ [{key}]에 올바른 형식의 값을 입력하세요.")


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
    mtf = "ON" if getattr(settings, "MTF_FILTER", True) else "OFF"

    msg = (
        "⚙️ [현재 설정 파라미터]\n"
        f"모드      : {mode}\n"
        f"K-Value   : {getattr(settings, 'K_VALUE', 2.0)}\n"
        f"레버리지  : {settings.LEVERAGE}x\n"
        f"리스크    : {settings.RISK_PERCENTAGE * 100:.1f}%\n"
        f"캔들타임  : {getattr(settings, 'TIMEFRAME', '3m')}\n"
        f"MaxTrades : {getattr(settings, 'MAX_TRADES', 3)}\n"
        f"Time Exit : {getattr(settings, 'TIME_EXIT_MINUTES', 0)}분\n"
        f"MTF Filter: {mtf}\n"
        f"HTF 1H    : {getattr(settings, 'HTF_TIMEFRAME_1H', '1h')}\n"
        f"HTF 15m   : {getattr(settings, 'HTF_TIMEFRAME_15M', '15m')}\n"
        f"RSI 주기  : {getattr(settings, 'RSI_PERIOD', 14)}\n"
        f"RSI 반전  : (롱: {getattr(settings, 'RSI_OS', 30)}, 숏: {getattr(settings, 'RSI_OB', 70)})\n"
        f"거래량배수: {getattr(settings, 'VOL_MULT', 1.5)}x\n"
        f"ATR Ratio : {getattr(settings, 'ATR_RATIO_MULT', 1.2)}\n"
        f"SL 배수   : {getattr(settings, 'SL_MULT', 3.0)}\n"
        f"TP 배수   : {getattr(settings, 'TP_MULT', 6.0)}\n"
        f"재진입대기: {getattr(settings, 'LOSS_COOLDOWN_MINUTES', 15)}분\n\n"
        "💡 변경은 /setparam [옵션] [값] 이용"
    )
    await update.message.reply_text(msg)


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

    logger.info("텔레그램 Interactive 커맨더(Poller) 세팅이 완료되었습니다.")
    return application
