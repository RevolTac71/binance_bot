#!/bin/bash
# watchdog.sh - 봇 크래시 시 텔레그램 알림 후 자동 재시작

# 경로 및 환경 설정
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PATH=$PATH:/usr/local/bin:/usr/bin:/bin
LOG_FILE="$SCRIPT_DIR/watchdog.log"
ERR_FILE="$SCRIPT_DIR/watchdog_error.log"

exec 2>> "$ERR_FILE"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] Watchdog script started in $SCRIPT_DIR" >> "$LOG_FILE"

source "$SCRIPT_DIR/.env"

send_telegram() {
    local msg="$1"
    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d "chat_id=${TELEGRAM_CHAT_ID}" \
        -d "text=${msg}" \
        -d "parse_mode=HTML" > /dev/null
}

cd "$SCRIPT_DIR"
source venv/bin/activate

RESTART_COUNT=0

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP] [START] Starting Bot (Restart count: $RESTART_COUNT)" >> "$LOG_FILE"

    # 봇 실행 (콘솔 출력은 console.log로, 에러 포함)
    python3 main.py >> console.log 2>&1
    EXIT_CODE=$?

    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP] [EXIT] Bot process finished (Exit Code: $EXIT_CODE)" >> "$LOG_FILE"

    if [ $EXIT_CODE -eq 0 ]; then
        # 정상 종료 (예: /stop 명령어)
        MSG="<b>[BOT EXIT]</b>&#10;${TIMESTAMP}&#10;Exit Code: 0&#10;Stopping watchdog."
        send_telegram "$MSG"
        echo "[$TIMESTAMP] [INFO] Normal exit detected. Stopping watchdog." >> "$LOG_FILE"
        break
    elif [ $EXIT_CODE -eq 42 ]; then
        # 의도된 재시작 (텔레그램 /restart 커맨드)
        echo "[$TIMESTAMP] [RESTART] /restart command detected. Restarting in 3 sec..." >> "$LOG_FILE"
        sleep 3
    elif [ $EXIT_CODE -eq 143 ]; then
        # 외부 요인에 의한 강제 종료 (SIGTERM)
        echo "[$TIMESTAMP] [TERM] External SIGTERM/143 received. Stopping watchdog." >> "$LOG_FILE"
        break
    else
        # 비정상 종료 (크래시)
        RESTART_COUNT=$((RESTART_COUNT + 1))
        MSG="🚨 <b>[BOT CRASH]</b>&#10;${TIMESTAMP}&#10;Exit Code: ${EXIT_CODE}&#10;Restart Count: ${RESTART_COUNT}&#10;Restarting in 5 sec..."
        send_telegram "$MSG"
        echo "[$TIMESTAMP] [CRASH] Abnormal exit. Restarting in 5 sec (Count: $RESTART_COUNT)..." >> "$LOG_FILE"
        sleep 5
    fi
done
