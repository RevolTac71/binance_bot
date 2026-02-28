#!/bin/bash
# watchdog.sh - 봇 크래시 시 텔레그램 알림 후 자동 재시작

# .env에서 텔레그램 설정 로드
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
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
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 봇 시작 (재시작 횟수: $RESTART_COUNT)"

    # 봇 실행
    python3 main.py >> app.log 2>&1
    EXIT_CODE=$?

    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    if [ $EXIT_CODE -eq 0 ]; then
        # 정상 종료 (예: /stop 명령어)
        MSG="🛑 <b>[봇 정상 종료]</b>&#10;${TIMESTAMP}&#10;Exit Code: 0&#10;자동 재시작을 하지 않습니다."
        send_telegram "$MSG"
        echo "[$TIMESTAMP] 정상 종료 감지. watchdog 종료."
        break
    else
        # 비정상 종료 (크래시)
        RESTART_COUNT=$((RESTART_COUNT + 1))
        MSG="🚨 <b>[봇 크래시 감지]</b>&#10;${TIMESTAMP}&#10;Exit Code: ${EXIT_CODE}&#10;재시작 횟수: ${RESTART_COUNT}&#10;5초 후 자동 재시작합니다..."
        send_telegram "$MSG"
        echo "[$TIMESTAMP] 비정상 종료(Exit: $EXIT_CODE). 5초 후 재시작..."
        sleep 5
    fi
done
