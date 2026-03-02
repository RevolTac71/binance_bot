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

    # 봇 실행 (파이썬 내부 RotatingFileHandler와 중복 저장 방지를 위해 콘솔 출력은 console.log로 리다이렉션)
    python3 main.py >> console.log 2>&1
    EXIT_CODE=$?

    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    if [ $EXIT_CODE -eq 0 ]; then
        # 정상 종료 (예: /stop 명령어)
        MSG="🛑 <b>[봇 정상 종료]</b>&#10;${TIMESTAMP}&#10;Exit Code: 0&#10;자동 재시작을 하지 않습니다."
        send_telegram "$MSG"
        echo "[$TIMESTAMP] 정상 종료 감지. watchdog 종료."
        break
    elif [ $EXIT_CODE -eq 42 ]; then
        # 의도된 재시작 (텔레그램 /restart 커맨드)
        echo "[$TIMESTAMP] 사용자의 /restart 명령어 감지. 3초 후 재기동..."
        sleep 3
    elif [ $EXIT_CODE -eq 143 ]; then
        # 배포 등 외부 요인에 의한 강제 종료 (SIGTERM)
        echo "[$TIMESTAMP] 외부 종료 신호(SIGTERM, 143) 수신. 배포 또는 강제 중단으로 간주하여 재시작 알림 생략..."
        sleep 5
    else
        # 비정상 종료 (크래시)
        RESTART_COUNT=$((RESTART_COUNT + 1))
        MSG="🚨 <b>[봇 크래시 감지]</b>&#10;${TIMESTAMP}&#10;Exit Code: ${EXIT_CODE}&#10;재시작 횟수: ${RESTART_COUNT}&#10;5초 후 자동 재시작합니다..."
        send_telegram "$MSG"
        echo "[$TIMESTAMP] 비정상 종료(Exit: $EXIT_CODE). 5초 후 재시작..."
        sleep 5
    fi
done
