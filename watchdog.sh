#!/bin/bash
# watchdog.sh - simplified version

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 가상환경 활성화
if [ -d "venv" ]; then
    source venv/bin/activate
fi

RESTART_COUNT=0

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP] Starting Bot (Count: $RESTART_COUNT)" >> watchdog.log

    # 봇 실행
    python3 main.py >> console.log 2>&1
    EXIT_CODE=$?

    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP] Bot Exited (Code: $EXIT_CODE)" >> watchdog.log

    if [ $EXIT_CODE -eq 0 ] || [ $EXIT_CODE -eq 143 ]; then
        echo "[$TIMESTAMP] Normal exit or TERM. Stopping watchdog." >> watchdog.log
        break
    elif [ $EXIT_CODE -eq 42 ]; then
        echo "[$TIMESTAMP] Restart requested. Waiting 3s..." >> watchdog.log
        sleep 3
    else
        RESTART_COUNT=$((RESTART_COUNT + 1))
        echo "[$TIMESTAMP] Crash detected. Restarting in 5s (Total: $RESTART_COUNT)..." >> watchdog.log
        sleep 5
    fi
done
