@echo off
setlocal enabledelayedexpansion

:: watchdog.bat - Windows version of the bot watcher
:: Usage: Run this script to start the bot with auto-restart capability.

set RESTART_COUNT=0
set LOG_FILE=watchdog_win.log

echo [%DATE% %TIME%] Watchdog started. > %LOG_FILE%

:loop
echo [%DATE% %TIME%] Starting Bot (Count: !RESTART_COUNT!)... 
echo [%DATE% %TIME%] Starting Bot (Count: !RESTART_COUNT!) >> %LOG_FILE%

:: Start the bot and wait for it to exit
python main.py
set EXIT_CODE=%ERRORLEVEL%

echo [%DATE% %TIME%] Bot Exited (Code: !EXIT_CODE!)
echo [%DATE% %TIME%] Bot Exited (Code: !EXIT_CODE!) >> %LOG_FILE%

if !EXIT_CODE! equ 0 (
    echo [%DATE% %TIME%] Normal exit. Stopping watchdog. >> %LOG_FILE%
    pause
    exit /b 0
)

if !EXIT_CODE! equ 42 (
    echo [%DATE% %TIME%] Restart requested. Waiting 3s... >> %LOG_FILE%
    timeout /t 3 /nobreak > nul
    goto loop
)

:: For any other exit code (crash)
set /a RESTART_COUNT+=1
echo [%DATE% %TIME%] Crash detected. Restarting in 5s (Total: !RESTART_COUNT!)... >> %LOG_FILE%
timeout /t 5 /nobreak > nul
goto loop
