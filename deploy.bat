@echo off
chcp 65001 >nul
echo ======= 1. Github에 코드 반영 (Push) =======
git add .
git commit -m "Auto-deploy update"
git push origin main

echo.
echo ======= 2. Ubuntu 서버 배포 및 재시작 =======
ssh -i "c:\Y\Study\4.Database\3. Oracle\ssh-key-2026-02-26.key" ubuntu@134.185.110.44 "cd ~/Binance_Bot2 && git pull origin main && pkill -f main.py || true && sleep 2 && source venv/bin/activate && nohup python3 main.py > app.log 2>&1 &"

echo.
echo ======= 배포 완료! (서버에서 main.py 재가동됨) =======
pause
