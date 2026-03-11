@echo off
echo ======= 1. Git Push (Local to Github) =======
git add *.py *.txt *.sh *.md dashboard/ .gitignore requirements.txt
git commit -m "Auto-deploy update"
git push origin main

echo.
echo ======= 2. Remote Deploy (Github to Server) =======
ssh -i "c:\Y\Study\4.Database\3. Oracle\ssh-key-2026-02-26.key" ubuntu@134.185.110.44 "cd ~/Binance_Bot2 && git pull origin main && find . -maxdepth 1 -name '*.log.*' -mtime +30 -delete && find . -maxdepth 1 -name '*.log' -mtime +60 -delete && source venv/bin/activate && pip install -r requirements.txt && chmod +x watchdog.sh && pkill -f watchdog.sh || true && pkill -f main.py || true && echo '[DEPLOY] %DATE% %TIME% - Starting Bot...' >> deploy.log && nohup ./watchdog.sh >> watchdog.log 2>&1 & sleep 2 && ps aux | grep -E 'watchdog.sh|main.py' | grep -v grep"

echo.
echo ======= Deployment Completed =======
pause
