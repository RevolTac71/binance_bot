@echo off
chcp 65001 >nul
echo ======= 1. Github에 코드 반영 (Push) =======
echo 필수 파일들만 Git에 추가
git add *.py
git add *.txt
git add *.sh
git add *.md
git add dashboard/
git add .gitignore
git add requirements.txt

echo 변경사항 커밋
git commit -m "Auto-deploy update"

echo Github에 푸시
git push origin main

echo.
echo ======= 2. Ubuntu 서버 배포 및 정리 =======
ssh -i "c:\Y\Study\4.Database\3. Oracle\ssh-key-2026-02-26.key" ubuntu@134.185.110.44 "cd ~/Binance_Bot2 && [ -f console.log ] && [ \"$(find console.log -size +20M)\" ] && mv console.log console.log.$(date +%%Y%%m%%d_%%H%%M%%S) || true && find . -name '*.log.*' -mtime +30 -delete && find . -name '*.log' -mtime +60 -delete && git pull origin main && source venv/bin/activate && pip install -r requirements.txt && pkill -f main.py || true"

echo.
echo ======= 배포 완료! (서버에서 main.py 재가동됨) =======
echo - 코드 업데이트 및 찌꺼기 로그(3~7일 경과분) 정리 완료
pause
pause
