@echo off
REM 수집 윈도우 종료 — producer 중지 (quota 보호)
cd /d C:\Users\user\OneDrive\Desktop\Seoul_Metro
docker compose stop subway-producer
