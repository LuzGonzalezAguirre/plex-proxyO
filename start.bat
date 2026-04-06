@echo off
cd /d C:\Users\Luz.Aguirre\plex-proxy
py -3.11 -m uvicorn main:app --host 0.0.0.0 --port 8001 --reload
pause