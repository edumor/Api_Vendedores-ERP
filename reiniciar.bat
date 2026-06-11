@echo off
title API Microbell Vendedores
echo Deteniendo proceso anterior en puerto 8000...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8000" ^| findstr "LISTENING"') do (
    taskkill /PID %%a /F >nul 2>&1
)
timeout /t 2 /nobreak >nul
echo Iniciando servidor...
cd /d C:\api_vendedores
start "" /b C:\Python311\Scripts\uvicorn.exe main:app --host 0.0.0.0 --port 8000
timeout /t 4 /nobreak >nul
echo Listo. API corriendo en http://193.168.160.10:8000
echo Reiniciando tunel Cloudflare...
powershell -Command "Restart-Service cloudflared"
echo Tunel Cloudflare reconectado.
pause
