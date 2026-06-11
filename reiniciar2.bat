@echo off
title API Microbell Vendedores - Servidor .5
echo Deteniendo tarea programada...
schtasks /End /TN "ApiVendedoresMicrobell" >nul 2>&1
timeout /t 2 /nobreak >nul
echo Deteniendo proceso anterior en puerto 8000...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8000" ^| findstr "LISTENING"') do (
    taskkill /PID %%a /F >nul 2>&1
)
timeout /t 2 /nobreak >nul
echo Iniciando servidor...
schtasks /Run /TN "ApiVendedoresMicrobell"
echo Listo. API corriendo en http://193.168.160.5:8000
pause
