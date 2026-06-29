@echo off
title API Microbell Vendedores

echo Deteniendo proceso anterior en puerto 8000...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8000" ^| findstr "LISTENING"') do (
    taskkill /PID %%a /F >nul 2>&1
)

echo Iniciando servidor...
cd /d C:\api_vendedores
py main.py
