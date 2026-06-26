@echo off
echo =====================================================
echo   API Vendedores Microbell — Puerto 8000
echo =====================================================
echo.
echo Acceder desde: http://190.111.231.86:8000
echo (en la red local: http://193.168.160.5:8000)
echo.
echo Presionar Ctrl+C para detener.
echo.
uvicorn main:app --host 0.0.0.0 --port 8000
pause
