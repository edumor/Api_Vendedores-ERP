@echo off
:: Instala la API Vendedores como tarea programada de Windows
:: Ejecutar como Administrador

for /f "delims=" %%i in ('where python') do set PYTHON=%%i
if not defined PYTHON (
    echo ERROR: Python no encontrado en PATH. Instalar Python 3.11+ primero.
    pause
    exit /b 1
)
echo Usando Python: %PYTHON%
set SCRIPT=C:\api_vendedores\main.py
set TASK_NAME=ApiVendedoresMicrobell

echo Eliminando tarea anterior si existe...
schtasks /delete /tn "%TASK_NAME%" /f 2>nul

echo Creando tarea programada para inicio automatico...
schtasks /create /tn "%TASK_NAME%" ^
  /tr "\"%PYTHON%\" \"%SCRIPT%\"" ^
  /sc ONSTART ^
  /ru SYSTEM ^
  /rl HIGHEST ^
  /delay 0000:30 ^
  /f

echo.
echo Tarea creada. La API arrancara automaticamente al iniciar Windows.
echo.
echo Para iniciarla ahora sin reiniciar:
schtasks /run /tn "%TASK_NAME%"
echo.
echo Para verificar que esta corriendo:
echo   Abrir navegador en http://localhost:8000
pause
