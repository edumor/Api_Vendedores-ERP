@echo off
echo =====================================================
echo   Instalando API Vendedores Microbell
echo =====================================================

:: Verificar Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no instalado. Instalar Python 3.11+ primero.
    pause
    exit /b 1
)

echo Instalando dependencias...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo.
echo Instalacion completada.
echo Para iniciar la API ejecutar: iniciar.bat
pause
