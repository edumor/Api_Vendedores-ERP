@echo off
echo ============================================
echo VERIFICACION DE SEGURIDAD - API VENDEDORES
echo ============================================
echo.
echo [1] PERMISOS NTFS - main.py:
icacls "C:\api_vendedores\main.py"
echo.
echo [2] PERMISOS NTFS - frontend.html:
icacls "C:\api_vendedores\frontend.html"
echo.
echo [3] PERMISOS NTFS - .env:
icacls "C:\api_vendedores\.env"
echo.
echo [4] TAREA PROGRAMADA:
schtasks /query /tn "ApiVendedoresMicrobell" /fo LIST
echo.
echo [5] PROCESO EN PUERTO 8000:
netstat -aon | findstr ":8000"
echo.
echo ============================================
echo FIN VERIFICACION - guardando resultado...
echo ============================================
(
echo [1] PERMISOS main.py:
icacls "C:\api_vendedores\main.py"
echo.
echo [2] PERMISOS frontend.html:
icacls "C:\api_vendedores\frontend.html"
echo.
echo [3] PERMISOS .env:
icacls "C:\api_vendedores\.env"
echo.
echo [4] TAREA PROGRAMADA:
schtasks /query /tn "ApiVendedoresMicrobell" /fo LIST
echo.
echo [5] PUERTO 8000:
netstat -aon | findstr ":8000"
) > "C:\api_vendedores\resultado_verificacion.txt"
echo Resultado guardado en C:\api_vendedores\resultado_verificacion.txt
pause
