@echo off
echo Aplicando permisos a toda la carpeta api_vendedores...
icacls "C:\api_vendedores" /inheritance:r /grant "NT AUTHORITY\SYSTEM:(OI)(CI)F" /grant "MICROBELL\emoreno:(OI)(CI)F" /grant "FLEXXUS\emoreno:(OI)(CI)F" /T /C
echo Listo. Permisos aplicados.
pause
