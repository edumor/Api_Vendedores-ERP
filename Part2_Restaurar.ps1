# ================================================================
# SCRIPT PARTE 2: Restauracion de contrasena original de Firebird
# ================================================================
# Ejecutar DESPUES de que Eduardo confirme que termino de conectarse.
# Este script restaura el archivo de seguridad original (security2.fdb.BAK)
# dejando el sistema exactamente como estaba antes del mantenimiento.
# Los usuarios podran loguearse normalmente al dia siguiente.
# ================================================================

$ErrorActionPreference = "Stop"

$FIREBIRD_DIR  = "C:\Program Files\Firebird\Firebird_2_5"
$SECURITY_DB   = "$FIREBIRD_DIR\security2.fdb"
$SECURITY_BAK  = "$FIREBIRD_DIR\security2.fdb.BAK"
$SERVICE       = "FirebirdServerDefaultInstance"
$LOG           = "C:\FLEXXUS\maintenance_log.txt"

function Log($msg) {
    $time = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $linea = "$time | $msg"
    Write-Host $linea
    Add-Content -Path $LOG -Value $linea
}

Log "========================================================"
Log "INICIO SCRIPT PARTE 2 - Restauracion de acceso original"
Log "========================================================"

# Verificar que existe el backup con la contrasena original
if (-not (Test-Path $SECURITY_BAK)) {
    Log "ERROR: No se encontro el backup $SECURITY_BAK"
    Log "El Script Parte 1 no fue ejecutado o el backup fue eliminado."
    Log "Contactar a Eduardo urgentemente."
    exit 1
}

$tamBAK = (Get-Item $SECURITY_BAK).Length
Log "Backup encontrado: $SECURITY_BAK ($tamBAK bytes) - OK"

# Paso 1: Detener Firebird
Log "PASO 1: Deteniendo Firebird para restaurar contrasena original..."
Stop-Service $SERVICE -Force
Start-Sleep -Seconds 4
$estado = (Get-Service $SERVICE).Status
Log "Estado del servicio: $estado"

if ($estado -ne "Stopped") {
    Log "ERROR: El servicio no se detuvo. Intentando forzar..."
    Stop-Service $SERVICE -Force
    Start-Sleep -Seconds 5
}

# Paso 2: Eliminar el security2.fdb temporal (el que tiene masterkey)
Log "PASO 2: Eliminando acceso temporal..."
if (Test-Path $SECURITY_DB) {
    Remove-Item $SECURITY_DB -Force
    Log "security2.fdb temporal eliminado."
} else {
    Log "AVISO: No existia security2.fdb temporal (puede ser normal)."
}

# Paso 3: Restaurar el backup original
Log "PASO 3: Restaurando contrasena original..."
Copy-Item $SECURITY_BAK $SECURITY_DB -Force
$tamRestaurado = (Get-Item $SECURITY_DB).Length
Log "security2.fdb original restaurado ($tamRestaurado bytes)."

# Verificar integridad: el tamano debe coincidir con el backup
if ($tamRestaurado -eq $tamBAK) {
    Log "Verificacion de integridad: OK (tamanos coinciden)"
} else {
    Log "ADVERTENCIA: Diferencia de tamano detectada. Verificar manualmente."
}

# Paso 4: Iniciar Firebird con la contrasena original
Log "PASO 4: Iniciando Firebird con configuracion original..."
Start-Service $SERVICE
Start-Sleep -Seconds 5
$estado = (Get-Service $SERVICE).Status
Log "Estado final del servicio: $estado"

if ($estado -ne "Running") {
    Log "ERROR CRITICO: Firebird no arranco. Intervenir manualmente."
    Write-Host "ERROR: Firebird no arranco. Llamar al administrador." -ForegroundColor Red
    exit 1
}

# Paso 5: Limpiar el backup (ya no se necesita)
Log "PASO 5: Limpiando archivos temporales..."
Remove-Item $SECURITY_BAK -Force -ErrorAction SilentlyContinue
Log "Backup temporal eliminado."

Log "========================================================"
Log "RESTAURACION COMPLETADA EXITOSAMENTE"
Log "========================================================"
Log ""
Log "Estado final:"
Log "  - Servicio Firebird: $estado"
Log "  - Contrasena: RESTAURADA a la original (previa al mantenimiento)"
Log "  - Los usuarios pueden conectarse normalmente"
Log ""

Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
Write-Host " SISTEMA RESTAURADO CORRECTAMENTE" -ForegroundColor Green
Write-Host " La contrasena original de SYSDBA esta activa." -ForegroundColor Green
Write-Host " Los usuarios pueden loguearse normalmente manana." -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green
