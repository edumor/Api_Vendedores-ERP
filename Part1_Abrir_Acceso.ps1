# ================================================================
# SCRIPT PARTE 1: Apertura de acceso temporal a Firebird
# ================================================================
# IMPORTANTE: Ejecutar SOLO cuando no haya usuarios en Flexxus ERP
# Este script NO borra ni modifica la base de datos de produccion.
# Solo reemplaza temporalmente el archivo de seguridad (usuarios/passwords).
# La contrasena original se guarda en security2.fdb.BAK y se restaura
# con el Script Parte 2.
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
Log "INICIO SCRIPT PARTE 1 - Apertura de acceso temporal"
Log "========================================================"

# Verificar que no existe un backup previo sin restaurar
if (Test-Path $SECURITY_BAK) {
    Log "ERROR: Ya existe un backup previo en $SECURITY_BAK"
    Log "Esto indica que el Script Parte 2 no fue ejecutado la vez anterior."
    Log "Ejecute primero el Script Parte 2 para restaurar, luego reintente."
    Write-Host ""
    Write-Host "ACCION REQUERIDA: Ejecute Part2_Restaurar.ps1 primero." -ForegroundColor Red
    exit 1
}

# Paso 1: Detener Firebird
Log "PASO 1: Deteniendo servicio Firebird..."
Stop-Service $SERVICE -Force
Start-Sleep -Seconds 4
$estado = (Get-Service $SERVICE).Status
Log "Estado del servicio: $estado"

if ($estado -ne "Stopped") {
    Log "ERROR: El servicio no se detuvo correctamente. Abortando sin cambios."
    exit 1
}

# Paso 2: Backup del archivo de seguridad original (contiene la contrasena real)
Log "PASO 2: Guardando backup de la contrasena original..."
Copy-Item $SECURITY_DB $SECURITY_BAK -Force
$tamBAK = (Get-Item $SECURITY_BAK).Length
Log "Backup guardado: $SECURITY_BAK ($tamBAK bytes)"

# Paso 3: Eliminar el security2.fdb para que Firebird cree uno nuevo con masterkey
Log "PASO 3: Preparando acceso temporal..."
Remove-Item $SECURITY_DB -Force
Log "Archivo de seguridad original removido temporalmente."

# Paso 4: Iniciar Firebird - crea automaticamente un nuevo security2.fdb con SYSDBA/masterkey
Log "PASO 4: Iniciando Firebird con acceso temporal (SYSDBA/masterkey)..."
Start-Service $SERVICE
Start-Sleep -Seconds 5
$estado = (Get-Service $SERVICE).Status
Log "Estado del servicio: $estado"

if ($estado -ne "Running") {
    Log "ERROR: Firebird no arranco. Restaurando backup automaticamente..."
    Stop-Service $SERVICE -Force -ErrorAction SilentlyContinue
    if (Test-Path $SECURITY_BAK) {
        Copy-Item $SECURITY_BAK $SECURITY_DB -Force
        Log "Backup restaurado automaticamente. Sin cambios para los usuarios."
    }
    Start-Service $SERVICE
    exit 1
}

# Verificar que se creo el nuevo security2.fdb
if (Test-Path $SECURITY_DB) {
    $tamNuevo = (Get-Item $SECURITY_DB).Length
    Log "Nuevo security2.fdb creado correctamente ($tamNuevo bytes)"
} else {
    Log "ERROR: No se creo el nuevo security2.fdb. Restaurando..."
    Stop-Service $SERVICE -Force -ErrorAction SilentlyContinue
    Copy-Item $SECURITY_BAK $SECURITY_DB -Force
    Start-Service $SERVICE
    exit 1
}

Log "========================================================"
Log "PARTE 1 COMPLETADA EXITOSAMENTE"
Log "========================================================"
Log ""
Log ">>> AHORA: Notificar a Eduardo para que se conecte remotamente."
Log ">>> Credenciales temporales: SYSDBA / masterkey"
Log ">>> Puerto Firebird: 190.111.231.86:3050"
Log ">>> Base de datos: c:\flexxus\db\DB-Microbell.gdb"
Log ""
Log ">>> Cuando Eduardo confirme que termino, ejecutar: Part2_Restaurar.ps1"
Log ""

Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
Write-Host " ACCESO TEMPORAL ACTIVO" -ForegroundColor Green
Write-Host " Credenciales: SYSDBA / masterkey" -ForegroundColor Green
Write-Host " Avisar a Eduardo para que se conecte ahora." -ForegroundColor Green
Write-Host " Luego ejecutar Part2_Restaurar.ps1 para cerrar el acceso." -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green
