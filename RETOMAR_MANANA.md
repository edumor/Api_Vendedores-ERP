# Retomar — 29/04/2026

## Estado actual
- API corriendo en puerto 8000, apuntando a DB-Prueba.gdb ✅
- Presupuestos y pedidos se guardan correctamente en DB-Prueba.gdb ✅
- Fixes de CABEZAPRESUPUESTOS / CUERPOPRESUPUESTOS aplicados ✅
- Reparto Propio implementado en frontend ✅
- SW numeración duplicada corregida ✅

## Problema pendiente: Flexxus con perfil Prueba
Necesitamos abrir Flexxus apuntando a DB-Prueba.gdb (no DB-Microbell.gdb).

### Lo que hicimos en FlexxusServer.ini (C:\Flexxus\FlexxusERP\BIN\FlexxusServer.ini)
En la sección `[Prueba]` cambiamos:
- `Ejecutable=03.41.048.0135.02-251020 - copia.exe` → `03.41.048.0135.02-251020.exe`
- `Servidor=Flexxus` → `Servidor=FLEXXUS`

### Último estado
- `FlexxusLauncher.exe "Prueba"` corre (PID 7292 activo) pero no aparece ventana visible
- Posiblemente hay una ventana abierta detrás — buscar con Alt+Tab al iniciar sesión mañana
- Si no aparece ventana: verificar si FlexxusLauncher abrió algo en la barra de tareas

### Próximo paso al retomar
1. Revisar si FlexxusLauncher dejó algo abierto (Alt+Tab, barra de tareas)
2. Si no: desde PowerShell en `C:\Flexxus\FlexxusERP\BIN\`:
   ```powershell
   & ".\FlexxusLauncher.exe" "Prueba"
   ```
3. Si sigue sin abrir: revisar el log de Flexxus en `C:\Flexxus\FlexxusERP\BIN\` o `C:\Flexxus\Logs\`
4. Una vez Flexxus Prueba abierto: verificar que el presupuesto PR 7843 aparece y se puede confirmar

## Pendientes secundarios
- `discrimina_iva` hardcodeado en True — buscar columna correcta en CLIENTES
- Test end-to-end pedido desde frontend → confirmar en Flexxus Prueba
