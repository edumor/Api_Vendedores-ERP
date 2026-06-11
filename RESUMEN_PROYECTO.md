# API Vendedores Microbell — Estado del Proyecto
**Última actualización: 24/04/2026 - 17:15 hs**

---

## Arquitectura

- **Backend:** FastAPI + Uvicorn, puerto 8000
- **BD:** Firebird 2.5 — `127.0.0.1:3050` — `c:/flexxus/db/DB-Microbell.gdb`
  - Usuario: `SYSDBA` / Pass: `3122414422`
- **Python:** `C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe`
- **Archivos en servidor:** `C:\api_vendedores\`
- **IP interna:** `193.168.160.10:8000`
- **IP externa:** `190.111.231.86:8000` (requiere configurar port forwarding en el router)

---

## Archivos del proyecto

| Archivo | Descripción |
|---|---|
| `main.py` | Backend FastAPI (16.7 KB — versión actualizada hoy) |
| `frontend.html` | SPA responsive (30.7 KB — versión actualizada hoy) |
| `crear_servicio.ps1` | Crea tarea programada de Windows (inicio automático) |
| `instalar_servicio.bat` | Alternativa al .ps1 (usa schtasks) |
| `iniciar.bat` | Inicio manual (requiere ejecutar con `.\iniciar.bat` en PowerShell) |
| `requirements.txt` | `fastapi==0.111.0`, `uvicorn==0.29.0`, `firebirdsql==1.4.5`, `pydantic==2.7.1` |

---

## Conceptos clave de la BD Flexxus/Firebird

### Clientes — Dos códigos distintos
- **CODIGOCLIENTE** (ej: `03870`): clave interna usada en las queries
- **CODIGOPARTICULAR** (ej: `100863`): el que Flexxus muestra como "Código" en su UI
- ⚠️ Algunos comprobantes en CABEZACOMPROBANTES tienen `CODIGOCLIENTE = CODIGOPARTICULAR` (ej: FA en SW/Línea 2), por eso el fix busca en ambos campos.

### Tipos de comprobante en Cuenta Corriente
| Tipo | Descripción |
|---|---|
| FA | Factura normal |
| FCA | Factura de crédito |
| FE | Factura electrónica |
| FCE | Factura de crédito electrónica |
| SIV | Sin IVA = "en negro" / SW / movimientos informales |
| RE | Remito — **NO debe aparecer en CTA corriente** |

- **NROPUNTODEVENTA = 1** = "Punto 0001" = SW / Línea 2 (facturas informales formales)
- **TOTAL** en CABEZACOMPROBANTES es el neto **sin IVA** → el total real es `TOTAL + IVA1 + IVA2`

### Depósitos para stock
- Solo sumar depósitos **001 (VAC-LOG)** y **003 (PACHECO)**
- Solo mostrar artículos con stock > 0

### Tres bases de datos Firebird
| Archivo | Uso |
|---|---|
| `DB-Microbell.gdb` | BD principal — facturas electrónicas oficiales |
| `DB-MLT-Microbell.gdb` | BD SW / en negro / Línea 2 — CABEZACOMPROBANTES con IVA1=0 |
| `DB-EST-Microbell.gdb` | BD estadísticas — solo tablas temporales, sin comprobantes |

La cuenta corriente consulta **DB-Microbell + DB-MLT** y combina los resultados.

### Charsets por tabla
- **LATIN1:** CABEZACOMPROBANTES y otras tablas de comprobantes
- **WIN1252:** CLIENTES, USUARIOS y tablas maestras

---

## Endpoints disponibles

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/` | Frontend HTML |
| POST | `/auth/login` | Login con usuario/password de Flexxus |
| GET | `/stock` | Artículos con stock positivo (depósitos 001+003) |
| GET | `/stock/{codigo}` | Detalle de un artículo |
| GET | `/clientes?vendedor=X` | Clientes del vendedor (incluye CODIGOPARTICULAR) |
| GET | `/clientes/{codigo}` | Detalle de un cliente |
| GET | `/clientes/{codigo}/cuenta_corriente` | Comprobantes pendientes (FA/FCA/FE/FCE/SIV) |
| GET | `/pedidos?vendedor=X` | Pedidos del vendedor |
| POST | `/pedidos` | Crear nuevo pedido |
| GET | `/presupuestos?vendedor=X` | Presupuestos del vendedor |
| POST | `/presupuestos` | Crear nuevo presupuesto |

---

## Fix más reciente (pendiente de aplicar en servidor)

**Problema:** FA 0001-00001606 de MANO A MANO SAS (SW/Línea 2, $1.285.047,40) no aparecía en CTA corriente.

**Causa:** El comprobante tiene `CODIGOCLIENTE = '100863'` (CODIGOPARTICULAR) en lugar de `'03870'` (CODIGOCLIENTE interno).

**Fix en `main.py`:**
- Primero busca el CODIGOPARTICULAR del cliente en CLIENTES (usando charset WIN1252)
- Luego filtra CABEZACOMPROBANTES con `CODIGOCLIENTE IN ('03870', '100863')`

**Fix en `frontend.html`:**
- Muestra CODIGOPARTICULAR como "Código" visible al usuario
- Busca clientes también por CODIGOPARTICULAR
- La búsqueda acepta tanto el código interno como el particular

---

## Cómo reiniciar la API en el servidor

```powershell
# Matar proceso existente
taskkill /IM python.exe /F

# Iniciar en background (sin ventana)
Start-Process "C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe" `
  -ArgumentList "C:\api_vendedores\main.py" `
  -WorkingDirectory "C:\api_vendedores" `
  -WindowStyle Hidden

# Verificar que está corriendo
netstat -ano | findstr :8000
```

## Tarea programada (inicio automático con Windows)

Ya fue creada: **ApiVendedoresMicrobell**

Para ejecutarla manualmente sin reiniciar:
```powershell
schtasks /run /tn "ApiVendedoresMicrobell"
```

---

## Lo que funciona hoy (24/04/2026)

- ✅ Login con usuario/password de Flexxus
- ✅ Cada vendedor ve solo sus clientes
- ✅ Búsqueda de clientes por nombre O por código (CODIGOPARTICULAR)
- ✅ Cuenta corriente toma datos de DB-Microbell + DB-MLT (SW/en negro)
- ✅ Tipos de comprobante: FA, FB, FE, FCA, FCB, DI, SIV
- ✅ Totales con IVA correctos (TOTAL + IVA1 + IVA2)
- ✅ Buscador por código a la izquierda en Cta. Corriente
- ✅ Stock con artículos activos y STOCKACTUAL > 0

## Próximo paso: Stock con jerarquía y Remanente

### Lo que se sabe de la BD:
- **STOCK** no tiene CODIGODEPOSITO — tiene CODIGOSUCURSAL = "PRINCIPAL" (total sin separar por depósito)
- **CIERRESSTOCK** tiene snapshot histórico de 2022 con zeros, no sirve para stock actual
- **Remanente** = `STOCKACTUAL - STOCKRMACLIENTES` (total entre todos los depósitos)
- Los depósitos 001 (VAC-LOG) y 003 (PACHECO) mapean a CODIGOSUCURSAL="CASA_CENTRAL" pero Flexxus los separa via movimientos complejos

### Lo que falta implementar en Stock:
1. Verificar que ARTICULOS tiene campos: `CODIGORUBRO`, `CODIGOSUPERRUBRO`, `CODIGOGRUPOSUPERRUBRO`
2. Agregar endpoints: `/gruposuperrubros`, `/superrubros?grupo=X`, `/rubros?superrubro=X`
3. Actualizar endpoint `/stock` para filtrar por rubro/superrubro/gruposuperrubro
4. Mostrar columna **Remanente** = STOCKACTUAL - STOCKRMACLIENTES
5. En el frontend: 3 dropdowns en cascada (Grupo SR → Super Rubro → Rubro)

### Tarea pendiente de debug:
Ejecutar `http://localhost:8000/debug/stock` para confirmar que ARTICULOS tiene los campos de jerarquía. El endpoint ya está en el código listo.

## Pendiente general

- [ ] **Configurar port forwarding** en el router: puerto 8000 → 193.168.160.10 (para acceso externo)
- [ ] **Probar acceso externo** desde `http://190.111.231.86:8000`
- [ ] Agregar Python al PATH del sistema para que `iniciar.bat` funcione sin ruta completa
- [ ] Implementar filtros de stock por jerarquía (ver arriba)

---

## Acceso para vendedores externos (una vez que el router esté configurado)

URL: `http://190.111.231.86:8000`  
Credenciales: las mismas que usan para entrar a Flexxus
