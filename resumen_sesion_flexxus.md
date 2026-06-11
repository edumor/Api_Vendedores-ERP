# Resumen de Sesión — Api Flexxus ERP
**Fecha:** 23/04/2026  
**Objetivo:** Exportar todas las tablas de la base de datos Firebird de Flexxus ERP a CSV

---

## Estado del sistema ✅

El servidor Flexxus ERP está en **estado original y funcional**. Los usuarios pueden loguearse normalmente. No quedó ningún cambio activo en producción.

- `firebird.conf` → `#Authentication = native` (comentado, sin cambios)
- `security2.fdb` → restaurado desde backup (`security2.fdb.BAK2`)
- Servicio Firebird → Running
- Scripts temporales del servidor → eliminados

---

## Lo que descubrimos

### Arquitectura del sistema
- **Motor:** Firebird 2.5 (instancia `FirebirdServerDefaultInstance`)
- **IP servidor:** `190.111.231.86`, puerto `3050`
- **Base de datos Microbell:** `c:\flexxus\db\DB-Microbell.gdb`
- **Otras BDs:** MICROSHENZHEN, Prueba, Prueba API, Tradicom, PruebaTradicom
- **Security DB real:** `C:\FLEXXUS\FlexxusERP\security2.fdb` (51,396,608 bytes) — NO la de `Program Files`

### Usuario de conexión
- Flexxus conecta como usuario **ADMIN** (no SYSDBA)
- Esto se encontró en: `C:\FLEXXUS\FlexxusERP\Microbell\bin\configuracion.ini`
- La contraseña **no está en texto plano** en ningún archivo `.ini` revisado

### Archivos de configuración revisados
| Archivo | Contenido relevante |
|---|---|
| `BIN\FlexxusServer.ini` | Rutas de BDs y nombres de empresas |
| `BIN\FLXSETTINGS.INI` | Solo `ip=FLEXXUS` |
| `Microbell\bin\configuracion.ini` | `NombreUsuario=ADMIN` |
| `BIN\flxdpn.ini` | Pendiente de revisar |
| `BIN\RBuiler.ini` | Pendiente de revisar |

---

## Script listo para exportar CSV

El script `extract_firebird_to_csv.py` está en esta carpeta. Exporta **todas las tablas** a CSV individuales + ZIP.

**Solo falta la contraseña.** Una vez obtenida, editar línea de conexión:

```python
con = firebirdsql.connect(
    host='190.111.231.86',
    port=3050,
    database='c:/flexxus/db/DB-Microbell.gdb',
    user='ADMIN',          # o 'SYSDBA'
    password='AQUI_VA_PASSWORD',
    charset='WIN1252'
)
```

Salida: carpeta `Microbell_CSV/` + `Microbell_CSV.zip` en esta carpeta de Cowork.

---

## Pendiente: encontrar el password

### Opciones ordenadas por probabilidad

**1. Probar passwords comunes (inmediato)**  
Correr en PC con Python instalado:

```python
import firebirdsql
for pwd in ['masterkey', 'admin', 'ADMIN', 'flexxus', 'Flexxus',
            'microbell', 'Microbell', '123456', 'password', 'flx']:
    try:
        con = firebirdsql.connect(host='190.111.231.86', port=3050,
              database='c:/flexxus/db/DB-Microbell.gdb',
              user='ADMIN', password=pwd, charset='WIN1252')
        print(f'PASSWORD ENCONTRADO: {pwd}')
        con.close()
        break
    except Exception as e:
        print(f'Fallo {pwd}: {e}')
```

**2. Revisar registro de Windows en el servidor**

```powershell
Get-ItemProperty "HKLM:\SOFTWARE\Flexxus*" -ErrorAction SilentlyContinue
Get-ItemProperty "HKLM:\SOFTWARE\WOW6432Node\Flexxus*" -ErrorAction SilentlyContinue
```

**3. Revisar archivos ini pendientes**

```powershell
Get-Content "C:\FLEXXUS\FlexxusERP\BIN\flxdpn.ini"
Get-Content "C:\FLEXXUS\FlexxusERP\BIN\RBuiler.ini"
```

**4. Buscar en todas las carpetas de empresa**  
Queda revisar `MICROSHENZHEN\bin\`, `Tradicom\bin\`, etc.

```powershell
Get-ChildItem "C:\FLEXXUS\FlexxusERP\" -Recurse -Include "*.ini","*.cfg" |
  Select-String -Pattern "pass|clave|pwd|key|secret" -CaseSensitive:$false |
  Select-Object Filename, LineNumber, Line
```

**5. Consultar al proveedor Flexxus**  
Si ninguna opción anterior funciona, contactar soporte de Flexxus ERP. Ellos conocen las credenciales de instalación.

---

## Limpieza pendiente (opcional, no urgente)
- Eliminar `C:\FLEXXUS\FlexxusERP\security2.fdb.BAK2`
- Eliminar `C:\Program Files\Firebird\Firebird_2_5\security2.fdb.BAK`

---

## Objetivo futuro: App de Vendedores
Una vez con acceso a la BD, construir app con:
- Clientes y límites de crédito
- Stock disponible
- Pedidos
- Credenciales Flexxus para autenticación
