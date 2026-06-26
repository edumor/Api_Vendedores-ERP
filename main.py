"""
API Vendedores Microbell — FastAPI
Puerto: 8000
"""
import os
import math
import uuid
import smtplib
import mimetypes
import sqlite3
import json
import shutil
import urllib.parse
import urllib.request
import urllib.error
from io import BytesIO
from typing import Optional, List
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from fastapi import FastAPI, HTTPException, Query, File, UploadFile, Form, Depends, Request, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, RedirectResponse, FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import uvicorn
import threading
import asyncio
import time
import firebirdsql
from dotenv import load_dotenv
from jose import JWTError, jwt
from datetime import datetime, timedelta
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, HRFlowable
from reportlab.pdfbase import pdfmetrics

# ── Cargar variables de entorno desde .env ────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

HOST          = os.getenv('FB_HOST', '127.0.0.1')
PORT          = int(os.getenv('FB_PORT', 3050))
DATABASE      = os.getenv('DB_L1',  'c:/flexxus/DB/DB-Prueba.gdb')
DATABASE_EST  = os.getenv('DB_EST', 'c:/flexxus/DB/DB-EST-Prueba.gdb')
DATABASE_MLT  = os.getenv('DB_MLT', 'c:/flexxus/DB/DB-MLT-Prueba.gdb')
DB_USER       = os.getenv('FB_USER', 'SYSDBA')
DB_PASS       = os.getenv('FB_PASS', '')

# ── SMTP Microbell ─────────────────────────────────────────────────────────────
SMTP_HOST     = os.getenv('SMTP_HOST', '')
SMTP_PORT     = int(os.getenv('SMTP_PORT', 587))
SMTP_USER     = os.getenv('SMTP_USER', '')
SMTP_PASS     = os.getenv('SMTP_PASS', '')
SMTP_FROM     = os.getenv('SMTP_FROM', '')

SMTP_TO_PAGOS = os.getenv('SMTP_TO_PAGOS', '')

# ── OneSignal Push Notifications ─────────────────────────────────────────────
ONESIGNAL_APP_ID  = os.getenv('ONESIGNAL_APP_ID', '')
ONESIGNAL_API_KEY = os.getenv('ONESIGNAL_API_KEY', '')

# ── WhatsApp Business (Meta Cloud API) ────────────────────────────────────────
WA_PHONE_NUMBER_ID = os.getenv('WA_PHONE_NUMBER_ID', '')
WA_ACCESS_TOKEN    = os.getenv('WA_ACCESS_TOKEN', '')
WA_WABA_ID         = os.getenv('WA_WABA_ID', '')        # para crear plantillas
WA_TEMPLATE_CAT    = os.getenv('WA_TEMPLATE_CAT', 'microbell_catalogo')   # nombre plantilla catálogo
WA_TEMPLATE_SLIDE  = os.getenv('WA_TEMPLATE_SLIDE', 'microbell_catalogo') # nombre plantilla slide (puede ser la misma)

# ── Catálogos ──────────────────────────────────────────────────────────────────
_BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
CATALOGOS_DIR = os.path.join(_BASE_DIR, os.getenv('CATALOGOS_DIR', 'catalogos'))
os.makedirs(CATALOGOS_DIR, exist_ok=True)
# ──────────────────────────────────────────────────────────────────────────────

def conn(charset='WIN1252', db=None):
    return firebirdsql.connect(host=HOST, port=PORT, database=db or DATABASE,
                               user=DB_USER, password=DB_PASS, charset=charset)

# ── Cache FMA_STOCK ───────────────────────────────────────────────────────────
# TTL configurable via .env: FMA_CACHE_TTL=45 (segundos). 0 = sin caché.
_FMA_CACHE_TTL   = int(os.getenv('FMA_CACHE_TTL', 45))   # default 45 segundos
_FMA_ALL_DEPS    = ['001', '002', '003', '005', '013', '016']
_fma_cache: dict = {}
_fma_cache_lock  = threading.Lock()

def _fma_stock_bulk(dep: str, charset='WIN1252') -> dict:
    """Devuelve {ID_ARTICULO: STOCKREMANENTE} para un depósito con caché TTL."""
    if _FMA_CACHE_TTL > 0:
        with _fma_cache_lock:
            entry = _fma_cache.get(dep)
        if entry and (time.time() - entry[0]) < _FMA_CACHE_TTL:
            return entry[1]
    try:
        c2 = conn(charset)
        cur2 = c2.cursor()
        cur2.execute(f'SELECT ID_ARTICULO, STOCKREMANENTE FROM "FMA_STOCK"(NULL, NULL, \'{dep}\', 1, 1)')
        data = {row[0]: float(row[1] or 0) for row in cur2.fetchall()}
        c2.close()
    except Exception:
        data = {}
    if _FMA_CACHE_TTL > 0:
        with _fma_cache_lock:
            _fma_cache[dep] = (time.time(), data)
    return data

def _fma_stock_parallel(deps: list, charset='WIN1252') -> dict:
    """Ejecuta _fma_stock_bulk para varios depósitos en paralelo."""
    from concurrent.futures import ThreadPoolExecutor
    result = {}
    with ThreadPoolExecutor(max_workers=max(len(deps), 1)) as ex:
        futures = {ex.submit(_fma_stock_bulk, dep, charset): dep for dep in deps}
        for fut, dep in futures.items():
            try:
                result[dep] = fut.result()
            except Exception:
                result[dep] = {}
    return result

def _fma_cache_invalidate(deps: list = None):
    """Invalida el caché de los depósitos indicados (o todos si deps=None)."""
    with _fma_cache_lock:
        if deps is None:
            _fma_cache.clear()
        else:
            for d in deps:
                _fma_cache.pop(d, None)

def _prewarm_fma_cache():
    """Precalienta el caché FMA_STOCK en background al arrancar el servidor.
    Así la primera búsqueda del día ya es instantánea."""
    if _FMA_CACHE_TTL <= 0:
        return
    time.sleep(4)   # espera que el servidor termine de arrancar
    try:
        _fma_stock_parallel(_FMA_ALL_DEPS)   # los 6 depósitos en paralelo
    except Exception:
        pass        # si falla el pre-calentamiento, no tumbar el servidor

threading.Thread(target=_prewarm_fma_cache, daemon=True).start()

# ── Cache Catálogo Artículos ──────────────────────────────────────────────────
# TTL configurable via .env: CATALOG_CACHE_TTL=1800 (segundos). 0 = sin caché.
_CATALOG_CACHE_TTL  = int(os.getenv('CATALOG_CACHE_TTL', 1800))  # default 30 min
_catalog_cache: dict = {}       # {CODIGOARTICULO: {...campos...}}
_catalog_cache_ts: float = 0.0
_catalog_cache_lock = threading.Lock()
_catalog_cambio_usd: float = 1.0

def _s(v):
    """Convierte cualquier valor Firebird a str limpio."""
    if v is None: return ''
    return str(v).strip()

def _load_catalog(charset='WIN1252') -> tuple:
    """Carga todos los artículos activos con jerarquía completa desde Firebird."""
    c = conn(charset)
    cur = c.cursor()
    try:
        cur.execute('SELECT CAMBIO FROM "MONEDAS" WHERE CODIGOMONEDA=?', ('DOLARES',))
        row_m = cur.fetchone()
        cambio_usd = float(row_m[0]) if row_m else 1.0
    except Exception:
        cambio_usd = 1.0
    cur.execute("""
        SELECT
            a.CODIGOARTICULO, a.CODIGOPARTICULAR, a.DESCRIPCION, a.CODIGOMARCA,
            a.PRECIOLISTA1, a.PRECIOLISTA2, a.PRECIOLISTA3, a.PRECIOLISTA5,
            a.ALICUOTAIVA, a.COEFICIENTE, a.CODIGOUNIDADMEDIDA, a.CODIGOMONEDA,
            a.CODIGORUBRO,
            r.DESCRIPCION, r.CODIGOSUPERRUBRO,
            sr.DESCRIPCION, sr.CODIGOGRUPOSUPERRUBRO,
            g.DESCRIPCION
        FROM "ARTICULOS" a
        LEFT JOIN "RUBROS" r ON r.CODIGORUBRO = a.CODIGORUBRO
        LEFT JOIN "SUPERRUBROS" sr ON sr.CODIGOSUPERRUBRO = r.CODIGOSUPERRUBRO
        LEFT JOIN "GRUPOSUPERRUBROS" g ON g.CODIGOGRUPOSUPERRUBRO = sr.CODIGOGRUPOSUPERRUBRO
        WHERE a.ACTIVO = '1'
    """)
    catalog = {}
    for row in cur.fetchall():
        art_id = row[0]
        catalog[art_id] = {
            'codigo':                 art_id,
            'codigoparticular':       _s(row[1]) or _s(art_id),
            'descripcion':            _s(row[2]),
            'codigomarca':            _s(row[3]),
            'precio1':                float(row[4] or 0),
            'precio2':                float(row[5] or 0),
            'precio3':                float(row[6] or 0),
            'precio5':                float(row[7] or 0),
            'alicuotaiva':            row[8],
            'coeficiente':            float(row[9] or 0),
            'unidad':                 _s(row[10]),
            'codigomoneda':           _s(row[11]).upper(),
            'codigo_rubro':           _s(row[12]),
            'rubro':                  _s(row[13]),
            'codigo_superrubro':      _s(row[14]),
            'superrubro':             _s(row[15]),
            'codigo_gruposuperrubro': _s(row[16]),
            'gruposuperrubro':        _s(row[17]),
        }
    c.close()
    return catalog, cambio_usd

def _get_catalog() -> tuple:
    """Devuelve (catalog_dict, cambio_usd) desde caché o recarga si TTL expiró."""
    global _catalog_cache, _catalog_cache_ts, _catalog_cambio_usd
    now = time.time()
    if _CATALOG_CACHE_TTL > 0 and _catalog_cache and (now - _catalog_cache_ts) < _CATALOG_CACHE_TTL:
        return _catalog_cache, _catalog_cambio_usd
    with _catalog_cache_lock:
        now = time.time()
        if _CATALOG_CACHE_TTL > 0 and _catalog_cache and (now - _catalog_cache_ts) < _CATALOG_CACHE_TTL:
            return _catalog_cache, _catalog_cambio_usd
        try:
            cat, usd = _load_catalog()
            _catalog_cache = cat
            _catalog_cache_ts = time.time()
            _catalog_cambio_usd = usd
        except Exception as _e:
            if not _catalog_cache:
                raise  # primera carga fallida: propagar para que el endpoint devuelva 500
            # recarga fallida pero hay caché viejo: lo usamos
    return _catalog_cache, _catalog_cambio_usd

def _catalog_invalidate():
    """Fuerza recarga del catálogo en la próxima consulta."""
    global _catalog_cache_ts
    with _catalog_cache_lock:
        _catalog_cache_ts = 0.0

def _search_stock_cache(
    buscar=None, gruposuperrubro=None, superrubro=None, rubro=None, marca=None,
    dep_lista=None, limit=100, offset=0, cambio_usd_override=None
):
    """Búsqueda de stock en memoria combinando catálogo + FMA cache.
    Retorna (pagina, total, cambio_usd)."""
    if dep_lista is None:
        dep_lista = ['001', '003']

    catalog, cambio_usd = _get_catalog()
    if cambio_usd_override is not None:
        cambio_usd = cambio_usd_override

    # Asegurar caché FMA actualizado para los depósitos pedidos
    _fma_stock_parallel(dep_lista)
    with _fma_cache_lock:
        dep_caches = {dep: (_fma_cache.get(dep) or (0, {}))[1] for dep in dep_lista}

    buscar_norm = None
    if buscar:
        buscar = _sanitizar_buscar(buscar)
        buscar_norm = buscar.upper()

    resultados = []
    for art_id, art in catalog.items():
        # Filtro texto
        if buscar_norm:
            desc_up = art['descripcion'].upper()
            cod_up  = art['codigoparticular'].upper()
            if buscar_norm not in desc_up and buscar_norm not in cod_up:
                continue
        # Filtros jerarquía
        if rubro           and art['codigo_rubro']            != rubro:           continue
        if superrubro      and art['codigo_superrubro']       != superrubro:      continue
        if gruposuperrubro and art['codigo_gruposuperrubro']  != gruposuperrubro: continue
        if marca           and art['codigomarca']             != marca:           continue

        # Stock remanente en los depósitos solicitados
        rem_dep = {dep: dep_caches[dep].get(art_id, 0) for dep in dep_lista}
        rem_total = sum(rem_dep.values())
        if rem_total <= 0:
            continue

        resultados.append((art, rem_dep, rem_total))

    # Orden igual al SQL original
    resultados.sort(key=lambda x: x[0]['codigoparticular'])
    total = len(resultados)
    return resultados[offset:offset + limit], total, cambio_usd

def _prewarm_catalog():
    """Precalienta el catálogo en background al arrancar."""
    if _CATALOG_CACHE_TTL <= 0:
        return
    time.sleep(8)  # después del FMA prewarm
    try:
        _get_catalog()
    except Exception:
        pass

threading.Thread(target=_prewarm_catalog, daemon=True).start()

# ── Debug global: captura errores y conteos de _query_db ─────────────────────
_QV_LAST_ERRORS: dict = {}
_QV_LAST_COUNTS: dict = {}

app = FastAPI(title="API Vendedores Microbell")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── JWT ───────────────────────────────────────────────────────────────────────
JWT_SECRET = os.getenv('JWT_SECRET_KEY', 'dev-secret-CAMBIAR')
JWT_ALGO   = os.getenv('JWT_ALGORITHM', 'HS256')
JWT_HOURS  = int(os.getenv('JWT_EXPIRE_HOURS', 10))
_bearer    = HTTPBearer(auto_error=False)

def _create_token(data: dict) -> str:
    payload = data.copy()
    payload['exp'] = datetime.utcnow() + timedelta(hours=JWT_HOURS)
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(_bearer)):
    if not credentials:
        raise HTTPException(401, "No autenticado", headers={"WWW-Authenticate": "Bearer"})
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGO])
        if not payload.get('sub'):
            raise HTTPException(401, "Token inválido")
        return payload
    except JWTError:
        raise HTTPException(401, "Token inválido o expirado", headers={"WWW-Authenticate": "Bearer"})

class _LoginReq(BaseModel):
    usuario: str
    password: str

class _CambiarPassReq(BaseModel):
    usuario: str
    password_actual: str
    nueva_password: str

@app.post("/auth/login")
def auth_login(req: _LoginReq):
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute(
            'SELECT CODIGOUSUARIO, RAZONSOCIAL, CODIGOPERFIL, ESVENDEDOR, ACTIVO, '
            'BONIFICACIONMAXIMA, PORCENTAJEINCREMENTOPRECIO, PORCENTAJEDECREMENTOPRECIO '
            'FROM "USUARIOS" WHERE UPPER(CODIGOUSUARIO)=? AND UPPER(PASSWORD1)=?',
            (req.usuario.upper(), req.password.upper())
        )
        row = cur.fetchone()
        c.close()
    except Exception as e:
        raise HTTPException(500, f"Error DB: {e}")
    if not row or str(row[4] or '').strip() != '1':
        raise HTTPException(401, "Usuario o contraseña incorrectos")
    cod    = str(row[0] or '').strip()
    razon  = str(row[1] or '').strip()
    perfil = str(row[2] or '').strip()
    esvend = str(row[3] or '0').strip()
    bonif_max   = float(row[5]) if row[5] is not None else 0.0
    pct_inc     = float(row[6]) if row[6] is not None else 0.0
    pct_dec     = float(row[7]) if row[7] is not None else 0.0
    _PERFILES_OK = {'VENDEDORES', 'ADV', 'ADVJUAN', 'GERENTES', 'GTES FE'}
    if perfil not in _PERFILES_OK:
        raise HTTPException(403, "Sin acceso: perfil no autorizado")
    token = _create_token({'sub': cod, 'nombre': razon, 'perfil': perfil, 'esvendedor': esvend,
                           'bonificacion_maxima': bonif_max, 'pct_incremento': pct_inc, 'pct_decremento': pct_dec})
    return {"codigousuario": cod, "razonsocial": razon, "perfil": perfil,
            "esvendedor": esvend, "token": token,
            "bonificacion_maxima": bonif_max, "pct_incremento": pct_inc, "pct_decremento": pct_dec}

@app.post("/auth/cambiar-password")
def auth_cambiar_password(req: _CambiarPassReq, user=Depends(get_current_user)):
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute(
            'SELECT CODIGOUSUARIO FROM "USUARIOS" '
            'WHERE UPPER(CODIGOUSUARIO)=? AND UPPER(PASSWORD1)=? AND ACTIVO=?',
            (req.usuario.upper(), req.password_actual.upper(), '1')
        )
        if not cur.fetchone():
            c.close()
            raise HTTPException(401, "Contraseña actual incorrecta")
        cur.execute(
            'UPDATE "USUARIOS" SET PASSWORD1=? WHERE UPPER(CODIGOUSUARIO)=?',
            (req.nueva_password.upper(), req.usuario.upper())
        )
        c.commit()
        c.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error DB: {e}")
    return {"ok": True}
# ─────────────────────────────────────────────────────────────────────────────

FRONTEND_PATH = os.path.join(os.path.dirname(__file__), "frontend.html")
ADMIN_PATH    = os.path.join(os.path.dirname(__file__), "admin.html")
LOGO_PATH     = os.path.join(os.path.dirname(__file__), "microbellSA-color.png")
FAVICON_PATH  = os.path.join(os.path.dirname(__file__), "favicon.ico")
ADMIN_DB_PATH = os.path.join(os.path.dirname(__file__), "admin.db")

# ─── Perfiles autorizados para el Control Panel ───────────────────────────────
_ADMIN_PERFILES = {'ADV', 'DISENO', 'GERENTES', 'GTES FE', 'ADVJUAN'}

# ─── SQLite: inicialización ───────────────────────────────────────────────────
def _admin_db():
    c = sqlite3.connect(ADMIN_DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def _init_admin_db():
    c = _admin_db()
    cur = c.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS vendor_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT UNIQUE NOT NULL,
        nombre TEXT NOT NULL,
        activo INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS vendor_profile_assignments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigousuario TEXT NOT NULL,
        profile_id INTEGER NOT NULL,
        FOREIGN KEY (profile_id) REFERENCES vendor_profiles(id) ON DELETE CASCADE,
        UNIQUE(codigousuario, profile_id)
    );
    CREATE TABLE IF NOT EXISTS feature_flags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigousuario TEXT,
        feature TEXT NOT NULL,
        enabled INTEGER DEFAULT 1,
        UNIQUE(codigousuario, feature)
    );
    CREATE TABLE IF NOT EXISTS multiplazos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        dias TEXT NOT NULL,
        activo INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS vendor_multiplazos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigousuario TEXT,
        multiplazo_id INTEGER NOT NULL,
        FOREIGN KEY (multiplazo_id) REFERENCES multiplazos(id) ON DELETE CASCADE,
        UNIQUE(codigousuario, multiplazo_id)
    );
    CREATE TABLE IF NOT EXISTS vendor_multiplazos_fb (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigousuario TEXT NOT NULL,
        codigo_multiplazo TEXT NOT NULL,
        UNIQUE(codigousuario, codigo_multiplazo)
    );
    CREATE TABLE IF NOT EXISTS catalogs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        descripcion TEXT,
        url TEXT NOT NULL,
        activo INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS catalog_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        catalog_id INTEGER NOT NULL,
        profile_id INTEGER NOT NULL,
        FOREIGN KEY (catalog_id) REFERENCES catalogs(id) ON DELETE CASCADE,
        FOREIGN KEY (profile_id) REFERENCES vendor_profiles(id) ON DELETE CASCADE,
        UNIQUE(catalog_id, profile_id)
    );
    CREATE TABLE IF NOT EXISTS offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        tipo TEXT NOT NULL,
        descripcion TEXT,
        fecha_desde TEXT,
        fecha_hasta TEXT,
        activo INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS offer_product_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        codigo_producto TEXT,
        descuento_pct REAL DEFAULT 0,
        bonificacion_pct REAL DEFAULT 0,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS offer_financial_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        porcentaje REAL NOT NULL,
        orden INTEGER DEFAULT 0,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS offer_conditions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        condicion_comercial TEXT NOT NULL,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE,
        UNIQUE(offer_id, condicion_comercial)
    );
    CREATE TABLE IF NOT EXISTS offer_vendors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        codigousuario TEXT,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE,
        UNIQUE(offer_id, codigousuario)
    );
    CREATE TABLE IF NOT EXISTS stock_ajuste_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT DEFAULT (datetime('now','localtime')),
        usuario TEXT NOT NULL,
        deposito TEXT NOT NULL,
        filtro_desc TEXT,
        total_articulos INTEGER DEFAULT 0,
        con_pendientes INTEGER DEFAULT 0,
        estado TEXT DEFAULT 'ok',
        detalle TEXT
    );
    CREATE TABLE IF NOT EXISTS stock_ajuste_backup (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        log_id INTEGER NOT NULL,
        codigo_articulo TEXT NOT NULL,
        codigo_particular TEXT NOT NULL,
        descripcion TEXT,
        stock_anterior REAL,
        stock_nuevo REAL,
        diferencia REAL,
        remanente_anterior REAL,
        pedidos_pendientes INTEGER DEFAULT 0,
        FOREIGN KEY (log_id) REFERENCES stock_ajuste_log(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS admin_audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT DEFAULT (datetime('now','localtime')),
        usuario TEXT NOT NULL,
        metodo TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        ip TEXT
    );
    CREATE TABLE IF NOT EXISTS stock_reservas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo TEXT NOT NULL,
        codigo_articulo TEXT,
        codigo_particular TEXT,
        descripcion_articulo TEXT,
        tipo_grupo TEXT,
        valor_grupo TEXT,
        nombre_grupo TEXT,
        tipo_cantidad TEXT NOT NULL DEFAULT 'unidades',
        cantidad REAL NOT NULL DEFAULT 0,
        deposito TEXT DEFAULT '',
        cantidad_utilizada REAL DEFAULT 0,
        motivo TEXT NOT NULL DEFAULT '',
        fecha_hasta TEXT,
        creado_por TEXT NOT NULL DEFAULT '',
        creado_at TEXT DEFAULT (datetime('now','localtime')),
        activo INTEGER DEFAULT 1
    );
    -- Migración: agregar columnas si ya existía la tabla sin ellas

    """)
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS vendedores_contacto (
        codigo      TEXT PRIMARY KEY,
        nombre      TEXT NOT NULL,
        mail        TEXT DEFAULT '',
        celular     TEXT DEFAULT '',
        apikey_wa   TEXT DEFAULT '',
        activo      INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS catalogos (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre      TEXT NOT NULL,
        descripcion TEXT DEFAULT '',
        filename    TEXT NOT NULL,
        token       TEXT NOT NULL UNIQUE,
        subido_por  TEXT NOT NULL,
        fecha       TEXT DEFAULT (datetime('now','localtime')),
        activo        INTEGER DEFAULT 1,
        email_enviado INTEGER DEFAULT 0,
        email_count   INTEGER DEFAULT 0,
        push_enviado  INTEGER DEFAULT 0,
        push_count    INTEGER DEFAULT 0,
        wa_enviado    INTEGER DEFAULT 0,
        wa_count      INTEGER DEFAULT 0,
        perfiles_texto TEXT DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS catalogo_vendedores (
        catalogo_id INTEGER NOT NULL,
        codigo      TEXT    NOT NULL,
        PRIMARY KEY (catalogo_id, codigo),
        FOREIGN KEY (catalogo_id) REFERENCES catalogos(id) ON DELETE CASCADE
    );
    """)
    # Tabla: perfiles de vendedor asociados a la oferta
    cur.execute("""
    CREATE TABLE IF NOT EXISTS offer_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        perfil_codigo TEXT NOT NULL,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE,
        UNIQUE(offer_id, perfil_codigo)
    )""")
    # Tabla: filtros de categoría de artículos (gruposuperrubro / superrubro / rubro / marca)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS offer_category_filters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        nivel TEXT NOT NULL,
        valor TEXT NOT NULL,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE,
        UNIQUE(offer_id, nivel, valor)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS offer_combo_escalones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        min_combos INTEGER NOT NULL,
        descuento_pct REAL NOT NULL,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS app_config (
        clave TEXT PRIMARY KEY,
        valor TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    )""")
    # Migraciones no destructivas
    try: cur.execute("ALTER TABLE offers ADD COLUMN deposito TEXT DEFAULT ''")
    except Exception: pass
    try: cur.execute("ALTER TABLE offers ADD COLUMN cupo INTEGER DEFAULT 0")
    except Exception: pass
    try: cur.execute("ALTER TABLE offers ADD COLUMN usos INTEGER DEFAULT 0")
    except Exception: pass
    try: cur.execute("ALTER TABLE offers ADD COLUMN tipo_financiero TEXT DEFAULT 'descuento_total'")
    except Exception: pass
    try: cur.execute("ALTER TABLE offers ADD COLUMN monto_minimo REAL DEFAULT 0")
    except Exception: pass
    try: cur.execute("ALTER TABLE offer_product_details ADD COLUMN descripcion TEXT DEFAULT ''")
    except Exception: pass
    try: cur.execute("ALTER TABLE offer_product_details ADD COLUMN cantidad REAL DEFAULT 1")
    except Exception: pass
    try: cur.execute("ALTER TABLE stock_ajuste_log ADD COLUMN deposito_nombre TEXT")
    except Exception: pass
    try: cur.execute("ALTER TABLE admin_audit_log ADD COLUMN accion TEXT")
    except Exception: pass
    try: cur.execute("ALTER TABLE admin_audit_log ADD COLUMN detalle TEXT")
    except Exception: pass
    try: cur.execute("ALTER TABLE admin_audit_log ADD COLUMN seccion TEXT")
    except Exception: pass
    # Migraciones catalogos: estado de notificaciones y perfiles asociados
    try: cur.execute("ALTER TABLE catalogos ADD COLUMN email_enviado INTEGER DEFAULT 0")
    except Exception: pass
    try: cur.execute("ALTER TABLE catalogos ADD COLUMN wa_enviado INTEGER DEFAULT 0")
    except Exception: pass
    try: cur.execute("ALTER TABLE catalogos ADD COLUMN push_enviado INTEGER DEFAULT 0")
    except Exception: pass
    try: cur.execute("ALTER TABLE catalogos ADD COLUMN perfiles_texto TEXT DEFAULT ''")
    except Exception: pass
    try: cur.execute("ALTER TABLE catalogos ADD COLUMN email_count INTEGER DEFAULT 0")
    except Exception: pass
    try: cur.execute("ALTER TABLE catalogos ADD COLUMN wa_count INTEGER DEFAULT 0")
    except Exception: pass
    try: cur.execute("ALTER TABLE catalogos ADD COLUMN push_count INTEGER DEFAULT 0")
    except Exception: pass
    # Seed: depósitos exclusivos para ECOMMERCE (002 y 013)
    for _dep_seed in ('deposito_exclusivo_002', 'deposito_exclusivo_013'):
        try:
            cur.execute(
                "INSERT OR IGNORE INTO feature_flags (codigousuario, feature, enabled) VALUES (?,?,?)",
                ('ECOMMERCE', _dep_seed, 1)
            )
        except Exception: pass
    # Seed: ECOMMERCE usa Lista 5 de precios
    try:
        cur.execute(
            "INSERT OR IGNORE INTO feature_flags (codigousuario, feature, enabled) VALUES (?,?,?)",
            ('ECOMMERCE', 'usa_lista5', 1)
        )
    except Exception: pass
    # Seed: ECOMMERCE puede crear pedidos en L1 (DATABASE) — forzar habilitado (UPSERT)
    try:
        cur.execute(
            """INSERT INTO feature_flags (codigousuario, feature, enabled) VALUES (?,?,?)
               ON CONFLICT(codigousuario, feature) DO UPDATE SET enabled=1""",
            ('ECOMMERCE', 'pedidos', 1)
        )
    except Exception: pass
    # MIGRACION: eliminar TODOS los flags pedidos=false (global e individuales)
    # El flag pedidos=false incorrectamente fuerza a L1 vendors a usar la BD SW.
    # Solo debe existir pedidos=false para usuarios exclusivamente SW-only.
    # El admin debe volver a configurar pedidos=false solo para usuarios SW puros.
    try:
        cur.execute("DELETE FROM feature_flags WHERE feature='pedidos' AND enabled=0")
    except Exception: pass

    # Limpiar filas duplicadas en feature_flags con codigousuario IS NULL
    # (SQLite no detecta duplicados con NULL en UNIQUE, pueden haberse acumulado)
    try:
        cur.execute("""
            DELETE FROM feature_flags
            WHERE codigousuario IS NULL
              AND rowid NOT IN (
                SELECT MAX(rowid) FROM feature_flags
                WHERE codigousuario IS NULL
                GROUP BY feature
              )
        """)
    except Exception: pass

    c.commit()
    c.close()

_init_admin_db()

# ─── Config persistente (key-value en admin.db) ──────────────────────────────
def _config_get(clave: str):
    db = _admin_db()
    row = db.execute("SELECT valor FROM app_config WHERE clave=?", (clave,)).fetchone()
    db.close()
    return row['valor'] if row else None

def _config_set(clave: str, valor: str):
    db = _admin_db()
    db.execute(
        "INSERT INTO app_config (clave, valor, updated_at) VALUES (?,?,datetime('now')) "
        "ON CONFLICT(clave) DO UPDATE SET valor=excluded.valor, updated_at=excluded.updated_at",
        (clave, valor)
    )
    db.commit()
    db.close()

# ─── Helper auditoría ────────────────────────────────────────────────────────
def _audit(usuario: str, accion: str, detalle: str = '', ip: str = '', seccion: str = ''):
    try:
        db = _admin_db()
        db.execute(
            "INSERT INTO admin_audit_log (usuario, metodo, endpoint, ip, accion, detalle, seccion) VALUES (?,?,?,?,?,?,?)",
            (usuario, '', '', ip, accion, detalle, seccion)
        )
        db.commit(); db.close()
    except Exception:
        pass

# ─── Admin JWT ────────────────────────────────────────────────────────────────
JWT_ADMIN_HOURS = 8

def _create_admin_token(data: dict) -> str:
    payload = data.copy()
    payload['role'] = 'admin'
    payload['exp'] = datetime.utcnow() + timedelta(hours=JWT_ADMIN_HOURS)
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

def get_admin_user(credentials: HTTPAuthorizationCredentials = Depends(_bearer)):
    if not credentials:
        raise HTTPException(401, "No autenticado")
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGO])
        if payload.get('role') != 'admin':
            raise HTTPException(403, "Acceso denegado")
        return payload
    except JWTError:
        raise HTTPException(401, "Token inválido o expirado")

def get_admin_download_auth(
    request: Request,
    access_token: Optional[str] = Query(None)
):
    """Dependency para descargas: acepta Bearer header O ?access_token= (WebView)."""
    token = access_token
    if not token:
        auth = request.headers.get('Authorization', '')
        if auth.startswith('Bearer '):
            token = auth[7:]
    if not token:
        raise HTTPException(401, "No autenticado")
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        if payload.get('role') != 'admin':
            raise HTTPException(403, "Acceso denegado")
        return payload
    except JWTError:
        raise HTTPException(401, "Token inválido o expirado")

# ─── Admin: login ─────────────────────────────────────────────────────────────
@app.post("/admin/login")
def admin_login(req: _LoginReq, request: Request):
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute(
            'SELECT CODIGOUSUARIO, RAZONSOCIAL, CODIGOPERFIL, ACTIVO '
            'FROM "USUARIOS" WHERE UPPER(CODIGOUSUARIO)=? AND UPPER(PASSWORD1)=?',
            (req.usuario.upper(), req.password.upper())
        )
        row = cur.fetchone()
        c.close()
    except Exception as e:
        raise HTTPException(500, f"Error DB: {e}")
    if not row or str(row[3] or '').strip() != '1':
        raise HTTPException(401, "Usuario o contraseña incorrectos")
    perfil = str(row[2] or '').strip().upper()
    if perfil not in _ADMIN_PERFILES:
        raise HTTPException(403, f"Perfil '{perfil}' no tiene acceso al panel de control")
    cod   = str(row[0] or '').strip()
    razon = str(row[1] or '').strip()
    token = _create_admin_token({'sub': cod, 'nombre': razon, 'perfil': perfil})
    _ip = request.client.host if request.client else ''
    _audit(cod, 'Inicio de sesión', f'Perfil: {perfil}', _ip)
    return {"token": token, "usuario": cod, "nombre": razon, "perfil": perfil}

# ─── Admin: token de impersonación (crea JWT de vendedor para usar en iframe) ─
@app.get("/admin/impersonate-token")
def admin_impersonate_token(codigousuario: str, _u=Depends(get_admin_user)):
    """Genera un JWT temporal (1 hora) como si fuera el vendedor indicado.
    Permite al admin cargar el frontend con el contexto de ese vendedor."""
    cod = codigousuario.strip().upper()
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute(
            'SELECT CODIGOUSUARIO, RAZONSOCIAL, CODIGOPERFIL, ESVENDEDOR, ACTIVO '
            'FROM "USUARIOS" WHERE UPPER(CODIGOUSUARIO)=?', (cod,)
        )
        row = cur.fetchone()
        # Permisos del admin actuante (no del vendedor impersonado)
        admin_cod = _u.get('sub', '')
        cur.execute(
            'SELECT BONIFICACIONMAXIMA, PORCENTAJEINCREMENTOPRECIO, PORCENTAJEDECREMENTOPRECIO, CODIGOPERFIL '
            'FROM "USUARIOS" WHERE UPPER(CODIGOUSUARIO)=?', (admin_cod.upper(),)
        )
        row_admin = cur.fetchone()
        c.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    if not row:
        raise HTTPException(404, f"Vendedor '{cod}' no encontrado")
    razon  = str(row[1] or '').strip()
    perfil = str(row[2] or '').strip()
    esvend = str(row[3] or '').strip()
    bonif_max = float(row_admin[0]) if row_admin and row_admin[0] is not None else 0.0
    pct_inc   = float(row_admin[1]) if row_admin and row_admin[1] is not None else 0.0
    pct_dec   = float(row_admin[2]) if row_admin and row_admin[2] is not None else 0.0
    admin_perfil = str(row_admin[3] or '').strip() if row_admin else ''
    payload = {'sub': cod, 'nombre': razon, 'perfil': perfil,
               'esvendedor': esvend, 'impersonated_by': admin_cod,
               'admin_perfil': admin_perfil,
               'bonificacion_maxima': bonif_max, 'pct_incremento': pct_inc, 'pct_decremento': pct_dec,
               'exp': datetime.utcnow() + timedelta(hours=1)}
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)
    return {"token": token, "codigousuario": cod, "nombre": razon}

# ─── Admin: sirve admin.html ──────────────────────────────────────────────────
@app.get("/admin", response_class=HTMLResponse)
@app.get("/ctrl", response_class=HTMLResponse)
def admin_panel():
    if os.path.exists(ADMIN_PATH):
        with open(ADMIN_PATH, encoding="utf-8") as f:
            return f.read()
    return HTMLResponse("<h1>admin.html no encontrado</h1>", status_code=404)

# ─── Admin: vendedores (lista desde Firebird) ─────────────────────────────────
@app.get("/admin/vendedores")
def admin_get_vendedores(_u=Depends(get_admin_user)):
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute(
            "SELECT CODIGOUSUARIO, RAZONSOCIAL, CODIGOPERFIL FROM \"USUARIOS\" "
            "WHERE ACTIVO='1' AND UPPER(TRIM(CODIGOPERFIL))='VENDEDORES' ORDER BY RAZONSOCIAL"
        )
        rows = cur.fetchall()
        c.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    return [{"codigo": str(r[0] or '').strip(), "nombre": str(r[1] or '').strip(),
             "perfil_flexxus": str(r[2] or '').strip()} for r in rows]

@app.get("/vendedores-lista")
def get_vendedores_lista(u=Depends(get_current_user)):
    """Lista de vendedores accesible con token de impersonación admin."""
    if not u.get('admin_perfil'):
        raise HTTPException(403, "Requiere perfil admin")
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute(
            "SELECT CODIGOUSUARIO, RAZONSOCIAL FROM \"USUARIOS\" "
            "WHERE ACTIVO='1' AND UPPER(TRIM(CODIGOPERFIL))='VENDEDORES' ORDER BY RAZONSOCIAL"
        )
        rows = cur.fetchall()
        c.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    return [{"codigo": str(r[0] or '').strip(), "nombre": str(r[1] or '').strip()} for r in rows]

# ─── Admin: perfiles de vendedor ABM ─────────────────────────────────────────
@app.get("/admin/perfiles")
def admin_get_perfiles(_u=Depends(get_admin_user)):
    c = _admin_db(); rows = c.execute("SELECT * FROM vendor_profiles ORDER BY nombre").fetchall(); c.close()
    return [dict(r) for r in rows]

@app.post("/admin/perfiles")
def admin_create_perfil(data: dict, _u=Depends(get_admin_user)):
    codigo = (data.get('codigo') or '').strip().upper()
    nombre = (data.get('nombre') or '').strip()
    if not codigo or not nombre:
        raise HTTPException(400, "codigo y nombre requeridos")
    try:
        c = _admin_db()
        c.execute("INSERT INTO vendor_profiles (codigo, nombre) VALUES (?,?)", (codigo, nombre))
        c.commit(); id_ = c.execute("SELECT last_insert_rowid()").fetchone()[0]; c.close()
    except sqlite3.IntegrityError:
        raise HTTPException(409, f"Perfil '{codigo}' ya existe")
    return {"id": id_, "codigo": codigo, "nombre": nombre, "activo": 1}

@app.put("/admin/perfiles/{id}")
def admin_update_perfil(id: int, data: dict, _u=Depends(get_admin_user)):
    c = _admin_db()
    c.execute("UPDATE vendor_profiles SET codigo=?, nombre=?, activo=? WHERE id=?",
              (data.get('codigo','').strip().upper(), data.get('nombre','').strip(), data.get('activo',1), id))
    c.commit(); c.close()
    return {"ok": True}

@app.delete("/admin/perfiles/{id}")
def admin_delete_perfil(id: int, _u=Depends(get_admin_user)):
    c = _admin_db(); c.execute("DELETE FROM vendor_profiles WHERE id=?", (id,)); c.commit(); c.close()
    return {"ok": True}

# ─── Admin: asignación perfiles ↔ vendedores ──────────────────────────────────
@app.get("/admin/asignaciones")
def admin_get_asignaciones(_u=Depends(get_admin_user)):
    c = _admin_db()
    rows = c.execute("""
        SELECT a.codigousuario, p.id, p.codigo, p.nombre
        FROM vendor_profile_assignments a
        JOIN vendor_profiles p ON p.id=a.profile_id
        ORDER BY a.codigousuario, p.nombre
    """).fetchall(); c.close()
    return [dict(r) for r in rows]

@app.post("/admin/asignaciones")
def admin_set_asignacion(data: dict, _u=Depends(get_admin_user)):
    cod = (data.get('codigousuario') or '').strip().upper()
    pid = data.get('profile_id')
    if not cod or not pid:
        raise HTTPException(400, "codigousuario y profile_id requeridos")
    c = _admin_db()
    try:
        c.execute("INSERT OR IGNORE INTO vendor_profile_assignments (codigousuario, profile_id) VALUES (?,?)", (cod, pid))
        c.commit()
    finally:
        c.close()
    return {"ok": True}

@app.delete("/admin/asignaciones")
def admin_del_asignacion(codigousuario: str, profile_id: int, _u=Depends(get_admin_user)):
    c = _admin_db()
    c.execute("DELETE FROM vendor_profile_assignments WHERE codigousuario=? AND profile_id=?",
              (codigousuario.upper(), profile_id))
    c.commit(); c.close()
    return {"ok": True}

# ─── Admin: feature flags ─────────────────────────────────────────────────────
@app.get("/admin/flags")
def admin_get_flags(_u=Depends(get_admin_user)):
    c = _admin_db()
    rows = c.execute("SELECT * FROM feature_flags ORDER BY codigousuario, feature").fetchall(); c.close()
    return [dict(r) for r in rows]

@app.post("/admin/flags")
def admin_set_flag(data: dict, _u=Depends(get_admin_user)):
    cod     = (data.get('codigousuario') or None)
    if cod: cod = cod.strip().upper() or None
    feature = (data.get('feature') or '').strip()
    enabled = int(data.get('enabled', 1))
    if not feature:
        raise HTTPException(400, "feature requerido")
    c = _admin_db()
    if cod is None:
        # SQLite no detecta duplicados con NULL en UNIQUE, usar DELETE+INSERT explícito
        c.execute("DELETE FROM feature_flags WHERE codigousuario IS NULL AND feature=?", (feature,))
        c.execute("INSERT INTO feature_flags (codigousuario, feature, enabled) VALUES (NULL,?,?)", (feature, enabled))
    else:
        c.execute("""INSERT INTO feature_flags (codigousuario, feature, enabled) VALUES (?,?,?)
                     ON CONFLICT(codigousuario, feature) DO UPDATE SET enabled=excluded.enabled""",
                  (cod, feature, enabled))
    c.commit(); c.close()
    return {"ok": True}

# Endpoint público para que el frontend consulte sus flags
@app.post("/ofertas/{id}/usar")
def registrar_uso_oferta(id: int):
    """Registra un uso de la promo. Si usos >= cupo (y cupo > 0) la desactiva."""
    c = _admin_db()
    row = c.execute("SELECT cupo, usos, activo FROM offers WHERE id=?", (id,)).fetchone()
    if not row:
        c.close(); raise HTTPException(404, "Oferta no encontrada")
    cupo = row['cupo'] or 0
    usos = (row['usos'] or 0) + 1
    activo = row['activo']
    if cupo > 0 and usos >= cupo:
        activo = 0
    c.execute("UPDATE offers SET usos=?, activo=? WHERE id=?", (usos, activo, id))
    c.commit(); c.close()
    return {"usos": usos, "cupo": cupo, "activo": activo, "agotada": cupo > 0 and usos >= cupo}

@app.get("/vendor-perfiles")
def get_vendor_perfiles(vendedor: str):
    """Devuelve los codigos de perfil (TECNOLOGIA, JUGUETERIA, etc.) asignados a un vendedor."""
    c = _admin_db()
    rows = c.execute("""
        SELECT vp.codigo FROM vendor_profiles vp
        JOIN vendor_profile_assignments vpa ON vpa.profile_id = vp.id
        WHERE UPPER(vpa.codigousuario) = ?
    """, (vendedor.upper(),)).fetchall()
    c.close()
    return [r[0] for r in rows]

@app.get("/flags")
def get_flags_for_vendor(vendedor: Optional[str] = None):
    c = _admin_db()
    vend_upper = vendedor.upper() if vendedor else ''

    # 1. Global flags (codigousuario IS NULL)
    global_rows = c.execute(
        "SELECT feature, enabled FROM feature_flags WHERE codigousuario IS NULL"
    ).fetchall()

    # 2. Profile flags del vendedor (codigousuario = 'PERFIL:XXX')
    perfil_rows = []
    if vend_upper:
        perfil_codigos = c.execute("""
            SELECT 'PERFIL:' || vp.codigo
            FROM vendor_profiles vp
            JOIN vendor_profile_assignments vpa ON vpa.profile_id = vp.id
            WHERE UPPER(vpa.codigousuario) = ?
        """, (vend_upper,)).fetchall()
        if perfil_codigos:
            placeholders = ','.join('?' * len(perfil_codigos))
            keys = [r[0] for r in perfil_codigos]
            perfil_rows = c.execute(
                f"SELECT feature, enabled FROM feature_flags WHERE codigousuario IN ({placeholders})",
                keys
            ).fetchall()

    # 3. Individual flags del vendedor
    ind_rows = c.execute(
        "SELECT feature, enabled FROM feature_flags WHERE codigousuario=?",
        (vend_upper,)
    ).fetchall() if vend_upper else []

    c.close()

    # Aplicar precedencia: global → perfil → individual
    result = {}
    for r in global_rows:
        result[r['feature']] = bool(r['enabled'])
    for r in perfil_rows:
        result[r['feature']] = bool(r['enabled'])
    for r in ind_rows:
        result[r['feature']] = bool(r['enabled'])

    # Derivar deposito_exclusivo desde flags deposito_exclusivo_XXX
    # Si hay ALGÚN flag configurado (aunque todos estén deshabilitados), siempre incluir la clave
    # para que el frontend distinga "sin configurar" (clave ausente) de "configurado a vacío" (clave = "")
    dep_all = [k for k in result if k.startswith('deposito_exclusivo_')]
    dep_enabled = sorted([k.replace('deposito_exclusivo_', '') for k in dep_all if result[k]])
    if dep_all:
        result['deposito_exclusivo'] = ','.join(dep_enabled)  # puede ser '' si todos deshabilitados
    return result

@app.post("/admin/flags/bulk")
def admin_set_flags_bulk(data: dict, _u=Depends(get_admin_user)):
    feature  = (data.get('feature') or '').strip()
    enabled  = 1 if data.get('enabled') else 0
    usuarios = data.get('codigousuarios') or []
    if not feature or not usuarios:
        raise HTTPException(400, "feature y codigousuarios requeridos")
    c = _admin_db()
    for cod in usuarios:
        cod = str(cod).strip().upper()
        if cod:
            c.execute("""INSERT INTO feature_flags (codigousuario, feature, enabled) VALUES (?,?,?)
                         ON CONFLICT(codigousuario, feature) DO UPDATE SET enabled=excluded.enabled""",
                      (cod, feature, enabled))
    c.commit(); c.close()
    return {"ok": True, "actualizados": len(usuarios)}

@app.post("/admin/flags/reset-individuales")
def admin_reset_flags_individuales(data: dict, _u=Depends(get_admin_user)):
    """Borra todos los overrides individuales de una feature, dejando solo el global."""
    feature = (data.get('feature') or '').strip()
    if not feature:
        raise HTTPException(400, "feature requerida")
    c = _admin_db()
    c.execute("DELETE FROM feature_flags WHERE feature=? AND codigousuario IS NOT NULL AND TRIM(codigousuario)<>''", (feature,))
    c.commit(); c.close()
    return {"ok": True}

@app.delete("/admin/flags/depositos-vendedor/{codigousuario}")
def admin_delete_depositos_vendedor(codigousuario: str, _u=Depends(get_admin_user)):
    """Elimina todas las filas deposito_exclusivo_* para un vendedor o perfil, dejando que herede global/perfil."""
    cod = codigousuario.strip()
    if not cod:
        raise HTTPException(400, "codigousuario requerido")
    c = _admin_db()
    c.execute("DELETE FROM feature_flags WHERE codigousuario=? AND feature LIKE 'deposito_exclusivo_%'", (cod,))
    c.commit(); c.close()
    return {"ok": True}

@app.delete("/admin/flags/depositos-todos-individuales")
def admin_delete_depositos_todos_individuales(_u=Depends(get_admin_user)):
    """Elimina TODOS los overrides individuales y de perfil de deposito_exclusivo_*, dejando solo el global."""
    c = _admin_db()
    # Borrar individual y de perfiles (PERFIL:XXX), mantener solo codigousuario IS NULL (global)
    c.execute("DELETE FROM feature_flags WHERE feature LIKE 'deposito_exclusivo_%' AND codigousuario IS NOT NULL")
    c.commit(); c.close()
    return {"ok": True}

# ─── Admin: multiplazos ───────────────────────────────────────────────────────
@app.get("/admin/multiplazos")
def admin_get_multiplazos(_u=Depends(get_admin_user)):
    c = _admin_db(); rows = c.execute("SELECT * FROM multiplazos ORDER BY nombre").fetchall(); c.close()
    return [dict(r) for r in rows]

@app.post("/admin/multiplazos")
def admin_create_multiplazo(data: dict, _u=Depends(get_admin_user)):
    nombre = (data.get('nombre') or '').strip()
    dias   = (data.get('dias') or '').strip()
    if not nombre or not dias:
        raise HTTPException(400, "nombre y dias requeridos")
    c = _admin_db()
    c.execute("INSERT INTO multiplazos (nombre, dias) VALUES (?,?)", (nombre, dias))
    c.commit(); id_ = c.execute("SELECT last_insert_rowid()").fetchone()[0]; c.close()
    return {"id": id_, "nombre": nombre, "dias": dias, "activo": 1}

@app.put("/admin/multiplazos/{id}")
def admin_update_multiplazo(id: int, data: dict, _u=Depends(get_admin_user)):
    c = _admin_db()
    c.execute("UPDATE multiplazos SET nombre=?, dias=?, activo=? WHERE id=?",
              (data.get('nombre','').strip(), data.get('dias','').strip(), data.get('activo',1), id))
    c.commit(); c.close()
    return {"ok": True}

@app.delete("/admin/multiplazos/{id}")
def admin_delete_multiplazo(id: int, _u=Depends(get_admin_user)):
    c = _admin_db(); c.execute("DELETE FROM multiplazos WHERE id=?", (id,)); c.commit(); c.close()
    return {"ok": True}

@app.get("/admin/vendor-multiplazos")
def admin_get_vendor_multiplazos(_u=Depends(get_admin_user)):
    c = _admin_db()
    rows = c.execute("""
        SELECT vm.codigousuario, m.id, m.nombre, m.dias
        FROM vendor_multiplazos vm JOIN multiplazos m ON m.id=vm.multiplazo_id
        ORDER BY vm.codigousuario, m.nombre
    """).fetchall(); c.close()
    return [dict(r) for r in rows]

@app.post("/admin/vendor-multiplazos")
def admin_set_vendor_multiplazo(data: dict, _u=Depends(get_admin_user)):
    cod = (data.get('codigousuario') or None)
    if cod: cod = cod.strip().upper() or None
    mid = data.get('multiplazo_id')
    c = _admin_db()
    c.execute("INSERT OR IGNORE INTO vendor_multiplazos (codigousuario, multiplazo_id) VALUES (?,?)", (cod, mid))
    c.commit(); c.close()
    return {"ok": True}

@app.delete("/admin/vendor-multiplazos")
def admin_del_vendor_multiplazo(codigousuario: str, multiplazo_id: int, _u=Depends(get_admin_user)):
    cod = codigousuario.upper() if codigousuario != '__global__' else None
    c = _admin_db()
    c.execute("DELETE FROM vendor_multiplazos WHERE codigousuario IS ? AND multiplazo_id=?", (cod, multiplazo_id))
    c.commit(); c.close()
    return {"ok": True}

# ─── Multiplazos desde Firebird ───────────────────────────────────────────────
@app.get("/admin/multiplazos-fb")
def admin_get_multiplazos_fb(_u=Depends(get_admin_user)):
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute('SELECT CODIGOMULTIPLAZO, DESCRIPCION FROM "MULTIPLAZOS" WHERE ACTIVO=? ORDER BY DESCRIPCION', ('1',))
        rows = cur.fetchall()
        c.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    return [{"codigo": str(r[0] or '').strip(), "descripcion": str(r[1] or '').strip()} for r in rows]

@app.get("/admin/vendor-multiplazos-fb")
def admin_get_vendor_multiplazos_fb(_u=Depends(get_admin_user)):
    db = _admin_db()
    rows = db.execute("SELECT codigousuario, codigo_multiplazo FROM vendor_multiplazos_fb ORDER BY codigousuario, codigo_multiplazo").fetchall()
    db.close()
    return [dict(r) for r in rows]

@app.post("/admin/vendor-multiplazos-fb")
def admin_set_vendor_multiplazo_fb(data: dict, _u=Depends(get_admin_user)):
    cod  = (data.get('codigousuario') or '').strip().upper()
    mps  = data.get('codigos_multiplazo') or []  # lista de códigos FB
    if not cod or not mps:
        raise HTTPException(400, "codigousuario y codigos_multiplazo requeridos")
    db = _admin_db()
    for mp in mps:
        mp = str(mp).strip()
        if mp:
            db.execute("INSERT OR IGNORE INTO vendor_multiplazos_fb (codigousuario, codigo_multiplazo) VALUES (?,?)", (cod, mp))
    db.commit(); db.close()
    return {"ok": True}

@app.delete("/admin/vendor-multiplazos-fb")
def admin_del_vendor_multiplazo_fb(codigousuario: str, codigo_multiplazo: str, _u=Depends(get_admin_user)):
    db = _admin_db()
    db.execute("DELETE FROM vendor_multiplazos_fb WHERE codigousuario=? AND codigo_multiplazo=?",
               (codigousuario.upper(), codigo_multiplazo.strip()))
    db.commit(); db.close()
    return {"ok": True}

@app.delete("/admin/vendor-multiplazos-fb/bulk")
def admin_del_vendor_multiplazo_fb_bulk(codigousuario: str, _u=Depends(get_admin_user)):
    """Elimina TODOS los multiplazos asignados a un vendedor"""
    db = _admin_db()
    db.execute("DELETE FROM vendor_multiplazos_fb WHERE codigousuario=?", (codigousuario.upper(),))
    db.commit(); db.close()
    return {"ok": True}

@app.post("/admin/vendor-multiplazos-fb/bulk-perfil")
def admin_set_multiplazos_fb_perfil(data: dict, _u=Depends(get_admin_user)):
    """Asigna una lista de multiplazos a todos los vendedores de un perfil (vendor_profile_assignments)"""
    profile_id       = data.get('profile_id')
    codigos_mp       = data.get('codigos_multiplazo') or []
    reemplazar       = data.get('reemplazar', False)
    if not profile_id or not codigos_mp:
        raise HTTPException(400, "profile_id y codigos_multiplazo requeridos")
    db = _admin_db()
    # Buscar vendedores con ese perfil
    vendors = db.execute("SELECT DISTINCT codigousuario FROM vendor_profile_assignments WHERE profile_id=?", (profile_id,)).fetchall()
    if not vendors:
        db.close(); raise HTTPException(404, "Sin vendedores en ese perfil")
    for v in vendors:
        cod = v['codigousuario']
        if reemplazar:
            db.execute("DELETE FROM vendor_multiplazos_fb WHERE codigousuario=?", (cod,))
        for mp in codigos_mp:
            mp = str(mp).strip()
            if mp:
                db.execute("INSERT OR IGNORE INTO vendor_multiplazos_fb (codigousuario, codigo_multiplazo) VALUES (?,?)", (cod, mp))
    db.commit(); db.close()
    return {"ok": True, "vendedores": len(vendors)}

# Endpoint público para frontend — devuelve multiplazos de Firebird filtrados por asignación del vendedor
@app.get("/multiplazos")
def get_multiplazos_for_vendor(vendedor: Optional[str] = None):
    try:
        # Obtener todos los activos de Firebird
        fb = conn('WIN1252')
        cur = fb.cursor()
        cur.execute('SELECT CODIGOMULTIPLAZO, DESCRIPCION FROM "MULTIPLAZOS" WHERE ACTIVO=? ORDER BY DESCRIPCION', ('1',))
        todos = [{"codigo": str(r[0] or '').strip(), "descripcion": str(r[1] or '').strip()} for r in cur.fetchall()]
        fb.close()
    except Exception:
        todos = []
    if not vendedor:
        return todos
    # Filtrar por asignaciones del vendedor en admin.db
    db = _admin_db()
    asignados = [r[0] for r in db.execute(
        "SELECT codigo_multiplazo FROM vendor_multiplazos_fb WHERE codigousuario=?",
        (vendedor.upper(),)
    ).fetchall()]
    db.close()
    if not asignados:
        return todos  # sin asignaciones: devuelve todos
    return [m for m in todos if m['codigo'] in asignados]

# ─── Admin: catálogos ─────────────────────────────────────────────────────────
@app.get("/admin/catalogs-legacy")
def admin_get_catalogos(_u=Depends(get_admin_user)):
    """Endpoint legacy — tabla 'catalogs' (vieja). Usar /admin/catalogos para la nueva."""
    c = _admin_db()
    cats = [dict(r) for r in c.execute("SELECT * FROM catalogs ORDER BY nombre").fetchall()]
    for cat in cats:
        profs = c.execute("""
            SELECT p.id, p.codigo, p.nombre FROM catalog_profiles cp
            JOIN vendor_profiles p ON p.id=cp.profile_id WHERE cp.catalog_id=?
        """, (cat['id'],)).fetchall()
        cat['profiles'] = [dict(p) for p in profs]
    c.close()
    return cats

@app.post("/admin/catalogos")
def admin_create_catalogo(data: dict, _u=Depends(get_admin_user)):
    nombre = (data.get('nombre') or '').strip()
    url    = (data.get('url') or '').strip()
    if not nombre or not url:
        raise HTTPException(400, "nombre y url requeridos")
    c = _admin_db()
    c.execute("INSERT INTO catalogs (nombre, descripcion, url) VALUES (?,?,?)",
              (nombre, data.get('descripcion','').strip(), url))
    c.commit(); id_ = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    for pid in (data.get('profile_ids') or []):
        try: c.execute("INSERT OR IGNORE INTO catalog_profiles (catalog_id, profile_id) VALUES (?,?)", (id_, pid))
        except: pass
    c.commit(); c.close()
    return {"id": id_, "ok": True}

@app.put("/admin/catalogos/{id}")
def admin_update_catalogo(id: int, data: dict, _u=Depends(get_admin_user)):
    c = _admin_db()
    c.execute("UPDATE catalogs SET nombre=?, descripcion=?, url=?, activo=? WHERE id=?",
              (data.get('nombre','').strip(), data.get('descripcion','').strip(),
               data.get('url','').strip(), data.get('activo',1), id))
    if 'profile_ids' in data:
        c.execute("DELETE FROM catalog_profiles WHERE catalog_id=?", (id,))
        for pid in (data['profile_ids'] or []):
            try: c.execute("INSERT OR IGNORE INTO catalog_profiles (catalog_id, profile_id) VALUES (?,?)", (id, pid))
            except: pass
    c.commit(); c.close()
    return {"ok": True}

@app.delete("/admin/catalogs-legacy/{id}")
def admin_delete_catalogo_legacy(id: int, _u=Depends(get_admin_user)):
    """Legacy: borraba de tabla 'catalogs' vieja. Reemplazado por /admin/catalogos/{cat_id}."""
    c = _admin_db(); c.execute("DELETE FROM catalogs WHERE id=?", (id,)); c.commit(); c.close()
    return {"ok": True}

# Endpoint público para frontend
@app.get("/catalogos")
def get_catalogos_for_vendor(vendedor: Optional[str] = None):
    c = _admin_db()
    if vendedor:
        rows = c.execute("""
            SELECT DISTINCT ca.id, ca.nombre, ca.descripcion, ca.url
            FROM catalogs ca
            JOIN catalog_profiles cp ON cp.catalog_id=ca.id
            JOIN vendor_profile_assignments vpa ON vpa.profile_id=cp.profile_id
            WHERE ca.activo=1 AND vpa.codigousuario=?
        """, (vendedor.upper(),)).fetchall()
    else:
        rows = c.execute("SELECT id, nombre, descripcion, url FROM catalogs WHERE activo=1").fetchall()
    c.close()
    return [dict(r) for r in rows]

# ─── Admin: invalidación manual de cachés ────────────────────────────────────
@app.post("/admin/cache/refresh")
def admin_cache_refresh(target: str = Query("all", description="'catalog', 'stock' o 'all'"), _u=Depends(get_admin_user)):
    """Fuerza recarga del caché de catálogo y/o stock sin reiniciar el servidor."""
    if target in ("catalog", "all"):
        _catalog_invalidate()
        _get_catalog()   # recarga sincrónica
    if target in ("stock", "all"):
        _fma_cache_invalidate()
        _fma_stock_parallel(_FMA_ALL_DEPS)  # recarga sincrónica
    return {"ok": True, "refreshed": target}

@app.get("/admin/cache/status")
def admin_cache_status(_u=Depends(get_admin_user)):
    """Devuelve estado de los cachés (edad en segundos)."""
    now = time.time()
    catalog_age = int(now - _catalog_cache_ts) if _catalog_cache_ts else None
    fma_ages = {}
    with _fma_cache_lock:
        for dep, entry in _fma_cache.items():
            fma_ages[dep] = int(now - entry[0])
    return {
        "catalog": {
            "articulos": len(_catalog_cache),
            "edad_seg":  catalog_age,
            "ttl_seg":   _CATALOG_CACHE_TTL,
        },
        "fma_stock": {
            "depositos": fma_ages,
            "ttl_seg":   _FMA_CACHE_TTL,
        }
    }

# ─── Admin: depósitos disponibles ────────────────────────────────────────────
@app.get("/admin/depositos")
def admin_get_depositos(_u=Depends(get_admin_user)):
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute('SELECT CODIGODEPOSITO, DESCRIPCION FROM "DEPOSITOS" WHERE ACTIVO=1 ORDER BY CODIGODEPOSITO')
        rows = cur.fetchall()
        c.close()
        result = [
            {"codigo": str(r[0] or '').strip(), "nombre": str(r[1] or '').strip()}
            for r in rows
            if str(r[0] or '').strip()  # excluir vacios
        ]
        if result:
            return result
    except Exception:
        pass
    return [
        {"codigo": "001", "nombre": "DEPOSITO VAC-LOG"},
        {"codigo": "002", "nombre": "DEPOSITO MARKET PLACE"},
        {"codigo": "003", "nombre": "DEPOSITO PACHECO"},
        {"codigo": "005", "nombre": "DEPOSITO OUTLET"},
        {"codigo": "016", "nombre": "DEPOSITO EXPO"},
    ]

@app.get("/depositos")
def get_depositos_publico():
    """Endpoint público: lista de depósitos activos (para frontend de vendedores)."""
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute('SELECT CODIGODEPOSITO, DESCRIPCION FROM "DEPOSITOS" WHERE ACTIVO=1 ORDER BY CODIGODEPOSITO')
        rows = cur.fetchall()
        c.close()
        result = [
            {"codigo": str(r[0] or '').strip(), "nombre": str(r[1] or '').strip()}
            for r in rows
            if str(r[0] or '').strip()
        ]
        if result:
            return result
    except Exception:
        pass
    return [
        {"codigo": "001", "nombre": "DEPOSITO VAC-LOG"},
        {"codigo": "002", "nombre": "DEPOSITO MARKET PLACE"},
        {"codigo": "003", "nombre": "DEPOSITO PACHECO"},
        {"codigo": "005", "nombre": "DEPOSITO OUTLET"},
        {"codigo": "013", "nombre": "DEPOSITO FULL ML"},
        {"codigo": "016", "nombre": "DEPOSITO EXPO"},
    ]

# ─── Admin: Ajuste de Stock ───────────────────────────────────────────────────
_PERFILES_GERENTES = {'GERENTES', 'GTES FE'}

def get_gerente_user(credentials: HTTPAuthorizationCredentials = Depends(_bearer)):
    if not credentials:
        raise HTTPException(401, "No autenticado")
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGO])
        if payload.get('role') != 'admin':
            raise HTTPException(403, "Acceso denegado")
        perfil = str(payload.get('perfil') or '').strip().upper()
        if perfil not in _PERFILES_GERENTES:
            raise HTTPException(403, f"Perfil '{perfil}' no tiene acceso a Ajuste de Stock")
        return payload
    except JWTError:
        raise HTTPException(401, "Token inválido o expirado")

@app.post("/admin/ajuste-stock/preview")
def ajuste_stock_preview(body: dict, deposito: str, _u=Depends(get_gerente_user)):
    """CSV define los SKUs. Depósito obligatorio. Sin filtros adicionales."""
    try:
        articulos_csv = body.get('articulos') or []
        if not articulos_csv:
            raise HTTPException(400, "El CSV está vacío")
        if not deposito:
            raise HTTPException(400, "Seleccioná un depósito")
        csv_map = {str(a['codigo']).strip(): float(a.get('cantidad', 0))
                   for a in articulos_csv if a.get('codigo')}
        cod_list = list(csv_map.keys())
        placeholders = ','.join(['?' for _ in cod_list])
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute(f'''
            SELECT CODIGOARTICULO, CODIGOPARTICULAR, DESCRIPCION
            FROM "ARTICULOS"
            WHERE CODIGOPARTICULAR IN ({placeholders})
            ORDER BY CODIGOPARTICULAR
        ''', cod_list)
        arts = cur.fetchall()
        # Remanente del depósito seleccionado
        stock_map = {}
        try:
            cur2 = c.cursor()
            cur2.execute(f'SELECT ID_ARTICULO, STOCKREMANENTE FROM "FMA_STOCK"(NULL, NULL, \'{deposito}\', 1, 1)')
            stock_map = {str(r[0] or '').strip(): float(r[1] or 0) for r in cur2.fetchall()}
        except Exception:
            pass
        # Stock total Flexxus (STOCK table, CODIGOSUCURSAL=PRINCIPAL)
        stock_total_map = {}
        try:
            cur_st = c.cursor()
            cur_st.execute("SELECT CODIGOARTICULO, STOCKACTUAL FROM \"STOCK\" WHERE CODIGOSUCURSAL='PRINCIPAL' AND LOTE='000'")
            stock_total_map = {str(r[0] or '').strip(): float(r[1] or 0) for r in cur_st.fetchall()}
        except Exception:
            pass
        # Remanente total Flexxus = suma FMA_STOCK de todos los depósitos activos
        _DEPS_ACTIVOS = ['001','002','003','005','013','016']
        rem_total_map = {}
        try:
            cur_rt = c.cursor()
            cur_rt.execute("SELECT ID_ARTICULO, STOCKREMANENTE FROM \"FMA_STOCK\"(NULL, NULL, NULL, 1, 1)")
            for r in cur_rt.fetchall():
                k = str(r[0] or '').strip()
                rem_total_map[k] = rem_total_map.get(k, 0.0) + float(r[1] or 0)
        except Exception:
            # fallback: sumar por cada depósito activo
            try:
                for dep in _DEPS_ACTIVOS:
                    cur_rt2 = c.cursor()
                    cur_rt2.execute(f"SELECT ID_ARTICULO, STOCKREMANENTE FROM \"FMA_STOCK\"(NULL, NULL, '{dep}', 1, 1)")
                    for r in cur_rt2.fetchall():
                        k = str(r[0] or '').strip()
                        rem_total_map[k] = rem_total_map.get(k, 0.0) + float(r[1] or 0)
            except Exception:
                pass
        pendientes_map = {}
        try:
            cur3 = c.cursor()
            cur3.execute('''
                SELECT cc.CODIGOARTICULO, SUM(cc.CANTIDAD - cc.CANTIDADPREPARADA)
                FROM "CUERPOCOMPROBANTES" cc
                JOIN "CABEZACOMPROBANTES" cab ON cab.NUMEROCOMPROBANTE = cc.NUMEROCOMPROBANTE
                WHERE cab.TIPOCOMPROBANTE = 'PE'
                  AND cc.CANTIDADPREPARADA < cc.CANTIDAD
                  AND cc.CODIGODEPOSITO = ?
                GROUP BY cc.CODIGOARTICULO
            ''', (deposito,))
            pendientes_map = {str(r[0] or '').strip(): int(r[1] or 0) for r in cur3.fetchall()}
        except Exception:
            pass
        c.close()
        encontrados = {str(a[1] or '').strip() for a in arts}
        no_encontrados = [x for x in cod_list if x not in encontrados]
        resultado = []
        for a in arts:
            cod_art  = str(a[0] or '').strip()
            cod_part = str(a[1] or '').strip()
            stock_ant = stock_map.get(cod_art, 0.0)
            stock_nvo = csv_map.get(cod_part, stock_ant)
            diff = round(stock_nvo - stock_ant, 4)
            rem_ant = stock_map.get(cod_art, 0.0)
            rem_nvo = round(rem_ant + diff, 4)
            resultado.append({
                "codigo_articulo": cod_art, "codigo_particular": cod_part,
                "descripcion": str(a[2] or '').strip(),
                "stock_anterior": stock_ant, "stock_nuevo": stock_nvo,
                "diferencia": diff,
                "remanente_anterior": rem_ant,
                "remanente_nuevo": rem_nvo,
                "stock_total_flexxus": stock_total_map.get(cod_art, 0.0),
                "remanente_total_flexxus": round(rem_total_map.get(cod_art, 0.0), 4),
                "pedidos_pendientes": pendientes_map.get(cod_art, 0)
            })
        return {"deposito": deposito, "articulos": resultado, "no_encontrados": no_encontrados}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/admin/ajuste-stock/jerarquia")
def ajuste_stock_jerarquia(_u=Depends(get_gerente_user)):
    """Devuelve SuperRubro -> Rubros usando WEB_STOCK (sin JOIN a tablas faltantes)."""
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        # WEB_STOCK ya tiene CODIGOSUPERRUBRO y CODIGORUBRO directamente
        cur.execute("""
            SELECT DISTINCT
                TRIM(CODIGOSUPERRUBRO),
                TRIM(CODIGORUBRO)
            FROM "WEB_STOCK"
            WHERE CODIGOSUPERRUBRO IS NOT NULL
              AND CODIGORUBRO IS NOT NULL
            ORDER BY 1, 2
        """)
        rows = cur.fetchall()
        c.close()
        from collections import defaultdict
        tree = defaultdict(list)
        for sr_val, rub in rows:
            sr_val = (sr_val or '').strip()
            rub = (rub or '').strip()
            if sr_val and rub:
                tree[sr_val].append(rub)
        # Sin tabla GRUPOSUPERRUBROS accesible, usamos SR como nivel principal
        result = [
            {"gsr": sr_val, "superrubros": [{"sr": sr_val, "rubros": sorted(rubros)}]}
            for sr_val, rubros in sorted(tree.items())
        ]
        return result
    except Exception:
        return []

@app.post("/admin/ajuste-stock/procesar")
def ajuste_stock_procesar(data: dict, _u=Depends(get_gerente_user)):
    deposito  = (data.get('deposito') or '').strip()
    articulos = data.get('articulos') or []
    filtro    = data.get('filtro') or {}
    usuario   = str(_u.get('sub') or '').strip().upper()
    if not deposito or not articulos:
        raise HTTPException(400, "deposito y articulos requeridos")
    filtro_parts = []
    if filtro.get('gsr'):   filtro_parts.append(f"GSR:{filtro['gsr']}")
    if filtro.get('sr'):    filtro_parts.append(f"SR:{filtro['sr']}")
    if filtro.get('rubro'): filtro_parts.append(f"RUB:{filtro['rubro']}")
    filtro_desc = ', '.join(filtro_parts) or 'Todos'
    con_pendientes = sum(1 for a in articulos if a.get('pedidos_pendientes', 0) > 0)
    estado = 'ok'; detalle_errors = []
    # Resolver nombre del depósito
    dep_nombre = deposito
    try:
        _c = conn('WIN1252'); _cur = _c.cursor()
        _cur.execute('SELECT DESCRIPCION FROM "DEPOSITOS" WHERE CODIGODEPOSITO=?', (deposito,))
        _r = _cur.fetchone()
        if _r: dep_nombre = str(_r[0] or '').strip()
        _c.close()
    except Exception: pass
    # 1. Backup SQLite
    db = _admin_db()
    try:
        cur_db = db.cursor()
        cur_db.execute(
            "INSERT INTO stock_ajuste_log (usuario, deposito, deposito_nombre, filtro_desc, total_articulos, con_pendientes, estado) VALUES (?,?,?,?,?,?,?)",
            (usuario, deposito, dep_nombre, filtro_desc, len(articulos), con_pendientes, 'procesando'))
        log_id = cur_db.lastrowid
        for a in articulos:
            cur_db.execute(
                "INSERT INTO stock_ajuste_backup (log_id,codigo_articulo,codigo_particular,descripcion,stock_anterior,stock_nuevo,diferencia,remanente_anterior,pedidos_pendientes) VALUES (?,?,?,?,?,?,?,?,?)",
                (log_id, a.get('codigo_articulo',''), a.get('codigo_particular',''),
                 a.get('descripcion',''), a.get('stock_anterior',0), a.get('stock_nuevo',0),
                 a.get('diferencia',0), a.get('remanente',0), a.get('pedidos_pendientes',0)))
        db.commit()
    except Exception as e:
        db.close(); raise HTTPException(500, f"Error backup: {e}")
    # 2. Escribir Firebird
    procesados = 0
    try:
        fb = conn('WIN1252'); fb_cur = fb.cursor()
        fb_cur.execute('SELECT MAX(NUMEROMOVIMIENTO) FROM "CORRECCIONESSTOCKMANUALES"')
        next_num = int(fb_cur.fetchone()[0] or 0) + 1
        from datetime import date as _date, datetime as _dt, time as _time
        hoy_date = _date.today()          # objeto date para campos DATE de Firebird
        hoy_str  = hoy_date.isoformat()   # string solo para observaciones
        for a in articulos:
            cod_art = a.get('codigo_articulo','').strip()
            diff    = float(a.get('diferencia', 0))
            stock_nvo = float(a.get('stock_nuevo', 0))
            if diff == 0: continue
            ingreso = max(diff, 0); egreso = max(-diff, 0)
            try:
                ahora = _dt.now()
                hora_time = ahora.time().replace(microsecond=0)
                # STOCK en CORRECCIONESSTOCKMANUALES es VARCHAR (auditoría)
                stock_desc = f'AJUSTE API {hoy_str}'
                fb_cur.execute("""
                    INSERT INTO "CORRECCIONESSTOCKMANUALES"
                    (NUMEROMOVIMIENTO,FECHA,CODIGOUSUARIO,INGRESO,EGRESO,
                     CODIGOARTICULO,LOTE,STOCK,OBSERVACIONES,CODIGODEPOSITO,
                     NUMEROTRANSACCION,COSTOUNITARIO,CODIGOMOTIVOAJUSTE,
                     FECHAMODIFICACION,HORA)
                    VALUES (?,?,?,?,?,?,?,?,?,?,0,0.0,1,?,?)
                """, (next_num, hoy_date, usuario, ingreso, egreso,
                      cod_art, '000', stock_desc,
                      f'Ajuste inventario {hoy_str} dep {deposito}', deposito,
                      ahora, ahora))
                next_num += 1
                # 1. CASILLEROS: stock real por depósito (FMA_STOCK lee de aquí)
                fb_cur.execute(
                    "UPDATE \"CASILLEROS\" SET STOCKACTUAL=? WHERE CODIGOARTICULO=? AND CODIGODEPOSITO=? AND LOTE='000'",
                    (stock_nvo, cod_art, deposito))
                if fb_cur.rowcount == 0:
                    fb_cur.execute(
                        "UPDATE \"CASILLEROS\" SET STOCKACTUAL=? WHERE CODIGOARTICULO=? AND CODIGODEPOSITO=?",
                        (stock_nvo, cod_art, deposito))
                # 2. STOCK global: recalcular como SUM(CASILLEROS) para mantener consistencia
                fb_cur.execute(
                    "UPDATE \"STOCK\" SET STOCKACTUAL=(SELECT SUM(STOCKACTUAL) FROM \"CASILLEROS\" WHERE CODIGOARTICULO=?),FECHAMODIFICACION=? WHERE CODIGOARTICULO=?",
                    (cod_art, ahora, cod_art))
                procesados += 1
            except Exception as e_art:
                detalle_errors.append(f"{cod_art}: {e_art}")
        fb.commit(); fb.close()
        estado = 'ok' if not detalle_errors else 'parcial'
    except Exception as e:
        estado = 'error'; detalle_errors.append(str(e))
    db.execute("UPDATE stock_ajuste_log SET estado=?,detalle=?,total_articulos=? WHERE id=?",
               (estado, '; '.join(detalle_errors)[:500] or None, procesados, log_id))
    db.commit(); db.close()
    if estado == 'error':
        raise HTTPException(500, detalle_errors[0] if detalle_errors else 'Error')
    return {"ok": True, "procesados": procesados, "con_pendientes": con_pendientes,
            "estado": estado, "errores": detalle_errors}

@app.post("/admin/ajuste-stock/revertir/{log_id}")
def ajuste_stock_revertir(log_id: int, _u=Depends(get_gerente_user)):
    """Revierte un ajuste restaurando CASILLEROS y STOCK desde el backup de SQLite."""
    db = _admin_db()
    log = db.execute(
        "SELECT id, deposito, estado FROM stock_ajuste_log WHERE id=?", (log_id,)
    ).fetchone()
    if not log:
        db.close(); raise HTTPException(404, f"Log {log_id} no encontrado")
    deposito = log["deposito"]
    backups = db.execute(
        "SELECT codigo_articulo, stock_anterior FROM stock_ajuste_backup WHERE log_id=?", (log_id,)
    ).fetchall()
    if not backups:
        db.close(); raise HTTPException(404, "Sin backup para este ajuste")
    db.close()

    from datetime import datetime as _dt
    ahora_rev = _dt.now()
    revertidos = []; errores = []
    try:
        fb = conn('WIN1252'); cur = fb.cursor()
        for b in backups:
            cod_art = str(b["codigo_articulo"]).strip()
            stock_ant = float(b["stock_anterior"])
            try:
                cur.execute(
                    "UPDATE \"CASILLEROS\" SET STOCKACTUAL=? WHERE CODIGOARTICULO=? AND CODIGODEPOSITO=? AND LOTE='000'",
                    (stock_ant, cod_art, deposito))
                if cur.rowcount == 0:
                    cur.execute(
                        "UPDATE \"CASILLEROS\" SET STOCKACTUAL=? WHERE CODIGOARTICULO=? AND CODIGODEPOSITO=?",
                        (stock_ant, cod_art, deposito))
                cur.execute(
                    "UPDATE \"STOCK\" SET STOCKACTUAL=(SELECT SUM(STOCKACTUAL) FROM \"CASILLEROS\" WHERE CODIGOARTICULO=?),FECHAMODIFICACION=? WHERE CODIGOARTICULO=?",
                    (cod_art, ahora_rev, cod_art))
                revertidos.append(cod_art)
            except Exception as e:
                errores.append(f"{cod_art}: {e}")
        fb.commit(); fb.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    return {"ok": True, "log_id": log_id, "deposito": deposito,
            "revertidos": len(revertidos), "errores": errores}

@app.get("/admin/ajuste-stock/historial")
def ajuste_stock_historial(_u=Depends(get_gerente_user)):
    db = _admin_db()
    rows = db.execute(
        "SELECT id,fecha,usuario,deposito,deposito_nombre,filtro_desc,total_articulos,con_pendientes,estado,detalle "
        "FROM stock_ajuste_log ORDER BY fecha DESC LIMIT 50"
    ).fetchall()
    result = [dict(r) for r in rows]
    db.close()
    # Resolver nombres faltantes desde Firebird y persistirlos
    codigos_sin_nombre = list({r['deposito'] for r in result if not r.get('deposito_nombre')})
    if codigos_sin_nombre:
        try:
            placeholders = ','.join('?' * len(codigos_sin_nombre))
            _c = conn('WIN1252'); _cur = _c.cursor()
            _cur.execute(f'SELECT CODIGODEPOSITO, DESCRIPCION FROM "DEPOSITOS" WHERE CODIGODEPOSITO IN ({placeholders})', codigos_sin_nombre)
            mapa = {str(r[0]).strip(): str(r[1] or '').strip() for r in _cur.fetchall()}
            _c.close()
            # Actualizar SQLite para que próximas consultas ya traigan el nombre
            _db2 = _admin_db()
            for cod, nombre in mapa.items():
                if nombre:
                    _db2.execute("UPDATE stock_ajuste_log SET deposito_nombre=? WHERE deposito=? AND (deposito_nombre IS NULL OR deposito_nombre='')", (nombre, cod))
            _db2.commit(); _db2.close()
            # Aplicar al resultado en memoria
            for r in result:
                if not r.get('deposito_nombre'):
                    r['deposito_nombre'] = mapa.get(str(r['deposito']).strip(), r['deposito'])
        except Exception:
            pass
    return result

@app.post("/admin/audit-event")
def admin_audit_event(data: dict, request: Request, _u=Depends(get_admin_user)):
    """El frontend registra acciones con contexto semántico (navegación, creación, etc.)"""
    accion  = str(data.get('accion')  or '').strip()[:120]
    detalle = str(data.get('detalle') or '').strip()[:300]
    seccion = str(data.get('seccion') or '').strip()[:60]
    if not accion:
        raise HTTPException(400, "accion requerida")
    ip = request.client.host if request.client else ''
    _audit(_u.get('sub', '?'), accion, detalle, ip, seccion)
    return {"ok": True}

@app.get("/admin/audit-log")
def admin_audit_log(
    desde:  Optional[str] = Query(None),
    hasta:  Optional[str] = Query(None),
    offset: int = Query(0, ge=0),
    limit:  int = Query(30, ge=1, le=200),
    _u=Depends(get_gerente_user)
):
    db = _admin_db()
    conds, params = [], []
    if desde:
        conds.append("fecha >= ?"); params.append(desde + " 00:00:00")
    if hasta:
        conds.append("fecha <= ?"); params.append(hasta + " 23:59:59")
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    total = db.execute(f"SELECT COUNT(*) FROM admin_audit_log {where}", params).fetchone()[0]
    rows  = db.execute(
        f"SELECT id,fecha,usuario,seccion,accion,detalle,ip,metodo,endpoint FROM admin_audit_log {where} ORDER BY fecha DESC LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()
    db.close()
    return {"total": total, "offset": offset, "limit": limit, "rows": [dict(r) for r in rows]}

# ═══════════════════════════════════════════════════════════════════════════════
# ─── CATÁLOGOS (archivos PDF/Excel) ──────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

# ── Helpers de notificación ───────────────────────────────────────────────────
def _send_email_catalogo(destinatarios: list[str], nombre_catalogo: str, url: str, descripcion: str = ''):
    """Envía email de nuevo catálogo a lista de mails."""
    if not destinatarios or not SMTP_HOST:
        return
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f'Nuevo catálogo publicado: {nombre_catalogo}'
        msg['From']    = SMTP_FROM
        msg['To']      = ', '.join(destinatarios)
        html = f"""
        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
          <div style="background:#1a56db;padding:20px;text-align:center">
            <h2 style="color:#fff;margin:0">Microbell S.A.</h2>
          </div>
          <div style="padding:24px;background:#f9fafb;border:1px solid #e5e7eb">
            <h3 style="color:#1a1a2e">📚 Nuevo catálogo disponible</h3>
            <p style="color:#374151">Se publicó el catálogo <strong>{nombre_catalogo}</strong>.</p>
            <p style="text-align:center;margin:28px 0">
              <a href="{url}" style="background:#1a56db;color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:700">
                Ver catálogo
              </a>
            </p>
            <p style="color:#9ca3af;font-size:.82rem">Si el botón no funciona, copiá este link: {url}</p>
          </div>
        </div>"""
        msg.attach(MIMEText(html, 'html', 'utf-8'))
        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
                s.login(SMTP_USER, SMTP_PASS)
                s.sendmail(SMTP_FROM, destinatarios, msg.as_bytes())
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
                s.ehlo(); s.starttls(); s.ehlo()
                s.login(SMTP_USER, SMTP_PASS)
                s.sendmail(SMTP_FROM, destinatarios, msg.as_bytes())
    except Exception as e:
        import traceback
        print(f"[EMAIL] Error: {e}\n{traceback.format_exc()}")
        raise  # propagar para que _notificar_catalogo_bg lo capture

def _send_push_catalogo(nombre_catalogo: str, url: str, descripcion: str = '') -> int:
    """Envía push notification via OneSignal a todos los suscriptores.
    Retorna 1 si se envió OK, 0 si no configurado o error.
    """
    if not ONESIGNAL_APP_ID or not ONESIGNAL_API_KEY:
        return 0
    try:
        mensaje = descripcion if descripcion else f"Nuevo catálogo disponible: {nombre_catalogo}"
        payload = json.dumps({
            "app_id": ONESIGNAL_APP_ID,
            "included_segments": ["Total Subscriptions"],
            "headings": {"es": nombre_catalogo, "en": nombre_catalogo},
            "contents": {"es": mensaje, "en": mensaje},
            "url": url,
        }).encode()
        req = urllib.request.Request(
            "https://onesignal.com/api/v1/notifications",
            data=payload,
            headers={
                'Authorization': f'Key {ONESIGNAL_API_KEY}',
                'Content-Type': 'application/json'
            },
            method='POST'
        )
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode('utf-8', errors='replace')
        print(f"[PUSH] OK: {body}")
        return 1
    except urllib.error.HTTPError as e:
        print(f"[PUSH] HTTPError {e.code}: {e.read().decode('utf-8', errors='replace')}")
    except Exception as e:
        print(f"[PUSH] Error: {e}")
    return 0

def _send_whatsapp_catalogo(celulares: list, nombre_catalogo: str, url: str, descripcion: str = '', template_name: str = None) -> int:
    """Envía WA vía Meta Cloud API con template aprobada. Retorna cantidad enviada OK."""
    if not WA_PHONE_NUMBER_ID or not WA_ACCESS_TOKEN:
        print("[WA] WA_PHONE_NUMBER_ID o WA_ACCESS_TOKEN no configurados")
        return 0
    tpl = template_name or WA_TEMPLATE_CAT
    api_url = f"https://graph.facebook.com/v20.0/{WA_PHONE_NUMBER_ID}/messages"
    headers = {'Authorization': f'Bearer {WA_ACCESS_TOKEN}', 'Content-Type': 'application/json'}
    ok_count = 0
    for cel in celulares:
        cel = str(cel).strip().replace(' ', '').replace('-', '').replace('+', '')
        if not cel:
            continue
        payload = json.dumps({
            "messaging_product": "whatsapp",
            "to": cel,
            "type": "template",
            "template": {
                "name": tpl,
                "language": {"code": "es"},
                "components": [{
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": nombre_catalogo},
                        {"type": "text", "text": descripcion or '-'},
                        {"type": "text", "text": url}
                    ]
                }]
            }
        }).encode()
        try:
            req_http = urllib.request.Request(api_url, data=payload, headers=headers, method='POST')
            with urllib.request.urlopen(req_http, timeout=15) as resp:
                body = resp.read().decode('utf-8', errors='replace')
                print(f"[WA] OK → {cel}: {body}")
                ok_count += 1
        except urllib.error.HTTPError as e:
            err_body = ''
            try: err_body = e.read().decode('utf-8', errors='replace')
            except Exception: pass
            print(f"[WA] HTTPError {e.code} → {cel}: {err_body}")
        except Exception as e:
            print(f"[WA] Error → {cel}: {e}")
    return ok_count


def _notificar_catalogo_bg(catalogo_id: int, nombre: str, token: str, base_url: str):
    """Corre en background: lee destinatarios, envía email + push y registra resultado."""
    db = _admin_db()
    rows = db.execute("""
        SELECT vc.mail, vc.nombre, vc.celular
        FROM catalogo_vendedores cv
        JOIN vendedores_contacto vc ON vc.codigo = cv.codigo
        WHERE cv.catalogo_id = ? AND vc.activo = 1
    """, (catalogo_id,)).fetchall()
    cat_row = db.execute("SELECT descripcion FROM catalogos WHERE id=?", (catalogo_id,)).fetchone()
    descripcion = (cat_row['descripcion'] or '') if cat_row else ''
    url   = f"{base_url}/catalogo/{token}"
    mails    = [r['mail']    for r in rows if (r['mail']    or '').strip()]
    celulares = [r['celular'] for r in rows if (r['celular'] or '').strip()]
    # Enviar email
    email_ok = 0
    if mails:
        try:
            _send_email_catalogo(mails, nombre, url, descripcion)
            email_ok = len(mails)
        except Exception as e:
            print(f"[CAT] Email error: {e}")
    # Enviar push notification
    push_ok = _send_push_catalogo(nombre, url, descripcion)
    # Enviar WhatsApp
    wa_ok = _send_whatsapp_catalogo(celulares, nombre, url, descripcion)
    print(f"[CAT] Notificaciones: email={email_ok} push={push_ok} wa={wa_ok}")
    db.execute(
        "UPDATE catalogos SET email_enviado=?, email_count=?, push_enviado=?, push_count=?, wa_enviado=?, wa_count=? WHERE id=?",
        (1 if email_ok > 0 else 0, email_ok,
         push_ok, push_ok,
         1 if wa_ok > 0 else 0, wa_ok,
         catalogo_id)
    )
    db.commit()
    db.close()

# ── CRUD contactos de vendedores ──────────────────────────────────────────────
@app.get("/admin/vendedores-contacto")
def get_vendedores_contacto(_u=Depends(get_admin_user)):
    db = _admin_db()
    rows = db.execute("SELECT * FROM vendedores_contacto ORDER BY nombre").fetchall()
    db.close()
    return [dict(r) for r in rows]

@app.post("/admin/vendedores-contacto")
def upsert_vendedor_contacto(data: dict, _u=Depends(get_admin_user)):
    codigo  = str(data.get('codigo') or '').strip().upper()
    nombre  = str(data.get('nombre') or '').strip()
    if not codigo or not nombre:
        raise HTTPException(400, "codigo y nombre requeridos")
    db = _admin_db()
    db.execute("""INSERT INTO vendedores_contacto (codigo,nombre,mail,celular,activo)
                  VALUES (?,?,?,?,?)
                  ON CONFLICT(codigo) DO UPDATE SET
                    nombre=excluded.nombre, mail=excluded.mail,
                    celular=excluded.celular, activo=excluded.activo""",
               (codigo, nombre, data.get('mail',''), data.get('celular',''),
                1 if data.get('activo',1) else 0))
    db.commit(); db.close()
    return {"ok": True}

@app.delete("/admin/vendedores-contacto/{codigo}")
def delete_vendedor_contacto(codigo: str, _u=Depends(get_admin_user)):
    db = _admin_db()
    db.execute("DELETE FROM vendedores_contacto WHERE codigo=?", (codigo.upper(),))
    db.commit(); db.close()
    return {"ok": True}

# ── Upload catálogo ───────────────────────────────────────────────────────────
@app.post("/admin/catalogos/upload")
async def upload_catalogo(
    request: Request,
    background_tasks: BackgroundTasks,
    nombre:      str        = Form(...),
    descripcion: str        = Form(''),
    codigos:     str        = Form(''),      # JSON array de codigos individuales
    perfil_ids:  str        = Form(''),      # JSON array de profile_ids (resuelve codigos desde vendor_profile_assignments)
    archivo:     UploadFile = File(...),
    _u=Depends(get_admin_user)
):
    # Validar extensión
    ext = os.path.splitext(archivo.filename or '')[1].lower()
    if ext not in ('.pdf', '.xlsx', '.xls'):
        raise HTTPException(400, "Solo se permiten archivos PDF o Excel (.xlsx/.xls)")
    # Guardar archivo con nombre único
    token    = uuid.uuid4().hex
    filename = f"{token}{ext}"
    dest     = os.path.join(CATALOGOS_DIR, filename)
    with open(dest, 'wb') as f:
        shutil.copyfileobj(archivo.file, f)
    # Parsear vendedores individuales
    try:
        vend_codigos = set(str(c).strip().upper() for c in json.loads(codigos) if c) if codigos else set()
    except Exception:
        vend_codigos = set()
    # Resolver vendedores por perfil (intersectar con vendedores_contacto)
    try:
        pids = [int(x) for x in json.loads(perfil_ids) if str(x).isdigit()] if perfil_ids else []
    except Exception:
        pids = []
    if pids:
        db_tmp = _admin_db()
        ph = ','.join('?' * len(pids))
        rows_pa = db_tmp.execute(
            f"SELECT DISTINCT vpa.codigousuario FROM vendor_profile_assignments vpa "
            f"JOIN vendedores_contacto vc ON UPPER(vc.codigo)=UPPER(vpa.codigousuario) "
            f"WHERE vpa.profile_id IN ({ph}) AND vc.activo=1", pids
        ).fetchall()
        db_tmp.close()
        for r in rows_pa:
            vend_codigos.add(str(r['codigousuario']).strip().upper())
    vend_codigos = list(vend_codigos)
    # Insertar en DB
    db = _admin_db()
    cur = db.execute(
        "INSERT INTO catalogos (nombre, descripcion, filename, token, subido_por) VALUES (?,?,?,?,?)",
        (nombre.strip(), descripcion.strip(), filename, token, _u.get('sub','?'))
    )
    cat_id = cur.lastrowid
    for cod in vend_codigos:
        if cod:
            try:
                db.execute("INSERT OR IGNORE INTO catalogo_vendedores (catalogo_id,codigo) VALUES (?,?)", (cat_id, cod))
            except Exception:
                pass
    # Guardar texto de perfiles asociados para auditoría/display
    perfiles_texto = ''
    if pids:
        db_p = _admin_db()
        ph2 = ','.join('?' * len(pids))
        prows = db_p.execute(f"SELECT codigo FROM vendor_profiles WHERE id IN ({ph2})", pids).fetchall()
        db_p.close()
        perfiles_texto = ', '.join(r['codigo'] for r in prows) if prows else ''
    if perfiles_texto:
        db.execute("UPDATE catalogos SET perfiles_texto=? WHERE id=?", (perfiles_texto, cat_id))
    db.commit(); db.close()
    _audit(_u.get('sub','?'), 'Subió catálogo', f'{nombre} ({ext[1:].upper()}) → {len(vend_codigos)} vendedores', '', 'Catálogos')
    # Notificar en background
    base_url = str(request.base_url).rstrip('/')
    background_tasks.add_task(_notificar_catalogo_bg, cat_id, nombre.strip(), token, base_url)
    return {"ok": True, "id": cat_id, "token": token}

# ── Listar catálogos (admin, paginado) ────────────────────────────────────────
@app.get("/admin/catalogos")
def admin_list_catalogos(
    offset: int = Query(0, ge=0),
    limit:  int = Query(30, ge=1, le=100),
    _u=Depends(get_admin_user)
):
    db = _admin_db()
    total = db.execute("SELECT COUNT(*) FROM catalogos WHERE activo=1").fetchone()[0]
    try:
        rows = db.execute(
            "SELECT id,nombre,descripcion,filename,token,subido_por,fecha,"
            "COALESCE(email_enviado,0) AS email_enviado,"
            "COALESCE(email_count,0)   AS email_count,"
            "COALESCE(push_enviado,0)  AS push_enviado,"
            "COALESCE(push_count,0)    AS push_count,"
            "COALESCE(perfiles_texto,'') AS perfiles_texto "
            "FROM catalogos WHERE activo=1 ORDER BY fecha DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ).fetchall()
    except Exception:
        # Fallback si las columnas nuevas aún no existen (servidor no reiniciado)
        rows = db.execute(
            "SELECT id,nombre,descripcion,filename,token,subido_por,fecha,"
            "0 AS email_enviado,0 AS email_count,0 AS push_enviado,0 AS push_count,'' AS perfiles_texto "
            "FROM catalogos WHERE activo=1 ORDER BY fecha DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ).fetchall()
    result = []
    for r in rows:
        rd = dict(r)
        vends = db.execute("SELECT codigo FROM catalogo_vendedores WHERE catalogo_id=?", (r['id'],)).fetchall()
        rd['vendedores'] = [v['codigo'] for v in vends]
        result.append(rd)
    db.close()
    return {"total": total, "offset": offset, "limit": limit, "rows": result}

# ── Eliminar catálogo ─────────────────────────────────────────────────────────
@app.delete("/admin/catalogos/{cat_id}")
def delete_catalogo_file(cat_id: int, _u=Depends(get_admin_user)):
    db = _admin_db()
    row = db.execute("SELECT filename, nombre FROM catalogos WHERE id=?", (cat_id,)).fetchone()
    if not row:
        db.close(); raise HTTPException(404, "Catálogo no encontrado")
    # Borrar físico
    try:
        os.remove(os.path.join(CATALOGOS_DIR, row['filename']))
    except Exception:
        pass
    db.execute("DELETE FROM catalogos WHERE id=?", (cat_id,))
    db.commit(); db.close()
    _audit(_u.get('sub','?'), 'Eliminó catálogo', row['nombre'], '', 'Catálogos')
    return {"ok": True}

# ── Reenviar catálogo a destinatarios ────────────────────────────────────────
@app.post("/admin/catalogos/{cat_id}/reenviar")
async def reenviar_catalogo(cat_id: int, data: dict, background_tasks: BackgroundTasks,
                            request: Request, _u=Depends(get_admin_user)):
    db = _admin_db()
    cat = db.execute("SELECT id,nombre,token FROM catalogos WHERE id=? AND activo=1", (cat_id,)).fetchone()
    if not cat:
        db.close(); raise HTTPException(404, "Catálogo no encontrado")
    nombre = cat['nombre']
    token  = cat['token']
    # Resolver destinatarios igual que en upload
    perfil_ids = [int(x) for x in (data.get('perfil_ids') or []) if str(x).isdigit()]
    codigos    = list({str(c).strip().upper() for c in (data.get('codigos') or []) if c})
    if perfil_ids:
        ph = ','.join('?' * len(perfil_ids))
        rows_pa = db.execute(
            f"SELECT DISTINCT vpa.codigousuario FROM vendor_profile_assignments vpa "
            f"JOIN vendedores_contacto vc ON UPPER(vc.codigo)=UPPER(vpa.codigousuario) "
            f"WHERE vpa.profile_id IN ({ph}) AND vc.activo=1", perfil_ids
        ).fetchall()
        for r in rows_pa:
            codigos.append(str(r['codigousuario']).strip().upper())
    codigos = list(set(codigos))
    db.close()
    if not codigos:
        raise HTTPException(400, "No hay destinatarios con datos de contacto para los perfiles/vendedores seleccionados")
    # Buscar contactos y enviar
    db2 = _admin_db()
    ph2 = ','.join('?' * len(codigos))
    contactos = [dict(r) for r in db2.execute(
        f"SELECT mail, celular, nombre FROM vendedores_contacto WHERE UPPER(codigo) IN ({ph2}) AND activo=1",
        [c.upper() for c in codigos]
    ).fetchall()]
    db2.close()
    base_url = str(request.base_url).rstrip('/')
    url = f"{base_url}/catalogo/{token}"
    mails = [c['mail'] for c in contactos if (c['mail'] or '').strip()]
    background_tasks.add_task(_send_email_catalogo, mails, nombre, url)
    background_tasks.add_task(_send_push_catalogo, nombre, url)
    _audit(_u.get('sub','?'), 'Reenvió catálogo', f'{nombre} → {len(contactos)} destinatarios', '', 'Catálogos')
    return {"ok": True, "destinatarios": len(contactos), "emails": len(mails)}

# ── Servir catálogo por token (público) ───────────────────────────────────────
@app.get("/catalogo/{token}")
def serve_catalogo(token: str):
    db = _admin_db()
    row = db.execute(
        "SELECT filename, nombre FROM catalogos WHERE token=? AND activo=1", (token,)
    ).fetchone()
    db.close()
    if not row:
        raise HTTPException(404, "Catálogo no encontrado")
    filepath = os.path.join(CATALOGOS_DIR, row['filename'])
    if not os.path.exists(filepath):
        raise HTTPException(404, "Archivo no disponible")
    ext  = os.path.splitext(row['filename'])[1].lower()
    mime = 'application/pdf' if ext == '.pdf' else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    nombre_archivo = f"{row['nombre']}{ext}"
    return FileResponse(filepath, media_type=mime, headers={"Content-Disposition": f'inline; filename="{nombre_archivo}"'})

# ── Catálogos disponibles para un vendedor (frontend) ────────────────────────
@app.get("/vendedor/catalogos")
def vendedor_catalogos(current_user=Depends(get_current_user)):
    cod = current_user.get('sub', '').upper()
    db  = _admin_db()
    # Muestra: catálogos asignados al vendedor O catálogos sin restricción (sin entradas en catalogo_vendedores = "para todos")
    rows = db.execute("""
        SELECT DISTINCT c.nombre, c.token, c.descripcion, c.fecha
        FROM catalogos c
        WHERE c.activo = 1
          AND (
            EXISTS (SELECT 1 FROM catalogo_vendedores cv WHERE cv.catalogo_id = c.id AND UPPER(cv.codigo) = ?)
            OR NOT EXISTS (SELECT 1 FROM catalogo_vendedores cv2 WHERE cv2.catalogo_id = c.id)
          )
        ORDER BY c.fecha DESC LIMIT 20
    """, (cod,)).fetchall()
    db.close()
    return [dict(r) for r in rows]

@app.get("/admin/debug/catalogos")
def debug_catalogos():
    """Endpoint de diagnóstico: muestra estado real de la tabla catalogos."""
    db = _admin_db()
    cols = [r[1] for r in db.execute("PRAGMA table_info(catalogos)").fetchall()]
    cats = db.execute("SELECT id,nombre,activo,fecha,subido_por FROM catalogos ORDER BY fecha DESC LIMIT 10").fetchall()
    cv   = db.execute("SELECT catalogo_id, GROUP_CONCAT(codigo) as vends FROM catalogo_vendedores GROUP BY catalogo_id").fetchall()
    db.close()
    return {
        "columnas": cols,
        "total": len(cats),
        "catalogos": [dict(r) for r in cats],
        "asignaciones": [dict(r) for r in cv]
    }

# ─── Admin: catálogos de Firebird para selects ───────────────────────────────
@app.get("/admin/gruposuperrubros")
def admin_get_gsr(_u=Depends(get_admin_user)):
    c = conn('WIN1252'); cur = c.cursor()
    cur.execute("""
        SELECT DISTINCT g.CODIGOGRUPOSUPERRUBRO, g.DESCRIPCION
        FROM "GRUPOSUPERRUBROS" g
        JOIN "SUPERRUBROS" sr ON sr.CODIGOGRUPOSUPERRUBRO = g.CODIGOGRUPOSUPERRUBRO
        JOIN "RUBROS" r ON r.CODIGOSUPERRUBRO = sr.CODIGOSUPERRUBRO
        JOIN "ARTICULOS" a ON a.CODIGORUBRO = r.CODIGORUBRO
        WHERE a.ACTIVO = '1'
        ORDER BY g.DESCRIPCION
    """)
    rows = cur.fetchall(); c.close()
    return [{"codigo": str(r[0] or '').strip(), "descripcion": str(r[1] or '').strip()} for r in rows]

@app.get("/admin/superrubros")
def admin_get_sr(grupo: Optional[str] = None, _u=Depends(get_admin_user)):
    c = conn('WIN1252'); cur = c.cursor()
    if grupo:
        cur.execute("""
            SELECT DISTINCT sr.CODIGOSUPERRUBRO, sr.DESCRIPCION
            FROM "SUPERRUBROS" sr
            JOIN "RUBROS" r ON r.CODIGOSUPERRUBRO = sr.CODIGOSUPERRUBRO
            JOIN "ARTICULOS" a ON a.CODIGORUBRO = r.CODIGORUBRO
            WHERE sr.CODIGOGRUPOSUPERRUBRO = ? AND a.ACTIVO = '1'
            ORDER BY sr.DESCRIPCION
        """, (grupo,))
    else:
        cur.execute('SELECT CODIGOSUPERRUBRO, DESCRIPCION FROM "SUPERRUBROS" ORDER BY DESCRIPCION')
    rows = cur.fetchall(); c.close()
    return [{"codigo": str(r[0] or '').strip(), "descripcion": str(r[1] or '').strip()} for r in rows]

@app.get("/admin/rubros")
def admin_get_rubros_admin(superrubro: Optional[str] = None, _u=Depends(get_admin_user)):
    c = conn('WIN1252'); cur = c.cursor()
    if superrubro:
        cur.execute("""
            SELECT DISTINCT r.CODIGORUBRO, r.DESCRIPCION
            FROM "RUBROS" r
            JOIN "ARTICULOS" a ON a.CODIGORUBRO = r.CODIGORUBRO
            WHERE r.CODIGOSUPERRUBRO = ? AND a.ACTIVO = '1'
            ORDER BY r.DESCRIPCION
        """, (superrubro,))
    else:
        cur.execute('SELECT CODIGORUBRO, DESCRIPCION FROM "RUBROS" ORDER BY DESCRIPCION')
    rows = cur.fetchall(); c.close()
    return [{"codigo": str(r[0] or '').strip(), "descripcion": str(r[1] or '').strip()} for r in rows]

@app.get("/admin/marcas")
def admin_get_marcas_list(_u=Depends(get_admin_user)):
    c = conn('WIN1252'); cur = c.cursor()
    cur.execute("""
        SELECT DISTINCT m.CODIGOMARCA, m.DESCRIPCION
        FROM "MARCAS" m
        JOIN "ARTICULOS" a ON a.CODIGOMARCA = m.CODIGOMARCA
        WHERE a.ACTIVO = '1'
        ORDER BY m.DESCRIPCION
    """)
    rows = cur.fetchall(); c.close()
    return [{"codigo": str(r[0] or '').strip(), "descripcion": str(r[1] or '').strip()} for r in rows]

class _TestEmailReq(BaseModel):
    destinatario: str

class _TestWAReq(BaseModel):
    celular: str  # con código de país, ej: 5491112345678

@app.post("/admin/test-email")
def admin_test_email(req: _TestEmailReq, _u=Depends(get_admin_user)):
    """Envía un email de prueba y retorna diagnóstico detallado."""
    resultado = {
        "config": {
            "SMTP_HOST": SMTP_HOST or "(vacío)",
            "SMTP_PORT": SMTP_PORT,
            "SMTP_USER": SMTP_USER or "(vacío)",
            "SMTP_FROM": SMTP_FROM or "(vacío)",
            "destinatario": req.destinatario,
        },
        "ok": False,
        "error": None,
        "etapa": None,
    }
    if not SMTP_HOST:
        resultado["error"] = "SMTP_HOST no configurado en .env"
        return resultado
    if not SMTP_USER or not SMTP_PASS:
        resultado["error"] = "SMTP_USER o SMTP_PASS no configurados en .env"
        return resultado
    try:
        resultado["etapa"] = "construyendo_mensaje"
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'Test diagnóstico — API Microbell'
        msg['From']    = SMTP_FROM
        msg['To']      = req.destinatario
        msg.attach(MIMEText('<p>Email de prueba desde API Microbell. Si ves esto, SMTP funciona correctamente.</p>', 'html', 'utf-8'))
        resultado["etapa"] = "conectando_smtp"
        if SMTP_PORT == 465:
            ctx = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=10)
        else:
            ctx = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10)
        with ctx as s:
            if SMTP_PORT != 465:
                resultado["etapa"] = "starttls"
                s.ehlo(); s.starttls(); s.ehlo()
            resultado["etapa"] = "login"
            s.login(SMTP_USER, SMTP_PASS)
            resultado["etapa"] = "enviando"
            s.sendmail(SMTP_FROM, [req.destinatario], msg.as_bytes())
        resultado["ok"] = True
        resultado["etapa"] = "enviado"
    except Exception as e:
        resultado["error"] = f"{type(e).__name__}: {e}"
    return resultado

@app.post("/admin/test-whatsapp")
def admin_test_whatsapp(req: _TestWAReq, _u=Depends(get_admin_user)):
    """Envía un WhatsApp de prueba via Meta Cloud API y retorna diagnóstico detallado."""
    cel = req.celular.strip().replace(' ', '').replace('-', '').replace('+', '')
    resultado = {
        "config": {
            "WA_PHONE_NUMBER_ID": WA_PHONE_NUMBER_ID or "(vacío)",
            "WA_ACCESS_TOKEN": (WA_ACCESS_TOKEN[:8] + "***") if WA_ACCESS_TOKEN else "(vacío)",
            "celular_original": req.celular,
            "celular_enviado": cel,
        },
        "ok": False,
        "error": None,
        "response_status": None,
        "response_body": None,
    }
    if not WA_PHONE_NUMBER_ID or not WA_ACCESS_TOKEN:
        resultado["error"] = "WA_PHONE_NUMBER_ID o WA_ACCESS_TOKEN no configurados en .env"
        return resultado
    if not cel:
        resultado["error"] = "Número de celular vacío o inválido"
        return resultado
    try:
        api_url = f"https://graph.facebook.com/v20.0/{WA_PHONE_NUMBER_ID}/messages"
        payload = json.dumps({
            "messaging_product": "whatsapp",
            "to": cel,
            "type": "template",
            "template": {
                "name": "microbell_catalogo",
                "language": {"code": "es_AR"},
                "components": [{
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": "Test Catálogo"},
                        {"type": "text", "text": "Prueba de envío desde API"}
                    ]
                }]
            }
        }).encode()
        req_http = urllib.request.Request(
            api_url, data=payload,
            headers={'Authorization': f'Bearer {WA_ACCESS_TOKEN}', 'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req_http, timeout=15) as resp:
            resultado["response_status"] = resp.status
            resultado["response_body"]   = resp.read().decode('utf-8', errors='replace')
        resultado["ok"] = True
    except urllib.error.HTTPError as e:
        resultado["error"]           = f"HTTPError {e.code}: {e.reason}"
        resultado["response_status"] = e.code
        try:
            resultado["response_body"] = e.read().decode('utf-8', errors='replace')
        except Exception:
            pass
    except Exception as e:
        resultado["error"] = f"{type(e).__name__}: {e}"
    return resultado

@app.post("/admin/wa/crear-plantilla")
def admin_wa_crear_plantilla(_u=Depends(get_admin_user)):
    """Crea (o actualiza) la plantilla 'microbell_catalogo' en Meta Business.
    Requiere WA_WABA_ID y WA_ACCESS_TOKEN configurados en .env"""
    if not WA_WABA_ID or not WA_ACCESS_TOKEN:
        raise HTTPException(400, "WA_WABA_ID y WA_ACCESS_TOKEN deben estar configurados en .env")
    api_url = f"https://graph.facebook.com/v20.0/{WA_WABA_ID}/message_templates"
    payload = json.dumps({
        "name": "microbell_catalogo",
        "language": "es_AR",
        "category": "MARKETING",
        "components": [
            {
                "type": "HEADER",
                "format": "TEXT",
                "text": "📚 Nuevo catálogo Microbell"
            },
            {
                "type": "BODY",
                "text": (
                    "Se publicó el catálogo *{{1}}*.\n\n"
                    "Podés verlo desde la app Vendedores Microbell S.A.:\n{{2}}"
                )
            },
            {
                "type": "FOOTER",
                "text": "Microbell S.A. — Sistema de Vendedores"
            }
        ]
    }).encode()
    try:
        req_http = urllib.request.Request(
            api_url, data=payload,
            headers={'Authorization': f'Bearer {WA_ACCESS_TOKEN}', 'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req_http, timeout=15) as resp:
            body = resp.read().decode('utf-8', errors='replace')
        return {"ok": True, "response": json.loads(body)}
    except urllib.error.HTTPError as e:
        err = ''
        try: err = e.read().decode('utf-8', errors='replace')
        except Exception: pass
        raise HTTPException(e.code, f"Meta API error: {err}")
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/admin/cambiar-password")
def admin_cambiar_password(req: _CambiarPassReq, _u=Depends(get_admin_user)):
    try:
        c = conn('WIN1252'); cur = c.cursor()
        cur.execute(
            'SELECT CODIGOUSUARIO FROM "USUARIOS" '
            'WHERE UPPER(CODIGOUSUARIO)=? AND UPPER(PASSWORD1)=? AND ACTIVO=?',
            (req.usuario.upper(), req.password_actual.upper(), '1')
        )
        if not cur.fetchone():
            c.close(); raise HTTPException(401, "Contraseña actual incorrecta")
        cur.execute(
            'UPDATE "USUARIOS" SET PASSWORD1=? WHERE UPPER(CODIGOUSUARIO)=?',
            (req.nueva_password, req.usuario.upper())
        )
        c.commit(); c.close()
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))
    return {"ok": True}

# ─── Admin: stock con múltiples depósitos ─────────────────────────────────────
# ─── Admin: Reservas de Stock ─────────────────────────────────────────────────
def _migrate_stock_reservas():
    """Agrega columnas nuevas si la tabla ya existía sin ellas."""
    c = _admin_db()
    cols = [row[1] for row in c.execute("PRAGMA table_info(stock_reservas)").fetchall()]
    if 'deposito' not in cols:
        c.execute("ALTER TABLE stock_reservas ADD COLUMN deposito TEXT DEFAULT ''")
    if 'cantidad_utilizada' not in cols:
        c.execute("ALTER TABLE stock_reservas ADD COLUMN cantidad_utilizada REAL DEFAULT 0")
    if 'es_preventa' not in cols:
        c.execute("ALTER TABLE stock_reservas ADD COLUMN es_preventa INTEGER DEFAULT 0")
    if 'codigo_multiplazo' not in cols:
        c.execute("ALTER TABLE stock_reservas ADD COLUMN codigo_multiplazo TEXT")
    c.commit()
    c.close()

try:
    _migrate_stock_reservas()
except Exception:
    pass

def _sanitizar_buscar(buscar: str) -> str:
    """Limpia el parámetro buscar que puede venir del autocomplete con formato
    'CODE — DESCRIPTION'. Extrae solo el primer token antes del separador y
    limita la longitud para evitar el error Firebird -303 (string truncation)."""
    if not buscar:
        return buscar
    for sep in [' — ', ' — ', ' - ']:   # em dash, guión largo, guión
        if sep in buscar:
            buscar = buscar.split(sep)[0].strip()
            break
    return buscar[:80]

def _purgar_reservas_vencidas():
    """Marca como inactivas las reservas cuya fecha_hasta ya pasó."""
    c = _admin_db()
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute(
        "UPDATE stock_reservas SET activo=0 WHERE activo=1 AND fecha_hasta IS NOT NULL AND fecha_hasta < ?",
        (today,)
    )
    c.commit()
    c.close()

def _job_purgar_reservas():
    """Hilo de fondo: purga reservas vencidas al inicio y luego cada hora."""
    _purgar_reservas_vencidas()          # ejecución inmediata al arrancar
    while True:
        time.sleep(3600)                 # espera 1 hora
        try:
            _purgar_reservas_vencidas()
        except Exception:
            pass                         # nunca tumbar el hilo por error puntual

_t = threading.Thread(target=_job_purgar_reservas, daemon=True)
_t.start()

def _get_reservas_activas():
    """Purga vencidas y retorna reservas activas vigentes."""
    _purgar_reservas_vencidas()
    c = _admin_db()
    today = datetime.now().strftime('%Y-%m-%d')
    rows = c.execute(
        "SELECT * FROM stock_reservas WHERE activo=1 AND (fecha_hasta IS NULL OR fecha_hasta >= ?)",
        (today,)
    ).fetchall()
    c.close()
    return [dict(r) for r in rows]

def _apply_reservas(resultado, reservas, rem_key='remanente_total'):
    """Agrega 'reservado' y 'reservado_por_deposito' a cada item,
    y descuenta directamente los campos remanente_XXX por depósito."""
    _DEP_FIELDS = {
        '001': 'remanente_001', '002': 'remanente_002', '003': 'remanente_003',
        '005': 'remanente_005', '013': 'remanente_013', '016': 'remanente_016',
    }
    for item in resultado:
        reservado = 0.0
        reservado_por_dep: dict = {}
        rem = float(item.get(rem_key) or 0)
        for rv in reservas:
            applies = False
            if rv['tipo'] == 'articulo':
                # Comparar por codigo_articulo (interno) Y por codigo_particular
                # para cubrir el caso donde uno u otro fue almacenado en la reserva
                rv_art  = str(rv.get('codigo_articulo')  or '').strip()
                rv_part = str(rv.get('codigo_particular') or '').strip()
                it_cod  = str(item.get('codigo')           or '').strip()
                it_part = str(item.get('codigoparticular') or '').strip()
                applies = bool(
                    (rv_art  and rv_art  == it_cod)  or
                    (rv_part and rv_part == it_part) or
                    (rv_art  and rv_art  == it_part) or
                    (rv_part and rv_part == it_cod)
                )
            elif rv['tipo'] == 'grupo':
                tg = rv.get('tipo_grupo', '')
                vg = (rv.get('valor_grupo') or '').strip().upper()
                if tg == 'gruposuperrubro':
                    applies = (item.get('codigo_gruposuperrubro') or '').strip().upper() == vg
                elif tg == 'superrubro':
                    applies = (item.get('codigo_superrubro') or '').strip().upper() == vg
                elif tg == 'rubro':
                    applies = (item.get('codigo_rubro') or '').strip().upper() == vg
                elif tg == 'marca':
                    applies = (item.get('marca') or '').strip().upper() == vg
            if applies:
                cant_total     = float(rv.get('cantidad') or 0)
                cant_utilizada = float(rv.get('cantidad_utilizada') or 0)
                cant_neta      = cant_total - cant_utilizada
                # Reserva agotada: ignorar
                if cant_neta <= 0:
                    continue
                es_pct = rv.get('tipo_cantidad') == 'porcentaje'
                # ── Columna "Res." y validaciones → saldo real (cant_neta)
                amount_display = (rem * cant_neta / 100.0) if es_pct else cant_neta
                reservado += amount_display
                # ── Resta del remanente visible → cantidad TOTAL reservada
                # Los utilizados están comprometidos en presupuestos pero Firebird
                # aún no los descontó: el remanente NO debe subir al consumir la reserva.
                amount_stock = (rem * cant_total / 100.0) if es_pct else cant_total
                dep = (rv.get('deposito') or '').strip()
                if dep:
                    reservado_por_dep[dep] = reservado_por_dep.get(dep, 0.0) + amount_stock
        item['reservado'] = round(reservado)
        item['reservado_por_deposito'] = {k: round(v) for k, v in reservado_por_dep.items()}
        # Descontar directamente en los campos remanente por depósito (mínimo 0)
        for dep, amount in reservado_por_dep.items():
            field = _DEP_FIELDS.get(dep)
            if field and field in item:
                item[field] = max(0.0, float(item[field] or 0) - amount)
    return resultado

class ReservaStock(BaseModel):
    tipo: str
    codigo_articulo: Optional[str] = None
    codigo_particular: Optional[str] = None
    descripcion_articulo: Optional[str] = None
    tipo_grupo: Optional[str] = None
    valor_grupo: Optional[str] = None
    nombre_grupo: Optional[str] = None
    tipo_cantidad: str = 'unidades'
    cantidad: float = 0
    deposito: str = ''
    motivo: str = ''
    fecha_hasta: Optional[str] = None
    activo: int = 1
    es_preventa: bool = False
    codigo_multiplazo: Optional[str] = None

class ConsumoReserva(BaseModel):
    cantidad: float
    pedido_id: str = ''

@app.get("/admin/reservas-stock")
def get_reservas_stock(_u=Depends(get_admin_user)):
    c = _admin_db()
    rows = c.execute("SELECT * FROM stock_reservas ORDER BY creado_at DESC").fetchall()
    c.close()
    return [dict(r) for r in rows]

@app.get("/reservas-activas")
def get_reservas_activas_frontend(_u=Depends(get_current_user)):
    """Devuelve reservas activas y vigentes. Accesible con token de vendedor o impersonación."""
    return _get_reservas_activas()

@app.post("/admin/reservas-stock")
def create_reserva_stock(body: ReservaStock, _u=Depends(get_admin_user)):
    # Validar stock disponible en depósito si es reserva por artículo (no aplica para preventa)
    if not body.es_preventa and body.tipo == 'articulo' and body.codigo_articulo and body.deposito and body.tipo_cantidad == 'unidades':
        try:
            fb = conn('WIN1252')
            cur = fb.cursor()
            cur.execute(f'SELECT STOCKREMANENTE FROM "FMA_STOCK"(NULL, NULL, \'{body.deposito}\', 1, 1) WHERE ID_ARTICULO=?',
                        (body.codigo_articulo,))
            row = cur.fetchone()
            fb.close()
            rem_dep = float(row[0] or 0) if row else 0.0
            # Sumar reservas activas existentes para ese artículo+depósito
            c2 = _admin_db()
            today = datetime.now().strftime('%Y-%m-%d')
            rows_act = c2.execute(
                """SELECT cantidad, cantidad_utilizada FROM stock_reservas
                   WHERE activo=1 AND tipo='articulo' AND codigo_articulo=? AND deposito=?
                   AND (fecha_hasta IS NULL OR fecha_hasta >= ?)""",
                (body.codigo_articulo, body.deposito, today)
            ).fetchall()
            c2.close()
            ya_reservado = sum(max(0, float(r[0] or 0) - float(r[1] or 0)) for r in rows_act)
            disponible = rem_dep - ya_reservado
            if body.cantidad > disponible:
                raise HTTPException(400, f"Stock insuficiente en depósito {body.deposito}: remanente={round(rem_dep)}, ya reservado={round(ya_reservado)}, disponible={round(disponible)}, solicitado={round(body.cantidad)}")
        except HTTPException:
            raise
        except Exception as e:
            pass  # Si Firebird no responde, no bloquear creación

    c = _admin_db()
    c.execute(
        """INSERT INTO stock_reservas (tipo, codigo_articulo, codigo_particular, descripcion_articulo,
           tipo_grupo, valor_grupo, nombre_grupo, tipo_cantidad, cantidad, deposito, cantidad_utilizada,
           motivo, fecha_hasta, creado_por, activo, es_preventa, codigo_multiplazo)
           VALUES (?,?,?,?,?,?,?,?,?,?,0,?,?,?,1,?,?)""",
        (body.tipo, body.codigo_articulo, body.codigo_particular, body.descripcion_articulo,
         body.tipo_grupo, body.valor_grupo, body.nombre_grupo,
         body.tipo_cantidad, body.cantidad, body.deposito,
         body.motivo, body.fecha_hasta, _u['sub'], int(body.es_preventa), body.codigo_multiplazo)
    )
    c.commit()
    new_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    c.close()
    _audit(_u['sub'], 'Nueva reserva stock', f"tipo={body.tipo} dep={body.deposito} motivo={body.motivo}")
    return {"id": new_id}

@app.put("/admin/reservas-stock/{rid}")
def update_reserva_stock(rid: int, body: ReservaStock, _u=Depends(get_admin_user)):
    c = _admin_db()
    c.execute(
        """UPDATE stock_reservas SET tipo=?, codigo_articulo=?, codigo_particular=?, descripcion_articulo=?,
           tipo_grupo=?, valor_grupo=?, nombre_grupo=?, tipo_cantidad=?, cantidad=?, deposito=?,
           motivo=?, fecha_hasta=?, activo=?, es_preventa=?, codigo_multiplazo=?
           WHERE id=?""",
        (body.tipo, body.codigo_articulo, body.codigo_particular, body.descripcion_articulo,
         body.tipo_grupo, body.valor_grupo, body.nombre_grupo,
         body.tipo_cantidad, body.cantidad, body.deposito,
         body.motivo, body.fecha_hasta, body.activo, int(body.es_preventa), body.codigo_multiplazo, rid)
    )
    c.commit()
    c.close()
    _audit(_u['sub'], 'Actualizar reserva stock', f"id={rid}")
    return {"ok": True}

@app.patch("/admin/reservas-stock/{rid}")
def patch_reserva_stock(rid: int, body: dict, _u=Depends(get_admin_user)):
    """Actualización parcial: solo los campos enviados en el body."""
    allowed = {'cantidad','activo','motivo','deposito','fecha_hasta','tipo_cantidad',
               'codigo_articulo','codigo_particular','descripcion_articulo','es_preventa','codigo_multiplazo'}
    fields = {k: v for k, v in body.items() if k in allowed}
    if not fields:
        raise HTTPException(400, "No se enviaron campos válidos para actualizar")
    set_sql = ', '.join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [rid]
    c = _admin_db()
    c.execute(f"UPDATE stock_reservas SET {set_sql} WHERE id=?", vals)
    c.commit()
    c.close()
    _audit(_u['sub'], 'Patch reserva stock', f"id={rid} campos={list(fields)}")
    return {"ok": True}

@app.post("/admin/reservas-stock/{rid}/consumir")
def consumir_reserva(rid: int, body: ConsumoReserva, _u=Depends(get_current_user)):
    """Registra uso de cantidad de una reserva (desde pedido/presupuesto)."""
    c = _admin_db()
    row = c.execute("SELECT * FROM stock_reservas WHERE id=? AND activo=1", (rid,)).fetchone()
    if not row:
        c.close()
        raise HTTPException(404, "Reserva no encontrada o inactiva")
    r = dict(row)
    cant_neta = float(r['cantidad'] or 0) - float(r['cantidad_utilizada'] or 0)
    if body.cantidad > cant_neta + 0.001:
        c.close()
        raise HTTPException(400, f"Cantidad solicitada ({body.cantidad}) supera disponible en reserva ({round(cant_neta)})")
    nueva_utilizada = float(r['cantidad_utilizada'] or 0) + body.cantidad
    c.execute("UPDATE stock_reservas SET cantidad_utilizada=? WHERE id=?", (nueva_utilizada, rid))
    c.commit()
    c.close()
    _audit(_u['sub'], 'Consumo de reserva stock', f"reserva_id={rid} cantidad={body.cantidad} pedido={body.pedido_id}")
    return {"ok": True, "cantidad_utilizada": nueva_utilizada, "restante": float(r['cantidad'] or 0) - nueva_utilizada}

@app.get("/debug/reserva/{codigo}")
def debug_reserva(codigo: str):
    """Endpoint de diagnóstico — muestra reservas raw de admin.db para un código. Sin auth."""
    c = _admin_db()
    rows = c.execute(
        "SELECT id, codigo_articulo, codigo_particular, deposito, cantidad, cantidad_utilizada, es_preventa, codigo_multiplazo, activo, motivo, fecha_hasta FROM stock_reservas WHERE codigo_articulo=? OR codigo_particular=? ORDER BY id DESC",
        (codigo, codigo)
    ).fetchall()
    c.close()
    return [dict(r) for r in rows]

@app.get("/reservas-activas-articulo/{codigo}")
def get_reservas_activas_articulo(codigo: str, _u=Depends(get_current_user)):
    """Devuelve reservas activas con remanente disponible para un artículo (para usar en pedido)."""
    _purgar_reservas_vencidas()
    c = _admin_db()
    today = datetime.now().strftime('%Y-%m-%d')
    rows = c.execute(
        """SELECT * FROM stock_reservas
           WHERE activo=1 AND tipo='articulo'
           AND (codigo_articulo=? OR codigo_particular=?)
           AND (fecha_hasta IS NULL OR fecha_hasta >= ?)
           ORDER BY CASE WHEN fecha_hasta IS NULL THEN 1 ELSE 0 END, fecha_hasta ASC""",
        (codigo, codigo, today)
    ).fetchall()
    c.close()
    reservas = [dict(row) for row in rows]

    # Consultar remanente en Firebird para reservas tipo articulo con es_preventa
    art_preventa = [r for r in reservas if r.get('es_preventa') and r.get('codigo_articulo')]
    rem_map = {}
    if art_preventa:
        try:
            fb = conn('WIN1252')
            cur = fb.cursor()
            cur.execute('SELECT ID_ARTICULO, STOCKREMANENTE FROM "FMA_STOCK"(NULL, NULL, NULL, 1, 1)')
            art_set = {str(r['codigo_articulo']) for r in art_preventa}
            for row in cur.fetchall():
                if str(row[0]) in art_set:
                    rem_map[str(row[0])] = float(row[1] or 0)
            fb.close()
        except Exception:
            pass

    result = []
    for r in reservas:
        restante = float(r['cantidad'] or 0) - float(r['cantidad_utilizada'] or 0)
        if restante > 0:
            r['restante'] = round(restante)
            if r.get('es_preventa') and r.get('codigo_articulo'):
                r['remanente_firebird'] = rem_map.get(str(r['codigo_articulo']))
            else:
                r['remanente_firebird'] = None
            result.append(r)
    return result

@app.delete("/admin/reservas-stock/{rid}")
def delete_reserva_stock(rid: int, _u=Depends(get_admin_user)):
    c = _admin_db()
    c.execute("DELETE FROM stock_reservas WHERE id=?", (rid,))
    c.commit()
    c.close()
    _audit(_u['sub'], 'Eliminar reserva stock', f"id={rid}")
    return {"ok": True}

@app.get("/admin/reservas-stock/con-stock")
def get_reservas_con_stock(_u=Depends(get_admin_user)):
    """Devuelve reservas enriquecidas con remanente actual de Firebird."""
    c = _admin_db()
    rows = c.execute("SELECT * FROM stock_reservas ORDER BY creado_at DESC").fetchall()
    c.close()
    reservas = [dict(r) for r in rows]

    # Para reservas por artículo, consultar remanente en Firebird
    art_codes = [r['codigo_articulo'] for r in reservas if r['tipo'] == 'articulo' and r.get('codigo_articulo')]
    rem_map = {}
    if art_codes:
        try:
            fb = conn('WIN1252')
            cur = fb.cursor()
            cur.execute("SELECT ID_ARTICULO, STOCKREMANENTE FROM \"FMA_STOCK\"(NULL, NULL, NULL, 1, 1)")
            for row in cur.fetchall():
                if str(row[0]) in [str(c) for c in art_codes]:
                    rem_map[str(row[0])] = float(row[1] or 0)
            fb.close()
        except Exception:
            pass

    for r in reservas:
        if r['tipo'] == 'articulo' and r.get('codigo_articulo'):
            rem = rem_map.get(str(r['codigo_articulo']), 0)
            r['remanente_firebird'] = rem
            if r['tipo_cantidad'] == 'porcentaje':
                r['reservado_unidades'] = round(rem * float(r['cantidad'] or 0) / 100)
            else:
                r['reservado_unidades'] = round(float(r['cantidad'] or 0))
            r['disponible'] = max(0, round(rem - r['reservado_unidades']))
        else:
            r['remanente_firebird'] = None
            r['reservado_unidades'] = None
            r['disponible'] = None
    return reservas

@app.get("/admin/reservas-stock/exportar-pdf")
def exportar_reservas_pdf(token: Optional[str] = None, request: Request = None):
    # Acepta token por header (Bearer) o por query param (?token=...) para window.open
    _u = None
    auth_header = request.headers.get('Authorization', '') if request else ''
    raw_token = token or (auth_header.replace('Bearer ', '') if auth_header.startswith('Bearer ') else None)
    if not raw_token:
        raise HTTPException(401, "No autenticado")
    try:
        _u = jwt.decode(raw_token, JWT_SECRET, algorithms=[JWT_ALGO])
        if _u.get('role') != 'admin':
            raise HTTPException(403, "Acceso denegado")
    except JWTError:
        raise HTTPException(401, "Token inválido o expirado")
    """PDF de todas las reservas de stock activas con impacto en remanente."""
    import io
    from reportlab.lib.pagesizes import landscape
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT

    # Obtener datos enriquecidos
    resp = get_reservas_con_stock(_u=_u)
    reservas = resp if isinstance(resp, list) else []
    today = datetime.now().strftime('%Y-%m-%d')

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                            leftMargin=12*mm, rightMargin=12*mm,
                            topMargin=14*mm, bottomMargin=12*mm)

    styles = getSampleStyleSheet()
    s_title = ParagraphStyle('t', parent=styles['Heading1'], fontSize=13, textColor=colors.HexColor('#1a56db'), spaceAfter=4)
    s_sub   = ParagraphStyle('s', parent=styles['Normal'], fontSize=8, textColor=colors.HexColor('#6b7280'), spaceAfter=6)
    s_hdr   = ParagraphStyle('h', parent=styles['Normal'], fontSize=7, textColor=colors.white, alignment=TA_CENTER, fontName='Helvetica-Bold', leading=9)
    s_cell  = ParagraphStyle('c', parent=styles['Normal'], fontSize=7, leading=9)
    s_cell_c= ParagraphStyle('cc', parent=styles['Normal'], fontSize=7, leading=9, alignment=TA_CENTER)
    s_cell_r= ParagraphStyle('cr', parent=styles['Normal'], fontSize=7, leading=9, alignment=TA_RIGHT)
    s_green = ParagraphStyle('g', parent=styles['Normal'], fontSize=7, leading=9, alignment=TA_RIGHT, textColor=colors.HexColor('#059669'))
    s_red   = ParagraphStyle('r', parent=styles['Normal'], fontSize=7, leading=9, alignment=TA_RIGHT, textColor=colors.HexColor('#dc2626'))
    s_orange= ParagraphStyle('o', parent=styles['Normal'], fontSize=7, leading=9, alignment=TA_RIGHT, textColor=colors.HexColor('#c2410c'))

    fmt_n = lambda v: f"{v:,.0f}".replace(',', '.') if v is not None else '—'
    col_names = ["Tipo", "Alcance / Artículo", "Cant. Reservada", "Rem. Firebird", "Disponible", "Motivo", "Vence", "Estado"]
    table_data = [[Paragraph(h, s_hdr) for h in col_names]]

    for r in reservas:
        activo = bool(r.get('activo')) and (not r.get('fecha_hasta') or r['fecha_hasta'] >= today)
        tipo_label = 'Artículo' if r['tipo'] == 'articulo' else {
            'gruposuperrubro': 'G.S.Rubro', 'superrubro': 'S.Rubro',
            'rubro': 'Rubro', 'marca': 'Marca'
        }.get(r.get('tipo_grupo', ''), r.get('tipo_grupo', 'Grupo'))

        if r['tipo'] == 'articulo':
            alcance = f"{r.get('codigo_particular','')} — {r.get('descripcion_articulo','')}"
        else:
            alcance = f"{r.get('nombre_grupo') or r.get('valor_grupo', '')}"

        cant_str = f"{r['cantidad']:.0f}%" if r.get('tipo_cantidad') == 'porcentaje' else f"{r['cantidad']:,.0f} u."
        rem_str = fmt_n(r.get('remanente_firebird'))
        disp = r.get('disponible')
        disp_style = s_green if (disp or 0) > 0 else s_red
        disp_str = fmt_n(disp)
        vence = r.get('fecha_hasta') or 'Sin venc.'
        estado = 'Activa' if activo else 'Vencida'

        table_data.append([
            Paragraph(tipo_label, s_cell_c),
            Paragraph(alcance[:60], s_cell),
            Paragraph(cant_str, s_orange),
            Paragraph(rem_str, s_cell_r),
            Paragraph(disp_str, disp_style),
            Paragraph((r.get('motivo') or '')[:40], s_cell),
            Paragraph(vence, s_cell_c),
            Paragraph(estado, s_cell_c),
        ])

    col_widths = [22*mm, 80*mm, 28*mm, 28*mm, 28*mm, 60*mm, 24*mm, 18*mm]
    t = Table(table_data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1a56db')),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#fff7ed')]),
        ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#e5e7eb')),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
    ]))

    fecha_gen = datetime.now().strftime('%d/%m/%Y %H:%M')
    story = [
        Paragraph("📌 Reservas de Stock — Microbell S.A.", s_title),
        Paragraph(f"Generado el {fecha_gen} por {_u.get('sub','')}", s_sub),
        Spacer(1, 4*mm),
        t
    ]
    doc.build(story)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/pdf",
                             headers={"Content-Disposition": "inline; filename=reservas_stock.pdf"})

@app.get("/admin/stock")
def admin_get_stock(
    buscar: Optional[str] = None,
    depositos: Optional[str] = None,
    gruposuperrubro: Optional[str] = None,
    superrubro: Optional[str] = None,
    rubro: Optional[str] = None,
    marca: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    _u=Depends(get_admin_user)
):
    dep_lista = [d.strip() for d in (depositos or '001,003').split(',') if d.strip()]
    # Admin siempre lee directo de Firebird (sin caché)
    _fma_cache_invalidate(dep_lista)
    try:
        pagina, total_count, cambio_usd = _search_stock_cache(
            buscar=buscar, gruposuperrubro=gruposuperrubro, superrubro=superrubro,
            rubro=rubro, marca=marca, dep_lista=dep_lista, limit=limit, offset=offset
        )
        resultado = []
        for art, rem_dep, rem_total in pagina:
            factor = cambio_usd if art['codigomoneda'] == 'DOLARES' else 1.0
            precio = math.ceil(art['precio1'] * factor * 100) / 100
            item = {
                "codigo":               art['codigo'],
                "codigoparticular":     art['codigoparticular'],
                "descripcion":          art['descripcion'],
                "marca":                art['codigomarca'],
                "precio1":              precio,
                "iva":                  round(art['coeficiente'] * 21, 1),
                "unidad":               art['unidad'],
                "stock_total":          rem_total,
                "remanente_total":      rem_total,
                "rubro":                art['rubro'],
                "superrubro":           art['superrubro'],
                "gruposuperrubro":      art['gruposuperrubro'],
                "codigo_rubro":         art['codigo_rubro'],
                "codigo_superrubro":    art['codigo_superrubro'],
                "codigo_gruposuperrubro": art['codigo_gruposuperrubro'],
            }
            for dep in dep_lista:
                item[f"rem_{dep}"] = rem_dep.get(dep, 0)
            resultado.append(item)
        _apply_reservas(resultado, _get_reservas_activas())
        resp = JSONResponse(content=resultado)
        resp.headers['X-Total-Count'] = str(total_count)
        resp.headers['Access-Control-Expose-Headers'] = 'X-Total-Count'
        return resp
    except Exception as e:
        raise HTTPException(500, str(e))

# ─── Admin: exportar stock ───────────────────────────────────────────────────
def _admin_stock_data(buscar=None, depositos=None, gruposuperrubro=None,
                      superrubro=None, rubro=None, marca=None):
    """Devuelve todos los artículos activos según filtros (sin paginación) para export.
    Usa catálogo + FMA por depósito individual (igual que Rotación) porque FMA_STOCK
    no acepta CSV de depósitos — retorna resultados incorrectos/vacíos con varios deps.
    """
    dep_lista = [d.strip() for d in (depositos or '001,002,003,005,013,016').split(',') if d.strip()]

    catalog, cambio_usd = _get_catalog()
    fma_data = _fma_stock_parallel(dep_lista)  # {dep: {art_id: stock}}

    buscar_norm = _sanitizar_buscar(buscar).upper() if buscar else None

    resultado = []
    for art_id, art in catalog.items():
        # Filtros de texto y jerarquía
        if buscar_norm:
            if buscar_norm not in art['descripcion'].upper() and buscar_norm not in art['codigoparticular'].upper():
                continue
        if rubro           and art.get('codigo_rubro')            != rubro:           continue
        if superrubro      and art.get('codigo_superrubro')       != superrubro:      continue
        if gruposuperrubro and art.get('codigo_gruposuperrubro')  != gruposuperrubro: continue
        if marca           and art.get('codigomarca')             != marca:           continue

        rem_dep = {dep: fma_data.get(dep, {}).get(art_id, 0) for dep in dep_lista}
        rem_total = sum(rem_dep.values())
        if rem_total <= 0:
            continue

        factor = cambio_usd if art.get('codigomoneda', '').upper() == 'DOLARES' else 1.0
        precio = math.ceil(float(art.get('precio1', 0)) * factor * 100) / 100

        item = {
            "codigo":        art.get('codigoparticular', ''),
            "descripcion":   art.get('descripcion', ''),
            "precio1":       precio,
            "iva":           round(float(art.get('coeficiente', 0)) * 21, 1),
            "marca":         art.get('codigomarca', ''),
            "rubro":         art.get('rubro', ''),
            "superrubro":    art.get('superrubro', ''),
            "gruposuperrubro": art.get('gruposuperrubro', ''),
        }
        for dep in dep_lista:
            item[f"rem_{dep}"] = rem_dep[dep]
        item["rem_total"] = rem_total
        resultado.append(item)

    resultado.sort(key=lambda x: x['codigo'])
    return resultado, dep_lista


@app.get("/admin/stock/exportar-excel")
def admin_exportar_stock_excel(
    buscar: Optional[str] = None,
    depositos: Optional[str] = None,
    gruposuperrubro: Optional[str] = None,
    superrubro: Optional[str] = None,
    rubro: Optional[str] = None,
    marca: Optional[str] = None,
    _u=Depends(get_admin_download_auth)
):
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter

        rows, dep_lista = _admin_stock_data(buscar, depositos, gruposuperrubro, superrubro, rubro, marca)

        DEP_LABELS = {'001':'VAC-LOG','002':'MARKET PL.','003':'PACHECO','005':'OUTLET','013':'FULL ML','016':'EXPO'}

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Stock Admin"

        hdr_fill = PatternFill("solid", fgColor="1A56DB")
        hdr_font = Font(bold=True, color="FFFFFFFF", size=10)
        alt_fill = PatternFill("solid", fgColor="FFEFF6FF")
        right_al = Alignment(horizontal="right", vertical="center")
        center   = Alignment(horizontal="center", vertical="center", wrap_text=True)

        headers = ["Gr.SR", "Super Rubro", "Rubro", "Código", "Descripción"] + [DEP_LABELS.get(d, d) for d in dep_lista] + ["R.Total", "P.Unit.", "IVA%"]
        for ci, h in enumerate(headers, 1):
            cell = ws.cell(row=1, column=ci, value=h)
            cell.font = hdr_font; cell.fill = hdr_fill; cell.alignment = center
        ws.row_dimensions[1].height = 28

        for ri, row in enumerate(rows, 2):
            ws.cell(ri, 1, row.get("gruposuperrubro", ""))
            ws.cell(ri, 2, row.get("superrubro", ""))
            ws.cell(ri, 3, row.get("rubro", ""))
            ws.cell(ri, 4, row["codigo"])
            ws.cell(ri, 5, row["descripcion"])
            for di, dep in enumerate(dep_lista, 6):
                c = ws.cell(ri, di, round(row[f"rem_{dep}"], 2))
                c.alignment = right_al
                v = row[f"rem_{dep}"]
                if v < 0: c.font = Font(color="DC2626")
                elif v > 0: c.font = Font(color="059669")
            col_rt = 6 + len(dep_lista)
            ws.cell(ri, col_rt, round(row["rem_total"], 2)).alignment = right_al
            ws.cell(ri, col_rt+1, row["precio1"]).alignment = right_al
            ws.cell(ri, col_rt+2, row["iva"]).alignment = right_al
            if ri % 2 == 0:
                for ci2 in range(1, len(headers)+1):
                    ws.cell(ri, ci2).fill = alt_fill

        ws.column_dimensions['A'].width = 14
        ws.column_dimensions['B'].width = 16
        ws.column_dimensions['C'].width = 14
        ws.column_dimensions['D'].width = 12
        ws.column_dimensions['E'].width = 45
        for ci2 in range(6, len(headers)+1):
            ws.column_dimensions[get_column_letter(ci2)].width = 13

        import io
        buf = io.BytesIO()
        wb.save(buf); buf.seek(0)
        from fastapi.responses import StreamingResponse
        return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                 headers={"Content-Disposition": "attachment; filename=stock_admin.xlsx"})
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/admin/stock/exportar-pdf")
def admin_exportar_stock_pdf(
    buscar: Optional[str] = None,
    depositos: Optional[str] = None,
    gruposuperrubro: Optional[str] = None,
    superrubro: Optional[str] = None,
    rubro: Optional[str] = None,
    marca: Optional[str] = None,
    _u=Depends(get_admin_download_auth)
):
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.units import mm
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_RIGHT, TA_CENTER

        rows, dep_lista = _admin_stock_data(buscar, depositos, gruposuperrubro, superrubro, rubro, marca)
        DEP_LABELS = {'001':'VAC-LOG','002':'MARKET PL.','003':'PACHECO','005':'OUTLET','013':'FULL ML','016':'EXPO'}

        import io
        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                                leftMargin=10*mm, rightMargin=10*mm,
                                topMargin=12*mm, bottomMargin=10*mm)

        styles = getSampleStyleSheet()
        s_title = ParagraphStyle('t', parent=styles['Heading1'], fontSize=12, textColor=colors.HexColor('#1a56db'))
        s_hdr   = ParagraphStyle('h', parent=styles['Normal'], fontSize=6, textColor=colors.white, alignment=TA_CENTER, fontName='Helvetica-Bold', leading=8)
        s_cell  = ParagraphStyle('c', parent=styles['Normal'], fontSize=6, leading=8)
        s_cell_r= ParagraphStyle('cr', parent=styles['Normal'], fontSize=6, alignment=TA_RIGHT, leading=8)
        s_neg   = ParagraphStyle('neg', parent=styles['Normal'], fontSize=6, alignment=TA_RIGHT, textColor=colors.HexColor('#dc2626'), leading=8)
        s_pos   = ParagraphStyle('pos', parent=styles['Normal'], fontSize=6, alignment=TA_RIGHT, textColor=colors.HexColor('#059669'), leading=8)

        dep_headers = [DEP_LABELS.get(d, d) for d in dep_lista]
        col_names = ["Gr.SR", "Super Rubro", "Rubro", "Código", "Descripción"] + dep_headers + ["R.Total", "P.Unit.", "IVA%"]

        table_data = [[Paragraph(h, s_hdr) for h in col_names]]
        fmt_n = lambda v: f"{v:,.0f}".replace(',','.') if v else '—'
        fmt_p = lambda v: f"${v:,.2f}".replace(',','#').replace('.',',').replace('#','.')

        for row in rows:
            r_row = [
                Paragraph(row.get("gruposuperrubro", ""), s_cell),
                Paragraph(row.get("superrubro", ""), s_cell),
                Paragraph(row.get("rubro", ""), s_cell),
                Paragraph(row["codigo"], s_cell),
                Paragraph(row["descripcion"], s_cell),
            ]
            for dep in dep_lista:
                v = row[f"rem_{dep}"]
                style = s_neg if v < 0 else s_pos if v > 0 else s_cell_r
                r_row.append(Paragraph(fmt_n(v) if v != 0 else '—', style))
            r_row.append(Paragraph(fmt_n(row["rem_total"]), s_cell_r))
            r_row.append(Paragraph(fmt_p(row["precio1"]), s_cell_r))
            r_row.append(Paragraph(f"{row['iva']:.0f}%" if row['iva'] else '—', s_cell_r))
            table_data.append(r_row)

        n_dep = len(dep_lista)
        # Ancho disponible = A4 landscape - margenes
        page_w = landscape(A4)[0] - 20*mm
        # GSR/SR/Rub mas anchos, Desc acotada
        fixed_w = (22 + 28 + 20 + 14)*mm + n_dep*16*mm + (16 + 18 + 11)*mm
        desc_w = min(50*mm, max(30*mm, page_w - fixed_w))
        col_widths = [22*mm, 28*mm, 20*mm, 14*mm, desc_w] + [16*mm]*n_dep + [16*mm, 18*mm, 11*mm]
        t = Table(table_data, colWidths=col_widths, repeatRows=1)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1a56db')),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#eff6ff')]),
            ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#e5e7eb')),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('TOPPADDING', (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ]))

        story = [Paragraph("Stock por Depósito — Microbell", s_title), Spacer(1, 4*mm), t]
        doc.build(story)
        buf.seek(0)
        from fastapi.responses import StreamingResponse
        return StreamingResponse(buf, media_type="application/pdf",
                                 headers={"Content-Disposition": "inline; filename=stock_admin.pdf"})
    except Exception as e:
        raise HTTPException(500, str(e))


# ─── Admin: motor de ofertas ──────────────────────────────────────────────────
def _load_offer_relations(c, o):
    oid = o['id']
    o['product_details']    = [dict(r) for r in c.execute("SELECT * FROM offer_product_details WHERE offer_id=?", (oid,)).fetchall()]
    o['financial_details']  = [dict(r) for r in c.execute("SELECT * FROM offer_financial_details WHERE offer_id=? ORDER BY orden", (oid,)).fetchall()]
    o['conditions']         = [r[0] for r in c.execute("SELECT condicion_comercial FROM offer_conditions WHERE offer_id=?", (oid,)).fetchall()]
    o['vendors']            = [r[0] for r in c.execute("SELECT codigousuario FROM offer_vendors WHERE offer_id=?", (oid,)).fetchall()]
    o['profiles']           = [r[0] for r in c.execute("SELECT perfil_codigo FROM offer_profiles WHERE offer_id=?", (oid,)).fetchall()]
    o['category_filters']   = [dict(r) for r in c.execute("SELECT nivel, valor FROM offer_category_filters WHERE offer_id=?", (oid,)).fetchall()]
    o['combo_escalones']    = [dict(r) for r in c.execute("SELECT min_combos, descuento_pct FROM offer_combo_escalones WHERE offer_id=? ORDER BY min_combos", (oid,)).fetchall()]
    for k,d in [('deposito',''),('tipo_financiero','descuento_total'),('monto_minimo',0),('cupo',0),('usos',0)]:
        if k not in o: o[k] = d
    return o

@app.get("/admin/rotacion-filtros")
def admin_rotacion_filtros(_u=Depends(get_admin_user)):
    """Devuelve GrupoSuperRubros, SuperRubros, Rubros y Marcas para los filtros de Rotación."""
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        cur.execute('SELECT TRIM(CODIGOGRUPOSUPERRUBRO), TRIM(DESCRIPCION) FROM "GRUPOSUPERRUBROS" ORDER BY DESCRIPCION')
        grupos = [{'codigo': r[0], 'descripcion': r[1] or r[0]} for r in cur.fetchall() if r[0]]
        cur.execute('SELECT TRIM(CODIGOSUPERRUBRO), TRIM(DESCRIPCION), TRIM(CODIGOGRUPOSUPERRUBRO) FROM "SUPERRUBROS" ORDER BY DESCRIPCION')
        superrubros = [{'codigo': r[0], 'descripcion': r[1] or r[0], 'grupo': r[2] or ''} for r in cur.fetchall() if r[0]]
        cur.execute('SELECT TRIM(CODIGORUBRO), TRIM(DESCRIPCION), TRIM(CODIGOSUPERRUBRO) FROM "RUBROS" ORDER BY DESCRIPCION')
        rubros = [{'codigo': r[0], 'descripcion': r[1] or r[0], 'superrubro': r[2] or ''} for r in cur.fetchall() if r[0]]
        try:
            cur.execute('SELECT TRIM(CODIGOMARCA), TRIM(DESCRIPCION) FROM "MARCAS" ORDER BY DESCRIPCION')
            marcas = [{'codigo': r[0], 'descripcion': r[1] or r[0]} for r in cur.fetchall() if r[0]]
        except Exception:
            marcas = []
        c.close()
        return {'grupos': grupos, 'superrubros': superrubros, 'rubros': rubros, 'marcas': marcas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/analisis-rotacion")
def admin_analisis_rotacion(
    meses: int = 12,
    grupo: str = None,
    superrubro: str = None,
    rubro: str = None,
    marca: str = None,
    articulo: str = None,
    depositos: str = None,   # CSV de códigos, ej "001,003" — vacío/None = todos
    pct_operativo: float = 30,   # % gasto operativo sobre Costo
    pct_utilidad: float = 25,    # % utilidad sobre (Costo + Operativo)
    pct_meli: float = 80,        # % para Pub. ML sobre el Mayorista resultante
    pct_venta_max: float = 20,   # % máx. de (vendido en el período / stock) para entrar
    _u=Depends(get_admin_user)
):
    """
    Artículos con stockremanente > 0 (stock físico, siempre L1) y BAJA ROTACIÓN
    en el período: (cantidad vendida neta / stock remanente actual) × 100 <= pct_venta_max.
    Solo cuentan como "venta" las facturas/remitos (FA, FB, FE) — NO pedidos (NP)
    ni presupuestos, que no son ventas confirmadas. Las notas de crédito
    (FCA, FCB, FCE, FCCA, FCCB, FCCE) restan, son devoluciones.
    Considera ventas tanto de Línea 1 como de SW (comparten el mismo stock físico).
    Calcula precio lista1 en ARS, precio ML sugerido, costo, margen%, bulto sugerido.
    """
    fecha_corte = (datetime.now() - timedelta(days=meses * 30.44))

    try:
        # 1. Catálogo completo + cambio USD — misma fuente que "Stock por Depósito"
        #    (clave = CODIGOARTICULO interno, NO CODIGOPARTICULAR).
        catalog, cambio_usd = _get_catalog()

        # 2. Stock remanente — misma función _fma_stock_parallel que usa
        #    "Stock por Depósito" (probada y consistente). Pasar un CSV de
        #    varios depósitos directo a FMA_STOCK en una sola llamada daba
        #    resultados incorrectos; acá se llama una vez por depósito.
        dep_lista = [d.strip() for d in depositos.split(',') if d.strip()] if depositos and depositos.strip() else list(_FMA_ALL_DEPS)
        fma_data = _fma_stock_parallel(dep_lista)  # {dep: {art_id: stock}}
        stock_map = {}  # art_id (CODIGOARTICULO) -> stock total
        for art_id in catalog.keys():
            total = sum(fma_data.get(dep, {}).get(art_id, 0) for dep in dep_lista)
            if total > 0:
                stock_map[art_id] = total

        if not stock_map:
            return []

        # 3. Ventas reales (facturas/remitos) por artículo en el período —
        #    combinado de L1 y SW. NO se cuentan NP (pedido) ni presupuestos:
        #    no son ventas confirmadas. Facturas (FA/FB/FE) suman; notas de
        #    crédito (FCA/FCB/FCE/FCCA/FCCB/FCCE) restan (son devoluciones).
        #    También se guarda la fecha de la última venta (solo informativo).
        # FA/FB/FE: cantidad positiva; NCA/NCB/FC*: cantidad negativa (ya firmada en DB).
        # No se invierte signo: la suma directa da la cantidad neta correcta.
        TIPOS_VENTA_TODOS = ('FA', 'FB', 'FE', 'NCA', 'NCB', 'FCA', 'FCB', 'FCE', 'FCCA', 'FCCB', 'FCCE')
        vendido_map  = {}  # CODIGOARTICULO -> cantidad vendida neta en el período
        ultimo_venta = {}  # CODIGOARTICULO -> fecha de la venta más reciente (cualquier período)

        ph_tipos = ','.join(f"'{t}'" for t in TIPOS_VENTA_TODOS)

        # L1: facturas reales en CABEZACOMPROBANTES/CUERPOCOMPROBANTES.
        # CABEZAPEDIDOS/CUERPOPEDIDOS solo contiene pedidos NP, NO facturas FA.
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        try:
            cur.execute(f"""
                SELECT cu.CODIGOARTICULO, cb.FECHACOMPROBANTE, cu.CANTIDAD, cb.TIPOCOMPROBANTE
                FROM "CUERPOCOMPROBANTES" cu
                JOIN "CABEZACOMPROBANTES" cb
                  ON cb.TIPOCOMPROBANTE=cu.TIPOCOMPROBANTE
                 AND cb.NUMEROCOMPROBANTE=cu.NUMEROCOMPROBANTE
                WHERE cu.TIPOCOMPROBANTE IN ({ph_tipos}) AND cb.ANULADA=0
            """)
            for r in cur.fetchall():
                cod = (r[0] or '').strip()
                if not cod or not r[1]:
                    continue
                cant = float(r[2] or 0)
                if cant == 0:
                    continue
                dt = r[1] if isinstance(r[1], datetime) else datetime.strptime(str(r[1])[:10], '%Y-%m-%d')
                if cant > 0:
                    ultimo_venta[cod] = max(ultimo_venta.get(cod, datetime.min), dt)
                if dt >= fecha_corte:
                    vendido_map[cod] = vendido_map.get(cod, 0) + cant
        except Exception:
            pass
        c.close()

        # Ventas SW — conexión aparte (DATABASE_MLT, LATIN1). Si SW no está
        # disponible, no debe romper el análisis: se sigue solo con L1.
        try:
            c_sw = conn('LATIN1', db=DATABASE_MLT)
            cur_sw = c_sw.cursor()
            try:
                cur_sw.execute(f"""
                    SELECT cu.CODIGOARTICULO, cb.FECHACOMPROBANTE, cu.CANTIDAD, cb.TIPOCOMPROBANTE
                    FROM "CUERPOCOMPROBANTES" cu
                    JOIN "CABEZACOMPROBANTES" cb
                      ON cb.TIPOCOMPROBANTE=cu.TIPOCOMPROBANTE
                     AND cb.NUMEROCOMPROBANTE=cu.NUMEROCOMPROBANTE
                    WHERE cu.TIPOCOMPROBANTE IN ({ph_tipos}) AND cb.ANULADA=0
                """)
                for r in cur_sw.fetchall():
                    cod = (r[0] or '').strip()
                    if not cod or not r[1]:
                        continue
                    cant = float(r[2] or 0)
                    if cant == 0:
                        continue
                    dt = r[1] if isinstance(r[1], datetime) else datetime.strptime(str(r[1])[:10], '%Y-%m-%d')
                    if cant > 0:
                        ultimo_venta[cod] = max(ultimo_venta.get(cod, datetime.min), dt)
                    if dt >= fecha_corte:
                        vendido_map[cod] = vendido_map.get(cod, 0) + cant
            finally:
                c_sw.close()
        except Exception:
            pass

        # 4. Costo (PRECIOCOMPRA) — confirmado que solo aparece correcto
        #    buscando por CODIGOPARTICULAR (no por CODIGOARTICULO). Se indexa
        #    por codigoparticular y se cruza con catalog[art_id]['codigoparticular'].
        costo_por_particular = {}
        particulares = list({catalog[aid]['codigoparticular'] for aid in stock_map if aid in catalog})
        c2 = conn('WIN1252', db=DATABASE)
        cur2 = c2.cursor()
        for i in range(0, len(particulares), 400):
            chunk = particulares[i:i + 400]
            ph = ','.join(['?' for _ in chunk])
            try:
                cur2.execute(
                    f'SELECT CODIGOPARTICULAR, PRECIOCOMPRA FROM "ARTICULOS" WHERE CODIGOPARTICULAR IN ({ph})',
                    chunk
                )
                for row in cur2.fetchall():
                    cp = (row[0] or '').strip()
                    if cp:
                        costo_por_particular[cp] = float(row[1] or 0)
            except Exception:
                pass
        c2.close()

        # 5. Filtrar por jerarquía/marca/artículo usando el catálogo en memoria
        #    (igual criterio que _search_stock_cache) y construir resultado.
        resultado = []
        for art_id, rem in stock_map.items():
            art = catalog.get(art_id)
            if not art:
                continue
            if rubro and art['codigo_rubro'] != rubro:
                continue
            if superrubro and art['codigo_superrubro'] != superrubro:
                continue
            if grupo and art['codigo_gruposuperrubro'] != grupo:
                continue
            if marca and art['codigomarca'] != marca:
                continue
            if articulo:
                au = articulo.upper()
                if au not in (art['codigoparticular'] or '').upper() and au not in (art['descripcion'] or '').upper():
                    continue

            cod = art['codigoparticular']
            art_id_str = str(art_id).strip()
            vendido = vendido_map.get(art_id_str, 0) or vendido_map.get(cod, 0)
            vendido = max(vendido, 0)  # más notas de crédito que ventas no es "rotación negativa", es 0
            pct_venta = round((vendido / rem * 100), 1) if rem > 0 else 0.0
            # Baja rotación: solo entran los que vendieron <= pct_venta_max
            # de su stock actual en el período (0% = no vendieron nada).
            if pct_venta > pct_venta_max:
                continue
            ultimo_mov = ultimo_venta.get(art_id_str) or ultimo_venta.get(cod)

            precio1 = art['precio1']
            moneda  = art['codigomoneda']
            precio1_ars = precio1 * cambio_usd if moneda == 'DOLARES' else precio1

            # Costo en ARS y margen sobre lista
            costo = costo_por_particular.get(cod)
            costo_ars = None
            margen_lista = None
            if costo and costo > 0:
                costo_ars = costo * cambio_usd if moneda == 'DOLARES' else costo
                if precio1_ars > 0:
                    margen_lista = round((precio1_ars - costo_ars) / costo_ars * 100, 1)

            # Precio Oferta Mayorista (Lista1 ARS = techo, nunca se supera).
            # Mercadería sin reposición futura: el costo de reposición cargado
            # en Flexxus es la única referencia de costo disponible.
            # Cálculo EN CASCADA con los % ingresados como variables en el modal:
            #   1) Punto de equilibrio = Costo × (1 + pct_operativo/100)
            #   2) Precio Mayorista objetivo = equilibrio × (1 + pct_utilidad/100)
            #      (la utilidad se aplica sobre el resultado del paso 1, no
            #      sobre el costo original)
            #   3) Pub. ML = Mayorista × (1 + pct_meli/100)
            # margen_mayorista_pct = utilidad real lograda sobre el punto de
            # equilibrio: pct_utilidad = cumple objetivo, 0% = breakeven,
            # negativo = ni cubre el gasto operativo (tope de Lista1 de por medio).
            op_factor   = 1 + (pct_operativo / 100)
            util_factor = 1 + (pct_utilidad / 100)
            meli_factor = 1 + (pct_meli / 100)
            if costo_ars and costo_ars > 0:
                punto_equilibrio = costo_ars * op_factor
                precio_objetivo  = punto_equilibrio * util_factor
                precio_mayorista = round(min(precio1_ars, precio_objetivo), 2)
                margen_mayorista = round((precio_mayorista / punto_equilibrio - 1) * 100, 1)
                precio_bulto_unitario = round(min(precio1_ars, punto_equilibrio), 2)
            else:
                precio_mayorista  = round(precio1_ars, 2)
                margen_mayorista  = None
                precio_bulto_unitario = round(precio1_ars, 2)
                punto_equilibrio = None

            # Precio competitivo en MercadoLibre = Mayorista (oferta) × meli_factor.
            # Se basa en el precio de OFERTA, no en Lista1, para que la
            # publicación en ML refleje la liquidación real.
            precio_meli_pub = round(precio_mayorista * meli_factor, 2)
            puede_pub_meli = (precio_meli_pub > punto_equilibrio) if punto_equilibrio else True

            # Sugerencia de bulto según precio unitario en ARS (artículos de bajo valor)
            if precio1_ars < 1000:
                bulto = 100
            elif precio1_ars < 5000:
                bulto = 20
            elif precio1_ars < 15000:
                bulto = 10
            else:
                bulto = None

            # No sugerir bulto si el stock disponible no alcanza para armar al menos un pack
            if bulto and rem < bulto:
                bulto = None

            # Precios de bulto (total del pack)
            precio_bulto_meli      = round(precio_meli_pub        * bulto, 2) if bulto else None
            precio_bulto_mayorista = round(precio_bulto_unitario  * bulto, 2) if bulto else None

            ultimo_str = None
            if ultimo_mov and ultimo_mov != datetime.min:
                try:
                    ultimo_str = ultimo_mov.strftime('%Y-%m-%d')
                except Exception:
                    ultimo_str = str(ultimo_mov)[:10]

            resultado.append({
                'codigo':             cod,
                'art_id':             art_id_str,
                'codigoparticular':   art['codigoparticular'] or cod,
                'descripcion':        art['descripcion'],
                'stock':              round(rem),
                'precio1':            round(precio1, 2),
                'moneda':             moneda,
                'precio1_ars':        round(precio1_ars, 2),
                'precio_meli_pub':    precio_meli_pub,
                'precio_mayorista':   precio_mayorista,
                'margen_mayorista_pct': margen_mayorista,
                'costo':              round(costo_ars, 2) if costo_ars else None,
                'margen_lista_pct':   margen_lista,
                'puede_pub_meli':     puede_pub_meli,
                'ultimo_movimiento':  ultimo_str,
                'cantidad_vendida_periodo': round(vendido, 2),
                'pct_venta':          pct_venta,
                'bulto_sugerido':     bulto,
                'precio_bulto_meli':      precio_bulto_meli,
                'precio_bulto_mayorista': precio_bulto_mayorista,
                'alicuotaiva':        art['alicuotaiva'],
                'cambio_usd':         cambio_usd,
                'pct_operativo':      pct_operativo,
                'pct_utilidad':       pct_utilidad,
                'pct_meli':           pct_meli,
                'pct_venta_max':      pct_venta_max,
            })

        # Ordenar por valor de stock desc
        resultado.sort(key=lambda x: x['stock'] * x['precio1_ars'], reverse=True)
        return resultado

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/rotacion/exportar-excel")
def admin_rotacion_exportar_excel(
    meses: int = 12,
    grupo: str = None,
    superrubro: str = None,
    rubro: str = None,
    marca: str = None,
    articulo: str = None,
    depositos: str = None,
    pct_operativo: float = 30,
    pct_utilidad: float = 25,
    pct_meli: float = 80,
    pct_venta_max: float = 20,
    _u=Depends(get_admin_download_auth)
):
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
        import io

        rows = admin_analisis_rotacion(
            meses=meses, grupo=grupo, superrubro=superrubro, rubro=rubro, marca=marca,
            articulo=articulo, depositos=depositos, pct_operativo=pct_operativo,
            pct_utilidad=pct_utilidad, pct_meli=pct_meli, pct_venta_max=pct_venta_max,
            _u=_u
        )
        if not rows:
            raise HTTPException(404, "Sin datos de rotación para los filtros indicados")

        cambio   = rows[0].get('cambio_usd', 1)
        pct_op   = rows[0].get('pct_operativo', pct_operativo)
        pct_ut   = rows[0].get('pct_utilidad', pct_utilidad)
        pct_ml   = rows[0].get('pct_meli', pct_meli)
        pct_vtx  = rows[0].get('pct_venta_max', pct_venta_max)

        sin_ventas   = sum(1 for r in rows if not (r.get('cantidad_vendida_periodo', 0) > 0))
        total_costo  = sum(r['stock'] * (r.get('costo') or 0) for r in rows)
        sin_costo    = sum(1 for r in rows if not r.get('costo'))
        con_bulto    = sum(1 for r in rows if r.get('bulto_sugerido'))
        con_perdida  = sum(1 for r in rows if r.get('margen_mayorista_pct') is not None and r['margen_mayorista_pct'] < 0)

        linea_resumen = (
            f"{len(rows)} artículos de baja rotación (ventas ≤ {pct_vtx}% del stock)"
            f" · {sin_ventas} sin ninguna venta en el período"
            f" · Capital inmovilizado (costo): ${round(total_costo):,} ARS"
            f" · Cambio USD: ${cambio:,}"
            + (f" · {con_bulto} con venta por bulto sugerida" if con_bulto else "")
            + (f" · {con_perdida} no cubren ni el punto de equilibrio" if con_perdida else "")
            + (f" · {sin_costo} sin costo cargado (no incluidos en capital)" if sin_costo else "")
        )
        linea_leyenda = (
            f"Baja rotación = ventas NV en el período ÷ stock actual ≤ {pct_vtx}%"
            f"  |  Punto de equilibrio = Costo × (1+{pct_op}%)"
            f"  |  Mayorista = equilibrio × (1+{pct_ut}%), sin superar Lista1"
            f"  |  Pub. ML = Mayorista × (1+{pct_ml}%)"
            f"  |  Bulto = al punto de equilibrio (más agresivo)"
            f"  |  Margen May. = utilidad real ya cubierto el operativo"
        )
        fecha_gen = datetime.now().strftime('%d/%m/%Y %H:%M')

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Rotación"

        hdr_fill  = PatternFill("solid", fgColor="1A56DB")
        hdr_font  = Font(bold=True, color="FFFFFFFF", size=10)
        alt_fill  = PatternFill("solid", fgColor="EFF6FF")
        title_font = Font(bold=True, size=13, color="111827")
        sub_font   = Font(size=9, color="374151")
        leg_font   = Font(size=8, italic=True, color="6B7280")
        right_al  = Alignment(horizontal="right", vertical="center")
        center_al = Alignment(horizontal="center", vertical="center")
        left_al   = Alignment(horizontal="left", vertical="center", wrap_text=True)
        NUM_COLS  = 12

        # Fila 1: Título
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=NUM_COLS)
        c = ws.cell(1, 1, f"MICROBELL S.A. — Análisis de Rotación de Stock  ({meses} meses)  ·  Generado: {fecha_gen}")
        c.font = title_font; c.alignment = left_al
        ws.row_dimensions[1].height = 22

        # Fila 2: Resumen
        ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=NUM_COLS)
        c2 = ws.cell(2, 1, linea_resumen)
        c2.font = sub_font; c2.alignment = left_al
        ws.row_dimensions[2].height = 18

        # Fila 3: Leyenda
        ws.merge_cells(start_row=3, start_column=1, end_row=3, end_column=NUM_COLS)
        c3 = ws.cell(3, 1, linea_leyenda)
        c3.font = leg_font; c3.alignment = left_al
        ws.row_dimensions[3].height = 16

        # Fila 4: vacía
        ws.row_dimensions[4].height = 6

        # Fila 5: Encabezados de tabla
        HEADERS = ["Código", "Descripción", "Stock", "Lista1 ARS", "Mayorista",
                   "Pub. ML", "Margen May.%", "Bulto", "Pack May.", "Vendido (u.)", "Rotación %", "Último movimiento"]
        for ci, h in enumerate(HEADERS, 1):
            cell = ws.cell(5, ci, h)
            cell.font = hdr_font; cell.fill = hdr_fill; cell.alignment = center_al
        ws.row_dimensions[5].height = 20

        # Datos
        for ri, r in enumerate(rows, 6):
            is_alt = ri % 2 == 0
            fill = alt_fill if is_alt else None

            def wc(col, val, al=None, fmt=None):
                c = ws.cell(ri, col, val)
                if fill: c.fill = fill
                if al: c.alignment = al
                if fmt: c.number_format = fmt
                return c

            wc(1,  r.get('codigoparticular', ''))
            wc(2,  r.get('descripcion', ''),       left_al)
            wc(3,  round(r.get('stock', 0)),        right_al)
            wc(4,  round(r.get('precio1_ars', 0)),  right_al, '#,##0')
            wc(5,  round(r.get('precio_mayorista', 0) or 0), right_al, '#,##0')
            wc(6,  round(r.get('precio_meli_pub', 0) or 0),  right_al, '#,##0')
            mg = r.get('margen_mayorista_pct')
            c_mg = wc(7, (str(mg)+'%') if mg is not None else '—', center_al)
            if mg is not None:
                if mg < 0:   c_mg.font = Font(color="DC2626", bold=True)
                elif mg < pct_ut: c_mg.font = Font(color="D97706")
                else:         c_mg.font = Font(color="15803D", bold=True)
            bulto = r.get('bulto_sugerido')
            wc(8,  f"×{bulto}" if bulto else '—',  center_al)
            pbm = r.get('precio_bulto_mayorista')
            wc(9,  round(pbm) if pbm else '—',      right_al, '#,##0' if pbm else None)
            wc(10, round(r.get('cantidad_vendida_periodo', 0) or 0), right_al)
            wc(11, str(r.get('pct_venta', 0) or 0)+'%', center_al)
            wc(12, r.get('ultimo_movimiento') or '—')

        # Anchos de columna
        ws.column_dimensions['A'].width = 11
        ws.column_dimensions['B'].width = 42
        ws.column_dimensions['C'].width = 9
        ws.column_dimensions['D'].width = 13
        ws.column_dimensions['E'].width = 13
        ws.column_dimensions['F'].width = 13
        ws.column_dimensions['G'].width = 13
        ws.column_dimensions['H'].width = 9
        ws.column_dimensions['I'].width = 13
        ws.column_dimensions['J'].width = 13
        ws.column_dimensions['K'].width = 12
        ws.column_dimensions['L'].width = 16

        buf = io.BytesIO()
        wb.save(buf); buf.seek(0)
        from fastapi.responses import StreamingResponse
        fname = f"rotacion_{meses}m_{datetime.now().strftime('%Y%m%d')}.xlsx"
        return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                 headers={"Content-Disposition": f"attachment; filename={fname}"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/admin/rotacion/exportar-pdf")
def admin_rotacion_exportar_pdf(
    meses: int = 12,
    grupo: str = None,
    superrubro: str = None,
    rubro: str = None,
    marca: str = None,
    articulo: str = None,
    depositos: str = None,
    pct_operativo: float = 30,
    pct_utilidad: float = 25,
    pct_meli: float = 80,
    pct_venta_max: float = 20,
    _u=Depends(get_admin_download_auth)
):
    try:
        import io
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.units import mm
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer

        rows = admin_analisis_rotacion(
            meses=meses, grupo=grupo, superrubro=superrubro, rubro=rubro, marca=marca,
            articulo=articulo, depositos=depositos, pct_operativo=pct_operativo,
            pct_utilidad=pct_utilidad, pct_meli=pct_meli, pct_venta_max=pct_venta_max,
            _u=_u
        )
        if not rows:
            raise HTTPException(404, "Sin datos de rotación para los filtros indicados")

        cambio   = rows[0].get('cambio_usd', 1)
        pct_op   = rows[0].get('pct_operativo', pct_operativo)
        pct_ut   = rows[0].get('pct_utilidad', pct_utilidad)
        pct_ml   = rows[0].get('pct_meli', pct_meli)
        pct_vtx  = rows[0].get('pct_venta_max', pct_venta_max)

        sin_ventas   = sum(1 for r in rows if not (r.get('cantidad_vendida_periodo', 0) > 0))
        total_costo  = sum(r['stock'] * (r.get('costo') or 0) for r in rows)
        sin_costo    = sum(1 for r in rows if not r.get('costo'))
        con_bulto    = sum(1 for r in rows if r.get('bulto_sugerido'))
        con_perdida  = sum(1 for r in rows if r.get('margen_mayorista_pct') is not None and r['margen_mayorista_pct'] < 0)

        linea_resumen = (
            f"<b>{len(rows)}</b> artículos de baja rotación (ventas ≤ {pct_vtx}% del stock)"
            f" &nbsp;·&nbsp; {sin_ventas} sin ninguna venta en el período"
            f" &nbsp;·&nbsp; Capital inmovilizado (costo): <b>${round(total_costo):,} ARS</b>"
            f" &nbsp;·&nbsp; Cambio USD: ${cambio:,}"
            + (f" &nbsp;·&nbsp; <font color='#7c3aed'><b>{con_bulto} con venta por bulto sugerida</b></font>" if con_bulto else "")
            + (f" &nbsp;·&nbsp; <font color='#dc2626'><b>{con_perdida} no cubren el punto de equilibrio</b></font>" if con_perdida else "")
            + (f" &nbsp;·&nbsp; <font color='#d97706'>{sin_costo} sin costo cargado</font>" if sin_costo else "")
        )
        linea_leyenda = (
            f"Baja rotación = ventas NV ÷ stock actual ≤ {pct_vtx}%  |  "
            f"Punto de equilibrio = Costo × (1+{pct_op}%)  |  "
            f"Mayorista = equilibrio × (1+{pct_ut}%), sin superar Lista1  |  "
            f"Pub. ML = Mayorista × (1+{pct_ml}%)  |  "
            f"Bulto = al punto de equilibrio (más agresivo)  |  "
            f"Margen May. = utilidad real ya cubierto el operativo"
        )
        fecha_gen = datetime.now().strftime('%d/%m/%Y %H:%M')

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                                leftMargin=10*mm, rightMargin=10*mm,
                                topMargin=12*mm, bottomMargin=12*mm)

        styles = getSampleStyleSheet()
        st_title = ParagraphStyle('rot_title', fontName='Helvetica-Bold', fontSize=13,
                                  textColor=colors.HexColor('#111827'), spaceAfter=4)
        st_sub   = ParagraphStyle('rot_sub',   fontName='Helvetica', fontSize=9,
                                  textColor=colors.HexColor('#374151'), spaceAfter=2)
        st_leg   = ParagraphStyle('rot_leg',   fontName='Helvetica-Oblique', fontSize=7.5,
                                  textColor=colors.HexColor('#6b7280'), spaceAfter=8)
        st_desc  = ParagraphStyle('rot_desc',  fontName='Helvetica', fontSize=7.5,
                                  textColor=colors.HexColor('#111827'), leading=9,
                                  wordWrap='LTR')

        story = []
        story.append(Paragraph(f"MICROBELL S.A. — Análisis de Rotación de Stock &nbsp;({meses} meses) &nbsp;·&nbsp; Generado: {fecha_gen}", st_title))
        story.append(Paragraph(linea_resumen, st_sub))
        story.append(Paragraph(linea_leyenda, st_leg))

        # Tabla
        BLUE    = colors.HexColor('#1A56DB')
        RED     = colors.HexColor('#DC2626')
        ORANGE  = colors.HexColor('#D97706')
        GREEN   = colors.HexColor('#15803D')
        PURPLE  = colors.HexColor('#7C3AED')
        ALTBG   = colors.HexColor('#EFF6FF')

        def fmt_ars(v): return f"${round(v):,}".replace(',', '.') if v else '—'
        def fmt_pct(v): return f"{v}%" if v is not None else '—'

        col_headers = ["Código", "Descripción", "Stock", "Lista1", "Mayorista",
                       "Pub. ML", "Margen\nMay.%", "Bulto", "Pack\nMay.", "Vendido\n(u.)", "Rot.%", "Último\nmov."]
        col_widths  = [22*mm, 68*mm, 16*mm, 22*mm, 22*mm, 22*mm, 18*mm, 14*mm, 22*mm, 18*mm, 15*mm, 22*mm]

        tbl_data = [col_headers]
        row_colors = []  # (row_idx, color) for margen color
        for ri, r in enumerate(rows, 1):
            mg   = r.get('margen_mayorista_pct')
            bulto = r.get('bulto_sugerido')
            pbm   = r.get('precio_bulto_mayorista')
            if mg is not None:
                if mg < 0:      row_colors.append((ri, RED, 6))
                elif mg < pct_ut: row_colors.append((ri, ORANGE, 6))
                else:             row_colors.append((ri, GREEN, 6))
            tbl_data.append([
                r.get('codigoparticular', ''),
                Paragraph(r.get('descripcion', ''), st_desc),
                str(round(r.get('stock', 0))),
                fmt_ars(r.get('precio1_ars')),
                fmt_ars(r.get('precio_mayorista')),
                fmt_ars(r.get('precio_meli_pub')),
                fmt_pct(mg),
                f"×{bulto}" if bulto else '—',
                fmt_ars(pbm),
                str(round(r.get('cantidad_vendida_periodo', 0) or 0)),
                fmt_pct(r.get('pct_venta')),
                r.get('ultimo_movimiento') or '—',
            ])

        base_style = [
            ('BACKGROUND',  (0,0), (-1,0), BLUE),
            ('TEXTCOLOR',   (0,0), (-1,0), colors.white),
            ('FONTNAME',    (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',    (0,0), (-1,0), 8),
            ('ALIGN',       (0,0), (-1,0), 'CENTER'),
            ('VALIGN',      (0,0), (-1,-1), 'MIDDLE'),
            ('FONTNAME',    (0,1), (-1,-1), 'Helvetica'),
            ('FONTSIZE',    (0,1), (-1,-1), 7.5),
            ('ROWBACKGROUND', (0,1), (-1,-1), [colors.white, ALTBG]),
            ('GRID',        (0,0), (-1,-1), 0.3, colors.HexColor('#D1D5DB')),
            ('ALIGN',       (2,1), (2,-1), 'RIGHT'),
            ('ALIGN',       (3,1), (5,-1), 'RIGHT'),
            ('ALIGN',       (8,1), (9,-1), 'RIGHT'),
            ('ALIGN',       (6,1), (7,-1), 'CENTER'),
            ('ALIGN',       (10,1), (10,-1), 'CENTER'),
        ]
        for (ri, clr, ci) in row_colors:
            base_style.append(('TEXTCOLOR', (ci, ri), (ci, ri), clr))
            base_style.append(('FONTNAME',  (ci, ri), (ci, ri), 'Helvetica-Bold'))

        tbl = Table(tbl_data, colWidths=col_widths, repeatRows=1)
        tbl.setStyle(TableStyle(base_style))
        story.append(tbl)

        doc.build(story)
        buf.seek(0)
        from fastapi.responses import StreamingResponse
        fname = f"rotacion_{meses}m_{datetime.now().strftime('%Y%m%d')}.pdf"
        return StreamingResponse(buf, media_type="application/pdf",
                                 headers={"Content-Disposition": f"inline; filename={fname}"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/admin/rotacion-detalle-ventas")
def admin_rotacion_detalle_ventas(art_id: str, meses: int = 12, _u=Depends(get_admin_user)):
    """Detalle de comprobantes de venta (FA/FB/FE) y notas de crédito
    (FCA/FCB/FCE/FCCA/FCCB/FCCE, con signo negativo) de un artículo puntual
    en el período, para el modal que se abre al hacer clic en la columna de
    ventas de Rotación.

    art_id = CODIGOARTICULO interno (NO CODIGOPARTICULAR) — el mismo valor
    usado para construir vendido_map en /admin/analisis-rotacion. Se busca
    directo por ese campo en CUERPOPEDIDOS (L1) y CUERPOCOMPROBANTES (SW),
    sin pasar por un lookup adicional en ARTICULOS por CODIGOPARTICULAR,
    porque ese lookup puede resolver a un CODIGOARTICULO distinto cuando
    hay códigos particulares duplicados — causaba que el modal apareciera
    vacío aunque el análisis principal sí contara ventas."""
    # FA/FB/FE: cantidad positiva; NCA/NCB/FC*: cantidad negativa (ya firmada en DB).
    TIPOS_VENTA_TODOS = ('FA', 'FB', 'FE', 'NCA', 'NCB', 'FCA', 'FCB', 'FCE', 'FCCA', 'FCCB', 'FCCE')
    ph_tipos = ','.join(f"'{t}'" for t in TIPOS_VENTA_TODOS)
    fecha_corte = (datetime.now() - timedelta(days=meses * 30.44))
    detalle = []

    def _calc(cant_raw, precio_u, dto, iva_pct):
        cant = float(cant_raw or 0)  # ya firmada (negativa para NC*)
        precio_u = float(precio_u or 0)
        dto = float(dto or 0)
        iva_pct = float(iva_pct) if iva_pct is not None else 21.0
        importe_neto = round(cant * precio_u * (1 - dto / 100), 2)
        iva_monto = round(importe_neto * iva_pct / 100, 2)
        importe_con_iva = round(importe_neto + iva_monto, 2)
        return cant, importe_neto, iva_pct, iva_monto, importe_con_iva

    # L1: CABEZACOMPROBANTES/CUERPOCOMPROBANTES (DATABASE, WIN1252).
    # CABEZAPEDIDOS solo contiene pedidos NP, no facturas FA.
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        cur.execute(f"""
            SELECT cb.FECHACOMPROBANTE, cb.TIPOCOMPROBANTE, cb.NUMEROCOMPROBANTE,
                   cu.CANTIDAD, cu.PRECIOUNITARIO, cu.DESCUENTO, cu.PORCENTAJEIVA,
                   cb.RAZONSOCIAL
            FROM "CUERPOCOMPROBANTES" cu
            JOIN "CABEZACOMPROBANTES" cb
              ON cb.TIPOCOMPROBANTE=cu.TIPOCOMPROBANTE
             AND cb.NUMEROCOMPROBANTE=cu.NUMEROCOMPROBANTE
            WHERE cu.CODIGOARTICULO=? AND cu.TIPOCOMPROBANTE IN ({ph_tipos}) AND cb.ANULADA=0
              AND cb.FECHACOMPROBANTE >= ?
            ORDER BY cb.FECHACOMPROBANTE DESC
        """, (art_id, fecha_corte))
        for r in cur.fetchall():
            tipo = (r[1] or '').strip().upper()
            cant, importe, iva_pct, iva_monto, importe_con_iva = _calc(r[3], r[4], r[5], r[6])
            detalle.append({
                'origen': 'L1',
                'fecha': r[0].strftime('%Y-%m-%d') if hasattr(r[0], 'strftime') else str(r[0])[:10],
                'tipo': tipo,
                'numero': (r[2] or '').strip() if isinstance(r[2], str) else r[2],
                'razonsocial': (r[7] or '').strip() if isinstance(r[7], str) else r[7],
                'cantidad': cant,
                'importe': importe,
                'iva_pct': iva_pct,
                'iva_monto': iva_monto,
                'importe_con_iva': importe_con_iva,
            })
        c.close()
    except Exception:
        pass

    try:
        c_sw = conn('LATIN1', db=DATABASE_MLT)
        cur_sw = c_sw.cursor()
        cur_sw.execute(f"""
            SELECT cb.FECHACOMPROBANTE, cb.TIPOCOMPROBANTE, cb.NUMEROCOMPROBANTE,
                   cu.CANTIDAD, cu.PRECIOUNITARIO, cu.DESCUENTO, cu.PORCENTAJEIVA,
                   cb.RAZONSOCIAL
            FROM "CUERPOCOMPROBANTES" cu
            JOIN "CABEZACOMPROBANTES" cb
              ON cb.TIPOCOMPROBANTE=cu.TIPOCOMPROBANTE
             AND cb.NUMEROCOMPROBANTE=cu.NUMEROCOMPROBANTE
            WHERE cu.CODIGOARTICULO=? AND cu.TIPOCOMPROBANTE IN ({ph_tipos}) AND cb.ANULADA=0
              AND cb.FECHACOMPROBANTE >= ?
            ORDER BY cb.FECHACOMPROBANTE DESC
        """, (art_id, fecha_corte))
        for r in cur_sw.fetchall():
            tipo = (r[1] or '').strip().upper()
            cant, importe, iva_pct, iva_monto, importe_con_iva = _calc(r[3], r[4], r[5], r[6])
            detalle.append({
                'origen': 'SW',
                'fecha': r[0].strftime('%Y-%m-%d') if hasattr(r[0], 'strftime') else str(r[0])[:10],
                'tipo': tipo,
                'numero': (r[2] or '').strip() if isinstance(r[2], str) else r[2],
                'razonsocial': (r[7] or '').strip() if isinstance(r[7], str) else r[7],
                'cantidad': cant,
                'importe': importe,
                'iva_pct': iva_pct,
                'iva_monto': iva_monto,
                'importe_con_iva': importe_con_iva,
            })
        c_sw.close()
    except Exception:
        pass

    detalle.sort(key=lambda d: d['fecha'], reverse=True)
    return detalle


@app.get("/debug/rotacion-fca-articulo")
def debug_rotacion_fca_articulo(art_id: str = None, codigoparticular: str = None, _u=Depends(get_admin_user)):
    """DEBUG TEMPORAL: trae TODAS las filas de CUERPOPEDIDOS/CABEZAPEDIDOS (L1)
    y CUERPOCOMPROBANTES/CABEZACOMPROBANTES (SW) para un CODIGOARTICULO dado,
    de CUALQUIER tipo de comprobante, fecha y estado ANULADA — sin ningún
    filtro — para detectar si un comprobante puntual (ej. una FCA que el
    usuario sabe que existe en Flexxus) está en la base, y por qué no pasa
    los filtros normales de /admin/rotacion-detalle-ventas (fecha, ANULADA,
    o el tipo de comprobante).

    Acepta art_id (CODIGOARTICULO, recomendado) o, si no se tiene a mano,
    codigoparticular (resuelve a CODIGOARTICULO vía ARTICULOS — solo para
    este debug, no usar este camino en lógica de producción)."""
    if not art_id and codigoparticular:
        try:
            c0 = conn('WIN1252', db=DATABASE)
            cur0 = c0.cursor()
            cur0.execute('SELECT CODIGOARTICULO FROM "ARTICULOS" WHERE CODIGOPARTICULAR=?', (codigoparticular,))
            row0 = cur0.fetchone()
            art_id = str(row0[0]).strip() if row0 else None
            c0.close()
        except Exception:
            pass
    if not art_id:
        raise HTTPException(status_code=400, detail="Debes pasar art_id o codigoparticular")
    out = {'art_id_usado': art_id, 'l1': [], 'sw': [], 'error_l1': None, 'error_sw': None}
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        cur.execute("""
            SELECT cb.TIPOCOMPROBANTE, cb.NUMEROCOMPROBANTE, cb.FECHACOMPROBANTE,
                   cb.ANULADA, cp.CANTIDAD, cp.PRECIOUNITARIO, cb.RAZONSOCIAL
            FROM "CUERPOPEDIDOS" cp
            JOIN "CABEZAPEDIDOS" cb
              ON cb.TIPOCOMPROBANTE=cp.TIPOCOMPROBANTE
             AND cb.NUMEROCOMPROBANTE=cp.NUMEROCOMPROBANTE
            WHERE cp.CODIGOARTICULO=?
            ORDER BY cb.FECHACOMPROBANTE DESC
        """, (art_id,))
        for r in cur.fetchall():
            out['l1'].append({
                'tipo': (r[0] or '').strip() if isinstance(r[0], str) else r[0],
                'numero': (r[1] or '').strip() if isinstance(r[1], str) else r[1],
                'fecha': r[2].strftime('%Y-%m-%d') if hasattr(r[2], 'strftime') else str(r[2]),
                'anulada': r[3],
                'cantidad': r[4],
                'precio_u': r[5],
                'razonsocial': (r[6] or '').strip() if isinstance(r[6], str) else r[6],
            })
        c.close()
    except Exception as e:
        out['error_l1'] = str(e)

    # Test de hipótesis: ¿existe también CABEZACOMPROBANTES/CUERPOCOMPROBANTES
    # DENTRO de la base de Línea1 (no la de SW), con las facturas FA reales que
    # no aparecen en CABEZAPEDIDOS (que parece guardar solo NP/pedidos)?
    out['l1_comprobantes'] = []
    out['error_l1_comprobantes'] = None
    try:
        c1c = conn('WIN1252', db=DATABASE)
        cur1c = c1c.cursor()
        cur1c.execute("""
            SELECT cb.TIPOCOMPROBANTE, cb.NUMEROCOMPROBANTE, cb.FECHACOMPROBANTE,
                   cb.ANULADA, cu.CANTIDAD, cu.PRECIOUNITARIO, cb.RAZONSOCIAL
            FROM "CUERPOCOMPROBANTES" cu
            JOIN "CABEZACOMPROBANTES" cb
              ON cb.TIPOCOMPROBANTE=cu.TIPOCOMPROBANTE
             AND cb.NUMEROCOMPROBANTE=cu.NUMEROCOMPROBANTE
            WHERE cu.CODIGOARTICULO=?
            ORDER BY cb.FECHACOMPROBANTE DESC
        """, (art_id,))
        for r in cur1c.fetchall():
            out['l1_comprobantes'].append({
                'tipo': (r[0] or '').strip() if isinstance(r[0], str) else r[0],
                'numero': (r[1] or '').strip() if isinstance(r[1], str) else r[1],
                'fecha': r[2].strftime('%Y-%m-%d') if hasattr(r[2], 'strftime') else str(r[2]),
                'anulada': r[3],
                'cantidad': r[4],
                'precio_u': r[5],
                'razonsocial': (r[6] or '').strip() if isinstance(r[6], str) else r[6],
            })
        c1c.close()
    except Exception as e:
        out['error_l1_comprobantes'] = str(e)

    try:
        c_sw = conn('LATIN1', db=DATABASE_MLT)
        cur_sw = c_sw.cursor()
        cur_sw.execute("""
            SELECT cb.TIPOCOMPROBANTE, cb.NUMEROCOMPROBANTE, cb.FECHACOMPROBANTE,
                   cb.ANULADA, cu.CANTIDAD, cu.PRECIOUNITARIO, cb.RAZONSOCIAL
            FROM "CUERPOCOMPROBANTES" cu
            JOIN "CABEZACOMPROBANTES" cb
              ON cb.TIPOCOMPROBANTE=cu.TIPOCOMPROBANTE
             AND cb.NUMEROCOMPROBANTE=cu.NUMEROCOMPROBANTE
            WHERE cu.CODIGOARTICULO=?
            ORDER BY cb.FECHACOMPROBANTE DESC
        """, (art_id,))
        for r in cur_sw.fetchall():
            out['sw'].append({
                'tipo': (r[0] or '').strip() if isinstance(r[0], str) else r[0],
                'numero': (r[1] or '').strip() if isinstance(r[1], str) else r[1],
                'fecha': r[2].strftime('%Y-%m-%d') if hasattr(r[2], 'strftime') else str(r[2]),
                'anulada': r[3],
                'cantidad': r[4],
                'precio_u': r[5],
                'razonsocial': (r[6] or '').strip() if isinstance(r[6], str) else r[6],
            })
        c_sw.close()
    except Exception as e:
        out['error_sw'] = str(e)

    return out


@app.get("/debug/fma-stock-articulo")
def debug_fma_stock_articulo(codigo: str = '03375', _u=Depends(get_admin_user)):
    """DEBUG TEMPORAL: llama FMA_STOCK depósito por depósito (igual que el
    resto del proyecto) y muestra el STOCKREMANENTE crudo de un artículo
    puntual en cada uno, para comparar contra lo que muestra Stock por
    Depósito y aislar dónde está la discrepancia. Una sola conexión
    reutilizada (la versión anterior abría 24 conexiones y daba timeout)."""
    deps = ['001', '002', '003', '005', '006', '007', '008', '010', '011', '013', '016', '017']
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        cur.execute('SELECT CODIGOARTICULO, CODIGOPARTICULAR FROM "ARTICULOS" WHERE CODIGOPARTICULAR=?', (codigo,))
        row0 = cur.fetchone()
        codigo_articulo_interno = str(row0[0]).strip() if row0 and row0[0] is not None else None

        por_codigoparticular = {}
        por_codigoarticulo = {}
        for dep in deps:
            try:
                cur.execute(
                    f'SELECT ID_ARTICULO, STOCKREMANENTE FROM "FMA_STOCK"(NULL, NULL, \'{dep}\', 1, 1) '
                    f'WHERE ID_ARTICULO=?', (codigo,)
                )
                row = cur.fetchone()
                por_codigoparticular[dep] = float(row[1]) if row else None
            except Exception as e_dep:
                por_codigoparticular[dep] = f"ERROR: {e_dep}"
            if codigo_articulo_interno and codigo_articulo_interno != codigo:
                try:
                    cur.execute(
                        f'SELECT ID_ARTICULO, STOCKREMANENTE FROM "FMA_STOCK"(NULL, NULL, \'{dep}\', 1, 1) '
                        f'WHERE ID_ARTICULO=?', (codigo_articulo_interno,)
                    )
                    row_i = cur.fetchone()
                    por_codigoarticulo[dep] = float(row_i[1]) if row_i else None
                except Exception as e_dep2:
                    por_codigoarticulo[dep] = f"ERROR: {e_dep2}"
        c.close()
        suma_part = sum(v for v in por_codigoparticular.values() if isinstance(v, (int, float)))
        suma_art  = sum(v for v in por_codigoarticulo.values() if isinstance(v, (int, float)))
        return {
            "codigo_buscado_codigoparticular": codigo,
            "codigoarticulo_interno_real": codigo_articulo_interno,
            "stock_por_deposito_usando_CODIGOPARTICULAR": por_codigoparticular,
            "suma_usando_CODIGOPARTICULAR": suma_part,
            "stock_por_deposito_usando_CODIGOARTICULO_interno": por_codigoarticulo,
            "suma_usando_CODIGOARTICULO_interno": suma_art,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug/articulos-columnas-costo")
def debug_articulos_columnas_costo(codigo: str = '03748', _u=Depends(get_admin_user)):
    """DEBUG TEMPORAL: lista todas las columnas de ARTICULOS y sus valores para
    un código puntual, para identificar cuál corresponde al Costo Reposición
    que se ve en Flexxus (la columna COSTO no lo trae)."""
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        cur.execute('SELECT * FROM "ARTICULOS" WHERE CODIGOARTICULO=?', (codigo,))
        row = cur.fetchone()
        if not row:
            c.close()
            return {"error": f"artículo {codigo} no encontrado"}
        cols = [d[0] for d in cur.description]
        c.close()
        # Solo columnas que contengan COSTO o PRECIO en el nombre, para no saturar
        relevantes = {cols[i]: row[i] for i in range(len(cols)) if 'COSTO' in cols[i].upper() or 'PRECIO' in cols[i].upper()}
        return {"codigo": codigo, "columnas_costo_precio": relevantes, "todas_las_columnas": cols}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug/tablas-costo")
def debug_tablas_costo(_u=Depends(get_admin_user)):
    """DEBUG TEMPORAL: lista tablas del esquema cuyo nombre sugiere que guardan
    el costo de reposición de artículos (no está en ARTICULOS directamente)."""
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        cur.execute(
            "SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS "
            "WHERE RDB$SYSTEM_FLAG = 0 AND ("
            "UPPER(RDB$RELATION_NAME) CONTAINING 'COSTO' OR "
            "UPPER(RDB$RELATION_NAME) CONTAINING 'REPOSICION' OR "
            "UPPER(RDB$RELATION_NAME) CONTAINING 'ARTICULOPRECIO' OR "
            "UPPER(RDB$RELATION_NAME) CONTAINING 'PRECIOCOSTO'"
            ") ORDER BY 1"
        )
        tablas = [r[0] for r in cur.fetchall()]
        c.close()
        return {"tablas_candidatas": tablas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug/cambiosdecostos")
def debug_cambiosdecostos(codigo: str = '03748', _u=Depends(get_admin_user)):
    """DEBUG TEMPORAL: inspecciona CAMBIOSDECOSTOS, candidata a guardar el
    Costo Reposición actual de un artículo (no está en ARTICULOS)."""
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        cur.execute('SELECT FIRST 1 * FROM "CAMBIOSDECOSTOS"')
        row0 = cur.fetchone()
        cols = [d[0] for d in cur.description] if row0 else []
        # Buscar columna de artículo entre las disponibles
        col_art = next((cc for cc in cols if 'ARTICULO' in cc.upper()), None)
        registros = []
        if col_art:
            cur.execute(
                f'SELECT FIRST 5 * FROM "CAMBIOSDECOSTOS" WHERE {col_art}=? ORDER BY 1 DESC',
                (codigo,)
            )
            for r in cur.fetchall():
                registros.append({cols[i]: r[i] for i in range(len(cols))})
        c.close()
        return {"columnas": cols, "columna_articulo_detectada": col_art, "registros_para_codigo": registros}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/articulos-buscar")
def admin_articulos_buscar(q: str = Query("", min_length=2), _u=Depends(get_admin_user)):
    """Autocomplete rápido de artículos para filtro Rotación (sin FMA_STOCK)."""
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        cur.execute(
            "SELECT FIRST 15 CODIGOARTICULO, CODIGOPARTICULAR, DESCRIPCION "
            "FROM \"ARTICULOS\" WHERE ACTIVO = '1' "
            "AND (UPPER(CODIGOPARTICULAR) CONTAINING UPPER(?) OR UPPER(DESCRIPCION) CONTAINING UPPER(?)) "
            "ORDER BY CASE WHEN UPPER(CODIGOPARTICULAR) STARTING WITH UPPER(?) THEN 0 ELSE 1 END, "
            "CODIGOPARTICULAR, DESCRIPCION",
            (q, q, q)
        )
        rows = cur.fetchall()
        c.close()
        return [{'codigo': r[0], 'codigoparticular': r[1] or r[0], 'descripcion': r[2] or ''} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/ofertas")
def admin_get_ofertas(_u=Depends(get_admin_user)):
    c = _admin_db()
    offers = [dict(r) for r in c.execute("SELECT * FROM offers ORDER BY created_at DESC").fetchall()]
    if not offers:
        c.close()
        return offers
    ids = [o['id'] for o in offers]
    idx = {o['id']: o for o in offers}
    # Inicializar relaciones y defaults
    for o in offers:
        o['product_details']  = []
        o['financial_details'] = []
        o['conditions']       = []
        o['vendors']          = []
        o['profiles']         = []
        o['category_filters'] = []
        o['combo_escalones']  = []
        for k, d in [('deposito',''),('tipo_financiero','descuento_total'),('monto_minimo',0),('cupo',0),('usos',0)]:
            if k not in o: o[k] = d
    ph = ','.join('?' * len(ids))
    for r in c.execute(f"SELECT * FROM offer_product_details WHERE offer_id IN ({ph})", ids):
        d = dict(r)
        if d['offer_id'] in idx: idx[d['offer_id']]['product_details'].append(d)
    for r in c.execute(f"SELECT * FROM offer_financial_details WHERE offer_id IN ({ph}) ORDER BY orden", ids):
        d = dict(r)
        if d['offer_id'] in idx: idx[d['offer_id']]['financial_details'].append(d)
    for r in c.execute(f"SELECT offer_id, condicion_comercial FROM offer_conditions WHERE offer_id IN ({ph})", ids):
        if r[0] in idx: idx[r[0]]['conditions'].append(r[1])
    for r in c.execute(f"SELECT offer_id, codigousuario FROM offer_vendors WHERE offer_id IN ({ph})", ids):
        if r[0] in idx: idx[r[0]]['vendors'].append(r[1])
    for r in c.execute(f"SELECT offer_id, perfil_codigo FROM offer_profiles WHERE offer_id IN ({ph})", ids):
        if r[0] in idx: idx[r[0]]['profiles'].append(r[1])
    for r in c.execute(f"SELECT offer_id, nivel, valor FROM offer_category_filters WHERE offer_id IN ({ph})", ids):
        if r[0] in idx: idx[r[0]]['category_filters'].append({'nivel': r[1], 'valor': r[2]})
    for r in c.execute(f"SELECT offer_id, min_combos, descuento_pct FROM offer_combo_escalones WHERE offer_id IN ({ph}) ORDER BY min_combos", ids):
        if r[0] in idx: idx[r[0]]['combo_escalones'].append({'min_combos': r[1], 'descuento_pct': r[2]})
    c.close()
    return offers

def _save_offer_relations(c, id_, data):
    for e in (data.get('combo_escalones') or []):
        mc = int(e.get('min_combos') or 1)
        dp = float(e.get('descuento_pct') or 0)
        if mc >= 1 and dp > 0:
            c.execute("INSERT INTO offer_combo_escalones (offer_id, min_combos, descuento_pct) VALUES (?,?,?)", (id_, mc, dp))
    for d in (data.get('product_details') or []):
        c.execute("INSERT INTO offer_product_details (offer_id, codigo_producto, descripcion, cantidad, bonificacion_pct) VALUES (?,?,?,?,?)",
                  (id_, d.get('codigo_producto'), d.get('descripcion',''), d.get('cantidad',1), d.get('bonificacion_pct',0)))
    for i, d in enumerate(data.get('financial_details') or []):
        c.execute("INSERT INTO offer_financial_details (offer_id, porcentaje, orden) VALUES (?,?,?)", (id_, d.get('porcentaje',0), i))
    for cond in (data.get('conditions') or []):
        try: c.execute("INSERT OR IGNORE INTO offer_conditions (offer_id, condicion_comercial) VALUES (?,?)", (id_, cond))
        except: pass
    for vend in (data.get('vendors') or []):
        v = vend.upper() if vend else None
        if v:
            try: c.execute("INSERT OR IGNORE INTO offer_vendors (offer_id, codigousuario) VALUES (?,?)", (id_, v))
            except: pass
    for perf in (data.get('profiles') or []):
        if perf:
            try: c.execute("INSERT OR IGNORE INTO offer_profiles (offer_id, perfil_codigo) VALUES (?,?)", (id_, perf.upper()))
            except: pass
    for f in (data.get('category_filters') or []):
        if f.get('nivel') and f.get('valor'):
            try: c.execute("INSERT OR IGNORE INTO offer_category_filters (offer_id, nivel, valor) VALUES (?,?,?)", (id_, f['nivel'], f['valor']))
            except: pass
    c.commit()

@app.get("/admin/ofertas/{id}")
def admin_get_oferta_by_id(id: int, _u=Depends(get_admin_user)):
    c = _admin_db()
    row = c.execute("SELECT * FROM offers WHERE id=?", (id,)).fetchone()
    if not row: c.close(); raise HTTPException(404, "Oferta no encontrada")
    o = dict(row)
    _load_offer_relations(c, o)
    c.close()
    return o

@app.post("/admin/ofertas")
def admin_create_oferta(data: dict, _u=Depends(get_admin_user)):
    nombre = (data.get('nombre') or '').strip()
    tipo   = (data.get('tipo') or '').strip()
    if not nombre or not tipo:
        raise HTTPException(400, "nombre y tipo requeridos")
    c = _admin_db()
    c.execute("INSERT INTO offers (nombre, tipo, descripcion, fecha_desde, fecha_hasta, deposito, tipo_financiero, monto_minimo, cupo, usos) VALUES (?,?,?,?,?,?,?,?,?,0)",
              (nombre, tipo, data.get('descripcion','').strip(), data.get('fecha_desde'), data.get('fecha_hasta'),
               data.get('deposito','').strip(), data.get('tipo_financiero','descuento_total'),
               data.get('monto_minimo',0), int(data.get('cupo',0) or 0)))
    c.commit(); id_ = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    _save_offer_relations(c, id_, data)
    c.close()
    return {"id": id_, "ok": True}

@app.put("/admin/ofertas/{id}")
def admin_update_oferta(id: int, data: dict, _u=Depends(get_admin_user)):
    c = _admin_db()
    nuevo_cupo = int(data.get('cupo',0) or 0)
    # Al ampliar el cupo, reactivar si estaba inactiva por cupo agotado
    row = c.execute("SELECT usos, activo FROM offers WHERE id=?", (id,)).fetchone()
    usos_actual = row['usos'] if row else 0
    activo_nuevo = data.get('activo',1)
    if nuevo_cupo == 0 or usos_actual < nuevo_cupo:
        activo_nuevo = data.get('activo',1)  # respetar lo que manda el formulario
    c.execute("""UPDATE offers SET nombre=?, tipo=?, descripcion=?, fecha_desde=?, fecha_hasta=?,
                 activo=?, deposito=?, tipo_financiero=?, monto_minimo=?, cupo=? WHERE id=?""",
              (data.get('nombre','').strip(), data.get('tipo','').strip(),
               data.get('descripcion','').strip(), data.get('fecha_desde'), data.get('fecha_hasta'),
               activo_nuevo, data.get('deposito','').strip(),
               data.get('tipo_financiero','descuento_total'), data.get('monto_minimo',0), nuevo_cupo, id))
    for tbl in ('offer_product_details','offer_financial_details','offer_conditions',
                'offer_vendors','offer_profiles','offer_category_filters','offer_combo_escalones'):
        c.execute(f"DELETE FROM {tbl} WHERE offer_id=?", (id,))
    _save_offer_relations(c, id, data)
    c.close()
    return {"ok": True}

@app.delete("/admin/ofertas/{id}")
def admin_delete_oferta(id: int, _u=Depends(get_admin_user)):
    c = _admin_db(); c.execute("DELETE FROM offers WHERE id=?", (id,)); c.commit(); c.close()
    return {"ok": True}

# Endpoint público para frontend
@app.get("/ofertas")
def get_ofertas_for_vendor(vendedor: Optional[str] = None, perfil: Optional[str] = None):
    from datetime import date
    hoy = date.today().isoformat()
    c = _admin_db()
    vend_up = vendedor.upper() if vendedor else None
    perf_up = perfil.upper() if perfil else None
    # Una oferta aplica al vendedor si:
    #   - No tiene restricción de vendor/profile (tablas vacías), O
    #   - El vendedor está en offer_vendors, O
    #   - El perfil del vendedor está en offer_profiles
    base_cond = """
        SELECT DISTINCT o.* FROM offers o
        WHERE o.activo=1
          AND (o.fecha_desde IS NULL OR o.fecha_desde <= ?)
          AND (o.fecha_hasta IS NULL OR o.fecha_hasta >= ?)
          AND (
            (NOT EXISTS (SELECT 1 FROM offer_vendors ov WHERE ov.offer_id=o.id AND ov.codigousuario IS NOT NULL)
             AND NOT EXISTS (SELECT 1 FROM offer_profiles op WHERE op.offer_id=o.id))
    """
    params = [hoy, hoy]
    if vend_up:
        base_cond += " OR EXISTS (SELECT 1 FROM offer_vendors ov WHERE ov.offer_id=o.id AND ov.codigousuario=?)"
        params.append(vend_up)
    if perf_up:
        base_cond += " OR EXISTS (SELECT 1 FROM offer_profiles op WHERE op.offer_id=o.id AND op.perfil_codigo=?)"
        params.append(perf_up)
    base_cond += ")"
    offers = c.execute(base_cond, params).fetchall()
    if not offers:
        c.close()
        return []

    # ── Recolectar condiciones de texto para resolver en UNA sola conexión Firebird ──
    # (Evita abrir N conexiones Firebird — una por cada condición de texto — que
    # causaba demoras de 5-15 s en cargarOfertas() del frontend.)
    ids = [o['id'] for o in offers]
    ph  = ','.join('?' * len(ids))
    all_conds_rows = c.execute(
        f"SELECT offer_id, condicion_comercial FROM offer_conditions WHERE offer_id IN ({ph})", ids
    ).fetchall()

    # Separar las que ya son código numérico de las que son texto libre
    text_conds = set()
    for row in all_conds_rows:
        val = str(row[1] or '').strip()
        if val and not val.isdigit():
            text_conds.add(val)

    # Una sola conexión Firebird para resolver TODOS los textos pendientes
    text_to_code = {}
    if text_conds:
        try:
            fb = conn('WIN1252')
            cur_fb = fb.cursor()
            for txt in text_conds:
                cur_fb.execute(
                    'SELECT FIRST 1 CODIGOMULTIPLAZO FROM "MULTIPLAZOS" '
                    'WHERE UPPER(TRIM(DESCRIPCION)) = UPPER(TRIM(?))', (txt,))
                row = cur_fb.fetchone()
                text_to_code[txt] = str(row[0]).strip() if row else txt
            fb.close()
        except Exception:
            pass  # si Firebird falla, se usa el texto tal cual

    # Agrupar condiciones por offer_id con resolución ya lista
    conds_by_offer = {}
    for row in all_conds_rows:
        oid = row[0]
        val = str(row[1] or '').strip()
        resolved_val = val if val.isdigit() else text_to_code.get(val, val)
        conds_by_offer.setdefault(oid, []).append(resolved_val)

    # ── Batch queries SQLite para el resto de tablas relacionadas ──────────────
    fin_by_offer  = {}
    for row in c.execute(f"SELECT offer_id, porcentaje, orden FROM offer_financial_details WHERE offer_id IN ({ph}) ORDER BY orden", ids):
        fin_by_offer.setdefault(row[0], []).append({'porcentaje': row[1], 'orden': row[2]})

    prod_by_offer = {}
    for row in c.execute(f"SELECT * FROM offer_product_details WHERE offer_id IN ({ph})", ids):
        prod_by_offer.setdefault(row['offer_id'], []).append(dict(row))

    cat_by_offer  = {}
    for row in c.execute(f"SELECT offer_id, nivel, valor FROM offer_category_filters WHERE offer_id IN ({ph})", ids):
        cat_by_offer.setdefault(row[0], []).append({'nivel': row[1], 'valor': row[2]})

    esc_by_offer  = {}
    for row in c.execute(f"SELECT offer_id, min_combos, descuento_pct FROM offer_combo_escalones WHERE offer_id IN ({ph}) ORDER BY min_combos", ids):
        esc_by_offer.setdefault(row[0], []).append({'min_combos': row[1], 'descuento_pct': row[2]})

    result = []
    for o in offers:
        od = dict(o)
        oid = od['id']
        od['financial_details'] = fin_by_offer.get(oid, [])
        od['product_details']   = prod_by_offer.get(oid, [])
        od['conditions']        = conds_by_offer.get(oid, [])
        od['category_filters']  = cat_by_offer.get(oid, [])
        od['combo_escalones']   = esc_by_offer.get(oid, [])
        for k, d in [('deposito',''),('tipo_financiero','descuento_total'),('monto_minimo',0),('cupo',0),('usos',0)]:
            if k not in od: od[k] = d
        result.append(od)
    c.close()
    return result

_BANCOS = [
    ("Santander Rio",  "0720131420000001149872", "131",    "11498/7"),
    ("Provincia",      "0140004501400404115211", "4004",   "041152/1"),
    ("HSBC",           "1500607500060732055732", "607",    "607-3-205573"),
    ("Galicia",        "0070154520000005006724", "154",    "5006-7-154/2"),
]
_MP_EMAIL = "marketing@microbellsa.com.ar"
_MP_CVU   = "0000003100004756934965"

def _fmt(v):
    """Formato moneda argentina: $ 1.234,56"""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "$ 0,00"
    neg = v < 0
    v = abs(v)
    ent = int(v)
    dec = round((v - ent) * 100)
    s = f"{ent:,}".replace(",", ".")
    return f"{'- ' if neg else ''}$ {s},{dec:02d}"

def _d(v):
    """Convierte datetime o string a dd/mm/yyyy"""
    if v is None:
        return ""
    s = str(v)
    try:
        from datetime import datetime
        if "T" in s:
            s = s.split("T")[0]
        elif " " in s:
            s = s.split(" ")[0]
        parts = s.split("-")
        if len(parts) == 3:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"
    except Exception:
        pass
    return s

@app.get("/panel")
def panel_redirect():
    return RedirectResponse(url="/admin", status_code=302)

@app.get("/", response_class=HTMLResponse)
def index():
    with open(FRONTEND_PATH, encoding="utf-8") as f:
        return f.read()

@app.get("/logo")
def get_logo():
    from fastapi.responses import FileResponse as FR
    if os.path.exists(LOGO_PATH):
        return FR(LOGO_PATH, media_type="image/png")
    return HTMLResponse("", status_code=404)

@app.get("/manifest.json")
def get_manifest():
    from fastapi.responses import FileResponse as FR
    path = os.path.join(os.path.dirname(__file__), "manifest.json")
    if os.path.exists(path):
        return FR(path, media_type="application/manifest+json",
                  headers={"Cache-Control": "public, max-age=86400"})
    return HTMLResponse("", status_code=404)

@app.get("/sw.js")
def get_sw():
    from fastapi.responses import FileResponse as FR
    path = os.path.join(os.path.dirname(__file__), "sw.js")
    if os.path.exists(path):
        return FR(path, media_type="application/javascript",
                  headers={"Cache-Control": "no-cache, no-store, must-revalidate",
                           "Service-Worker-Allowed": "/"})
    return HTMLResponse("", status_code=404)

@app.get("/favicon.ico")
def get_favicon():
    from fastapi.responses import FileResponse as FR
    if os.path.exists(FAVICON_PATH):
        return FR(FAVICON_PATH, media_type="image/x-icon",
                  headers={"Cache-Control": "public, max-age=86400"})
    return HTMLResponse("", status_code=404)

@app.get("/icons/{filename}")
def get_icon(filename: str):
    from fastapi.responses import FileResponse as FR
    path = os.path.join(os.path.dirname(__file__), filename)
    if os.path.exists(path) and filename.lower().endswith('.png'):
        return FR(path, media_type="image/png",
                  headers={"Cache-Control": "public, max-age=86400"})
    return HTMLResponse("", status_code=404)


# ─── Stock ─────────────────────────────────────────────────────────────────────
@app.get("/stock/cache-ts")
def stock_cache_ts():
    """Devuelve la antigüedad en segundos de cada depósito en cache."""
    now = time.time()
    result = {}
    with _fma_cache_lock:
        for dep, (ts, _) in _fma_cache.items():
            result[dep] = round(now - ts)
    return {"cache": result, "ttl": _FMA_CACHE_TTL, "ts": now}

@app.get("/stock")
def get_stock(
    buscar: Optional[str] = None,
    gruposuperrubro: Optional[str] = None,
    superrubro: Optional[str] = None,
    rubro: Optional[str] = None,
    marca: Optional[str] = None,
    deposito: Optional[str] = None,
    limit: int = Query(100, le=300),
    offset: int = 0,
    _user=Depends(get_current_user)
):
    _dep_fma = deposito if deposito else '001,003'
    dep_lista = [d.strip() for d in _dep_fma.split(',') if d.strip()] or ['001', '003']
    try:
        pagina, _total, cambio_usd = _search_stock_cache(
            buscar=buscar, gruposuperrubro=gruposuperrubro, superrubro=superrubro,
            rubro=rubro, marca=marca, dep_lista=dep_lista, limit=limit, offset=offset
        )
        # Asegurar todos los depósitos en caché para los rem_* del frontend
        _fma_stock_parallel(_FMA_ALL_DEPS)
        with _fma_cache_lock:
            all_rem = {d: (_fma_cache.get(d) or (0, {}))[1] for d in _FMA_ALL_DEPS}

        resultado = []
        for art, rem_dep, rem_total in pagina:
            factor = cambio_usd if art['codigomoneda'] == 'DOLARES' else 1.0
            def conv(v): return math.ceil(v * factor * 100) / 100
            resultado.append({
                "codigo":           art['codigo'],
                "codigoparticular": art['codigoparticular'],
                "descripcion":      art['descripcion'],
                "marca":            art['codigomarca'],
                "precio1":          conv(art['precio1']),
                "precio2":          conv(art['precio2']),
                "precio3":          conv(art['precio3']),
                "precio5":          conv(art['precio5']),
                "iva":              art['alicuotaiva'],
                "unidad":           art['unidad'],
                "stock":            rem_total,
                "remanente":        rem_total,
                "remanente_001":    all_rem['001'].get(art['codigo'], 0),
                "remanente_002":    all_rem['002'].get(art['codigo'], 0),
                "remanente_003":    all_rem['003'].get(art['codigo'], 0),
                "remanente_005":    all_rem['005'].get(art['codigo'], 0),
                "remanente_013":    all_rem['013'].get(art['codigo'], 0),
                "remanente_016":    all_rem['016'].get(art['codigo'], 0),
                "rubro":            art['rubro'],
                "superrubro":       art['superrubro'],
                "gruposuperrubro":  art['gruposuperrubro'],
                "moneda":           art['codigomoneda'],
                "codigo_rubro":     art['codigo_rubro'],
                "codigo_superrubro": art['codigo_superrubro'],
                "codigo_gruposuperrubro": art['codigo_gruposuperrubro'],
            })
        _apply_reservas(resultado, _get_reservas_activas(), rem_key='remanente')
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

_bart_cache: dict = {}  # (q,db,skip_stock) -> (ts, rows)
_BART_TTL = 25  # segundos

@app.get("/buscar-articulos")
def buscar_articulos(q: str = Query("", min_length=2), db: str = Query("oficial"), deposito: Optional[str] = None, skip_stock: int = Query(0)):
    from concurrent.futures import ThreadPoolExecutor
    import re as _re
    ALL_DEPS = ['001','002','003','005','013','016']
    deps_to_fetch = [d.strip() for d in deposito.split(',')] if deposito else ALL_DEPS

    # ── 1. Articulos — con caché TTL para búsquedas repetidas ─────────────────
    _cache_key = (q.upper(), db, skip_stock)
    _now = time.time()
    _cached = _bart_cache.get(_cache_key)
    if _cached and (_now - _cached[0]) < _BART_TTL:
        rows, cambio_usd = _cached[1], _cached[2]
    else:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        try:
            try:
                cur.execute('SELECT CAMBIO FROM "MONEDAS" WHERE CODIGOMONEDA = ?', ('DOLARES',))
                rm = cur.fetchone()
                cambio_usd = float(rm[0]) if rm else 1.0
            except Exception:
                cambio_usd = 1.0

            # Detectar si la búsqueda es un código puro (solo dígitos/letras sin espacios)
            # → usar STARTING WITH o = en lugar de CONTAINING en DESCRIPCION (mucho más rápido)
            _es_codigo = bool(_re.match(r'^[A-Za-z0-9\-]{2,15}$', q) and ' ' not in q)
            if _es_codigo:
                sql_art = (
                    "SELECT FIRST 30 CODIGOARTICULO, CODIGOPARTICULAR, DESCRIPCION, PRECIOLISTA1, ALICUOTAIVA, CODIGOMONEDA, "
                    "DTOMAXIMO1, APLICABLEABONIFICACION, PRECIOLISTA5 "
                    "FROM \"ARTICULOS\" WHERE ACTIVO = '1' "
                    "AND (UPPER(CODIGOPARTICULAR) CONTAINING UPPER(?) "
                    "     OR UPPER(DESCRIPCION) CONTAINING UPPER(?)) "
                    "ORDER BY CASE WHEN UPPER(CODIGOPARTICULAR) STARTING WITH UPPER(?) THEN 0 ELSE 1 END, "
                    "         CODIGOPARTICULAR, DESCRIPCION"
                )
                params_art = (q, q, q)
            else:
                sql_art = (
                    "SELECT FIRST 30 CODIGOARTICULO, CODIGOPARTICULAR, DESCRIPCION, PRECIOLISTA1, ALICUOTAIVA, CODIGOMONEDA, "
                    "DTOMAXIMO1, APLICABLEABONIFICACION, PRECIOLISTA5 "
                    "FROM \"ARTICULOS\" WHERE ACTIVO = '1' "
                    "AND (UPPER(DESCRIPCION) CONTAINING UPPER(?) OR UPPER(CODIGOPARTICULAR) CONTAINING UPPER(?)) "
                    "ORDER BY DESCRIPCION, CODIGOPARTICULAR"
                )
                params_art = (q, q)

            cur.execute(sql_art, params_art)
            rows = cur.fetchall()
        finally:
            c.close()
        _bart_cache[_cache_key] = (_now, rows, cambio_usd)
        # Limpiar entradas viejas (evitar crecimiento ilimitado)
        if len(_bart_cache) > 200:
            _oldest = sorted(_bart_cache, key=lambda k: _bart_cache[k][0])[:50]
            for _k in _oldest:
                _bart_cache.pop(_k, None)

    # 2. FMA_STOCK en paralelo - una conexion por deposito
    def _fetch_dep(dep):
        try:
            cx = conn('WIN1252', db=DATABASE)
            cu = cx.cursor()
            cu.execute(f"SELECT ID_ARTICULO, STOCKREMANENTE FROM \"FMA_STOCK\"(NULL, NULL, '{dep}', 1, 1)")
            result = {row[0]: float(row[1] or 0) for row in cu.fetchall()}
            cx.close()
            return dep, result
        except Exception:
            return dep, {}

    rem_maps = {d: {} for d in ALL_DEPS}
    if rows and not skip_stock:
        with ThreadPoolExecutor(max_workers=len(deps_to_fetch)) as ex:
            for dep, data in ex.map(_fetch_dep, deps_to_fetch):
                rem_maps[dep] = data

    resultado = []
    for r in rows:
        moneda = (r[5] or '').strip().upper()
        factor = cambio_usd if moneda == 'DOLARES' else 1.0
        precio = math.ceil(float(r[3]) * factor * 100) / 100 if r[3] else 0
        dto_max = float(r[6]) if r[6] is not None else None
        aplica_bonif = str(r[7] or '0').strip() == '1'
        if not aplica_bonif:
            dto_max = None
        precio5_raw = float(r[8]) if r[8] else 0
        precio5 = math.ceil(precio5_raw * factor * 100) / 100 if precio5_raw else 0
        item = {"codigo": r[0], "codigoparticular": r[1] or r[0],
                "descripcion": r[2], "precio": precio, "precio5": precio5,
                "iva": float(r[4]) if r[4] else 21,
                "dto_max": dto_max}
        for dep in ALL_DEPS:
            item[f'rem{dep}'] = rem_maps.get(dep, {}).get(r[0], 0)
        resultado.append(item)
    return resultado

# ─── Helpers stock export ──────────────────────────────────────────────────────
def _fetch_stock_data(buscar=None, gruposuperrubro=None, superrubro=None, rubro=None, marca=None):
    """Devuelve todos los artículos con remanente > 0 según filtros, sin límite."""
    # ── 1. Cotización USD
    cambio_usd = 1.0
    try:
        cx = conn()
        cu = cx.cursor()
        cu.execute('SELECT CAMBIO FROM "MONEDAS" WHERE CODIGOMONEDA = ?', ('DOLARES',))
        rm = cu.fetchone()
        cambio_usd = float(rm[0]) if rm else 1.0
        cx.close()
    except Exception:
        pass

    # ── 2. Query principal (001+003 combinados)
    wheres = ["a.ACTIVO = '1'"]
    params = []
    if buscar:
        buscar = _sanitizar_buscar(buscar)
        wheres.append("(UPPER(a.DESCRIPCION) CONTAINING UPPER(?) OR a.CODIGOPARTICULAR CONTAINING ?)")
        params += [buscar, buscar]
    if rubro:
        wheres.append("a.CODIGORUBRO = ?")
        params.append(rubro)
    if superrubro:
        wheres.append("r.CODIGOSUPERRUBRO = ?")
        params.append(superrubro)
    if gruposuperrubro:
        wheres.append("sr.CODIGOGRUPOSUPERRUBRO = ?")
        params.append(gruposuperrubro)
    if marca:
        wheres.append("a.CODIGOMARCA = ?")
        params.append(marca)
    where_sql = " AND ".join(wheres)

    sql = f"""
        SELECT s.ID_ARTICULO, a.CODIGOPARTICULAR, a.DESCRIPCION,
               a.PRECIOLISTA1, a.ALICUOTAIVA, a.CODIGOUNIDADMEDIDA,
               s.STOCKREAL, s.STOCKREMANENTE, a.CODIGOMONEDA,
               r.DESCRIPCION, sr.DESCRIPCION, g.DESCRIPCION
        FROM "FMA_STOCK"(NULL, NULL, '001,003', 1, 1) s
        JOIN "ARTICULOS" a ON a.CODIGOARTICULO = s.ID_ARTICULO
        LEFT JOIN "RUBROS" r ON r.CODIGORUBRO = a.CODIGORUBRO
        LEFT JOIN "SUPERRUBROS" sr ON sr.CODIGOSUPERRUBRO = r.CODIGOSUPERRUBRO
        LEFT JOIN "GRUPOSUPERRUBROS" g ON g.CODIGOGRUPOSUPERRUBRO = sr.CODIGOGRUPOSUPERRUBRO
        WHERE {where_sql} AND s.STOCKREMANENTE > 0
        ORDER BY a.DESCRIPCION
    """
    c1 = conn()
    cur1 = c1.cursor()
    cur1.execute(sql, params)
    rows = cur1.fetchall()
    c1.close()

    # ── 3. Remanente por depósito (paralelo para mayor velocidad)
    from concurrent.futures import ThreadPoolExecutor

    def _fetch_rem(dep):
        try:
            cx = conn()
            cu = cx.cursor()
            cu.execute(f'SELECT ID_ARTICULO, STOCKREMANENTE FROM "FMA_STOCK"(NULL, NULL, \'{dep}\', 1, 1)')
            result = {row[0]: float(row[1] or 0) for row in cu.fetchall()}
            cx.close()
            return result
        except Exception:
            return {}

    with ThreadPoolExecutor(max_workers=2) as ex:
        f001 = ex.submit(_fetch_rem, '001')
        f003 = ex.submit(_fetch_rem, '003')
        rem_001_map = f001.result()
        rem_003_map = f003.result()

    # ── 4. Armar resultado
    result = []
    for r in rows:
        moneda = (r[8] or '').strip().upper()
        factor = cambio_usd if moneda == 'DOLARES' else 1.0
        precio = math.ceil(float(r[3]) * factor * 100) / 100 if r[3] else 0
        result.append({
            "codigo":           r[1] or r[0],
            "descripcion":      (r[2] or '').strip(),
            "stock":            float(r[6] or 0),
            "rem_001":          rem_001_map.get(r[0], 0),
            "rem_003":          rem_003_map.get(r[0], 0),
            "rem_total":        float(r[7] or 0),
            "precio":           precio,
            "iva":              float(r[4]) if r[4] else 21,
            "rubro":            (r[9] or '').strip(),
            "superrubro":       (r[10] or '').strip(),
            "gruposuperrubro":  (r[11] or '').strip(),
        })
    return result


@app.get("/stock/exportar-excel")
def exportar_stock_excel(
    buscar: Optional[str] = None,
    gruposuperrubro: Optional[str] = None,
    superrubro: Optional[str] = None,
    rubro: Optional[str] = None,
    marca: Optional[str] = None,
):
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter

        rows = _fetch_stock_data(buscar, gruposuperrubro, superrubro, rubro, marca)
        # Aplicar reservas (igual que en /stock)
        for r in rows:
            r['remanente_001'] = r['rem_001']
            r['remanente_003'] = r['rem_003']
            r['remanente'] = r['rem_total']
            r['codigoparticular'] = r['codigo']
        _apply_reservas(rows, _get_reservas_activas(), rem_key='remanente')
        for r in rows:
            r['rem_001'] = r['remanente_001']
            r['rem_003'] = r['remanente_003']
            r['rem_total'] = r['remanente_001'] + r['remanente_003']

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Stock Remanente"

        hdr_fill = PatternFill("solid", fgColor="1A56DB")
        hdr_font = Font(bold=True, color="FFFFFFFF", size=10)
        alt_fill = PatternFill("solid", fgColor="FFEFF6FF")
        center   = Alignment(horizontal="center", vertical="center", wrap_text=True)
        right_al = Alignment(horizontal="right",  vertical="center")
        left_al  = Alignment(horizontal="left",   vertical="center")

        headers    = ["Gr. Super Rubro", "Super Rubro", "Rubro",
                      "Código", "Descripción",
                      "Rem. VAC-LOG (001)", "Rem. Pacheco (003)", "Rem. Total",
                      "Precio s/IVA", "IVA %"]
        col_widths = [22, 22, 18, 14, 52, 18, 18, 14, 18, 8]

        ws.row_dimensions[1].height = 24
        for ci, (h, w) in enumerate(zip(headers, col_widths), 1):
            cell = ws.cell(row=1, column=ci, value=h)
            cell.fill = hdr_fill
            cell.font = hdr_font
            cell.alignment = center
            ws.column_dimensions[get_column_letter(ci)].width = w

        for ri, row in enumerate(rows, 2):
            vals = [row["gruposuperrubro"], row["superrubro"], row["rubro"],
                    row["codigo"], row["descripcion"],
                    int(row["rem_001"]), int(row["rem_003"]), int(row["rem_total"]),
                    row["precio"], int(row["iva"])]
            for ci, v in enumerate(vals, 1):
                cell = ws.cell(row=ri, column=ci, value=v)
                cell.alignment = left_al if ci <= 5 else right_al
                if ri % 2 == 0:
                    cell.fill = alt_fill

        ws.freeze_panes = "A2"
        buf = BytesIO()
        wb.save(buf)
        buf.seek(0)
        return StreamingResponse(buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=stock_remanente.xlsx"})
    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=traceback.format_exc())


@app.get("/stock/exportar-pdf")
def exportar_stock_pdf(
    buscar: Optional[str] = None,
    gruposuperrubro: Optional[str] = None,
    superrubro: Optional[str] = None,
    rubro: Optional[str] = None,
    marca: Optional[str] = None,
):
    from datetime import datetime
    from reportlab.lib.pagesizes import landscape
    rows = _fetch_stock_data(buscar, gruposuperrubro, superrubro, rubro, marca)
    # Aplicar reservas (igual que en /stock)
    for r in rows:
        r['remanente_001'] = r['rem_001']
        r['remanente_003'] = r['rem_003']
        r['remanente'] = r['rem_total']
        r['codigoparticular'] = r['codigo']
    _apply_reservas(rows, _get_reservas_activas(), rem_key='remanente')
    for r in rows:
        r['rem_001'] = r['remanente_001']
        r['rem_003'] = r['remanente_003']
        r['rem_total'] = r['remanente_001'] + r['remanente_003']

    # Datos empresa
    razon_soc = cuit_emp = dir_emp = tel_emp = email_emp = ''
    try:
        cp = conn()
        ccp = cp.cursor()
        ccp.execute('SELECT RAZONSOCIAL, CUIT, DIRECCION, TELEFONO, EMAIL FROM "PARAMETROS" WHERE CODIGOPARAMETRO = 1')
        rp = ccp.fetchone()
        cp.close()
        if rp:
            razon_soc, cuit_emp, dir_emp, tel_emp, email_emp = [(v or '').strip() for v in rp]
    except Exception:
        pass

    buf = BytesIO()
    PAGE_W, PAGE_H = landscape(A4)
    mg = 14 * mm

    s_title = ParagraphStyle('t', fontSize=13, leading=16, fontName='Helvetica-Bold',
                              textColor=colors.HexColor('#1a56db'))
    s_sub   = ParagraphStyle('s', fontSize=8,  leading=10, fontName='Helvetica',
                              textColor=colors.HexColor('#6b7280'))
    s_ft    = ParagraphStyle('f', fontSize=6.5, leading=9, fontName='Helvetica',
                              alignment=TA_CENTER, textColor=colors.HexColor('#6b7280'))
    s_hdr   = ParagraphStyle('h', fontSize=7.5, leading=9, fontName='Helvetica-Bold',
                              alignment=TA_CENTER, textColor=colors.white)
    s_cell  = ParagraphStyle('c', fontSize=7.5, leading=9, fontName='Helvetica')
    s_cell_r= ParagraphStyle('cr', fontSize=7.5, leading=9, fontName='Helvetica', alignment=TA_RIGHT)

    usable_w = PAGE_W - 2 * mg
    footer_txt = f'{razon_soc}  ·  CUIT {cuit_emp}  ·  {dir_emp}  ·  Tel {tel_emp}  ·  {email_emp}'

    def _on_page(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor('#e5e7eb'))
        canvas.setLineWidth(0.5)
        canvas.line(mg, 10*mm, PAGE_W - mg, 10*mm)
        canvas.setFont('Helvetica', 6.5)
        canvas.setFillColor(colors.HexColor('#6b7280'))
        canvas.drawCentredString(PAGE_W / 2, 7*mm, footer_txt)
        canvas.drawRightString(PAGE_W - mg, 7*mm, f"Pág. {doc.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                            leftMargin=mg, rightMargin=mg,
                            topMargin=mg, bottomMargin=18*mm)

    story = []

    # Logo + título
    logo_row = []
    if os.path.exists(LOGO_PATH):
        logo_row.append(Image(LOGO_PATH, width=38*mm, height=12*mm))
    else:
        logo_row.append(Paragraph('', s_sub))

    # Obtener descripcion de marca si aplica
    marca_desc = None
    if marca:
        try:
            cm = conn(); cum = cm.cursor()
            cum.execute('SELECT DESCRIPCION FROM "MARCAS" WHERE CODIGOMARCA = ?', (marca,))
            rm2 = cum.fetchone()
            cm.close()
            marca_desc = (rm2[0] or '').strip() if rm2 else marca
        except Exception:
            marca_desc = marca

    filtro_txt = " · ".join(filter(None, [
        f"Buscar: {buscar}" if buscar else None,
        f"GSR: {gruposuperrubro}" if gruposuperrubro else None,
        f"SR: {superrubro}" if superrubro else None,
        f"Rubro: {rubro}" if rubro else None,
        f"Marca: {marca_desc}" if marca_desc else None,
        "Sin filtro" if not any([buscar, gruposuperrubro, superrubro, rubro, marca]) else None,
    ]))
    titulo_cell = [
        Paragraph("Stock Remanente", s_title),
        Paragraph(f"{filtro_txt}  —  {datetime.now().strftime('%d/%m/%Y %H:%M')}  —  {len(rows)} artículos", s_sub),
    ]
    t_hdr = Table([[logo_row[0], titulo_cell]], colWidths=[42*mm, usable_w - 42*mm])
    t_hdr.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('ALIGN',  (1,0), (1,0),  'RIGHT'),
    ]))
    story.append(t_hdr)
    story.append(Spacer(1, 4*mm))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#1a56db')))
    story.append(Spacer(1, 3*mm))

    # Tabla de datos
    HDR_BG = colors.HexColor('#1a56db')
    ALT_BG = colors.HexColor('#eff6ff')
    COL_001 = colors.HexColor('#2563eb')
    COL_003 = colors.HexColor('#7c3aed')
    COL_TOT = colors.HexColor('#059669')

    # Anchos de columna — 10 cols llenando usable_w (landscape A4 ≈ 269mm)
    cw = [28*mm, 28*mm, 24*mm, 16*mm, 90*mm, 18*mm, 18*mm, 16*mm, 22*mm, 10*mm]
    # suma: 28+28+24+16+90+18+18+16+22+10 = 270mm ≈ usable_w

    data = [[
        Paragraph('Gr. Super\nRubro',   s_hdr),
        Paragraph('Super\nRubro',       s_hdr),
        Paragraph('Rubro',              s_hdr),
        Paragraph('Código',             s_hdr),
        Paragraph('Descripción',        s_hdr),
        Paragraph('Rem.\nVAC-LOG',      s_hdr),
        Paragraph('Rem.\nPacheco',      s_hdr),
        Paragraph('Rem.\nTotal',        s_hdr),
        Paragraph('Precio s/IVA',       s_hdr),
        Paragraph('IVA',                s_hdr),
    ]]

    for row in rows:
        data.append([
            Paragraph(row['gruposuperrubro'] or '—', s_cell),
            Paragraph(row['superrubro'] or '—',      s_cell),
            Paragraph(row['rubro'] or '—',           s_cell),
            Paragraph(str(row['codigo']),            s_cell),
            Paragraph(row['descripcion'],            s_cell),
            Paragraph(f"{int(row['rem_001']):,}".replace(',', '.'),   s_cell_r),
            Paragraph(f"{int(row['rem_003']):,}".replace(',', '.'),   s_cell_r),
            Paragraph(f"{int(row['rem_total']):,}".replace(',', '.'), s_cell_r),
            Paragraph('$'+f"{row['precio']:,.2f}".replace(',','X').replace('.',',').replace('X','.'), s_cell_r),
            Paragraph(f"{int(row['iva'])}%",         s_cell_r),
        ])

    tbl = Table(data, colWidths=cw, repeatRows=1)
    style = TableStyle([
        ('BACKGROUND', (0,0), (-1,0), HDR_BG),
        ('TEXTCOLOR',  (0,0), (-1,0), colors.white),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, ALT_BG]),
        ('GRID',       (0,0), (-1,-1), 0.4, colors.HexColor('#e5e7eb')),
        ('VALIGN',     (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        # Color columnas remanente (índices 5,6,7 sin Stock Total)
        ('TEXTCOLOR', (5,1), (5,-1), COL_001),
        ('TEXTCOLOR', (6,1), (6,-1), COL_003),
        ('TEXTCOLOR', (7,1), (7,-1), COL_TOT),
        ('FONTNAME',  (5,1), (7,-1), 'Helvetica-Bold'),
    ])
    tbl.setStyle(style)
    story.append(tbl)

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": "inline; filename=stock_remanente.pdf"})


@app.get("/stock/batch")
def get_articulos_batch(codigos: str = Query(..., description="Códigos separados por coma")):
    """Devuelve stock y datos de múltiples artículos en una sola llamada."""
    codes = [c.strip() for c in codigos.split(',') if c.strip()]
    if not codes:
        return []
    c = conn()
    cur = c.cursor()
    # Cotización USD
    try:
        cur.execute('SELECT CAMBIO FROM "MONEDAS" WHERE CODIGOMONEDA = ?', ('DOLARES',))
        rm = cur.fetchone()
        cambio_usd = float(rm[0]) if rm else 1.0
    except Exception:
        cambio_usd = 1.0
    _COLS = ('SELECT a.CODIGOARTICULO, a.DESCRIPCION, a.CODIGOMARCA, '
             'a.PRECIOLISTA1, a.PRECIOLISTA5, a.ALICUOTAIVA, '
             'a.CODIGOMONEDA, a.CODIGOPARTICULAR, a.DTOMAXIMO1, a.APLICABLEABONIFICACION, '
             'a.PERMITESTOCKNEGATIVO '
             'FROM "ARTICULOS" a WHERE ')
    placeholders = ','.join(['?' for _ in codes])
    # Buscar por CODIGOPARTICULAR
    cur.execute(_COLS + f'a.CODIGOPARTICULAR IN ({placeholders})', codes)
    rows_by_part = {str(r[7] or '').strip(): r for r in cur.fetchall()}
    # Los no encontrados, buscar por CODIGOARTICULO
    missing = [c2 for c2 in codes if c2 not in rows_by_part]
    rows_by_art = {}
    if missing:
        ph2 = ','.join(['?' for _ in missing])
        cur.execute(_COLS + f'a.CODIGOARTICULO IN ({ph2})', missing)
        rows_by_art = {str(r[0] or '').strip(): r for r in cur.fetchall()}
    c.close()
    # FMA stock (cache compartido, una llamada paralela)
    _DEPS = ['001', '002', '003', '005', '013', '016']
    rem_bulk = _fma_stock_parallel(_DEPS)
    reservas = _get_reservas_activas()
    results = []
    for cod in codes:
        row = rows_by_part.get(cod) or rows_by_art.get(cod)
        if not row:
            continue
        moneda = (row[6] or '').strip().upper()
        factor = cambio_usd if moneda == 'DOLARES' else 1.0
        def conv(v, f=factor): return math.ceil(float(v) * f * 100) / 100 if v else 0
        codigoarticulo = row[0]
        cod_key = str(codigoarticulo).strip()
        rem = {dep: rem_bulk[dep].get(codigoarticulo, rem_bulk[dep].get(cod_key, 0.0)) for dep in _DEPS}
        dto_max_raw = row[8]
        aplica_b = str(row[9] or '0').strip() == '1'
        dto_max = float(dto_max_raw) if (dto_max_raw is not None and aplica_b) else None
        permite_neg = str(row[10] or '0').strip() == '1'
        item = {
            "codigo": codigoarticulo,
            "codigoparticular": (row[7] or row[0] or '').strip(),
            "descripcion": row[1],
            "marca": (row[2] or '').strip(),
            "precio1": conv(row[3]), "precio5": conv(row[4]),
            "iva": row[5], "moneda": row[6],
            "remanente_001": rem['001'], "remanente_002": rem['002'],
            "remanente_003": rem['003'], "remanente_005": rem['005'],
            "remanente_013": rem['013'], "remanente_016": rem['016'],
            "dto_max": dto_max,
            "permite_stock_negativo": permite_neg,
            "_input_cod": cod,
        }
        _apply_reservas([item], reservas, rem_key='remanente_001')
        item.pop('reservado', None)
        item.pop('reservado_por_deposito', None)
        results.append(item)
    return results


@app.get("/conjunto/{codigo}/partes")
def get_conjunto_partes(codigo: str):
    """Dado CODIGOPARTICULAR de un Conjunto, devuelve sus artículos componentes."""
    c = conn()
    cur = c.cursor()
    cur.execute('SELECT CODIGOARTICULO FROM "ARTICULOS" WHERE CODIGOPARTICULAR = ?', (codigo,))
    row = cur.fetchone()
    if not row:
        cur.execute('SELECT CODIGOARTICULO FROM "ARTICULOS" WHERE CODIGOARTICULO = ?', (codigo,))
        row = cur.fetchone()
    if not row:
        c.close()
        raise HTTPException(404, "Conjunto no encontrado")
    cod_interno = row[0]
    cur.execute(
        'SELECT cj.CODIGOARTICULO, cj.CANTIDAD, a.DESCRIPCION, a.DESCRIPCIONADICIONAL, '
        'a.CODIGOPARTICULAR, a.PRECIOLISTA1, a.PRECIOLISTA2, a.PRECIOLISTA3, a.PRECIOLISTA5, '
        'a.ALICUOTAIVA, a.CODIGOUNIDADMEDIDA, a.CODIGOMONEDA, a.PERMITESTOCKNEGATIVO, cj.COEFICIENTEPRECIO '
        'FROM "CONJUNTOS" cj '
        'JOIN "ARTICULOS" a ON a.CODIGOARTICULO = cj.CODIGOARTICULO '
        'WHERE cj.CODIGOCONJUNTO = ? ORDER BY cj.LINEA',
        (cod_interno,)
    )
    partes_rows = cur.fetchall()
    c.close()
    try:
        c2 = conn(); cur2 = c2.cursor()
        cur2.execute('SELECT CAMBIO FROM "MONEDAS" WHERE CODIGOMONEDA = ?', ('DOLARES',))
        rm = cur2.fetchone()
        cambio_usd = float(rm[0]) if rm else 1.0
        c2.close()
    except Exception:
        cambio_usd = 1.0
    partes = []
    for p in partes_rows:
        moneda = (p[11] or '').strip().upper()
        factor = cambio_usd if moneda == 'DOLARES' else 1.0
        def conv(v): return math.ceil(float(v) * factor * 100) / 100 if v else 0
        partes.append({
            "codigo": (p[0] or '').strip(),
            "codigoparticular": (p[4] or p[0] or '').strip(),
            "cantidad": float(p[1] or 1),
            "descripcion": p[2],
            "descripcion_adicional": p[3],
            "precio1": conv(p[5]), "precio2": conv(p[6]),
            "precio3": conv(p[7]), "precio5": conv(p[8]),
            "iva": p[9], "unidad": p[10], "moneda": p[11],
            "permite_stock_negativo": str(p[12] or '0').strip() == '1',
            "coeficiente_precio": float(p[13] or 0)
        })
    return partes


@app.get("/stock/{codigo}")
def get_articulo(codigo: str):
    c = conn()
    cur = c.cursor()
    # Cotización USD actual
    try:
        cur.execute('SELECT CAMBIO FROM "MONEDAS" WHERE CODIGOMONEDA = ?', ('DOLARES',))
        rm = cur.fetchone()
        cambio_usd = float(rm[0]) if rm else 1.0
    except Exception:
        cambio_usd = 1.0
    _COLS = ('SELECT a.CODIGOARTICULO, a.DESCRIPCION, a.DESCRIPCIONADICIONAL, a.CODIGOMARCA, '
             'a.PRECIOLISTA1, a.PRECIOLISTA2, a.PRECIOLISTA3, a.PRECIOLISTA5, a.ALICUOTAIVA, a.CODIGOUNIDADMEDIDA, '
             'a.CODIGOMONEDA, a.CODIGOPARTICULAR, a.DTOMAXIMO1, a.APLICABLEABONIFICACION, '
             'a.CODIGORUBRO, r.CODIGOSUPERRUBRO, sr.CODIGOGRUPOSUPERRUBRO, a.PERMITESTOCKNEGATIVO, a.PARTECONJUNTO '
             'FROM "ARTICULOS" a '
             'LEFT JOIN "RUBROS" r ON r.CODIGORUBRO = a.CODIGORUBRO '
             'LEFT JOIN "SUPERRUBROS" sr ON sr.CODIGOSUPERRUBRO = r.CODIGOSUPERRUBRO '
             'WHERE ')
    # Buscar primero por CODIGOPARTICULAR (prioridad); si no existe, por CODIGOARTICULO.
    # El OR con una sola consulta puede devolver el artículo equivocado cuando un
    # CODIGOARTICULO de otro artículo coincide con el CODIGOPARTICULAR buscado.
    cur.execute(_COLS + 'a.CODIGOPARTICULAR = ?', (codigo,))
    row = cur.fetchone()
    if not row:
        cur.execute(_COLS + 'a.CODIGOARTICULO = ?', (codigo,))
        row = cur.fetchone()
    if not row:
        c.close()
        raise HTTPException(404, "Artículo no encontrado")
    moneda = (row[10] or '').strip().upper()
    factor = cambio_usd if moneda == 'DOLARES' else 1.0
    def conv(v): return math.ceil(float(v) * factor * 100) / 100 if v else 0
    codigoarticulo = row[0]
    c.close()
    # Remanente por depósito — paralelo + caché TTL
    _DEPS_ART = ['001', '002', '003', '005', '013', '016']
    rem_bulk = _fma_stock_parallel(_DEPS_ART)
    cod_key = str(codigoarticulo).strip()
    rem = {dep: rem_bulk[dep].get(codigoarticulo, rem_bulk[dep].get(cod_key, 0.0))
           for dep in _DEPS_ART}
    dto_max_raw = row[12]
    aplica_b    = str(row[13] or '0').strip() == '1'
    dto_max     = float(dto_max_raw) if (dto_max_raw is not None and aplica_b) else None
    codigoparticular = (row[11] or row[0] or '').strip()
    item = {
        "codigo": codigoarticulo, "codigoparticular": codigoparticular,
        "descripcion": row[1], "descripcion_adicional": row[2],
        "marca": (row[3] or '').strip(),
        "precio1": conv(row[4]), "precio2": conv(row[5]), "precio3": conv(row[6]), "precio5": conv(row[7]),
        "iva": row[8], "unidad": row[9], "moneda": row[10],
        "remanente_001": rem['001'], "remanente_002": rem['002'],
        "remanente_003": rem['003'], "remanente_005": rem['005'],
        "remanente_013": rem['013'], "remanente_016": rem['016'],
        "codigo_rubro":           (row[14] or '').strip(),
        "codigo_superrubro":      (row[15] or '').strip(),
        "codigo_gruposuperrubro": (row[16] or '').strip(),
        "dto_max": dto_max,
        "permite_stock_negativo": str(row[17] or '0').strip() == '1',
        "es_conjunto": str(row[18] or '').strip().upper() == 'C'
    }
    # Aplicar reservas activas: descuenta remanente_XXX por depósito
    _apply_reservas([item], _get_reservas_activas(), rem_key='remanente_001')
    item.pop('reservado', None)
    item.pop('reservado_por_deposito', None)
    item.pop('codigo_rubro', None)
    item.pop('codigo_superrubro', None)
    item.pop('codigo_gruposuperrubro', None)
    return item

# ─── Clientes (solo del vendedor) ─────────────────────────────────────────────
@app.get("/debug/cliente-iva/{codigo}")
def debug_cliente_iva(codigo: str, db: str = Query("oficial")):
    """Debug: columnas de CLIENTES + campos IVA de CABEZAPRESUPUESTOS."""
    db_path = DATABASE if db in ('oficial','l1') else (DATABASE_EST if db == 'est' else DATABASE_MLT)
    resultado = {}
    try:
        c = conn('WIN1252', db=db_path)
        cur = c.cursor()

        # 1) Ver columnas reales de CLIENTES (primer registro)
        try:
            cur.execute('SELECT FIRST 1 * FROM "CLIENTES"')
            row0 = cur.fetchone()
            cols0 = [d[0] for d in cur.description]
            resultado['columnas_CLIENTES'] = cols0
            resultado['primer_registro_sample'] = {cols0[i]: str(row0[i] or '').strip() for i in range(len(cols0))} if row0 else {}
        except Exception as e1:
            resultado['error_CLIENTES'] = str(e1)

        # 2) Buscar el cliente por código (puede ser int o char)
        try:
            cur.execute('SELECT * FROM "CLIENTES" WHERE CODIGOCLIENTE = ?', (int(codigo),))
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                resultado['cliente'] = {cols[i]: str(row[i] or '').strip() for i in range(len(cols))}
            else:
                resultado['cliente'] = f'No encontrado con codigo={codigo}'
        except Exception as e2:
            resultado['error_buscar_cliente'] = str(e2)

        # 3) Ver CABEZAPRESUPUESTOS del cliente — campos IVA
        try:
            cur.execute(
                'SELECT FIRST 5 NROPRESUPUESTO, CODIGOCLIENTE, COEFICIENTEIVA, '
                'DESCUENTOPORCENTAJE, DESCUENTOMONTO, DESCUENTODESCRIPCION, TOTAL '
                'FROM "CABEZAPRESUPUESTOS" WHERE CODIGOCLIENTE = ? '
                'ORDER BY NROPRESUPUESTO DESC', (int(codigo),)
            )
            rows_p = cur.fetchall()
            cols_p = [d[0] for d in cur.description]
            resultado['ultimos_presupuestos'] = [
                {cols_p[i]: str(r[i] or '').strip() for i in range(len(cols_p))} for r in rows_p
            ]
        except Exception as e3:
            resultado['error_presupuestos'] = str(e3)

        c.close()
    except Exception as e:
        resultado['error_conexion'] = str(e)
    return resultado

@app.get("/clientes")
def get_clientes(
    vendedor: Optional[str] = None,
    buscar: Optional[str] = None,
    limit: int = Query(100, le=300),
    offset: int = 0,
    _user=Depends(get_current_user)
):
    params = []
    where_vendedor = ""
    if vendedor:
        where_vendedor = "AND CODIGOVENDEDOR = ?"
        params = [vendedor.upper()]

    where_buscar = ""
    if buscar:
        where_buscar = "AND (UPPER(RAZONSOCIAL) CONTAINING UPPER(?) OR CODIGOCLIENTE CONTAINING ?)"
        params += [buscar, buscar]

    try:
        c1 = conn()
        cur1 = c1.cursor()
        cur1.execute(f"""
            SELECT FIRST {limit} SKIP {offset}
                CODIGOCLIENTE, RAZONSOCIAL, NOMBREFANTASIA, CUIT,
                TELEFONO, TELEFONOCELULAR, EMAIL, DIRECCION, LOCALIDAD,
                CODIGOPARTICULAR, REPARTOPROPIO, CONDICIONIVA,
                CODIGOMULTIPLAZO, MULTIPLAZOFIJO, COMENTARIOS,
                LIMITECREDITO, LIMITECREDITODOC
            FROM "CLIENTES"
            WHERE ACTIVO = '1' {where_vendedor}
                {where_buscar}
            ORDER BY RAZONSOCIAL
        """, params)
        rows = cur1.fetchall()
        c1.close()
        return [{
            "codigo": r[0], "razonsocial": r[1], "fantasia": r[2],
            "cuit":   r[3], "telefono":   r[4], "celular":  r[5],
            "email":  r[6], "direccion":  r[7], "localidad": r[8],
            "codigoparticular": (r[9] or "").strip(),
            "tipoiva": (r[11] or "").strip(),
            "discrimina_iva": (r[11] or "").strip().upper() == 'RI',
            "reparto_propio": str(r[10] or '0').strip() == '1',
            "codigomultiplazo": str(r[12] or '0').strip(),
            "multiplazofijo": int(r[13] or 0),
            "comentarios": (r[14] or '').strip(),
            "limitecredito": float(r[15] or 0),
            "limitecreditodoc": float(r[16] or 0),
        } for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clientes: {e}")

@app.get("/clientes/{codigo}")
def get_cliente(codigo: str):
    c = conn()
    cur = c.cursor()
    cur.execute(
        'SELECT CODIGOCLIENTE, RAZONSOCIAL, NOMBREFANTASIA, CUIT, '
        'TELEFONO, TELEFONOCELULAR, EMAIL, DIRECCION, LOCALIDAD, COMENTARIOS '
        'FROM "CLIENTES" WHERE CODIGOCLIENTE = ? OR CODIGOPARTICULAR = ?', (codigo, codigo)
    )
    row = cur.fetchone()
    c.close()
    if not row:
        raise HTTPException(404, "Cliente no encontrado")
    return {
        "codigo": row[0], "razonsocial": row[1], "fantasia": row[2],
        "cuit": row[3], "telefono": row[4], "celular": row[5],
        "email": row[6], "direccion": row[7], "localidad": row[8],
        "comentarios": row[9]
    }

@app.get("/debug/cliente/{codigo}")
def debug_cliente(codigo: str):
    """Muestra todas las columnas de CLIENTES para un cliente dado (busca en todas las DBs)."""
    _DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    for db_path in [DATABASE, _DB_PROD]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            cur.execute(
                'SELECT FIRST 1 * FROM "CLIENTES" WHERE CODIGOCLIENTE = ? '
                'OR TRIM(CODIGOCLIENTE) = ? OR RAZONSOCIAL CONTAINING ?',
                (codigo, codigo.strip(), codigo)
            )
            row = cur.fetchone()
            if not row:
                c.close(); continue
            cols = [d[0] for d in cur.description]
            data = {}
            for k, v in zip(cols, row):
                if v is not None:
                    data[k] = str(v).strip() if isinstance(v, str) else v
            c.close()
            keywords = ['MULTI','PLAZO','COMENT','LIMITE','CREDITO','FIJO','INHAB','DESHAB']
            filtrado = {k: v for k, v in data.items() if any(kw in k.upper() for kw in keywords)}
            return {"db": db_path, "filtrado": filtrado, "todos_los_campos": list(cols)}
        except Exception as e:
            continue
    return {"error": "cliente no encontrado en ninguna DB"}

@app.get("/debug/transportes")
def debug_transportes():
    """Diagnóstico: muestra qué transportes devuelve cada DB y el resultado del merge."""
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    resultado = {}
    rows_map: dict = {}
    for label, db_path in [("prueba", DATABASE), ("microbell", DB_PROD)]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            cur.execute('SELECT CODIGOTRANSPORTE, DESCRIPCION FROM "TRANSPORTES" ORDER BY DESCRIPCION')
            rows = cur.fetchall()
            c.close()
            items = [{"codigo": r[0], "descripcion": (r[1] or '').strip()} for r in rows if r[0]]
            resultado[label] = {"count": len(items), "transportes": items}
            for r in rows:
                cod = r[0]
                if cod is not None and str(cod).strip() not in ('', '0') and cod not in rows_map:
                    rows_map[cod] = (r[1] or '').strip()
        except Exception as e:
            resultado[label] = {"error": str(e)}
    merged = sorted(
        [{"codigo": cod, "descripcion": desc} for cod, desc in rows_map.items()],
        key=lambda x: x["descripcion"]
    )
    resultado["merged"] = {"count": len(merged), "transportes": merged}
    resultado["codigo_450"] = rows_map.get(450) or rows_map.get('450') or "NO ENCONTRADO"
    return resultado

@app.get("/debug/campos_bonif")
def debug_campos_bonif():
    """Muestra campos DTO/BONIF de ARTICULOS y campos de permiso de PERFILES."""
    c = conn('WIN1252')
    cur = c.cursor()
    res = {}
    try:
        cur.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME='ARTICULOS' AND (RDB$FIELD_NAME CONTAINING 'DTO' OR RDB$FIELD_NAME CONTAINING 'BONIF' OR RDB$FIELD_NAME CONTAINING 'MAXIMO') ORDER BY RDB$FIELD_POSITION")
        res["articulos_dto_bonif"] = [r[0] for r in cur.fetchall()]
        # Buscar en USUARIOS los campos de permisos
        cur.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME='USUARIOS' AND (RDB$FIELD_NAME CONTAINING 'PORCENTAJE' OR RDB$FIELD_NAME CONTAINING 'MAXIMO' OR RDB$FIELD_NAME CONTAINING 'INCREMENT' OR RDB$FIELD_NAME CONTAINING 'DECREMENT' OR RDB$FIELD_NAME CONTAINING 'BONIF') ORDER BY RDB$FIELD_POSITION")
        res["usuarios_permisos_campos"] = [r[0] for r in cur.fetchall()]
        # Valores de un usuario ADV como ejemplo (primer usuario ADV activo)
        cur.execute('SELECT FIRST 1 CODIGOUSUARIO, CODIGOPERFIL FROM "USUARIOS" WHERE CODIGOPERFIL=? AND ACTIVO=?', ('ADV', '1'))
        row_u = cur.fetchone()
        if row_u:
            uid = row_u[0]
            cur2 = c.cursor()
            cur2.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME='USUARIOS' ORDER BY RDB$FIELD_POSITION")
            cols_u = [r[0] for r in cur2.fetchall()]
            cur3 = c.cursor()
            cur3.execute('SELECT * FROM "USUARIOS" WHERE CODIGOUSUARIO=?', (uid,))
            row_vals = cur3.fetchone()
            if row_vals:
                res["usuario_ADV_sample"] = {k: str(v) for k, v in zip(cols_u, row_vals) if any(x in k for x in ['PORCENTAJE','MAXIMO','INCREMENT','DECREMENT','BONIF','CODIGO','PERFIL'])}
    except Exception as e:
        res["error"] = str(e)
    finally:
        c.close()
    return res

@app.get("/debug/sucursales/{codigo}")
def debug_sucursales(codigo: str):
    """Diagnóstico: muestra el paso a paso de resolución de sucursales para un cliente."""
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    resultado = {"codigo_recibido": codigo, "pasos": []}

    # Paso 1: DB-Prueba
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute('SELECT CODIGOCLIENTE, CODIGOPARTICULAR, DIRECCION, RAZONSOCIAL '
                    'FROM "CLIENTES" WHERE CODIGOCLIENTE = ? OR CODIGOPARTICULAR = ?',
                    (codigo, codigo))
        r = cur.fetchone()
        c.close()
        resultado["pasos"].append({"db": "DB-Prueba", "encontrado": r is not None,
            "fila": [str(x) for x in r] if r else None})
        codigoparticular = (r[1] or '').strip() if r else codigo
    except Exception as e:
        resultado["pasos"].append({"db": "DB-Prueba", "error": str(e)})
        codigoparticular = codigo

    resultado["codigoparticular_resuelto"] = codigoparticular

    # Paso 2: DB-Microbell - resolver CODIGOCLIENTE
    try:
        c = conn('WIN1252', db=DB_PROD)
        cur = c.cursor()
        cur.execute('SELECT CODIGOCLIENTE, CODIGOPARTICULAR, DIRECCION, RAZONSOCIAL '
                    'FROM "CLIENTES" WHERE CODIGOPARTICULAR = ? OR CODIGOCLIENTE = ?',
                    (codigoparticular, codigoparticular))
        r = cur.fetchone()
        resultado["pasos"].append({"db": "DB-Microbell CLIENTES", "encontrado": r is not None,
            "fila": [str(x) for x in r] if r else None})
        cod_mb = str(r[0]).strip() if r else codigoparticular
        resultado["codigocliente_microbell"] = cod_mb

        # Paso 3: transporte a nivel cliente en CLIENTES
        cur.execute('SELECT CODIGOTRANSPORTE, TRANSPORTEFIJO, REPARTOPROPIO '
                    'FROM "CLIENTES" WHERE CODIGOCLIENTE = ?', (cod_mb,))
        r_cli2 = cur.fetchone()
        cols_cli2 = [d[0] for d in cur.description] if cur.description else []
        resultado["clientes_columnas_transporte"] = [c for c in cols_cli2 if 'TRANS' in c.upper() or 'REPARTO' in c.upper()]
        transp_cli = str(r_cli2[0]).strip() if r_cli2 and r_cli2[0] else None
        transp_fijo = str(r_cli2[1] or '0').strip() == '1' if r_cli2 else False
        if r_cli2:
            resultado["clientes_transporte_valores"] = {
                "CODIGOTRANSPORTE": transp_cli, "TRANSPORTEFIJO": transp_fijo
            }

        # Paso 4: SUCURSALESXCLIENTES
        cur.execute('SELECT CODIGOSUCURSAL, NOMBRE, DIRECCION, CODIGOTRANSPORTE '
                    'FROM "SUCURSALESXCLIENTES" WHERE CODIGOCLIENTE = ? ORDER BY CODIGOSUCURSAL',
                    (cod_mb,))
        rows = cur.fetchall()
        resultado["sucursales_count"] = len(rows)
        resultado["sucursales"] = [{"cod": str(r[0]), "nombre": str(r[1]), "dir": str(r[2]),
                                    "transp_suc": str(r[3]) if r[3] else None,
                                    "transp_efectivo": str(r[3]).strip() if r[3] else transp_cli,
                                    "fijo": transp_fijo} for r in rows]
        c.close()
    except Exception as e:
        resultado["pasos"].append({"db": "DB-Microbell", "error": str(e)})

    return resultado


@app.get("/debug/despachos/{tipo}/{numero}")
def debug_despachos(tipo: str, numero: str):
    """Busca despachos para un comprobante en todas las tablas posibles."""
    result = {}
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    for db_label, db_path in [('prueba', DATABASE), ('prod', DB_PROD)]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            # 1. Columnas de CUERPOCOMPROBANTES
            try:
                cur.execute("SELECT TRIM(f.RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS f WHERE f.RDB$RELATION_NAME='CUERPOCOMPROBANTES' ORDER BY f.RDB$FIELD_POSITION")
                result[f'{db_label}_cuerpo_cols'] = [r[0] for r in cur.fetchall()]
            except Exception as e:
                result[f'{db_label}_cuerpo_cols_err'] = str(e)
            # 2. Primera fila del comprobante (todos los campos)
            try:
                cur.execute('SELECT FIRST 1 * FROM "CUERPOCOMPROBANTES" WHERE TIPOCOMPROBANTE=? AND NUMEROCOMPROBANTE=?', (tipo, numero))
                row = cur.fetchone()
                if row:
                    cols = [d[0] for d in cur.description]
                    result[f'{db_label}_cuerpo_row'] = dict(zip(cols, [str(v) for v in row]))
            except Exception as e:
                result[f'{db_label}_cuerpo_row_err'] = str(e)
            # 3. Buscar tablas con "DESP" en el nombre
            try:
                cur.execute("SELECT DISTINCT TRIM(r.RDB$RELATION_NAME) FROM RDB$RELATIONS r WHERE r.RDB$RELATION_NAME LIKE '%DESP%' ORDER BY 1")
                result[f'{db_label}_tablas_desp'] = [r[0] for r in cur.fetchall()]
            except Exception as e:
                result[f'{db_label}_tablas_desp_err'] = str(e)
            # 4. Intentar cada tabla DESP encontrada
            for tbl in result.get(f'{db_label}_tablas_desp', []):
                try:
                    cur.execute(f'SELECT FIRST 3 * FROM "{tbl}"')
                    cols2 = [d[0] for d in cur.description]
                    rows2 = cur.fetchall()
                    result[f'{db_label}_{tbl}_cols'] = cols2
                    result[f'{db_label}_{tbl}_sample'] = [dict(zip(cols2,[str(v) for v in r])) for r in rows2]
                    # Buscar si tiene NUMEROCOMPROBANTE o TIPOCOMPROBANTE
                    if any(c in cols2 for c in ['NUMEROCOMPROBANTE','NROCOMPROBANTE']):
                        num_col = 'NUMEROCOMPROBANTE' if 'NUMEROCOMPROBANTE' in cols2 else 'NROCOMPROBANTE'
                        cur.execute(f'SELECT FIRST 5 * FROM "{tbl}" WHERE {num_col}=?', (numero,))
                        rows3 = cur.fetchall()
                        result[f'{db_label}_{tbl}_match'] = [dict(zip(cols2,[str(v) for v in r])) for r in rows3]
                except Exception as e:
                    result[f'{db_label}_{tbl}_err'] = str(e)
            c.close()
        except Exception as e:
            result[f'{db_label}_conn_err'] = str(e)
    return result


@app.get("/debug/cae/{numero}")
def debug_cae(numero: str):
    """Busca el CAE para un comprobante en tablas conocidas de Flexxus."""
    result = {}
    # NUMEROCOMPROBANTE almacenado como float sin formato
    num_float = numero  # el caller pasa el número tal como viene del comp_cols
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        # 1. Buscar en CAMPOSDINAMICOSCOMPROBANTES
        try:
            cur.execute(
                'SELECT CODIGOCAMPODINAMICO, VALOR FROM "CAMPOSDINAMICOSCOMPROBANTES" '
                'WHERE NUMEROCOMPROBANTE = ?', (num_float,)
            )
            result['CAMPOSDINAMICOS'] = [{"cod": r[0], "valor": str(r[1])} for r in cur.fetchall()]
        except Exception as e:
            result['CAMPOSDINAMICOS'] = str(e)
        # 2. Buscar en COMPROBANTESCAE si existe
        try:
            cur.execute(
                'SELECT * FROM "COMPROBANTESCAE" WHERE NUMEROCOMPROBANTE = ?', (num_float,)
            )
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                result['COMPROBANTESCAE'] = dict(zip(cols, [str(v) for v in row]))
            else:
                result['COMPROBANTESCAE'] = 'no encontrado'
        except Exception as e:
            result['COMPROBANTESCAE'] = str(e)
        # 3. Buscar tablas que contengan "CAE" en campos o nombre
        try:
            cur.execute(
                "SELECT DISTINCT f.RDB$RELATION_NAME "
                "FROM RDB$RELATION_FIELDS f "
                "WHERE f.RDB$FIELD_NAME CONTAINING 'CAE' "
                "AND f.RDB$SYSTEM_FLAG = 0"
            )
            result['tablas_con_CAE'] = [r[0].strip() for r in cur.fetchall()]
        except Exception as e:
            result['tablas_con_CAE'] = str(e)
        # 4. Consultar CAEAFIP
        try:
            cur.execute('SELECT FIRST 1 * FROM "CAEAFIP" WHERE NUMEROCOMPROBANTE = ?', (num_float,))
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                result['CAEAFIP'] = dict(zip(cols, [str(v) if v is not None else None for v in row]))
            else:
                # Mostrar estructura + últimos registros para ver formato
                cur.execute('SELECT FIRST 3 * FROM "CAEAFIP" ORDER BY 1 DESC')
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                result['CAEAFIP'] = {
                    'sin_dato_para_numero': num_float,
                    'muestra': [dict(zip(cols, [str(v) if v is not None else None for v in r])) for r in rows]
                }
        except Exception as e:
            result['CAEAFIP'] = str(e)
        c.close()
    except Exception as e:
        result['error'] = str(e)
    return result

@app.get("/debug/comp_cols/{numero}")
def debug_comp_cols(numero: str, db: str = Query("oficial")):
    """Muestra TODOS los campos de CABEZACOMPROBANTES para un comprobante dado."""
    db_path = DATABASE_MLT if db == 'sw' else DATABASE
    try:
        c = conn('WIN1252', db=db_path)
        cur = c.cursor()
        # Intentar búsqueda exacta primero, luego por LIKE
        cur.execute(
            'SELECT FIRST 1 * FROM "CABEZACOMPROBANTES" '
            'WHERE NUMEROCOMPROBANTE = ? OR NUMEROCOMPROBANTE LIKE ?',
            (numero, f'%{numero}%')
        )
        row = cur.fetchone()
        if not row:
            # Devolver los últimos 5 comprobantes para ver el formato real
            cur.execute(
                'SELECT FIRST 5 TIPOCOMPROBANTE, NUMEROCOMPROBANTE, FECHACOMPROBANTE '
                'FROM "CABEZACOMPROBANTES" ORDER BY FECHACOMPROBANTE DESC'
            )
            sample = cur.fetchall()
            c.close()
            return {"error": f"No encontrado", "formato_muestra": [
                {"tipo": r[0], "numero": r[1], "fecha": str(r[2])} for r in sample
            ]}
        cols = [d[0] for d in cur.description]
        c.close()
        return dict(zip(cols, [str(v) if v is not None else None for v in row]))
    except Exception as e:
        return {"error": str(e)}

# ─── Cuenta Corriente ──────────────────────────────────────────────────────────
def _resolve_codigos_en_db(db_path, codigoparticular):
    """Dado un CODIGOPARTICULAR, devuelve los CODIGOCLIENTEs que lo usan en esa BD via CUERPOCOMPROBANTES."""
    if not codigoparticular or not str(codigoparticular).strip():
        return []
    try:
        c = conn('LATIN1', db=db_path)
        cur = c.cursor()
        cur.execute(
            'SELECT DISTINCT CODIGOCLIENTE FROM "CUERPOCOMPROBANTES" WHERE CODIGOPARTICULAR = ?',
            (str(codigoparticular).strip(),)
        )
        rows = cur.fetchall()
        c.close()
        return [str(r[0]).strip() for r in rows if r[0] and str(r[0]).strip()]
    except Exception:
        return []

def _query_cta_part(db_path, codigos_base, codigoparticular, limit, offset):
    """Como _query_cta pero resuelve el CODIGOCLIENTE correcto para esta BD vía CODIGOPARTICULAR."""
    codigos = set(c for c in codigos_base if c)
    codigos.update(_resolve_codigos_en_db(db_path, codigoparticular))
    if not codigos:
        return []
    return _query_cta(db_path, list(codigos), limit, offset)

def _get_cambios(db_path):
    """Retorna dict {CODIGOMONEDA: CAMBIO} desde tabla MONEDAS."""
    try:
        c = conn('WIN1252', db=db_path)
        cur = c.cursor()
        cur.execute('SELECT CODIGOMONEDA, CAMBIO FROM "MONEDAS"')
        result = {str(r[0]).strip(): float(r[1] or 1) for r in cur.fetchall()}
        c.close()
        return result
    except Exception:
        return {}

def _query_cta(db_path, codigos, limit, offset, vendedor=None):
    """Consulta CABEZACOMPROBANTES sin CAST/JOIN en SQL. Conversión de moneda en Python.
    Usa fetchone() en loop para tolerar registros individuales con valores problemáticos."""
    cambios = _get_cambios(db_path)
    result = []
    try:
        c = conn('WIN1252', db=db_path)
        cur = c.cursor()
        ph = ', '.join(['?'] * len(codigos))
        params = list(codigos)
        _NC_TIPOS = "('NCA','NCB','NCCA','NCCB','NCE','NCCE','SIV','NDA','NDB','NDCA','NDCB')"
        if vendedor:
            # FAs y NCs: ambas filtradas por CODIGOUSUARIO del vendedor logueado
            cur.execute(f"""
                SELECT FIRST {limit} SKIP {offset}
                    TIPOCOMPROBANTE, NUMEROCOMPROBANTE, FECHACOMPROBANTE,
                    TOTAL, IVA1, IVA2, PAGADO, COTIZACION, CODIGOMONEDA,
                    FECHAVENCIMIENTO, CLASECOMPROBANTE
                FROM "CABEZACOMPROBANTES"
                WHERE CODIGOCLIENTE IN ({ph})
                  AND ANULADA = '0'
                  AND TIPOCOMPROBANTE NOT IN ('RE', 'RI', 'INA')
                  AND UPPER(CODIGOUSUARIO) = ?
                  AND (CUENTACORRIENTE = '1' OR TIPOCOMPROBANTE IN {_NC_TIPOS})
                ORDER BY FECHAVENCIMIENTO ASC, FECHACOMPROBANTE ASC
            """, tuple(params + [vendedor.upper()]))
        else:
            cur.execute(f"""
                SELECT FIRST {limit} SKIP {offset}
                    TIPOCOMPROBANTE, NUMEROCOMPROBANTE, FECHACOMPROBANTE,
                    TOTAL, IVA1, IVA2, PAGADO, COTIZACION, CODIGOMONEDA,
                    FECHAVENCIMIENTO, CLASECOMPROBANTE
                FROM "CABEZACOMPROBANTES"
                WHERE CODIGOCLIENTE IN ({ph})
                  AND ANULADA = '0'
                  AND TIPOCOMPROBANTE NOT IN ('RE', 'RI', 'INA')
                  AND (CUENTACORRIENTE = '1' OR TIPOCOMPROBANTE IN {_NC_TIPOS})
                ORDER BY FECHAVENCIMIENTO ASC, FECHACOMPROBANTE ASC
            """, tuple(params))
        while True:
            try:
                r = cur.fetchone()
                if r is None:
                    break
                tipo   = r[0]; num   = r[1]; fecha = r[2]
                total  = float(r[3] or 0)
                iva1   = float(r[4] or 0)
                iva2   = float(r[5] or 0)
                pagado = float(r[6] or 0)
                cotiz  = float(r[7] or 1) or 1.0
                moneda = str(r[8] or '').strip()
                fvto   = r[9]; clase = r[10]
                neto   = total + iva1 + iva2
                debe   = neto - pagado
                cambio = cambios.get(moneda, 1.0) or 1.0
                deuda  = debe * cambio / cotiz
                if abs(deuda) >= 0.01:
                    result.append((tipo, num, fecha, neto, pagado, deuda, fvto, clase))
            except Exception:
                continue
        c.close()
    except Exception:
        pass
    return result

@app.get("/clientes/{codigo}/credito")
def get_credito_cliente(codigo: str):
    """Retorna límites de crédito y saldo deudor actual del cliente."""
    DB_PROD     = DATABASE      # DB-Prueba.gdb
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'  # SW producción
    lim_cred = lim_doc = 0.0
    cod_real = codigo
    part_real = None
    _db_fuente = None
    _db_errores = {}
    for db_path in [DATABASE, DB_PROD]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            cur.execute(
                'SELECT LIMITECREDITO, LIMITECREDITODOC, CODIGOCLIENTE, '
                'MULTIPLAZOFIJO, CODIGOMULTIPLAZO, CODIGOPARTICULAR '
                'FROM "CLIENTES" WHERE CODIGOCLIENTE = ? OR CODIGOPARTICULAR = ?',
                (codigo, codigo)
            )
            row = cur.fetchone()
            c.close()
            if row:
                lim_cred       = float(row[0] or 0)
                lim_doc        = float(row[1] or 0)
                cod_real       = str(row[2]).strip()
                multiplazofijo = int(row[3] or 0)
                codigomulti    = str(row[4] or '0').strip()
                part_real      = str(row[5] or '').strip() or None
                _db_fuente     = db_path
                break
        except Exception as _e:
            _db_errores[db_path] = str(_e)
    # Saldo deudor: solo CODIGOCLIENTE (nunca mezclar con CODIGOPARTICULAR en CABEZACOMPROBANTES)
    saldo_deudor = 0.0
    seen_sd = set()
    for db_path in [DB_PROD, DB_MLT_PROD]:
        try:
            rows = _query_cta(db_path, [cod_real], 500, 0)
            for r in rows:
                key = (r[0], r[1])
                if key not in seen_sd:
                    seen_sd.add(key)
                    saldo_deudor += float(r[5] or 0)
        except Exception:
            pass
    saldo_deudor = round(saldo_deudor, 2)
    # Pedidos "A preparar": OPERACION='1' (Flexxus setea FECHATERMINADA al confirmar,
    # no al entregar — por eso filtramos por OPERACION, no por FECHATERMINADA IS NULL).
    # TOTAL en Flexxus = monto bruto con IVA incluido.
    pedidos_pendientes = 0.0
    for db_path in [DATABASE, DB_PROD]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            # Excluye pedidos que ya tienen remito (CANTIDADREMITIDA > 0 en alguna línea)
            # aunque Flexxus no haya cambiado el OPERACION manualmente.
            cur.execute(
                'SELECT COALESCE(SUM(cb.TOTAL), 0) FROM "CABEZAPEDIDOS" cb '
                'WHERE cb.TIPOCOMPROBANTE = ? AND cb.CODIGOCLIENTE = ? '
                'AND cb.ANULADA = ? AND cb.OPERACION = ? '
                'AND NOT EXISTS ('
                '  SELECT 1 FROM "CUERPOPEDIDOS" cue '
                '  WHERE cue.TIPOCOMPROBANTE = ? '
                '    AND cue.NUMEROCOMPROBANTE = cb.NUMEROCOMPROBANTE '
                '    AND cue.CANTIDADREMITIDA > 0'
                ')',
                ('NP', cod_real, '0', '1', 'NP')
            )
            row = cur.fetchone()
            c.close()
            if row:
                pedidos_pendientes += float(row[0] or 0)
                break  # BD operativa encontrada, no sumar la otra
        except Exception:
            pass
    pedidos_pendientes = round(pedidos_pendientes, 2)
    disponible_total = round(max(0, lim_cred + lim_doc - saldo_deudor - pedidos_pendientes), 2)
    return {
        "limitecredito":      lim_cred,
        "limitecreditodoc":   lim_doc,
        "saldo_deudor":       saldo_deudor,
        "pedidos_pendientes": pedidos_pendientes,
        "disponible_cred":    round(max(0, lim_cred - saldo_deudor), 2),
        "disponible_doc":     round(max(0, lim_doc  - saldo_deudor), 2),
        "disponible_total":   disponible_total,
        "multiplazofijo":     multiplazofijo,
        "codigomultiplazo":   codigomulti,
        "_db_fuente":         _db_fuente,
        "_db_errores":        _db_errores,
    }


@app.get("/clientes/{codigo}/cuenta_corriente")
def cuenta_corriente(codigo: str, limit: int = Query(200, le=500), offset: int = 0, _user=Depends(get_current_user)):
    # Lookup en DB_PROD (igual que resumen-deudas)
    DB_PROD      = DATABASE                              # DB-Prueba.gdb
    DB_MLT_PROD  = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'  # SW producción
    c_cli = conn('WIN1252', db=DB_PROD)
    cur_cli = c_cli.cursor()
    cur_cli.execute(
        'SELECT CODIGOCLIENTE, CODIGOPARTICULAR FROM "CLIENTES" '
        'WHERE CODIGOCLIENTE = ? OR CODIGOPARTICULAR = ?',
        (codigo, codigo)
    )
    cli = cur_cli.fetchone()
    c_cli.close()
    # Misma lógica que PDF: buscar con AMBOS CODIGOCLIENTE y CODIGOPARTICULAR
    codigos_set = set()
    if cli:
        if cli[0] and cli[0].strip(): codigos_set.add(cli[0].strip())
        if cli[1] and cli[1].strip(): codigos_set.add(cli[1].strip())
    if not codigos_set:
        codigos_set.add(codigo)
    codigos = list(codigos_set)

    # Fetch ALL rows con límite SQL amplio; la paginación se aplica en Python
    # después del filtro de deuda para no cortar registros como NDAs tardías.
    SQL_LIMIT = 2000
    rows_prod     = _query_cta(DB_PROD,     codigos, SQL_LIMIT, 0)
    rows_mlt_prod = _query_cta(DB_MLT_PROD, codigos, SQL_LIMIT, 0)

    # Combinar y deduplicar por (tipo, numero)
    seen = set()
    combined = []
    for r in rows_prod + rows_mlt_prod:
        key = (r[0], r[1])  # tipo + numero
        if key not in seen:
            seen.add(key)
            combined.append(r)

    # Ordenar por fecha vencimiento
    combined.sort(key=lambda r: (r[6] or r[2], r[2]))

    # Paginación Python post-filtro
    combined = combined[offset: offset + limit]

    return [{
        "tipo":      r[0], "numero":    r[1], "fecha":   r[2],
        "total":     float(r[3]) if r[3] else 0,
        "pagado":    float(r[4]) if r[4] else 0,
        "deuda":     float(r[5]) if r[5] else 0,
        "fecha_vto": r[6].isoformat() if r[6] else None, "clase": r[7],
    } for r in combined]


@app.get("/que-vendi/clientes")
def que_vendi_clientes(vendedor: str, buscar: Optional[str] = None, _user=Depends(get_current_user)):
    """Clientes del vendedor para autocomplete en ¿Qué Vendí?"""
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    params = [vendedor.upper()]
    where_buscar = ""
    if buscar:
        where_buscar = "AND (UPPER(RAZONSOCIAL) CONTAINING UPPER(?) OR CODIGOCLIENTE CONTAINING ? OR CODIGOPARTICULAR CONTAINING ?)"
        params += [buscar, buscar, buscar]
    try:
        c = conn('WIN1252', DB_PROD)
        cur = c.cursor()
        cur.execute(f"""
            SELECT FIRST 30 CODIGOCLIENTE, RAZONSOCIAL, CODIGOPARTICULAR
            FROM "CLIENTES"
            WHERE ACTIVO = '1' AND UPPER(CODIGOVENDEDOR) = ?
            {where_buscar}
            ORDER BY RAZONSOCIAL
        """, params)
        rows = cur.fetchall()
        c.close()
        return [{"codigo": (r[0] or '').strip(),
                 "razonsocial": (r[1] or '').strip(),
                 "codigoparticular": (r[2] or '').strip()} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/que-vendi")
def que_vendi(
    vendedor: str,
    cliente: str,
    desde: Optional[str] = None,
    hasta: Optional[str] = None,
    limit: int = Query(500, le=2000),
    offset: int = 0,
    _user=Depends(get_current_user)
):
    """Artículos facturados a un cliente. Facturas en BDs Prueba; lookup de código en todas las BDs."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
    DB_PROD     = 'c:/flexxus/DB/DB-Microbell.gdb'
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    # Mismas 4 BDs que Cuenta Corriente — prueba + producción, L1 + SW
    DBS_FACT   = [DATABASE, DATABASE_MLT, DB_PROD, DB_MLT_PROD]
    # Lookup solo en las BDs que tienen tabla CLIENTES
    DBS_LOOKUP = [DATABASE, DB_PROD]

    # ── Lookup de códigos del cliente ────────────────────────────────────────
    def _lookup_codigos(db_path):
        try:
            c = conn('WIN1252', db_path)
            cur = c.cursor()
            cur.execute(
                'SELECT CODIGOCLIENTE, CODIGOPARTICULAR FROM "CLIENTES" '
                'WHERE CODIGOCLIENTE = ? OR CODIGOPARTICULAR = ?',
                (cliente, cliente)
            )
            row = cur.fetchone()
            c.close()
            if row:
                # str() porque CODIGOCLIENTE puede ser INTEGER en Firebird
                return [str(v).strip() for v in row if v is not None and str(v).strip()]
        except Exception:
            pass
        return []

    codigos = set([str(cliente).strip()])
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(_lookup_codigos, db): db for db in DBS_LOOKUP}
        for f in futs:
            try:
                for cod in f.result(timeout=8):
                    codigos.add(cod)
            except FutureTimeout:
                pass
    codigos = list(codigos)

    # WHERE dinámico — filtra por CODIGOCLIENTE usando todos los códigos resueltos
    ph = ','.join('?' * len(codigos))
    TIPOS_FACTURA = ("('FA','FB','FE','FCA','FCB','FCE',"
                     "'FCCA','FCCB','FCCE',"
                     "'NCA','NCB','NCCA','NCCB')")
    TIPOS_NC = {'NCA', 'NCB', 'NCCA', 'NCCB'}   # valores se negarán
    params_extra = []
    where_parts = [f"cb.CODIGOCLIENTE IN ({ph})",
                   "cb.ANULADA = '0'",
                   f"cb.TIPOCOMPROBANTE IN {TIPOS_FACTURA}"]
    if desde:
        where_parts.append("cb.FECHACOMPROBANTE >= ?")
        params_extra.append(desde)
    if hasta:
        where_parts.append("cb.FECHACOMPROBANTE <= ?")
        params_extra.append(hasta)
    params_base = list(codigos) + params_extra
    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT FIRST {limit} SKIP {offset}
            COALESCE(NULLIF(TRIM(cu.CODIGOPARTICULAR),''),
                     TRIM(cu.CODIGOARTICULO))             AS COD_ART,
            COALESCE(NULLIF(TRIM(cu.DESCRIPCION),''), '') AS DESCR,
            cb.FECHACOMPROBANTE,
            cb.TIPOCOMPROBANTE,
            cb.NUMEROCOMPROBANTE,
            CAST(cu.CANTIDAD       AS DOUBLE PRECISION)  AS CANT,
            CAST(cu.PRECIOUNITARIO AS DOUBLE PRECISION)  AS PU,
            CAST(cu.PRECIOTOTAL    AS DOUBLE PRECISION)  AS SUBTOTAL,
            COALESCE(CAST(cu.PORCENTAJEIVA AS DOUBLE PRECISION), 21) AS IVA_PCT
        FROM "CUERPOCOMPROBANTES" cu
        JOIN "CABEZACOMPROBANTES" cb
             ON cb.TIPOCOMPROBANTE   = cu.TIPOCOMPROBANTE
            AND cb.NUMEROCOMPROBANTE = cu.NUMEROCOMPROBANTE
        WHERE {where_sql}
        ORDER BY cb.FECHACOMPROBANTE DESC, cb.NUMEROCOMPROBANTE DESC
    """

    # ── Consulta paralela a las 4 BDs, timeout 25s por BD ────────────────────
    _db_errors = {}
    def _query_db(db_path):
        try:
            c = conn('LATIN1', db=db_path)
            cur = c.cursor()
            cur.execute(sql, params_base)
            rows = cur.fetchall()
            c.close()
            _QV_LAST_COUNTS[db_path] = len(rows)
            return rows
        except Exception as _e:
            _db_errors[db_path] = str(_e)
            _QV_LAST_ERRORS[db_path] = str(_e)
            _QV_LAST_COUNTS[db_path] = f"ERROR: {_e}"
            return []

    all_rows = []
    seen = set()
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_query_db, db): db for db in DBS_FACT}
        for f in futs:
            try:
                for r in f.result(timeout=25):
                    key = (str(r[3]).strip(), str(r[4]).strip(), str(r[0]).strip())
                    if key not in seen:
                        seen.add(key)
                        all_rows.append(r)
            except FutureTimeout:
                pass

    def _fmt(v):
        if hasattr(v, 'strftime'):
            return v.strftime('%Y-%m-%d')
        return str(v)[:10] if v else ''

    all_rows.sort(key=lambda r: (_fmt(r[2]), str(r[4] or '')), reverse=True)

    result = []
    for r in all_rows:
        try:
            tipo    = (r[3] or '').strip().upper()
            signo   = -1 if tipo in TIPOS_NC else 1
            cant    = signo * float(r[5] or 0)
            pu      = float(r[6] or 0)          # precio unitario siempre positivo
            subtot  = signo * float(r[7] or 0)
            iva_pct = float(r[8] or 21)
            total   = round(subtot * (1 + iva_pct / 100), 2)
            result.append({
                "cod_articulo":    (r[0] or '').strip(),
                "descripcion":     (r[1] or '').strip(),
                "fecha":           _fmt(r[2]),
                "tipo":            tipo,
                "numero":          str(r[4] or '').strip().replace('.0',''),
                "cantidad":        cant,
                "precio_unitario": pu,
                "importe":         subtot,
                "iva_pct":         iva_pct,
                "total":           total,
            })
        except Exception:
            pass   # fila con datos inválidos: se omite
    return result


@app.get("/que-vendi/pdf")
def que_vendi_pdf(vendedor: str, cliente: str,
                  desde: Optional[str] = None, hasta: Optional[str] = None,
                  razon: Optional[str] = None):
    from reportlab.lib.pagesizes import landscape, A4
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Image
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
    from io import BytesIO
    from datetime import datetime

    rows = que_vendi(vendedor=vendedor, cliente=cliente, desde=desde, hasta=hasta, limit=2000)

    buf = BytesIO()
    # Landscape A4: usable width = 297 - 24 = 273 mm
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                            leftMargin=12*mm, rightMargin=12*mm,
                            topMargin=10*mm, bottomMargin=10*mm)

    azul = colors.HexColor('#1e429f')
    AR   = lambda v: f"{float(v or 0):,.2f}".replace(',','X').replace('.',',').replace('X','.')
    sDesc= ParagraphStyle('desc', fontSize=7, fontName='Helvetica',    leading=8)
    sHdr = ParagraphStyle('hdr',  fontSize=7, fontName='Helvetica-Bold',
                          textColor=colors.white, leading=8, alignment=1)  # 1=CENTER
    sSub = ParagraphStyle('sub',  fontSize=8, fontName='Helvetica', leading=10)
    sTit = ParagraphStyle('tit',  fontSize=12, fontName='Helvetica-Bold', leading=14)

    # ── Encabezado: logo izquierda + título derecha ─────────────────────────
    per = f"{desde or ''} a {hasta or ''}"
    emi = datetime.now().strftime('%d/%m/%Y %H:%M')
    logo_cell = ''
    if os.path.exists(LOGO_PATH):
        logo_cell = Image(LOGO_PATH, width=38*mm, height=12*mm,
                          kind='proportional')
    hdr_table = Table(
        [[logo_cell,
          [Paragraph('Microbell S.A. — ¿Qué Vendí?', sTit),
           Paragraph(f'Cliente: {razon or cliente}', sSub),
           Paragraph(f'Período: {per}   |   Emisión: {emi}', sSub)]]],
        colWidths=[42*mm, 231*mm]
    )
    hdr_table.setStyle(TableStyle([
        ('VALIGN',  (0,0),(-1,-1), 'MIDDLE'),
        ('ALIGN',   (1,0),(1,0),   'LEFT'),
        ('LEFTPADDING',  (0,0),(-1,-1), 0),
        ('RIGHTPADDING', (0,0),(-1,-1), 4),
        ('TOPPADDING',   (0,0),(-1,-1), 0),
        ('BOTTOMPADDING',(0,0),(-1,-1), 4),
    ]))

    # ── Columnas: suma = 273 mm ──────────────────────────────────────────────
    # CodArt Descripcion Fecha  Tipo  Nro   Cant  PUnit  Importe IVA  Total
    cw = [16*mm, 100*mm, 18*mm, 12*mm, 24*mm, 13*mm, 22*mm, 22*mm, 10*mm, 22*mm]
    # Encabezados como Paragraph para que también hagan wrap si es necesario
    hdrs = [Paragraph(h, sHdr) for h in
            ['Cód.Art.','Descripción','Fecha','Tipo','Nro. Comp.','Cant.','P.Unit.','Importe','IVA%','Total']]
    data = [hdrs]
    tot_imp = tot_tot = 0.0
    for r in rows:
        nro = str(r['numero']).replace('.0','').zfill(10)
        tot_imp += float(r['importe'] or 0)
        tot_tot += float(r['total']   or 0)
        fecha = r['fecha'][8:10]+'/'+r['fecha'][5:7]+'/'+r['fecha'][:4] if r['fecha'] else ''
        data.append([
            r['cod_articulo'],
            Paragraph(r['descripcion'] or '', sDesc),   # ← wrap automático
            fecha, r['tipo'], nro,
            str(int(float(r['cantidad'] or 0))), f"${AR(r['precio_unitario'])}",
            f"${AR(r['importe'])}", AR(r['iva_pct']), f"${AR(r['total'])}",
        ])
    data.append(['','','','','','',
                 Paragraph('TOTALES', ParagraphStyle('tb', fontSize=7, fontName='Helvetica-Bold')),
                 f"${AR(tot_imp)}", '', f"${AR(tot_tot)}"])

    t = Table(data, colWidths=cw, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND',   (0,0), (-1,0),  azul),
        ('ROWBACKGROUNDS',(0,1),(-1,-2), [colors.white, colors.HexColor('#f3f4f6')]),
        ('BACKGROUND',   (0,-1),(-1,-1), colors.HexColor('#e0e7ff')),
        ('FONTNAME',     (0,1), (-1,-1), 'Helvetica'),
        ('FONTNAME',     (0,-1),(-1,-1), 'Helvetica-Bold'),
        ('FONTSIZE',     (0,0), (-1,-1), 7),
        ('ALIGN',        (0,0), (-1,-1),  'LEFT'),
        ('ALIGN',        (0,0), (-1,0),  'CENTER'),   # toda la fila de headers
        ('ALIGN',        (0,1), (0,-1),  'CENTER'),   # Cód.Art. (datos)
        ('ALIGN',        (2,0), (2,-1),  'CENTER'),   # Fecha
        ('ALIGN',        (3,0), (3,-1),  'CENTER'),   # Tipo
        ('ALIGN',        (4,0), (4,-1),  'CENTER'),   # Nro
        ('ALIGN',        (5,0), (5,-1),  'CENTER'),   # Cant: centrado
        ('ALIGN',        (6,0), (-1,-1), 'RIGHT'),    # P.Unit → Total
        ('VALIGN',       (0,0), (-1,-1), 'MIDDLE'),
        ('GRID',         (0,0), (-1,-1), 0.3, colors.HexColor('#d1d5db')),
        ('TOPPADDING',   (0,0), (-1,-1), 2),
        ('BOTTOMPADDING',(0,0), (-1,-1), 2),
        ('LEFTPADDING',  (0,0), (-1,-1), 3),
        ('RIGHTPADDING', (0,0), (-1,-1), 3),
    ]))

    story = [hdr_table, t]
    doc.build(story)
    buf.seek(0)
    from fastapi.responses import StreamingResponse
    return StreamingResponse(buf, media_type='application/pdf',
        headers={'Content-Disposition': f'inline; filename="que_vendi_{cliente}.pdf"'})


@app.get("/que-vendi/excel")
def que_vendi_excel(vendedor: str, cliente: str,
                    desde: Optional[str] = None, hasta: Optional[str] = None,
                    razon: Optional[str] = None):
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    from io import BytesIO

    rows = que_vendi(vendedor=vendedor, cliente=cliente, desde=desde, hasta=hasta, limit=2000)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Que Vendi'

    azul  = PatternFill('solid', fgColor='1e429f')
    gris  = PatternFill('solid', fgColor='f3f4f6')
    azulc = PatternFill('solid', fgColor='e0e7ff')
    bF    = Font(bold=True, color='FFFFFF', size=9)
    bN    = Font(bold=True, size=9)
    nN    = Font(size=9)
    cen   = Alignment(horizontal='center', vertical='center', wrap_text=True)
    der   = Alignment(horizontal='right',  vertical='center')
    izq   = Alignment(horizontal='left',   vertical='center', wrap_text=True)
    thin  = Side(style='thin', color='d1d5db')
    brd   = Border(left=thin, right=thin, top=thin, bottom=thin)

    hdrs  = ['Cód.Art.','Descripción','Fecha','Tipo','Nro. Comp.','Cant.','P.Unit.','Importe','IVA%','Total']
    widths= [12,        45,           12,     8,     14,          10,     16,       16,       8,     16]
    for ci, (h, w) in enumerate(zip(hdrs, widths), 1):
        cell = ws.cell(row=1, column=ci, value=h)
        cell.fill = azul; cell.font = bF; cell.alignment = cen; cell.border = brd
        ws.column_dimensions[get_column_letter(ci)].width = w

    tot_imp = tot_tot = 0.0
    for ri, r in enumerate(rows, 2):
        nro = str(r['numero']).replace('.0','').zfill(10)
        fg  = None if ri % 2 == 0 else gris
        fecha = r['fecha'][8:10]+'/'+r['fecha'][5:7]+'/'+r['fecha'][:4] if r['fecha'] else ''
        vals = [r['cod_articulo'], r['descripcion'], fecha, r['tipo'], nro,
                float(r['cantidad'] or 0), float(r['precio_unitario'] or 0),
                float(r['importe'] or 0), float(r['iva_pct'] or 0), float(r['total'] or 0)]
        tot_imp += float(r['importe'] or 0); tot_tot += float(r['total'] or 0)
        aligns = [cen, izq, cen, cen, cen, cen, der, der, der, der]
        for ci, (v, al) in enumerate(zip(vals, aligns), 1):
            cell = ws.cell(row=ri, column=ci, value=v)
            cell.font = nN; cell.alignment = al; cell.border = brd
            if fg: cell.fill = fg
            if ci == 6:              cell.number_format = '#,##0'           # Cant: entero
            elif ci in (7, 8, 10): cell.number_format = '"$"#,##0.00'    # P.Unit/Importe/Total: $
            elif ci >= 7:          cell.number_format = '#,##0.00'        # IVA%: decimal

    tr = len(rows) + 2
    for ci, v in enumerate(['']*6 + ['TOTALES', tot_imp, '', tot_tot], 1):
        cell = ws.cell(row=tr, column=ci, value=v)
        cell.fill = azulc; cell.font = bN; cell.border = brd
        cell.alignment = der if ci >= 6 else cen
        if ci in (8, 10): cell.number_format = '"$"#,##0.00'

    buf = BytesIO(); wb.save(buf); buf.seek(0)
    from fastapi.responses import StreamingResponse
    return StreamingResponse(buf,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="que_vendi_{cliente}.xlsx"'})


@app.get("/resumen-deudas")
def resumen_deudas(vendedor: Optional[str] = None, _user=Depends(get_current_user)):
    """Suma de deuda pendiente por cliente, ordenado por deuda desc."""
    DB_PROD     = DATABASE      # DB-Prueba.gdb
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'  # SW producción

    c_cli = conn('WIN1252', DB_PROD)
    cur_cli = c_cli.cursor()
    if vendedor:
        cur_cli.execute(
            'SELECT CODIGOCLIENTE, RAZONSOCIAL, CODIGOPARTICULAR FROM "CLIENTES" '
            'WHERE ACTIVO=? AND UPPER(CODIGOVENDEDOR)=? ORDER BY RAZONSOCIAL',
            ('1', vendedor.upper())
        )
    else:
        cur_cli.execute(
            'SELECT CODIGOCLIENTE, RAZONSOCIAL, CODIGOPARTICULAR FROM "CLIENTES" '
            "WHERE ACTIVO='1' ORDER BY RAZONSOCIAL"
        )
    clientes_rows = cur_cli.fetchall()
    c_cli.close()

    def _calcular_deuda_cliente(cod, razon, part, vend=None):
        codigos = list({cod, part} - {''}) or [cod]
        total_deuda = 0.0
        tiene_deuda_positiva = False
        seen_cta = set()
        for db_path in [DB_PROD, DB_MLT_PROD]:
            try:
                rows = _query_cta(db_path, codigos, 500, 0, vendedor=vend)
                for r in rows:
                    key = (r[0], r[1])
                    if key not in seen_cta:
                        seen_cta.add(key)
                        valor = float(r[5] or 0)
                        total_deuda += valor
                        if valor > 0:
                            tiene_deuda_positiva = True
            except Exception:
                pass
        return total_deuda, tiene_deuda_positiva

    deudas = {}
    for cod, razon, part in clientes_rows:
        cod = (cod or '').strip(); razon = (razon or '').strip(); part = (part or '').strip()
        if not cod: continue
        total_deuda, tiene_deuda_positiva = _calcular_deuda_cliente(cod, razon, part, vend=vendedor)
        # Mostrar positivos siempre; negativos solo si también tienen comprobantes con deuda
        if total_deuda >= 0.01 or (total_deuda <= -0.01 and tiene_deuda_positiva):
            deudas[cod] = {'codigo': part or cod, 'razonsocial': razon, 'deuda': round(total_deuda, 2)}

    # Agregar clientes con NCA/NCB/etc. emitidas por este vendedor
    # aunque no estén asignados como clientes del vendedor en CLIENTES
    if vendedor:
        _NC_TIPOS = "('NCA','NCB','NCCA','NCCB','NCE','NCCE','SIV')"
        for db_path in [DB_PROD, DB_MLT_PROD]:
            try:
                c_nc = conn('WIN1252', db=db_path)
                cur_nc = c_nc.cursor()
                cur_nc.execute(f"""
                    SELECT DISTINCT CODIGOCLIENTE FROM "CABEZACOMPROBANTES"
                    WHERE TIPOCOMPROBANTE IN {_NC_TIPOS}
                      AND UPPER(CODIGOUSUARIO) = ?
                      AND ANULADA = '0'
                """, (vendedor.upper(),))
                nc_codigos = [r[0].strip() for r in cur_nc.fetchall() if r[0] and r[0].strip()]
                c_nc.close()
                for nc_cod in nc_codigos:
                    if nc_cod in deudas:
                        continue  # ya incluido
                    # Buscar datos del cliente
                    c_li = conn('WIN1252', db=DB_PROD)
                    cur_li = c_li.cursor()
                    cur_li.execute(
                        'SELECT CODIGOCLIENTE, RAZONSOCIAL, CODIGOPARTICULAR FROM "CLIENTES" '
                        'WHERE CODIGOCLIENTE=? OR CODIGOPARTICULAR=?',
                        (nc_cod, nc_cod)
                    )
                    row = cur_li.fetchone()
                    c_li.close()
                    if not row:
                        continue
                    cod2  = (row[0] or '').strip()
                    razon2 = (row[1] or '').strip()
                    part2 = (row[2] or '').strip()
                    if not cod2 or cod2 in deudas:
                        continue
                    total_deuda, tiene_deuda_positiva = _calcular_deuda_cliente(cod2, razon2, part2, vend=vendedor)
                    if total_deuda >= 0.01 or (total_deuda <= -0.01 and tiene_deuda_positiva):
                        deudas[cod2] = {'codigo': part2 or cod2, 'razonsocial': razon2, 'deuda': round(total_deuda, 2)}
            except Exception:
                pass

    result = sorted(deudas.values(), key=lambda x: x['razonsocial'])
    return result

@app.get("/resumen-deudas/pdf")
def resumen_deudas_pdf(vendedor: Optional[str] = None):
    """PDF con resumen de deudas por cliente — logo + detalle de comprobantes."""
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable, KeepTogether
    from reportlab.platypus.flowables import Flowable
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT
    from reportlab.platypus import Image
    from datetime import date

    DB_PROD     = DATABASE      # DB-Prueba.gdb
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'  # SW producción

    CELESTE = colors.HexColor('#4A90D9')
    GRIS    = colors.HexColor('#f3f4f6')
    ROJO    = colors.HexColor('#dc2626')
    AZUL_CLI= colors.HexColor('#1e3a5f')

    sN  = ParagraphStyle('rdN',  fontSize=7, leading=9,  fontName='Helvetica')
    sNc = ParagraphStyle('rdNc', fontSize=7, leading=9,  fontName='Helvetica',      alignment=TA_CENTER)
    sNr = ParagraphStyle('rdNr', fontSize=7, leading=9,  fontName='Helvetica',      alignment=TA_RIGHT)
    sNb = ParagraphStyle('rdNb', fontSize=7, leading=9,  fontName='Helvetica-Bold', alignment=TA_CENTER)
    sNbr= ParagraphStyle('rdNbr',fontSize=7, leading=9,  fontName='Helvetica-Bold', alignment=TA_CENTER)

    def _fmt_num(v):
        """Convierte 4400014955.0 → '4400014955' (10 dígitos, sin .0)"""
        try:
            return str(int(float(v))).zfill(10)
        except Exception:
            return str(v or '')

    def _fmt_date(v):
        """Convierte datetime o 'yyyy-mm-dd' → 'dd/mm/yyyy'"""
        if not v:
            return '—'
        try:
            from datetime import date as _date, datetime as _dt
            if hasattr(v, 'strftime'):
                return v.strftime('%d/%m/%Y')
            s = str(v)[:10]
            d = _dt.strptime(s, '%Y-%m-%d')
            return d.strftime('%d/%m/%Y')
        except Exception:
            return str(v)[:10]
    sCli= ParagraphStyle('rdCli',fontSize=9, leading=11, fontName='Helvetica-Bold', textColor=AZUL_CLI)
    sT  = ParagraphStyle('rdT',  fontSize=12,leading=15, fontName='Helvetica-Bold')
    sSub= ParagraphStyle('rdSub',fontSize=8, leading=10, fontName='Helvetica', textColor=colors.HexColor('#6b7280'))

    hoy = date.today().strftime('%d/%m/%Y')

    # ── Obtener clientes con deuda y sus comprobantes ─────────────────────────
    # Leer CLIENTES y comprobantes L1 desde DB-Prueba; SW desde DB-MLT-Prueba
    DB_PROD     = DATABASE      # DB-Prueba.gdb
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'  # SW producción
    c_cli = conn('WIN1252', DB_PROD)
    cur_cli = c_cli.cursor()
    if vendedor:
        cur_cli.execute(
            'SELECT CODIGOCLIENTE, RAZONSOCIAL, CODIGOPARTICULAR, CUIT FROM "CLIENTES" '
            "WHERE ACTIVO='1' AND UPPER(CODIGOVENDEDOR)=? ORDER BY RAZONSOCIAL",
            (vendedor.upper(),)
        )
    else:
        cur_cli.execute(
            'SELECT CODIGOCLIENTE, RAZONSOCIAL, CODIGOPARTICULAR, CUIT FROM "CLIENTES" '
            "WHERE ACTIVO='1' ORDER BY RAZONSOCIAL"
        )
    clientes_rows = cur_cli.fetchall()
    c_cli.close()

    clientes_data = []
    gran_total_deuda = 0.0
    for cod, razon, part, cuit in clientes_rows:
        cod   = (cod   or '').strip()
        razon = (razon or '').strip()
        part  = (part  or '').strip()
        cuit  = (cuit  or '').strip()
        if not cod: continue
        comprobantes = []
        seen = set()

        for db_path in [DB_PROD, DB_MLT_PROD]:
            try:
                rows = _query_cta(db_path, [cod], 500, 0)
                for r in rows:
                    key = (r[0], r[1])
                    if key not in seen:
                        seen.add(key)
                        comprobantes.append(r)
            except Exception:
                pass

        comprobantes.sort(key=lambda r: (r[6] or r[2], r[2]))
        total_deuda = sum(float(r[5] or 0) for r in comprobantes)
        if total_deuda > 0:
            gran_total_deuda += total_deuda
            clientes_data.append({
                'codigo': part or cod, 'razonsocial': razon, 'cuit': cuit,
                'comprobantes': comprobantes, 'total_deuda': total_deuda
            })

    clientes_data.sort(key=lambda x: x['razonsocial'])

    # ── Armar PDF ─────────────────────────────────────────────────────────────
    buf = BytesIO()
    PAGE_W, PAGE_H = A4
    mg = 14 * mm
    usable_w = PAGE_W - 2 * mg

    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=mg, rightMargin=mg,
                            topMargin=12*mm, bottomMargin=12*mm)

    def _on_page(canvas, doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 6.5)
        canvas.setFillColor(colors.HexColor('#9ca3af'))
        canvas.drawCentredString(PAGE_W/2, 8*mm,
            f'microbell S.A.  ·  CUIT 30-70839018-2  ·  Resumen de Deudas al {hoy}  ·  Pág. {doc.page}')
        canvas.restoreState()

    story = []

    # ── Header con logo ───────────────────────────────────────────────────────
    logo_cell = Image(LOGO_PATH, width=38*mm, height=13*mm) if os.path.exists(LOGO_PATH) \
                else Paragraph('<b>microbell S.A.</b>', sT)
    titulo_txt = f'Resumen de Deudas Pendientes'
    subtitulo  = f'Fecha: {hoy}'
    if vendedor:
        subtitulo += f'   |   Vendedor: {vendedor}'
    hdr_tbl = Table([[
        logo_cell,
        [Paragraph(titulo_txt, sT), Paragraph(subtitulo, sSub)]
    ]], colWidths=[45*mm, usable_w - 45*mm])
    hdr_tbl.setStyle(TableStyle([
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('ALIGN',(1,0),(1,0),'RIGHT'),
    ]))
    story.append(hdr_tbl)
    story.append(HRFlowable(width=usable_w, thickness=1.5, color=CELESTE, spaceAfter=4))

    # ── Resumen global ────────────────────────────────────────────────────────
    story.append(Paragraph(
        f'<b>Total general deuda: {_pesos(gran_total_deuda)}</b>  &nbsp;·&nbsp;  '
        f'{len(clientes_data)} clientes con saldo pendiente',
        ParagraphStyle('rdResumen', fontSize=9, fontName='Helvetica-Bold',
                       textColor=ROJO, leading=12)
    ))
    story.append(Spacer(1, 5*mm))

    # ── Detalle por cliente ───────────────────────────────────────────────────
    cw_tipo = 14*mm; cw_num = 28*mm; cw_fcomp = 20*mm; cw_fvto = 20*mm
    cw_total = 30*mm; cw_pago = 30*mm; cw_deuda = 30*mm
    cw_cli = usable_w  # encabezado cliente ocupa todo el ancho

    for cli in clientes_data:
        bloque = []

        # Encabezado cliente
        cli_hdr = Table([[
            Paragraph(f'{cli["razonsocial"]}', sCli),
            Paragraph(f'Cód: {cli["codigo"]}  |  CUIT: {cli["cuit"] or "—"}', sN),
            Paragraph(f'Deuda: <b>{_pesos(cli["total_deuda"])}</b>',
                      ParagraphStyle('rdDeu', fontSize=8, fontName='Helvetica-Bold',
                                     textColor=ROJO, alignment=TA_RIGHT)),
        ]], colWidths=[usable_w*0.45, usable_w*0.3, usable_w*0.25])
        cli_hdr.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,-1), colors.HexColor('#e8f0fe')),
            ('TOPPADDING',(0,0),(-1,-1),3), ('BOTTOMPADDING',(0,0),(-1,-1),3),
            ('LEFTPADDING',(0,0),(-1,-1),5), ('RIGHTPADDING',(0,0),(-1,-1),5),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ]))
        bloque.append(cli_hdr)

        # Tabla de comprobantes
        comp_rows = [[
            Paragraph('<b>Tipo</b>',    sNb),
            Paragraph('<b>Número</b>',  sNb),
            Paragraph('<b>F.Comp.</b>', sNb),
            Paragraph('<b>F.Vto.</b>',  sNb),
            Paragraph('<b>Total</b>',   sNbr),
            Paragraph('<b>Pagado</b>',  sNbr),
            Paragraph('<b>Saldo</b>',   sNbr),
        ]]
        for r in cli['comprobantes']:
            deuda = float(r[5] or 0)
            if deuda <= 0:
                continue
            comp_rows.append([
                Paragraph(str(r[0] or ''),          sNc),
                Paragraph(_fmt_num(r[1]),            sNc),
                Paragraph(_fmt_date(r[2]),           sNc),
                Paragraph(_fmt_date(r[6]),           sNc),
                Paragraph(_pesos(float(r[3] or 0)), sNr),
                Paragraph(_pesos(float(r[4] or 0)), sNr),
                Paragraph(_pesos(deuda),             sNr),
            ])

        comp_tbl = Table(comp_rows,
                         colWidths=[cw_tipo, cw_num, cw_fcomp, cw_fvto, cw_total, cw_pago, cw_deuda],
                         repeatRows=1)
        comp_tbl.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0), CELESTE), ('TEXTCOLOR',(0,0),(-1,0), colors.white),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white, GRIS]),
            ('BOX',(0,0),(-1,-1),0.4,colors.grey),
            ('INNERGRID',(0,0),(-1,-1),0.2,colors.lightgrey),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('TOPPADDING',(0,0),(-1,-1),2), ('BOTTOMPADDING',(0,0),(-1,-1),2),
            ('LEFTPADDING',(0,0),(-1,-1),3),
        ]))
        bloque.append(comp_tbl)
        bloque.append(Spacer(1, 4*mm))

        story.append(KeepTogether(bloque))

    # ── Pie resumen ───────────────────────────────────────────────────────────
    story.append(HRFlowable(width=usable_w, thickness=1, color=CELESTE, spaceAfter=3))
    story.append(Paragraph(
        f'<b>TOTAL GENERAL DEUDA: {_pesos(gran_total_deuda)}</b>',
        ParagraphStyle('rdTot', fontSize=11, fontName='Helvetica-Bold',
                       textColor=ROJO, alignment=TA_RIGHT)
    ))

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    buf.seek(0)
    return StreamingResponse(buf, media_type='application/pdf',
        headers={'Content-Disposition': f'inline; filename="resumen_deudas_{hoy}.pdf"'})

@app.get("/clientes/{codigo}/cuenta_corriente/pdf")
def cuenta_corriente_pdf(codigo: str, limit: int = Query(500, le=2000), offset: int = 0, _user=Depends(get_current_user)):
    from datetime import datetime

    # ── 1. Datos cliente (lookup en DB_PROD igual que resumen-deudas)
    DB_PROD     = DATABASE      # DB-Prueba.gdb
    c_cli = conn('WIN1252', db=DB_PROD)
    cur_cli = c_cli.cursor()
    cur_cli.execute(
        'SELECT CODIGOCLIENTE, CODIGOPARTICULAR, RAZONSOCIAL, CUIT, TELEFONO, TELEFONOCELULAR, DIRECCION, LOCALIDAD '
        'FROM "CLIENTES" WHERE CODIGOCLIENTE = ? OR CODIGOPARTICULAR = ?',
        (codigo, codigo)
    )
    cli = cur_cli.fetchone()
    c_cli.close()
    if not cli:
        raise HTTPException(404, "Cliente no encontrado")
    cli_cod_visible = (cli[1] or cli[0] or '').strip()
    cli_razon = (cli[2] or '').strip()
    cli_cuit  = (cli[3] or '').strip()
    cli_tel   = (cli[4] or cli[5] or '').strip()
    cli_dir   = (cli[6] or '').strip()
    cli_loc   = (cli[7] or '').strip()

    # ── 2. Movimientos (misma logica que /cuenta_corriente)
    codigos = set()
    if cli[0] and cli[0].strip(): codigos.add(cli[0].strip())
    if cli[1] and cli[1].strip(): codigos.add(cli[1].strip())
    if not codigos: codigos.add(codigo)
    codigos = list(codigos)

    DB_PROD     = DATABASE      # DB-Prueba.gdb
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'  # SW producción
    rows_prod = _query_cta(DB_PROD,      codigos, limit, offset)
    rows_mp   = _query_cta(DB_MLT_PROD,  codigos, limit, offset)

    seen, combined = set(), []
    for r in rows_prod + rows_mp:
        key = (r[0], r[1])
        if key not in seen:
            seen.add(key)
            combined.append(r)
    combined.sort(key=lambda r: (r[6] or r[2], r[2]))

    # ── 3. Datos empresa
    razon_soc = cuit_emp = dir_emp = tel_emp = email_emp = ''
    try:
        cp = conn(); ccp = cp.cursor()
        ccp.execute('SELECT RAZONSOCIAL, CUIT, DIRECCION, TELEFONO, EMAIL FROM "PARAMETROS" WHERE CODIGOPARAMETRO = 1')
        rp = ccp.fetchone(); cp.close()
        if rp:
            razon_soc, cuit_emp, dir_emp, tel_emp, email_emp = [(v or '').strip() for v in rp]
    except Exception:
        pass

    # ── 4. PDF
    buf = BytesIO()
    PAGE_W, PAGE_H = A4
    mg = 14 * mm

    s_title = ParagraphStyle('t', fontSize=13, leading=16, fontName='Helvetica-Bold',
                              textColor=colors.HexColor('#1a56db'))
    s_sub   = ParagraphStyle('s', fontSize=8,  leading=10, fontName='Helvetica',
                              textColor=colors.HexColor('#6b7280'))
    s_label = ParagraphStyle('l', fontSize=8,  leading=10, fontName='Helvetica-Bold',
                              textColor=colors.HexColor('#374151'))
    s_val   = ParagraphStyle('v', fontSize=8,  leading=10, fontName='Helvetica')
    s_hdr   = ParagraphStyle('h', fontSize=7.5, leading=9, fontName='Helvetica-Bold',
                              alignment=TA_CENTER, textColor=colors.white)
    s_cell  = ParagraphStyle('c', fontSize=7.5, leading=9, fontName='Helvetica')
    s_cell_r= ParagraphStyle('cr', fontSize=7.5, leading=9, fontName='Helvetica', alignment=TA_RIGHT)
    s_total = ParagraphStyle('tt', fontSize=10, leading=12, fontName='Helvetica-Bold',
                              alignment=TA_RIGHT, textColor=colors.HexColor('#dc2626'))

    usable_w = PAGE_W - 2 * mg
    footer_txt = f'{razon_soc}  ·  CUIT {cuit_emp}  ·  {dir_emp}  ·  Tel {tel_emp}  ·  {email_emp}'

    def _on_page(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor('#e5e7eb'))
        canvas.setLineWidth(0.5)
        canvas.line(mg, 10*mm, PAGE_W - mg, 10*mm)
        canvas.setFont('Helvetica', 6.5)
        canvas.setFillColor(colors.HexColor('#6b7280'))
        canvas.drawCentredString(PAGE_W / 2, 7*mm, footer_txt)
        canvas.drawRightString(PAGE_W - mg, 7*mm, f"Pág. {doc.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=mg, rightMargin=mg,
                            topMargin=mg, bottomMargin=18*mm)
    story = []

    # Header logo + titulo
    logo_cell = Image(LOGO_PATH, width=38*mm, height=12*mm) if os.path.exists(LOGO_PATH) else Paragraph('', s_sub)
    titulo_cell = [
        Paragraph("Resumen de Cuenta Corriente", s_title),
        Paragraph(datetime.now().strftime('%d/%m/%Y %H:%M'), s_sub),
    ]
    t_hdr = Table([[logo_cell, titulo_cell]], colWidths=[42*mm, usable_w - 42*mm])
    t_hdr.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('ALIGN',  (1,0), (1,0),  'RIGHT'),
    ]))
    story.append(t_hdr)
    story.append(Spacer(1, 4*mm))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#1a56db')))
    story.append(Spacer(1, 3*mm))

    # Datos cliente
    cli_data = [
        [Paragraph('Cliente:',   s_label), Paragraph(cli_razon, s_val),
         Paragraph('Código:',    s_label), Paragraph(cli_cod_visible, s_val)],
        [Paragraph('CUIT:',      s_label), Paragraph(cli_cuit or '—', s_val),
         Paragraph('Teléfono:',  s_label), Paragraph(cli_tel or '—', s_val)],
        [Paragraph('Dirección:', s_label), Paragraph(cli_dir or '—', s_val),
         Paragraph('Localidad:', s_label), Paragraph(cli_loc or '—', s_val)],
    ]
    t_cli = Table(cli_data, colWidths=[22*mm, (usable_w/2 - 22*mm), 22*mm, (usable_w/2 - 22*mm)])
    t_cli.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('BOX',    (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
        ('INNERGRID', (0,0), (-1,-1), 0.3, colors.HexColor('#f3f4f6')),
        ('TOPPADDING',    (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LEFTPADDING',   (0,0), (-1,-1), 6),
        ('RIGHTPADDING',  (0,0), (-1,-1), 6),
    ]))
    story.append(t_cli)
    story.append(Spacer(1, 4*mm))

    # Tabla movimientos
    HDR_BG = colors.HexColor('#1a56db')
    ALT_BG = colors.HexColor('#eff6ff')
    cw = [16*mm, 32*mm, 24*mm, 24*mm, 28*mm, 28*mm, 30*mm]

    data = [[
        Paragraph('Tipo',     s_hdr),
        Paragraph('Número',   s_hdr),
        Paragraph('F. Comp.', s_hdr),
        Paragraph('F. Vto.',  s_hdr),
        Paragraph('Total',    s_hdr),
        Paragraph('Pagado',   s_hdr),
        Paragraph('Deuda',    s_hdr),
    ]]

    def _fmt_money(n):
        return '$' + f"{float(n or 0):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    def _fmt_fecha(f):
        if not f: return '—'
        s = str(f)
        return s[:10] if len(s) >= 10 else s

    total_total = total_pagado = total_deuda = 0.0
    hoy = datetime.now().date()

    for r in combined:
        tot = float(r[3] or 0); pag = float(r[4] or 0); deu = float(r[5] or 0)
        total_total += tot; total_pagado += pag; total_deuda += deu
        vto_str = _fmt_fecha(r[6])
        # Marcar vencido en rojo
        try:
            vto_d = r[6].date() if hasattr(r[6], 'date') else None
            vencido = bool(vto_d and vto_d < hoy and deu > 0.01)
        except Exception:
            vencido = False
        s_vto = ParagraphStyle('vt', parent=s_cell, textColor=colors.HexColor('#dc2626')) if vencido else s_cell

        data.append([
            Paragraph(str(r[0] or ''),    s_cell),
            Paragraph(str(r[1] or ''),    s_cell),
            Paragraph(_fmt_fecha(r[2]),   s_cell),
            Paragraph(vto_str + (' ⚠' if vencido else ''), s_vto),
            Paragraph(_fmt_money(tot),    s_cell_r),
            Paragraph(_fmt_money(pag),    s_cell_r),
            Paragraph(_fmt_money(deu),
                      ParagraphStyle('dr', parent=s_cell_r,
                                     textColor=colors.HexColor('#dc2626'),
                                     fontName='Helvetica-Bold')),
        ])

    tbl = Table(data, colWidths=cw, repeatRows=1)
    tbl.setStyle(TableStyle([
        ('BACKGROUND',     (0,0), (-1,0), HDR_BG),
        ('TEXTCOLOR',      (0,0), (-1,0), colors.white),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, ALT_BG]),
        ('GRID',           (0,0), (-1,-1), 0.4, colors.HexColor('#e5e7eb')),
        ('VALIGN',         (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING',     (0,0), (-1,-1), 3),
        ('BOTTOMPADDING',  (0,0), (-1,-1), 3),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 4*mm))

    # Totales
    tot_data = [[
        Paragraph(f'{len(combined)} comprobantes', s_sub),
        Paragraph(f'Total: {_fmt_money(total_total)}    Pagado: {_fmt_money(total_pagado)}', s_sub),
        Paragraph(f'Deuda: {_fmt_money(total_deuda)}', s_total),
    ]]
    t_tot = Table(tot_data, colWidths=[40*mm, usable_w - 40*mm - 60*mm, 60*mm])
    t_tot.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('ALIGN',  (2,0), (2,0),  'RIGHT'),
    ]))
    story.append(t_tot)

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    buf.seek(0)
    fname = f"CtaCte_{cli_cod_visible}.pdf"
    return StreamingResponse(buf, media_type='application/pdf',
        headers={"Content-Disposition": f"inline; filename={fname}"})


# ─── Transportes / Sucursales ────────────────────────────────────────────────
@app.get("/transportes")
def get_transportes():
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    rows_map: dict = {}
    for db_path in [DATABASE, DB_PROD]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            cur.execute('SELECT CODIGOTRANSPORTE, DESCRIPCION FROM "TRANSPORTES" ORDER BY DESCRIPCION')
            for r in cur.fetchall():
                cod = r[0]
                if cod is not None and str(cod).strip() not in ('', '0') and cod not in rows_map:
                    rows_map[cod] = (r[1] or '').strip()
            c.close()
        except Exception:
            pass
    return sorted(
        [{"codigo": cod, "descripcion": desc} for cod, desc in rows_map.items()],
        key=lambda x: x["descripcion"]
    )

@app.get("/clientes/{codigo}/sucursales")
def get_sucursales_cliente(codigo: str):
    """
    Devuelve domicilios de entrega (SUCURSALESXCLIENTES) desde DB-Microbell.gdb.
    Resolución de código:
      1. Busca CODIGOPARTICULAR en DB-Prueba (fuente del codigo recibido)
      2. Usa CODIGOPARTICULAR para encontrar CODIGOCLIENTE en DB-Microbell
      3. Consulta SUCURSALESXCLIENTES con ese CODIGOCLIENTE
    """
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    sucursales = []
    direccion_principal = ''
    transp_cli  = None
    transp_fijo = False
    reparto_propio_cli = False

    # ── Paso 1: resolver CODIGOPARTICULAR desde DB-Prueba ───────────────────
    codigoparticular = codigo   # fallback: usar el código tal cual
    try:
        c_pru = conn('WIN1252')   # DB-Prueba.gdb
        cur_pru = c_pru.cursor()
        cur_pru.execute(
            'SELECT CODIGOPARTICULAR, DIRECCION, LOCALIDAD FROM "CLIENTES" '
            'WHERE CODIGOCLIENTE = ? OR CODIGOPARTICULAR = ?',
            (codigo, codigo)
        )
        row_pru = cur_pru.fetchone()
        c_pru.close()
        if row_pru:
            cp = (row_pru[0] or '').strip()
            if cp:
                codigoparticular = cp
            # Dirección de fallback desde DB-Prueba
            partes_dir = [p for p in [(row_pru[1] or '').strip(),
                                      (row_pru[2] or '').strip()] if p]
            if partes_dir:
                direccion_principal = ' — '.join(partes_dir)
    except Exception:
        pass

    # ── Paso 2 y 3: buscar CODIGOCLIENTE en Microbell y traer sucursales ────
    c_prod = None
    try:
        c_prod = conn('WIN1252', db=DB_PROD)
        cur_prod = c_prod.cursor()

        # Resolver CODIGOCLIENTE interno en DB-Microbell via CODIGOPARTICULAR
        cur_prod.execute(
            'SELECT CODIGOCLIENTE, DIRECCION, LOCALIDAD FROM "CLIENTES" '
            'WHERE CODIGOPARTICULAR = ? OR CODIGOCLIENTE = ?',
            (codigoparticular, codigoparticular)
        )
        row_mb = cur_prod.fetchone()
        if row_mb:
            cod_mb = str(row_mb[0] or '').strip()
            partes_mb = [p for p in [str(row_mb[1] or '').strip(),
                                     str(row_mb[2] or '').strip()] if p]
            if partes_mb:
                direccion_principal = ' — '.join(partes_mb)
        else:
            cod_mb = codigoparticular

        # Transporte a nivel cliente (TRANSPORTEFIJO vive en CLIENTES, no en SUCURSALESXCLIENTES)
        cur_prod.execute(
            'SELECT CODIGOTRANSPORTE, TRANSPORTEFIJO, REPARTOPROPIO '
            'FROM "CLIENTES" WHERE CODIGOCLIENTE = ?', (cod_mb,)
        )
        row_transp = cur_prod.fetchone()
        transp_cli   = str(row_transp[0]).strip() if row_transp and row_transp[0] else None
        transp_fijo  = str(row_transp[1] or '0').strip() == '1' if row_transp else False
        reparto_propio_cli = str(row_transp[2] or '0').strip() == '1' if row_transp else False

        # Domicilios de entrega — intentar con REPARTOPROPIO, fallback sin él
        _suc_rows = []
        _suc_has_reparto = False
        try:
            cur_prod.execute(
                'SELECT s.CODIGOSUCURSAL, s.NOMBRE, s.DIRECCION, s.CODIGOTRANSPORTE, '
                'p.NOMBRE, l.NOMBRE, s.TELEFONO, s.OBSERVACIONES, s.REPARTOPROPIO '
                'FROM "SUCURSALESXCLIENTES" s '
                'LEFT JOIN "PROVINCIAS" p ON p.CODIGOPROVINCIA = s.CODIGOPROVINCIA '
                'LEFT JOIN "LOCALIDADES" l '
                '       ON l.CODIGOPROVINCIA = s.CODIGOPROVINCIA '
                '      AND l.CODIGOLOCALIDAD = s.CODIGOLOCALIDAD '
                'WHERE s.CODIGOCLIENTE = ? '
                'ORDER BY s.CODIGOSUCURSAL',
                (cod_mb,)
            )
            _suc_rows = cur_prod.fetchall()
            _suc_has_reparto = True
        except Exception:
            cur_prod.execute(
                'SELECT s.CODIGOSUCURSAL, s.NOMBRE, s.DIRECCION, s.CODIGOTRANSPORTE, '
                'p.NOMBRE, l.NOMBRE, s.TELEFONO, s.OBSERVACIONES '
                'FROM "SUCURSALESXCLIENTES" s '
                'LEFT JOIN "PROVINCIAS" p ON p.CODIGOPROVINCIA = s.CODIGOPROVINCIA '
                'LEFT JOIN "LOCALIDADES" l '
                '       ON l.CODIGOPROVINCIA = s.CODIGOPROVINCIA '
                '      AND l.CODIGOLOCALIDAD = s.CODIGOLOCALIDAD '
                'WHERE s.CODIGOCLIENTE = ? '
                'ORDER BY s.CODIGOSUCURSAL',
                (cod_mb,)
            )
            _suc_rows = cur_prod.fetchall()
        for r in _suc_rows:
            transp_suc = str(r[3]).strip() if r[3] is not None and r[3] != '' else None
            reparto_suc = (str(r[8] or '0').strip() == '1') if _suc_has_reparto else reparto_propio_cli
            sucursales.append({
                "codigo":        str(r[0] or "").strip(),
                "nombre":        str(r[1] or "").strip(),
                "direccion":     str(r[2] or "").strip(),
                "transporte":    transp_suc or transp_cli,
                "provincia":     str(r[4] or "").strip(),
                "localidad":     str(r[5] or "").strip(),
                "telefono":      str(r[6] or "").strip(),
                "observaciones": str(r[7] or "").strip(),
                "transporteFijo": transp_fijo,
                "repartoPropio": reparto_suc,
            })
    except Exception as _e_suc:
        sucursales = []
        _suc_error = str(_e_suc)
    else:
        _suc_error = None
    finally:
        if c_prod:
            try: c_prod.close()
            except Exception: pass

    # Fallback: si Microbell no devolvió sucursales, intentar en DATABASE (DB-Prueba)
    if not sucursales:
        try:
            c_fb = conn('WIN1252', db=DATABASE)
            cur_fb = c_fb.cursor()
            # Transporte del cliente en DATABASE
            if not transp_cli:
                cur_fb.execute(
                    'SELECT CODIGOTRANSPORTE, TRANSPORTEFIJO, REPARTOPROPIO FROM "CLIENTES" WHERE CODIGOCLIENTE = ?',
                    (codigo,)
                )
                row_fb = cur_fb.fetchone()
                if row_fb:
                    transp_cli = str(row_fb[0] or '').strip() or None
                    transp_fijo = str(row_fb[1] or '0').strip() == '1'
                    reparto_propio_cli = str(row_fb[2] or '0').strip() == '1'
            cur_fb.execute(
                'SELECT s.CODIGOSUCURSAL, s.NOMBRE, s.DIRECCION, s.CODIGOTRANSPORTE, '
                'p.NOMBRE, l.NOMBRE, s.TELEFONO, s.OBSERVACIONES '
                'FROM "SUCURSALESXCLIENTES" s '
                'LEFT JOIN "PROVINCIAS" p ON p.CODIGOPROVINCIA = s.CODIGOPROVINCIA '
                'LEFT JOIN "LOCALIDADES" l ON l.CODIGOPROVINCIA = s.CODIGOPROVINCIA '
                '   AND l.CODIGOLOCALIDAD = s.CODIGOLOCALIDAD '
                'WHERE s.CODIGOCLIENTE = ? ORDER BY s.CODIGOSUCURSAL',
                (codigo,)
            )
            for r in cur_fb.fetchall():
                sucursales.append({
                    "codigo":        str(r[0] or "").strip(),
                    "nombre":        str(r[1] or "").strip(),
                    "direccion":     str(r[2] or "").strip(),
                    "transporte":    str(r[3] or "").strip() or transp_cli,
                    "provincia":     str(r[4] or "").strip(),
                    "localidad":     str(r[5] or "").strip(),
                    "telefono":      str(r[6] or "").strip(),
                    "observaciones": str(r[7] or "").strip(),
                    "transporteFijo": transp_fijo,
                    "repartoPropio": reparto_propio_cli,
                })
            c_fb.close()
            if not direccion_principal:
                cur_fb2 = conn('WIN1252', db=DATABASE).cursor()
                cur_fb2.execute('SELECT DIRECCION, LOCALIDAD FROM "CLIENTES" WHERE CODIGOCLIENTE = ?', (codigo,))
                row_d = cur_fb2.fetchone()
                if row_d:
                    partes = [p for p in [(row_d[0] or '').strip(), (row_d[1] or '').strip()] if p]
                    direccion_principal = ' — '.join(partes)
        except Exception:
            pass

    resp = {
        "sucursales": sucursales,
        "direccion_principal": direccion_principal,
        "transporte_codigo": transp_cli,
        "transporte_fijo": transp_fijo,
        "reparto_propio": reparto_propio_cli,
    }
    if _suc_error:
        resp["_error"] = _suc_error   # solo para diagnóstico; se puede quitar luego
    return resp

# ─── Informar Pago ────────────────────────────────────────────────────────────
@app.post("/clientes/{id}/informar-pago")
async def informar_pago(
    id: str,
    nombre: str = Form(...),
    vendedor: str = Form(...),
    comentario: str = Form(""),
    comprobante: Optional[UploadFile] = File(None),
):
    """Envía notificación de pago por email con adjunto opcional."""
    if not SMTP_HOST or not SMTP_TO_PAGOS:
        raise HTTPException(status_code=503, detail="Servicio de email no configurado")

    msg = MIMEMultipart()
    msg["From"]    = SMTP_FROM or SMTP_USER
    msg["To"]      = SMTP_TO_PAGOS
    msg["Subject"] = f"Informar Pago - Cliente {nombre} (Vendedor: {vendedor})"

    cuerpo = f"""Se ha recibido un aviso de pago:

Cliente:  {nombre} (código: {id})
Vendedor: {vendedor}
Comentario: {comentario or '(sin comentario)'}
"""
    msg.attach(MIMEText(cuerpo, "plain", "utf-8"))

    if comprobante and comprobante.filename:
        datos = await comprobante.read()
        part = MIMEBase("application", "octet-stream")
        part.set_payload(datos)
        encoders.encode_base64(part)
        safe_fn = comprobante.filename.replace("\r", "").replace("\n", "")
        part.add_header("Content-Disposition", "attachment", filename=safe_fn)
        msg.attach(part)

    raw = msg.as_bytes()
    remitente = msg["From"]

    def _send():
        # Puerto 465 → SMTP_SSL directo (sin starttls)
        # Puerto 587 → SMTP + starttls
        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as srv:
                srv.login(SMTP_USER, SMTP_PASS)
                srv.sendmail(remitente, [SMTP_TO_PAGOS], raw)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as srv:
                srv.ehlo()
                srv.starttls()
                srv.login(SMTP_USER, SMTP_PASS)
                srv.sendmail(remitente, [SMTP_TO_PAGOS], raw)

    try:
        await asyncio.to_thread(_send)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al enviar email: {e}")

    return {"ok": True, "mensaje": "Pago informado correctamente"}

# ─── Pedidos (solo del vendedor) ───────────────────────────────────────────────
@app.get("/pedidos")
def get_pedidos(
    vendedor: str,
    cliente: Optional[str] = None,
    limit: int = Query(50, le=200),
    offset: int = 0,
    db: str = Query("oficial"),
    _user=Depends(get_current_user)
):
    if db == 'sw':
        # SW → DATABASE_MLT → CABEZACOMPROBANTES
        c = conn('WIN1252', db=DATABASE_MLT)
        cur = c.cursor()
        params = [vendedor.upper()]
        where_cli = ""
        if cliente:
            where_cli = "AND CODIGOCLIENTE = ?"
            params.append(cliente)
        cur.execute(f"""
            SELECT FIRST {limit} SKIP {offset}
                NUMEROCOMPROBANTE, CODIGOCLIENTE, RAZONSOCIAL,
                FECHACOMPROBANTE, TOTAL, ANULADA, COMENTARIOS, FECHAVENCIMIENTO,
                NULL, NULL, CODIGOUSUARIO2
            FROM "CABEZACOMPROBANTES"
            WHERE TIPOCOMPROBANTE = 'NP' AND CODIGOUSUARIO = ? AND ANULADA = 0
                {where_cli}
            ORDER BY FECHACOMPROBANTE DESC, NUMEROCOMPROBANTE DESC
        """, params)
        rows = cur.fetchall()
        c.close()
    else:
        # L1 → DATABASE → CABEZAPEDIDOS
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        params = [vendedor.upper()]
        where_cli = ""
        if cliente:
            where_cli = "AND CODIGOCLIENTE = ?"
            params.append(cliente)
        cur.execute(f"""
            SELECT FIRST {limit} SKIP {offset}
                NUMEROCOMPROBANTE, CODIGOCLIENTE, RAZONSOCIAL,
                FECHACOMPROBANTE, TOTAL, ANULADA, COMENTARIOS, FECHAENTREGA,
                OPERACION, FECHATERMINADA, CODIGOUSUARIO2
            FROM "CABEZAPEDIDOS"
            WHERE TIPOCOMPROBANTE = 'NP' AND CODIGOUSUARIO = ? AND ANULADA = '0'
                {where_cli} AND (PRIORIDAD IS NULL OR PRIORIDAD = '1')
            ORDER BY FECHACOMPROBANTE DESC, NUMEROCOMPROBANTE DESC
        """, params)
        rows = cur.fetchall()
        c.close()
    return [{
        "numero": r[0], "cod_cliente": r[1], "razonsocial": r[2],
        "fecha":  r[3], "total": r[4], "anulada": r[5],
        "comentarios": r[6], "fecha_entrega": r[7],
        "operacion": str(r[8]).strip() if r[8] is not None else "1",
        "fecha_terminada": str(r[9]) if r[9] else None,
        "responsable": str(r[10]).strip() if r[10] else None
    } for r in rows]

@app.post("/pedidos/{numero}/terminar")
def terminar_pedido(numero: str, codigousuario: str = Query(...)):
    """Marca el pedido como terminado: FECHATERMINADA = ahora, CODIGOUSUARIO2 = responsable."""
    from datetime import datetime
    ahora = datetime.now()
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute("""
            UPDATE "CABEZAPEDIDOS"
            SET FECHATERMINADA = ?, CODIGOUSUARIO2 = ?, FECHAMODIFICACION = ?
            WHERE TIPOCOMPROBANTE = 'NP' AND NUMEROCOMPROBANTE = ?
        """, (ahora, codigousuario.upper(), ahora, numero))
        c.commit()
        c.close()
        return {"ok": True, "numero": numero, "fechaterminada": str(ahora), "responsable": codigousuario.upper()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pedidos/{numero}/detalle")
def get_pedido_detalle(numero: str, db: str = Query("oficial")):
    if db == 'sw':
        # SW → DATABASE_MLT → CUERPOCOMPROBANTES (sin JOIN a ARTICULOS que no existe)
        c = conn('WIN1252', db=DATABASE_MLT)
        cur = c.cursor()
        cur.execute(
            "SELECT cc.LINEA, "
            "COALESCE(NULLIF(TRIM(cc.CODIGOPARTICULAR),''), TRIM(cc.CODIGOARTICULO)), "
            "cc.DESCRIPCION, cc.CANTIDAD, "
            "cc.DESCUENTO, cc.PRECIOUNITARIO, cc.PRECIOTOTAL, cc.PORCENTAJEIVA "
            "FROM \"CUERPOCOMPROBANTES\" cc "
            "WHERE cc.TIPOCOMPROBANTE = 'NP' AND cc.NUMEROCOMPROBANTE = ? ORDER BY cc.LINEA",
            (numero,)
        )
    else:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        cur.execute(
            "SELECT cp.LINEA, "
            "COALESCE(NULLIF(TRIM(cp.CODIGOPARTICULAR),''), NULLIF(TRIM(a.CODIGOPARTICULAR),''), TRIM(cp.CODIGOARTICULO)), "
            "cp.DESCRIPCION, cp.CANTIDAD, "
            "cp.DESCUENTO, cp.PRECIOUNITARIO, cp.PRECIOTOTAL, cp.PORCENTAJEIVA "
            "FROM \"CUERPOPEDIDOS\" cp "
            "LEFT JOIN \"ARTICULOS\" a ON a.CODIGOARTICULO = cp.CODIGOARTICULO "
            "WHERE cp.TIPOCOMPROBANTE = 'NP' AND cp.NUMEROCOMPROBANTE = ? ORDER BY cp.LINEA",
            (numero,)
        )
    rows = cur.fetchall()
    c.close()
    return [{
        "linea": r[0], "codigo": r[1], "descripcion": r[2],
        "cantidad": r[3], "descuento": r[4],
        "precio_unitario": r[5], "precio_total": r[6], "iva": r[7]
    } for r in rows]

@app.get("/pedidos/{numero}/copia-datos")
def get_pedido_copia(numero: str, db: str = Query("oficial"), _u=Depends(get_current_user)):
    """Devuelve cabecera + items de un pedido para pre-cargar en nuevo NV."""
    db_path = DATABASE_MLT if db == 'sw' else DATABASE
    c = conn(db=db_path)
    cur = c.cursor()
    cur.execute(
        'SELECT CODIGOCLIENTE, RAZONSOCIAL, CODIGOMULTIPLAZO, CODIGOTRANSPORTE, '
        'DIRECCION, CODIGOUSUARIO '
        'FROM "CABEZAPEDIDOS" WHERE NUMEROCOMPROBANTE = ? AND TIPOCOMPROBANTE = \'NP\'',
        (numero,)
    )
    cab = cur.fetchone()
    if not cab:
        c.close()
        raise HTTPException(404, f"Pedido {numero} no encontrado")
    codigocliente, razonsocial, codigomultiplazo, codigotransporte, direccion, codigousuario = cab
    # Deposito del primer item
    cur.execute(
        'SELECT FIRST 1 CODIGODEPOSITO FROM "CUERPOPEDIDOS" WHERE NUMEROCOMPROBANTE = ? AND TIPOCOMPROBANTE = \'NP\'',
        (numero,)
    )
    dep_row = cur.fetchone()
    codigodeposito = str(dep_row[0] or '001').strip() if dep_row else '001'
    # Items (misma conexión, mismo DB)
    cur.execute(
        'SELECT COALESCE(NULLIF(TRIM(it.CODIGOPARTICULAR),\'\'), NULLIF(TRIM(a.CODIGOPARTICULAR),\'\'), TRIM(it.CODIGOARTICULO)), '
        'it.DESCRIPCION, it.CANTIDAD, it.PRECIOUNITARIO, it.DESCUENTO, it.PORCENTAJEIVA '
        'FROM "CUERPOPEDIDOS" it '
        'LEFT JOIN "ARTICULOS" a ON a.CODIGOARTICULO = it.CODIGOARTICULO '
        'WHERE it.NUMEROCOMPROBANTE = ? AND it.TIPOCOMPROBANTE = \'NP\' ORDER BY it.LINEA',
        (numero,)
    )
    items = [{"codigo": r[0], "descripcion": r[1], "cantidad": float(r[2] or 1),
              "precio_unitario": float(r[3] or 0), "descuento": float(r[4] or 0),
              "iva": float(r[5] or 21)} for r in cur.fetchall()]
    c.close()
    return {
        "codigocliente": str(codigocliente or '').strip(),
        "razonsocial": str(razonsocial or '').strip(),
        "codigomultiplazo": str(codigomultiplazo or '').strip(),
        "codigotransporte": str(codigotransporte or '0').strip(),
        "domicilio_entrega": str(direccion or '').strip(),
        "codigodeposito": codigodeposito,
        "codigousuario": str(codigousuario or '').strip(),
        "items": items
    }

@app.get("/presupuestos/{numero}/debug-raw")
def get_presupuesto_debug(numero: str):
    """DEBUG TEMPORAL: devuelve todos los campos de CABEZAPRESUPUESTOS para comparar."""
    try:
        c = conn(db=DATABASE)
        cur = c.cursor()
        cur.execute(
            'SELECT TIPOCOMPROBANTE, NUMEROCOMPROBANTE, CODIGOCLIENTE, RAZONSOCIAL, '
            'FECHACOMPROBANTE, FECHAVENCIMIENTO, TOTAL, ANULADA, CODIGOUSUARIO, '
            'CODIGOMULTIPLAZO, CODIGOTRANSPORTE, CODIGOOPERACION, CLASECOMPROBANTE, '
            'CODIGORESPONSABLE, CODIGOUSUARIO2, DESCUENTOPORCENTAJE, FECHAAPROBADO, '
            'CODIGOUSUARIOAPROBACION, TIPOIVA '
            'FROM "CABEZAPRESUPUESTOS" WHERE NUMEROCOMPROBANTE = ?', (numero,)
        )
        row = cur.fetchone()
        c.close()
        if not row:
            raise HTTPException(404, f"Presupuesto {numero} no encontrado")
        cols = ['tipocomprobante','numerocomprobante','codigocliente','razonsocial',
                'fechacomprobante','fechavencimiento','total','anulada','codigousuario',
                'codigomultiplazo','codigotransporte','codigooperacion','clasecomprobante',
                'codigoresponsable','codigousuario2','descuentoporcentaje','fechaaprobado',
                'codigousuarioaprobacion','tipoiva']
        return {c: str(v) if v is not None else None for c, v in zip(cols, row)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/presupuestos/{numero}/copia-datos")
def get_presupuesto_copia(numero: str, _u=Depends(get_current_user)):
    """Devuelve cabecera + items de un presupuesto para pre-cargar en nuevo NV."""
    try:
        c = conn(db=DATABASE)
        cur = c.cursor()
        cur.execute(
            'SELECT CODIGOCLIENTE, RAZONSOCIAL, CODIGOMULTIPLAZO, CODIGOTRANSPORTE, '
            'DIRECCION, CODIGOUSUARIO '
            'FROM "CABEZAPRESUPUESTOS" WHERE NUMEROCOMPROBANTE = ?',
            (numero,)
        )
        cab = cur.fetchone()
        if not cab:
            c.close()
            raise HTTPException(404, f"Presupuesto {numero} no encontrado")
        codigocliente, razonsocial, codigomultiplazo, codigotransporte, direccion, codigousuario = cab
        codigodeposito = '001'  # CABEZAPRESUPUESTOS y CUERPOPRESUPUESTOS no tienen CODIGODEPOSITO
        cur.execute(
            'SELECT COALESCE(NULLIF(TRIM(cp.CODIGOPARTICULAR),\'\'), NULLIF(TRIM(a.CODIGOPARTICULAR),\'\'), TRIM(cp.CODIGOARTICULO)), '
            'cp.DESCRIPCION, cp.CANTIDAD, cp.PRECIOUNITARIO, cp.BONIFICACION, cp.PORCENTAJEIVA '
            'FROM "CUERPOPRESUPUESTOS" cp '
            'LEFT JOIN "ARTICULOS" a ON a.CODIGOARTICULO = cp.CODIGOARTICULO '
            'WHERE cp.NUMEROCOMPROBANTE = ? ORDER BY cp.LINEA',
            (numero,)
        )
        items = [{"codigo": r[0], "descripcion": r[1], "cantidad": float(r[2] or 1),
                  "precio_unitario": float(r[3] or 0), "descuento": float(r[4] or 0),
                  "iva": float(r[5] or 21)} for r in cur.fetchall()]
        c.close()
        return {
            "codigocliente": str(codigocliente or '').strip(),
            "razonsocial": str(razonsocial or '').strip(),
            "codigomultiplazo": str(codigomultiplazo or '').strip(),
            "codigotransporte": str(codigotransporte or '0').strip(),
            "domicilio_entrega": str(direccion or '').strip(),
            "codigodeposito": str(codigodeposito or '001').strip() or '001',
            "codigousuario": str(codigousuario or '').strip(),
            "items": items
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error interno al obtener presupuesto {numero}: {type(e).__name__}: {e}")

# ─── PDF Nota de Pedido ────────────────────────────────────────────────────────
@app.get("/pedidos/{numero}/pdf")
def pedido_pdf(numero: str, db: str = Query("oficial"),
               descuento_promo_pct: float = Query(0.0),
               descuento_promo_nombre: str = Query("")):
    db_path = DATABASE_MLT if db == 'sw' else DATABASE

    # ── 1. Datos empresa ───────────────────────────────────────────────────────
    razon_soc = 'MICROBELL S.A.'
    dir_emp   = 'AV. MONROE 5088 PISO 2'
    tel_emp   = '+54 11 3988-0024'
    email_emp = 'info@microbellsa.com.ar'
    web_emp   = 'www.microbellsa.com'
    cuit_emp  = '30-70839018-2'
    try:
        c_e = conn('WIN1252', db=db_path)
        cur_e = c_e.cursor()
        cur_e.execute(
            'SELECT RAZONSOCIAL, DIRECCION, TELEFONO, EMAIL, DIRECCIONWEB, CUIT '
            'FROM "SUCURSALES" WHERE CODIGOSUCURSAL = ?', ('PRINCIPAL',)
        )
        emp = cur_e.fetchone()
        c_e.close()
        if emp:
            razon_soc = (emp[0] or razon_soc).strip()
            dir_emp   = (emp[1] or dir_emp).strip()
            tel_emp   = (emp[2] or tel_emp).strip()
            email_emp = (emp[3] or email_emp).strip()
            web_emp   = (emp[4] or web_emp).strip()
            cuit_emp  = (emp[5] or cuit_emp).strip()
    except Exception:
        pass

    # ── 2. Cabeza pedido ── L1 y SW ambos en CABEZAPEDIDOS de DATABASE ──────────
    extra_where = " AND TIPOCOMPROBANTE = 'NP'"
    try:
        c_h = conn('WIN1252', db=DATABASE)
        cur_h = c_h.cursor()
        cur_h.execute(
            'SELECT CODIGOCLIENTE, RAZONSOCIAL, FECHACOMPROBANTE, FECHAENTREGA, '
            'TOTAL, IVA1, COMENTARIOS, CODIGOUSUARIO, CODIGOMULTIPLAZO, '
            'CODIGOTRANSPORTE, DIRECCION, TIPOIVA, TELEFONO '
            f'FROM "CABEZAPEDIDOS" WHERE NUMEROCOMPROBANTE = ?{extra_where}',
            (numero,)
        )
        cab = cur_h.fetchone()
        c_h.close()
    except Exception as ex:
        raise HTTPException(500, f"Cabeza: {ex}")
    if not cab:
        raise HTTPException(404, f"Pedido {numero} no encontrado")

    cod_cli, rs_cli, fec_comp, fec_entrega, total_cab, iva1_cab, \
    comentarios, cod_usu, cod_multi, cod_transp, dir_cli, tipo_iva, tel_cli = cab

    subtotal_cab = float(total_cab or 0)
    iva1_val     = float(iva1_cab or 0)
    total_final  = subtotal_cab + iva1_val

    # ── 3. Items ── L1 y SW ambos en CUERPOPEDIDOS de DATABASE ──────────────────
    try:
        c_it = conn('WIN1252', db=DATABASE)
        cur_it = c_it.cursor()
        cur_it.execute(
            'SELECT COALESCE(NULLIF(TRIM(it.CODIGOPARTICULAR),\'\'), NULLIF(TRIM(a.CODIGOPARTICULAR),\'\'), TRIM(it.CODIGOARTICULO)), '
            'it.DESCRIPCION, it.CANTIDAD, it.PRECIOUNITARIO, '
            'it.DESCUENTO, it.PRECIOTOTAL, it.PORCENTAJEIVA '
            'FROM "CUERPOPEDIDOS" it '
            'LEFT JOIN "ARTICULOS" a ON a.CODIGOARTICULO = it.CODIGOARTICULO '
            f'WHERE it.NUMEROCOMPROBANTE = ? AND it.TIPOCOMPROBANTE = \'NP\' ORDER BY it.LINEA',
            (numero,)
        )
        items = cur_it.fetchall()
        c_it.close()
    except Exception as ex:
        raise HTTPException(500, f"Items: {ex}")

    # ── 4. Datos cliente ───────────────────────────────────────────────────────
    # CLIENTES y USUARIOS siempre en DATABASE (tabla maestra)
    cuit_cli = ''
    tel_pdf  = str(tel_cli or '').strip().lstrip('-').strip().split()[0] if str(tel_cli or '').strip().lstrip('-').strip() else ''
    vendedor_nombre = cod_usu or ''
    try:
        c_cl = conn('WIN1252', db=DATABASE)
        cur_cl = c_cl.cursor()
        cur_cl.execute(
            'SELECT CUIT, TELEFONO, TELEFONOCELULAR, CODIGOPARTICULAR FROM "CLIENTES" WHERE CODIGOCLIENTE = ?',
            (cod_cli,)
        )
        r_cl = cur_cl.fetchone()
        if r_cl:
            cuit_cli = (r_cl[0] or '').strip()
            _cod_particular = (r_cl[3] or '').strip()
            if _cod_particular:
                cod_cli = _cod_particular
            if not tel_pdf:
                def _tp(v):
                    s = str(v or '').strip().lstrip('-').strip()
                    try: s = str(int(float(s))) if s and ('E' in s.upper() or '.' in s) else s
                    except: pass
                    return s
                tel_pdf = _tp(r_cl[1]) or _tp(r_cl[2])
        cur_cl.execute(
            'SELECT NOMBRE, APELLIDO FROM "USUARIOS" WHERE CODIGOUSUARIO = ?',
            (cod_usu,)
        )
        r_usu = cur_cl.fetchone()
        if r_usu:
            vendedor_nombre = f"{(r_usu[0] or '').strip()} {(r_usu[1] or '').strip()}".strip() or cod_usu
        c_cl.close()
    except Exception:
        pass

    # ── 5. Transporte descripción ──────────────────────────────────────────────
    transporte_desc = 'A CONVENIR'
    try:
        if cod_transp and str(cod_transp).strip() not in ('', '0'):
            # Buscar transporte en DATABASE principal (MLT no tiene la tabla completa)
            for _tr_db in ([db_path, DATABASE] if db == 'sw' else [db_path]):
                try:
                    c_tr = conn('WIN1252', db=_tr_db)
                    cur_tr = c_tr.cursor()
                    cur_tr.execute('SELECT DESCRIPCION FROM "TRANSPORTES" WHERE CODIGOTRANSPORTE = ?', (cod_transp,))
                    r_tr = cur_tr.fetchone()
                    c_tr.close()
                    if r_tr and (r_tr[0] or '').strip():
                        transporte_desc = r_tr[0].strip()
                        break
                except Exception:
                    pass
    except Exception:
        pass

    # ── 6. Condición de venta ──────────────────────────────────────────────────
    cond_venta = ''
    try:
        if cod_multi:
            c_mp = conn('WIN1252', db=DATABASE)
            cur_mp = c_mp.cursor()
            cur_mp.execute('SELECT DESCRIPCION FROM "MULTIPLAZOS" WHERE CODIGOMULTIPLAZO = ?', (cod_multi,))
            r_mp = cur_mp.fetchone()
            if r_mp: cond_venta = (r_mp[0] or '').strip()
            c_mp.close()
    except Exception:
        pass

    # ── 7. Depósito ────────────────────────────────────────────────────────────
    deposito_desc = ''
    try:
        c_dep = conn('WIN1252', db=db_path)
        cur_dep = c_dep.cursor()
        cur_dep.execute(
            f'SELECT FIRST 1 CODIGODEPOSITO FROM {item_table} WHERE NUMEROCOMPROBANTE = ?{extra_where}',
            (numero,)
        )
        r_dep = cur_dep.fetchone()
        if r_dep:
            cod_dep = str(r_dep[0] or '').strip()
            cur_dep.execute('SELECT NOMBRE FROM "DEPOSITOS" WHERE CODIGODEPOSITO = ?', (cod_dep,))
            r_dn = cur_dep.fetchone()
            deposito_desc = (r_dn[0] or cod_dep).strip() if r_dn else cod_dep
        c_dep.close()
    except Exception:
        pass

    # ── 8. Construcción PDF ────────────────────────────────────────────────────
    buf  = BytesIO()
    PAGE_W, PAGE_H = A4
    mg   = 14 * mm
    BOTTOM_BLOCK = 48 * mm   # espacio reservado al pie para totales + footer

    s_norm  = ParagraphStyle('pn',  fontSize=8,  leading=11, fontName='Helvetica')
    s_sm    = ParagraphStyle('psm', fontSize=7,  leading=9,  fontName='Helvetica')
    s_bold  = ParagraphStyle('pb',  fontSize=8,  leading=11, fontName='Helvetica-Bold')
    s_h2    = ParagraphStyle('ph2', fontSize=10, leading=13, fontName='Helvetica-Bold')
    s_c     = ParagraphStyle('pc',  fontSize=7.5,leading=10, fontName='Helvetica',      alignment=TA_CENTER)
    s_c_b   = ParagraphStyle('pcb', fontSize=7.5,leading=10, fontName='Helvetica-Bold', alignment=TA_CENTER)
    s_r     = ParagraphStyle('pr',  fontSize=8,  leading=11, fontName='Helvetica',      alignment=TA_RIGHT)
    s_r_b   = ParagraphStyle('prb', fontSize=9,  leading=12, fontName='Helvetica-Bold', alignment=TA_RIGHT)
    s_label = ParagraphStyle('pl',  fontSize=8,  leading=11, fontName='Helvetica-Bold', textColor=colors.HexColor('#374151'))
    s_val   = ParagraphStyle('pv',  fontSize=8,  leading=11, fontName='Helvetica',      textColor=colors.HexColor('#111827'))

    usable_w = PAGE_W - 2 * mg
    num_fmt  = f"0001-{int(numero):08d}"
    footer_txt = f'{razon_soc}  ·  CUIT {cuit_emp}  ·  {dir_emp}  ·  Tel {tel_emp}  ·  {email_emp}'
    s_ft = ParagraphStyle('ft', fontSize=6.5, leading=9, fontName='Helvetica',
                           alignment=TA_CENTER, textColor=colors.HexColor('#6b7280'))

    # ── Canvas callbacks: totales + firma al pie ───────────────────────────────
    def _draw_bottom(canvas, doc):
        canvas.saveState()
        x0    = mg
        right = PAGE_W - mg
        tot_y = BOTTOM_BLOCK - 8*mm        # Y desde la base de la hoja

        # Línea separadora
        canvas.setStrokeColor(colors.HexColor('#e5e7eb'))
        canvas.setLineWidth(0.5)
        canvas.line(x0, tot_y + 28*mm, right, tot_y + 28*mm)

        # Observaciones (si existen)
        obs_y = tot_y + 30*mm
        obs = str(comentarios or '').strip()
        if obs:
            canvas.setFont('Helvetica-Bold', 8)
            canvas.setFillColor(colors.HexColor('#374151'))
            canvas.drawString(x0, obs_y, 'Observaciones:')
            canvas.setFont('Helvetica', 8)
            canvas.drawString(x0 + 28*mm, obs_y, obs[:120])

        # Bloque totales (derecha)
        tw = 50 * mm
        tx = right - tw
        canvas.setFont('Helvetica', 8)
        canvas.setFillColor(colors.HexColor('#374151'))
        _dto_p = max(0.0, min(float(descuento_promo_pct or 0), 100.0))
        if _dto_p > 0:
            _sub_bruto = subtotal_cab / (1 - _dto_p / 100) if _dto_p < 100 else subtotal_cab
            _dto_monto = _sub_bruto - subtotal_cab
            _lbl_promo = descuento_promo_nombre.strip() or f'Desc. promo combo ({_dto_p:g}%)'
            canvas.drawRightString(tx - 2, tot_y + 26*mm, 'Subtotal sin descuento:')
            canvas.drawRightString(right,  tot_y + 26*mm, _fmt(_sub_bruto))
            canvas.setFillColor(colors.HexColor('#b45309'))
            canvas.drawRightString(tx - 2, tot_y + 20*mm, f'{_lbl_promo}:')
            canvas.drawRightString(right,  tot_y + 20*mm, f'- {_fmt(_dto_monto)}')
            canvas.setFillColor(colors.HexColor('#374151'))
            canvas.drawRightString(tx - 2, tot_y + 14*mm, 'Subtotal c/descuento:')
            canvas.drawRightString(right,  tot_y + 14*mm, _fmt(subtotal_cab))
            canvas.drawRightString(tx - 2, tot_y + 8*mm, 'IVA 21%:')
            canvas.drawRightString(right,  tot_y + 8*mm, _fmt(iva1_val))
            canvas.setLineWidth(0.8); canvas.setStrokeColor(colors.black)
            canvas.line(tx - 5*mm, tot_y + 6*mm, right, tot_y + 6*mm)
            canvas.setFont('Helvetica-Bold', 10); canvas.setFillColor(colors.black)
            canvas.drawRightString(tx - 2, tot_y + 1*mm, 'TOTAL:')
            canvas.drawRightString(right,  tot_y + 1*mm, _fmt(total_final))
        else:
            canvas.drawRightString(tx - 2, tot_y + 20*mm, 'Subtotal:')
            canvas.drawRightString(right,  tot_y + 20*mm, _fmt(subtotal_cab))
            canvas.drawRightString(tx - 2, tot_y + 12*mm, 'IVA 21%:')
            canvas.drawRightString(right,  tot_y + 12*mm, _fmt(iva1_val))
            # Línea sobre total
            canvas.setLineWidth(0.8)
            canvas.setStrokeColor(colors.black)
            canvas.line(tx - 5*mm, tot_y + 10*mm, right, tot_y + 10*mm)
            canvas.setFont('Helvetica-Bold', 10)
            canvas.setFillColor(colors.black)
            canvas.drawRightString(tx - 2, tot_y + 3*mm, 'TOTAL:')
            canvas.drawRightString(right,  tot_y + 3*mm, _fmt(total_final))

        # Líneas de firma (izquierda)
        sig_y = tot_y + 5*mm
        canvas.setLineWidth(0.4)
        canvas.setStrokeColor(colors.HexColor('#9ca3af'))
        canvas.line(x0, sig_y, x0 + 60*mm, sig_y)
        canvas.setFont('Helvetica', 7)
        canvas.setFillColor(colors.HexColor('#6b7280'))
        canvas.drawString(x0, sig_y - 4*mm, 'Firma y aclaración')

        # Footer empresa
        canvas.setFont('Helvetica', 6.5)
        canvas.setFillColor(colors.HexColor('#9ca3af'))
        canvas.drawCentredString(PAGE_W / 2, 8*mm, footer_txt)
        canvas.restoreState()

    doc_obj = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=mg, rightMargin=mg,
        topMargin=5*mm, bottomMargin=BOTTOM_BLOCK
    )

    story = []

    # ── Header ────────────────────────────────────────────────────────────────
    logo_w = 38 * mm
    if os.path.exists(LOGO_PATH):
        logo_cell = [Image(LOGO_PATH, width=logo_w, height=logo_w * 0.32)]
    else:
        logo_cell = [Paragraph(f'<b>{razon_soc}</b>', s_h2)]

    doc_w  = 52 * mm
    emp_fixed = 72 * mm                          # ancho fijo del bloque empresa
    spacer_w  = usable_w - logo_w - doc_w - emp_fixed  # empuja emp_cell al margen derecho

    doc_cell = [
        Paragraph('<b>NOTA DE PEDIDO</b>',
                  ParagraphStyle('dt', fontSize=13, fontName='Helvetica-Bold', alignment=TA_CENTER)),
        Paragraph(f'<b>N°  {num_fmt}</b>',
                  ParagraphStyle('dn', fontSize=11, fontName='Helvetica-Bold', alignment=TA_CENTER)),
        Spacer(1, 3*mm),
        Paragraph(f'Fecha: <b>{_d(fec_comp)}</b>',
                  ParagraphStyle('df', fontSize=8.5, fontName='Helvetica', alignment=TA_CENTER)),
        Paragraph(f'Fecha Entrega: <b>{_d(fec_entrega)}</b>',
                  ParagraphStyle('de', fontSize=8.5, fontName='Helvetica', alignment=TA_CENTER)),
    ]
    s_emp_l  = ParagraphStyle('pel',  fontSize=8, leading=11, fontName='Helvetica')
    s_emp_lb = ParagraphStyle('pelb', fontSize=8, leading=11, fontName='Helvetica-Bold')
    emp_cell = [
        Paragraph(f'<b>{razon_soc}</b>', s_emp_lb),
        Paragraph(dir_emp,              s_emp_l),
        Paragraph(f'Tel: {tel_emp}',    s_emp_l),
        Paragraph(f'CUIT: {cuit_emp}',  s_emp_l),
        Paragraph(email_emp,            s_emp_l),
        Paragraph(web_emp,              s_emp_l),
    ]

    hdr_tbl = Table([[logo_cell, doc_cell, Paragraph('', s_emp_l), emp_cell]],
                    colWidths=[logo_w, doc_w, spacer_w, emp_fixed])
    hdr_tbl.setStyle(TableStyle([
        ('VALIGN',       (0,0),(-1,-1), 'MIDDLE'),
        ('ALIGN',        (1,0),(1,0),   'CENTER'),
        ('ALIGN',        (3,0),(3,0),   'LEFT'),
        ('LEFTPADDING',  (0,0),(-1,-1), 4),
        ('RIGHTPADDING', (0,0),(-1,-1), 0),
        ('TOPPADDING',   (0,0),(-1,-1), 0),
        ('BOTTOMPADDING',(0,0),(-1,-1), 0),
    ]))
    story.append(hdr_tbl)
    story.append(HRFlowable(width='100%', thickness=1.5, color=colors.HexColor('#1a56db'),
                            spaceBefore=4*mm, spaceAfter=4*mm))

    # ── Datos cliente ──────────────────────────────────────────────────────────
    pad = TableStyle([
        ('TOPPADDING',   (0,0),(-1,-1), 3),
        ('BOTTOMPADDING',(0,0),(-1,-1), 3),
        ('LEFTPADDING',  (0,0),(-1,-1), 4),
        ('RIGHTPADDING', (0,0),(-1,-1), 4),
    ])
    lbl_w = 24 * mm
    half  = (usable_w - 5*mm) / 2
    cli_nombre = f"{cod_cli} - {rs_cli}".strip(' -') if cod_cli and rs_cli else (rs_cli or cod_cli or '')
    cli_l = Table([
        [Paragraph('Cliente:',   s_label), Paragraph(cli_nombre,    s_val)],
        [Paragraph('CUIT:',      s_label), Paragraph(cuit_cli or '', s_val)],
        [Paragraph('Dirección:', s_label), Paragraph(dir_cli or '',  s_val)],
        [Paragraph('Teléfono:',  s_label), Paragraph(tel_pdf or '',  s_val)],
    ], colWidths=[lbl_w, half - lbl_w])
    cli_r = Table([
        [Paragraph('Cond. Venta:', s_label), Paragraph(cond_venta or '',      s_val)],
        [Paragraph('Transporte:',  s_label), Paragraph(transporte_desc or '', s_val)],
        [Paragraph('Depósito:',    s_label), Paragraph(deposito_desc or '',   s_val)],
        [Paragraph('Vendedor:',    s_label), Paragraph(vendedor_nombre or '', s_val)],
    ], colWidths=[26*mm, half - 26*mm])
    for t in (cli_l, cli_r):
        t.setStyle(pad)

    cli_outer = Table([[cli_l, Spacer(5*mm,1), cli_r]], colWidths=[half, 5*mm, half])
    cli_outer.setStyle(TableStyle([
        ('VALIGN', (0,0),(-1,-1), 'TOP'),
        ('BOX',    (0,0),(0,0),   0.5, colors.HexColor('#d1d5db')),
        ('BOX',    (2,0),(2,0),   0.5, colors.HexColor('#d1d5db')),
        ('BACKGROUND',(0,0),(0,0), colors.HexColor('#f9fafb')),
        ('BACKGROUND',(2,0),(2,0), colors.HexColor('#f9fafb')),
    ]))
    story.append(cli_outer)
    story.append(Spacer(1, 5*mm))

    # ── Tabla artículos ────────────────────────────────────────────────────────
    # col_w suma = usable_w exacto
    col_w = [18*mm, 78*mm, 16*mm, 28*mm, 14*mm, 28*mm]   # = 182mm
    hdr_row = [
        Paragraph('Código',      s_c_b),
        Paragraph('Descripción', s_c_b),
        Paragraph('Cantidad',    s_c_b),
        Paragraph('P. Unitario', s_c_b),
        Paragraph('Dto %',       s_c_b),
        Paragraph('P. Total',    s_c_b),
    ]
    s_ri = ParagraphStyle('ri', fontSize=8, leading=11, fontName='Helvetica', alignment=TA_RIGHT)
    items_data = [hdr_row]
    for it in items:
        cod_art, desc_art, cant, pu, bonif, ptotal, piva = it
        cod_str = str(cod_art or '').strip()
        if cod_str.endswith('.0'):
            cod_str = cod_str[:-2]
        try:
            cant_str = str(int(float(cant))) if float(cant) == int(float(cant)) else str(cant)
        except Exception:
            cant_str = str(cant)
        items_data.append([
            Paragraph(cod_str,                    s_c),
            Paragraph(str(desc_art or '').strip(), s_norm),
            Paragraph(cant_str,                   s_c),
            Paragraph(_fmt(pu),                   s_ri),
            Paragraph(f"{float(bonif or 0):.2f}%", s_c),
            Paragraph(_fmt(ptotal),               s_ri),
        ])

    items_tbl = Table(items_data, colWidths=col_w, repeatRows=1)
    items_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0),  colors.HexColor('#1a56db')),
        ('TEXTCOLOR',     (0,0),(-1,0),  colors.white),
        ('FONTNAME',      (0,0),(-1,0),  'Helvetica-Bold'),
        ('ROWBACKGROUNDS',(0,1),(-1,-1), [colors.white, colors.HexColor('#eff6ff')]),
        ('GRID',          (0,0),(-1,-1), 0.3, colors.HexColor('#e5e7eb')),
        ('TOPPADDING',    (0,0),(-1,-1), 4),
        ('BOTTOMPADDING', (0,0),(-1,-1), 4),
        ('LEFTPADDING',   (0,0),(-1,-1), 4),
        ('RIGHTPADDING',  (0,0),(-1,-1), 4),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
    ]))
    story.append(items_tbl)

    doc_obj.build(story, onFirstPage=_draw_bottom, onLaterPages=_draw_bottom)
    buf.seek(0)
    fname = f"Nota_Pedido_{numero}.pdf"
    return StreamingResponse(buf, media_type='application/pdf',
                             headers={'Content-Disposition': f'inline; filename="{fname}"'})


class ItemDoc(BaseModel):
    codigoarticulo: str
    codigoparticular: str = ""
    descripcion: str
    cantidad: float
    preciounitario: float
    descuento: float = 0.0
    porcentajeiva: float = 21.0
    linea_presupuesto: Optional[int] = None   # línea en CUERPOPRESUPUESTOS si viene de presupuesto

class NuevoPedido(BaseModel):
    codigocliente: str
    razonsocial: str
    codigousuario: str
    comentarios: str = ""
    codigomultiplazo: Optional[str] = None
    codigotransporte: str = "0"
    codigodeposito: str = "001"
    domicilio_entrega: Optional[str] = None
    numero_presupuesto: Optional[str] = None  # si el pedido absorbe un presupuesto aprobado
    descuento_general: float = 0.0
    descuento_promo_pct: float = 0.0      # descuento por escalón de combos (tipo producto)
    descuento_promo_nombre: str = ""       # nombre del escalón aplicado
    items: list[ItemDoc]

@app.post("/pedidos")
def crear_pedido(body: NuevoPedido, db: str = Query("oficial")):
    db_path = DATABASE_MLT if db == 'sw' else DATABASE
    # LOG para debug de routing
    try:
        import datetime
        with open('C:/api_vendedores/pedido_debug.log', 'a', encoding='utf-8') as _lf:
            _lf.write(f"{datetime.datetime.now()} | db_param={db!r} | db_path={db_path!r} | usuario={body.codigousuario!r}\n")
    except Exception:
        pass
    # Obtener datos del cliente — busca en oficial si SW no tiene CLIENTES
    direccion, tipoiva, telefono, atencion_p, cuit_cli = '-', 'CF', '-', '', ''
    cli_transp_p, cli_reparto_p = '', ''
    def _tel_p(v):
        if v is None: return ''
        s = str(v).strip().lstrip('-').strip()
        try:
            s = str(int(float(s))) if s and ('E' in s.upper() or '.' in s) else s
        except Exception:
            pass
        return s
    for cli_db in ([db_path, DATABASE] if db == 'sw' else [db_path]):
        try:
            c_cli = conn('WIN1252', db=cli_db)
            cur_cli = c_cli.cursor()
            cur_cli.execute(
                'SELECT DIRECCION, TELEFONO, TELEFONOCELULAR, '
                'NOMBRE, APELLIDO, CODIGOTRANSPORTE, REPARTOPROPIO, CUIT, CONDICIONIVA '
                'FROM "CLIENTES" WHERE CODIGOCLIENTE = ?',
                (body.codigocliente,)
            )
            cli = cur_cli.fetchone()
            c_cli.close()
            if cli:
                direccion      = (cli[0] or '').strip() or '-'
                _VALID_IVA = {'CF','EX','EXPO','MOA','NR','RI','RM'}
                _cond  = (cli[8] or '').strip().upper()
                if _cond in _VALID_IVA:
                    tipoiva = _cond
                elif 'INSCRIPTO' in _cond:
                    tipoiva = 'RI'
                elif 'EXENTO' in _cond:
                    tipoiva = 'EX'
                elif 'MONOTRIB' in _cond:
                    tipoiva = 'MOA'
                elif 'NO RESPONSABL' in _cond:
                    tipoiva = 'NR'
                elif 'EXPORTAC' in _cond:
                    tipoiva = 'EXPO'
                else:
                    tipoiva = 'CF'
                tel_r          = _tel_p(cli[1])
                tel_c          = _tel_p(cli[2])
                telefono       = tel_r or tel_c or '-'
                nombre_c       = (cli[3] or '').strip()
                apellido_c     = (cli[4] or '').strip()
                atencion_p     = f"{nombre_c} {apellido_c}".strip()
                cli_transp_p   = str(cli[5] or '').strip()
                cli_reparto_p  = str(cli[6] or '').strip()
                cuit_cli       = str(cli[7] or '').strip()
                break
        except Exception:
            continue

    if body.domicilio_entrega:
        direccion = body.domicilio_entrega

    # Si cliente tiene reparto propio y no se eligió transporte, buscar código de REPARTO PROPIO
    if cli_reparto_p == '1' and (not body.codigotransporte or body.codigotransporte == '0'):
        try:
            c_tr = conn('WIN1252', db=DATABASE)
            cur_tr = c_tr.cursor()
            cur_tr.execute(
                "SELECT FIRST 1 CODIGOTRANSPORTE FROM \"TRANSPORTES\" "
                "WHERE UPPER(DESCRIPCION) CONTAINING 'REPARTO PROPIO' AND ACTIVO <> '0'"
            )
            row_tr = cur_tr.fetchone()
            c_tr.close()
            if row_tr:
                cli_transp_p = str(row_tr[0]).strip()
        except Exception:
            pass

    from datetime import datetime, timedelta
    now = datetime.now()
    fecha = now.strftime('%Y-%m-%d %H:%M:%S')
    fecha_entrega = (now + timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
    subtotal = sum(it.cantidad * it.preciounitario * (1 - it.descuento / 100) for it in body.items)
    iva1 = sum(it.cantidad * it.preciounitario * (1 - it.descuento / 100) * (it.porcentajeiva / 100) for it in body.items)
    dto_gral = max(0.0, min(float(body.descuento_general or 0), 100.0))
    dto_promo = max(0.0, min(float(body.descuento_promo_pct or 0), 100.0))
    total = subtotal * (1 - dto_gral / 100) * (1 - dto_promo / 100)

    # Resolver CODIGOCLIENTE correcto según BD destino
    # body.codigocliente viene siempre en código L1; para SW hay que traducirlo via CODIGOPARTICULAR
    codigocliente_destino = body.codigocliente
    if db == 'sw':
        try:
            _c1 = conn('WIN1252', db=DATABASE)
            _cur1 = _c1.cursor()
            _cur1.execute(
                'SELECT CODIGOPARTICULAR FROM "CLIENTES" WHERE CODIGOCLIENTE = ?',
                (body.codigocliente,)
            )
            _r1 = _cur1.fetchone()
            _c1.close()
            cod_particular = (_r1[0] or '').strip() if _r1 else ''
            if cod_particular:
                _c2 = conn('WIN1252', db=DATABASE_MLT)
                _cur2 = _c2.cursor()
                _cur2.execute(
                    'SELECT CODIGOCLIENTE FROM "CLIENTES" WHERE CODIGOPARTICULAR = ? OR CODIGOCLIENTE = ?',
                    (cod_particular, cod_particular)
                )
                _r2 = _cur2.fetchone()
                _c2.close()
                if _r2:
                    codigocliente_destino = str(_r2[0]).strip()
        except Exception:
            pass  # si falla la traducción, usa el código original

    # Pedidos siempre en DATABASE (CABEZAPEDIDOS existe solo en L1)
    c = conn('WIN1252', db=DATABASE)
    cur = c.cursor()
    try:
        # Transporte: usar el del cliente como fallback
        transp_final = (
            body.codigotransporte if body.codigotransporte and body.codigotransporte != '0'
            else (cli_transp_p if cli_transp_p and cli_transp_p != '0' else '0')
        )
        deposito = body.codigodeposito or '001'

        # L1 y SW → DATABASE → CABEZAPEDIDOS / CUERPOPEDIDOS
        # SW usa ENTREGAR='1', INTERES=0.0, CODIGOFINANCIACION='0'; L1 los deja en NULL
        cur.execute(
            "SELECT VALOR FROM \"PARAMETROS\" WHERE TRIM(TIPODOCUMENTO) = 'NP' WITH LOCK"
        )
        row_param_np = cur.fetchone()
        nuevo_num_int_o = int(float(row_param_np[0])) if row_param_np else 0
        cur.execute(
            'SELECT MAX(CAST(NUMEROCOMPROBANTE AS INTEGER)) FROM "CABEZAPEDIDOS"'
            ' WHERE TIPOCOMPROBANTE = ?', ('NP',)
        )
        max_ped = int(cur.fetchone()[0] or 0)
        if nuevo_num_int_o <= max_ped:
            nuevo_num_int_o = max_ped + 1
        nuevo_num = str(nuevo_num_int_o)
        cur.execute(
            "UPDATE \"PARAMETROS\" SET VALOR = ? WHERE TRIM(TIPODOCUMENTO) = 'NP'",
            (nuevo_num_int_o + 1,)
        )
        cur.execute("""
            INSERT INTO "CABEZAPEDIDOS"
            (TIPOCOMPROBANTE, NUMEROCOMPROBANTE, CODIGOCLIENTE, RAZONSOCIAL,
             FECHACOMPROBANTE, TOTAL, ANULADA, CODIGOUSUARIO, COMENTARIOS,
             PORCIVA1, IVA1, PORCIVA2, IVA2, PAGADO, OPERACION, ENTREGAR,
             DIRECCION, TIPOIVA, TELEFONO, FECHAENTREGA, PRIORIDAD,
             VENTECNICOS, COMENTARIOSST, FORMAPAGO, CODIGOTRANSPORTE,
             CODIGOMULTIPLAZO,
             COEFICIENTEIVA, CODIGOMONEDA, COTIZACION,
             NUMEROTRANSACCION, COTIZACIONFIJA, PORCENTAJEFLETE, MONTOFLETE,
             LISTAPRECIO, CLASECOMPROBANTE, CODIGOACOPIO,
             VALIDACTACTE, NUMEROAUTORIZACIONENTREGA, CUIT,
             DESCUENTOPORCENTAJE, DESCUENTOMONTO, DESCUENTODESCRIPCION,
             CODIGOUSUARIO2, FECHATERMINADA, FECHAMODIFICACION,
             INTERES, CODIGOFINANCIACION)
            VALUES ('NP', ?, ?, ?, ?, ?, '0', ?, ?,
                    0.0, ?, 0.0, 0.0, 0.0, '1', ?,
                    ?, ?, ?, ?, '1', '0', ' ', '', ?,
                    ?,
                    1.0, 'PESOS', 1.0, '0', '0', 0.0, 0.0,
                    '1', '0', '0',
                    1, '0', ?,
                    ?, ?, ?,
                    ?, NULL, ?,
                    ?, ?)
        """, (nuevo_num, body.codigocliente, body.razonsocial, fecha,
              round(total, 2), body.codigousuario.upper(), body.comentarios,
              iva1, '1' if db == 'sw' else '0',
              direccion, tipoiva, telefono, fecha_entrega,
              transp_final,
              int(body.codigomultiplazo) if body.codigomultiplazo else 0,
              cuit_cli,
              -round(dto_gral, 6), -round(subtotal * dto_gral / 100, 2),
              f'{dto_gral:.6f} %' if dto_gral > 0 else '0,000000 %',
              body.codigousuario.upper(), fecha,
              0.0 if db == 'sw' else None,
              '0' if db == 'sw' else None))
        # Resolver codigoparticular → CODIGOARTICULO real en Firebird (evita código incorrecto en ERP)
        _cod_parts = list({(it.codigoparticular or it.codigoarticulo).strip() for it in body.items if it.codigoarticulo})
        _cod_map = {}
        if _cod_parts:
            try:
                _ph = ','.join('?' * len(_cod_parts))
                _rows = c.cursor().execute(
                    f'SELECT TRIM(CODIGOARTICULO), TRIM(CODIGOPARTICULAR) FROM "ARTICULOS" WHERE TRIM(CODIGOPARTICULAR) IN ({_ph})',
                    _cod_parts).fetchall()
                for _r in _rows:
                    if _r[1]: _cod_map[str(_r[1]).strip()] = str(_r[0]).strip()
            except Exception: pass

        for i, it in enumerate(body.items, 1):
            _real_cod = _cod_map.get((it.codigoparticular or it.codigoarticulo).strip(), it.codigoarticulo)
            subtotal_item = it.cantidad * it.preciounitario * (1 - it.descuento / 100)
            cur.execute("""
                INSERT INTO "CUERPOPEDIDOS"
                (TIPOCOMPROBANTE, NUMEROCOMPROBANTE, LINEA, CODIGOARTICULO,
                 DESCRIPCION, CANTIDAD, DESCUENTO, PRECIOUNITARIO, PRECIOTOTAL,
                 PORCENTAJEIVA, CANTIDADREMITIDA, ESCONJUNTO,
                 GARANTIA, LOTE, FECHAMODIFICACION, NUMEROTRANSACCION,
                 CODIGODEPOSITO, CANTIDADPREPARADA, CANTIDADCOMPRA,
                 CANTIDADPRODUCCION, CANTIDADENVIADA, CANTIDADCANCELADA,
                 COEFICIENTECONVERSION, ESEXENTO, ESPRECIOPACTADO,
                 CODIGOPARTICULAR)
                VALUES ('NP', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '0',
                        0, '000', ?, '0', ?, ?, 0,
                        0, 0, 0,
                        0.0, '0', '0',
                        ?)
            """, (nuevo_num, i, _real_cod, it.descripcion,
                  it.cantidad, it.descuento, it.preciounitario, subtotal_item,
                  it.porcentajeiva, 0, fecha, deposito, 0,
                  it.codigoparticular or it.codigoarticulo))

        c.commit()

        # Si el pedido absorbe un presupuesto aprobado, actualizar CANTIDADREMITIDA
        if body.numero_presupuesto:
            try:
                c_pr = conn('WIN1252', db=db_path)
                cur_pr = c_pr.cursor()
                for it in body.items:
                    if it.linea_presupuesto is not None:
                        cur_pr.execute(
                            'UPDATE "CUERPOPRESUPUESTOS" '
                            'SET CANTIDADREMITIDA = COALESCE(CANTIDADREMITIDA, 0) + ? '
                            'WHERE NUMEROCOMPROBANTE = ? AND LINEA = ?',
                            (it.cantidad, body.numero_presupuesto, it.linea_presupuesto)
                        )
                c_pr.commit()
                c_pr.close()
            except Exception:
                pass  # No rollback del pedido por fallo en presupuesto

        # Invalidar caché del depósito afectado para que otros vendedores vean stock actualizado
        threading.Thread(target=_fma_cache_invalidate, args=([body.codigodeposito],), daemon=True).start()
        return {"ok": True, "numero": nuevo_num, "total": round(total + iva1, 2), "_db_usado": db, "_db_path": db_path}
    except Exception as e:
        c.rollback()
        raise HTTPException(500, str(e))
    finally:
        c.close()

@app.get("/debug/cab_full/{numero}")
def debug_cab_full(numero: str):
    """Muestra TODOS los campos de CABEZAPEDIDOS para un NP dado."""
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        cur.execute(
            'SELECT * FROM "CABEZAPEDIDOS" WHERE TIPOCOMPROBANTE = ? AND NUMEROCOMPROBANTE = ?',
            ('NP', numero)
        )
        row = cur.fetchone()
        if not row:
            c.close()
            return {"error": f"NP {numero} no encontrado"}
        cols = [d[0] for d in cur.description]
        c.close()
        return dict(zip(cols, [str(v) if v is not None else None for v in row]))
    except Exception as e:
        return {"error": str(e)}

@app.get("/pedidos/proximo")
def get_proximo_pedido(db: str = Query("oficial")):
    # El contador NP siempre está en DATABASE (BD principal), sin importar si es SW u oficial
    try:
        c = conn('LATIN1', db=DATABASE)
        cur = c.cursor()
        cur.execute("SELECT VALOR FROM \"PARAMETROS\" WHERE TRIM(TIPODOCUMENTO) = 'NP'")
        row = cur.fetchone()
        c.close()
        if not row:
            raise HTTPException(500, "No se encontró parámetro de numeración NP")
        return {"proximo": int(float(row[0]))}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/presupuestos/proximo")
def get_proximo_presupuesto(db: str = Query("oficial")):
    db_path = DATABASE_MLT if db == 'sw' else DATABASE
    try:
        c = conn('LATIN1', db=db_path)
        cur = c.cursor()
        cur.execute("SELECT VALOR FROM \"PARAMETROS\" WHERE TIPODOCUMENTO = 'PR'")
        row = cur.fetchone()
        c.close()
        if not row:
            raise HTTPException(500, "No se encontró parámetro de numeración PR")
        return {"proximo": int(float(row[0]))}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

# ─── Presupuestos (solo del vendedor) ─────────────────────────────────────────
@app.get("/presupuestos")
def get_presupuestos(
    vendedor: str,
    cliente: Optional[str] = None,
    limit: int = Query(50, le=200),
    offset: int = 0,
    _user=Depends(get_current_user)
):
    c = conn()
    cur = c.cursor()
    params = [vendedor.upper()]
    where_cli = ""
    if cliente:
        where_cli = "AND CODIGOCLIENTE = ?"
        params.append(cliente)

    cur.execute(f"""
        SELECT FIRST {limit} SKIP {offset}
            NUMEROCOMPROBANTE, CODIGOCLIENTE, RAZONSOCIAL,
            FECHACOMPROBANTE, FECHAVENCIMIENTO, TOTAL, ANULADA, COMENTARIOS
        FROM "CABEZAPRESUPUESTOS"
        WHERE CODIGOUSUARIO = ? AND ANULADA = '0'
            {where_cli}
        ORDER BY FECHACOMPROBANTE DESC, NUMEROCOMPROBANTE DESC
    """, params)
    rows = cur.fetchall()
    c.close()
    return [{
        "numero": r[0], "cod_cliente": r[1], "razonsocial": r[2],
        "fecha": r[3], "fecha_vto": r[4], "total": r[5],
        "anulada": r[6], "comentarios": r[7]
    } for r in rows]

@app.get("/presupuestos/pendientes/{codigocliente}")
def get_presupuestos_pendientes(codigocliente: str, db: str = Query("oficial")):
    """Presupuestos aprobados con items aún no completados para el cliente."""
    # CABEZAPRESUPUESTOS siempre en BD principal
    c = conn('LATIN1', db=DATABASE)
    cur = c.cursor()
    # Cabezas: aprobadas (FECHAAPROBADO IS NOT NULL), no anuladas, con items pendientes
    cur.execute("""
        SELECT DISTINCT cp.NUMEROCOMPROBANTE, cp.FECHACOMPROBANTE, cp.TOTAL, cp.COMENTARIOS,
               cp.CODIGOMULTIPLAZO, cp.CODIGOTRANSPORTE, cp.DIRECCION, cp.CODIGODEPOSITO
        FROM "CABEZAPRESUPUESTOS" cp
        JOIN "CUERPOPRESUPUESTOS" cu ON cu.NUMEROCOMPROBANTE = cp.NUMEROCOMPROBANTE
        WHERE cp.CODIGOCLIENTE = ?
          AND cp.ANULADA = '0'
          AND cp.FECHAAPROBADO IS NOT NULL
          AND cp.FECHAAPROBADO > CAST('1900-01-02 00:00:00' AS TIMESTAMP)
          AND (cu.CANTIDAD - COALESCE(cu.CANTIDADREMITIDA, 0)) > 0
        ORDER BY cp.NUMEROCOMPROBANTE DESC
    """, (codigocliente,))
    cabezas = cur.fetchall()
    result = []
    for cab in cabezas:
        numero = cab[0]
        cur2 = c.cursor()
        cur2.execute("""
            SELECT LINEA, CODIGOARTICULO, DESCRIPCION,
                   CANTIDAD, COALESCE(CANTIDADREMITIDA, 0),
                   BONIFICACION, PRECIOUNITARIO, PORCENTAJEIVA,
                   CODIGOPARTICULAR
            FROM "CUERPOPRESUPUESTOS"
            WHERE NUMEROCOMPROBANTE = ?
              AND (CANTIDAD - COALESCE(CANTIDADREMITIDA, 0)) > 0
            ORDER BY LINEA
        """, (numero,))
        items = []
        for r in cur2.fetchall():
            pendiente = float(r[3]) - float(r[4])
            cod_particular = (r[8] or '').strip() or (r[1] or '').strip()
            items.append({
                "linea": r[0], "codigo": r[1], "codigoparticular": cod_particular,
                "descripcion": r[2],
                "cantidad_total": float(r[3]),
                "cantidad_remitida": float(r[4]),
                "cantidad_pendiente": pendiente,
                "descuento": float(r[5]),
                "precio_unitario": float(r[6]),
                "iva": float(r[7])
            })
        result.append({
            "numero": numero,
            "fecha": str(cab[1]),
            "total": float(cab[2]) if cab[2] else 0,
            "comentarios": cab[3] or "",
            "codigomultiplazo": str(cab[4] or '0'),
            "codigotransporte": str(cab[5] or '0'),
            "direccion": cab[6] or "",
            "codigodeposito": str(cab[7] or '001').strip() or '001',
            "items": items
        })
    c.close()
    return result

@app.get("/presupuestos/{numero}/detalle")
def get_presupuesto_detalle(numero: str):
    c = conn()
    cur = c.cursor()
    cur.execute(
        "SELECT cp.LINEA, "
        "COALESCE(NULLIF(TRIM(cp.CODIGOPARTICULAR),''), NULLIF(TRIM(a.CODIGOPARTICULAR),''), TRIM(cp.CODIGOARTICULO)), "
        "cp.DESCRIPCION, cp.CANTIDAD, "
        "cp.BONIFICACION, cp.PRECIOUNITARIO, cp.PRECIOTOTAL, cp.PORCENTAJEIVA "
        "FROM \"CUERPOPRESUPUESTOS\" cp "
        "LEFT JOIN \"ARTICULOS\" a ON a.CODIGOARTICULO = cp.CODIGOARTICULO "
        "WHERE cp.NUMEROCOMPROBANTE = ? ORDER BY cp.LINEA",
        (numero,)
    )
    rows = cur.fetchall()
    c.close()
    return [{
        "linea": r[0], "codigo": r[1], "descripcion": r[2],
        "cantidad": r[3], "descuento": r[4],
        "precio_unitario": r[5], "precio_total": r[6], "iva": r[7]
    } for r in rows]

class NuevoPresupuesto(BaseModel):
    codigocliente: str
    razonsocial: str
    codigousuario: str
    comentarios: str = ""
    codigomultiplazo: Optional[str] = None
    codigotransporte: str = "0"
    domicilio_entrega: Optional[str] = None
    descuento_general: float = 0.0
    descuento_promo_pct: float = 0.0
    descuento_promo_nombre: str = ""
    items: list[ItemDoc]

@app.post("/presupuestos")
def crear_presupuesto(body: NuevoPresupuesto, db: str = Query("oficial")):
    db_path = DATABASE_MLT if db == 'sw' else DATABASE
    # Obtener datos del cliente (WIN1252)
    try:
        c_cli = conn('WIN1252', db=db_path)
        cur_cli = c_cli.cursor()
        cur_cli.execute(
            'SELECT DIRECCION, TELEFONO, TELEFONOCELULAR, NOMBRE, APELLIDO, '
            'CODIGOTRANSPORTE, REPARTOPROPIO, CONDICIONIVA '
            'FROM "CLIENTES" WHERE CODIGOCLIENTE = ?',
            (body.codigocliente,)
        )
        cli = cur_cli.fetchone()
        c_cli.close()
        if cli:
            direccion     = (cli[0] or '').strip() or ''
            _VALID_IVA_P = {'CF','EX','EXPO','MOA','NR','RI','RM'}
            _cond_pr = (cli[7] or '').strip().upper()
            if _cond_pr in _VALID_IVA_P:
                tipoiva = _cond_pr
            elif 'INSCRIPTO' in _cond_pr:
                tipoiva = 'RI'
            elif 'EXENTO' in _cond_pr:
                tipoiva = 'EX'
            elif 'MONOTRIB' in _cond_pr:
                tipoiva = 'MOA'
            elif 'NO RESPONSABL' in _cond_pr:
                tipoiva = 'NR'
            elif 'EXPORTAC' in _cond_pr:
                tipoiva = 'EXPO'
            else:
                tipoiva = 'CF'
            def _tel(v):
                if v is None: return ''
                s = str(v).strip().lstrip('-').strip()
                # Si Firebird devuelve numérico (ej: 4.51E+14), convertir a entero
                try:
                    s = str(int(float(s))) if s and ('E' in s.upper() or '.' in s) else s
                except Exception:
                    pass
                return s
            tel_raw       = _tel(cli[1])
            tel_cel       = _tel(cli[2])
            telefono      = tel_raw or tel_cel
            nombre_c      = (cli[3] or '').strip()
            apellido_c    = (cli[4] or '').strip()
            atencion      = f"{nombre_c} {apellido_c}".strip()
            cli_transp    = str(cli[5] or '').strip()
            cli_reparto   = str(cli[6] or '').strip()
        else:
            direccion, tipoiva, telefono, atencion = '', 'CF', '', ''
            cli_transp, cli_reparto = '', ''
    except Exception:
        direccion, tipoiva, telefono, atencion = '', 'CF', '', ''
        cli_transp, cli_reparto = '', ''

    if body.domicilio_entrega:
        direccion = body.domicilio_entrega

    from datetime import datetime, timedelta
    now = datetime.now()
    fecha = now.strftime('%Y-%m-%d %H:%M:%S')
    fecha_vto = (now + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    subtotal = sum(it.cantidad * it.preciounitario * (1 - it.descuento / 100) for it in body.items)
    dto_gral = max(0.0, min(float(body.descuento_general or 0), 100.0))
    dto_promo = max(0.0, min(float(body.descuento_promo_pct or 0), 100.0))
    _factor_dto = (1 - dto_gral / 100) * (1 - dto_promo / 100)
    iva1 = sum(it.cantidad * it.preciounitario * (1 - it.descuento / 100) * (it.porcentajeiva / 100) for it in body.items) * _factor_dto
    total = subtotal * _factor_dto

    c = conn('LATIN1', db=db_path)
    cur = c.cursor()
    try:
        cur.execute("SELECT VALOR FROM \"PARAMETROS\" WHERE TIPODOCUMENTO = 'PR'")
        row_param = cur.fetchone()
        if not row_param:
            raise Exception("No se encontró parámetro de numeración PR en PARAMETROS")
        nuevo_num = str(int(float(row_param[0])))

        cur.execute("""
                INSERT INTO "CABEZAPRESUPUESTOS"
                (TIPOCOMPROBANTE, NUMEROCOMPROBANTE, CODIGOCLIENTE, RAZONSOCIAL,
                 FECHACOMPROBANTE, FECHAVENCIMIENTO, TOTAL, ANULADA,
                 CODIGOUSUARIO, COMENTARIOS,
                 PORCIVA1, IVA1, PORCIVA2, IVA2,
                 DIRECCION, TIPOIVA, TELEFONO,
                 CODIGOUSUARIO2, COEFICIENTEIVA, FECHAMODIFICACION,
                 CODIGORESPONSABLE, CODIGOMONEDA, COTIZACION,
                 ATENCION, NUMEROTRANSACCION, FORMAPAGO, CODIGOOPERACION,
                 LISTAPRECIO, COTIZACIONFIJA, PORCENTAJEFLETE, MONTOFLETE,
                 CLASECOMPROBANTE, CODIGOACOPIO,
                 CODIGOMULTIPLAZO, CODIGOTRANSPORTE,
                 DESCUENTOPORCENTAJE, DESCUENTOMONTO, DESCUENTODESCRIPCION,
                 FECHAAPROBADO, CODIGOUSUARIOAPROBACION)
                VALUES ('PR', ?, ?, ?, ?, ?, ?, '0', ?, ?,
                        21.0, ?, 0.0, 0.0,
                        ?, ?, ?,
                        ?, 1.0, ?,
                        ?, 'PESOS', 1.0,
                        ?, '0', '', '1',
                        '1', '0', 0.0, 0.0,
                        '0', '0',
                        ?, ?,
                        ?, ?, ?,
                        '1900-01-01 00:00:00', '')
            """, (nuevo_num, body.codigocliente, body.razonsocial, fecha,
                  fecha_vto, round(total, 2), body.codigousuario.upper(), body.comentarios,
                  iva1,
                  direccion, tipoiva, telefono,
                  body.codigousuario.upper(), fecha,
                  body.codigousuario.upper(), atencion,
                  int(body.codigomultiplazo) if body.codigomultiplazo else 0,
                  body.codigotransporte if body.codigotransporte and body.codigotransporte != '0'
                  else (cli_transp if cli_transp and cli_transp != '0' else '0'),
                  -round(dto_gral, 6), -round(subtotal * dto_gral / 100, 2),
                  f'{dto_gral:.6f} %' if dto_gral > 0 else '0,000000 %'))

        # Resolver codigoparticular → CODIGOARTICULO real en Firebird
        _cod_parts_p = list({(it.codigoparticular or it.codigoarticulo).strip() for it in body.items if it.codigoarticulo})
        _cod_map_p = {}
        if _cod_parts_p:
            try:
                _ph_p = ','.join('?' * len(_cod_parts_p))
                _rows_p = c.cursor().execute(
                    f'SELECT TRIM(CODIGOARTICULO), TRIM(CODIGOPARTICULAR) FROM "ARTICULOS" WHERE TRIM(CODIGOPARTICULAR) IN ({_ph_p})',
                    _cod_parts_p).fetchall()
                for _r in _rows_p:
                    if _r[1]: _cod_map_p[str(_r[1]).strip()] = str(_r[0]).strip()
            except Exception: pass

        for i, it in enumerate(body.items, 1):
            _real_cod_p = _cod_map_p.get((it.codigoparticular or it.codigoarticulo).strip(), it.codigoarticulo)
            subtotal_item = it.cantidad * it.preciounitario * (1 - it.descuento / 100)
            cur.execute("""
                INSERT INTO "CUERPOPRESUPUESTOS"
                (TIPOCOMPROBANTE, NUMEROCOMPROBANTE, LINEA, CODIGOARTICULO,
                 DESCRIPCION, CANTIDAD, BONIFICACION, PRECIOUNITARIO, PRECIOTOTAL,
                 PORCENTAJEIVA, CANTIDADREMITIDA, ESCONJUNTO,
                 GARANTIA, FECHAMODIFICACION, NUMEROTRANSACCION,
                 CODIGOPARTICULAR,
                 LOTE, INTERES, COEFICIENTECONVERSION,
                 ITEMGANADO, ESEXENTO, ESALTERNATIVO, ESPRECIOPACTADO)
                VALUES ('PR', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '0',
                        0, ?, '0',
                        ?,
                        '000', 0.0, 0.0,
                        '1', '0', '0', '0')
            """, (nuevo_num, i, _real_cod_p, it.descripcion,
                  it.cantidad, it.descuento, it.preciounitario, subtotal_item,
                  it.porcentajeiva, 0, fecha,
                  it.codigoparticular or it.codigoarticulo))

        cur.execute(
            "UPDATE \"PARAMETROS\" SET VALOR = ? WHERE TIPODOCUMENTO = 'PR'",
            (int(float(row_param[0])) + 1,)
        )
        c.commit()
        c.close()
        # Invalidar caché (presupuesto no tiene deposito en modelo, invalidar todos)
        threading.Thread(target=_fma_cache_invalidate, daemon=True).start()
        return {"ok": True, "numero": nuevo_num, "total": round(total + iva1, 2)}

    except Exception as e:
        c.rollback()
        c.close()
        raise HTTPException(500, str(e))

# ─── PDF Presupuesto ───────────────────────────────────────────────────────────
@app.get("/presupuestos/{numero}/pdf")
def presupuesto_pdf(numero: str, db: str = Query("oficial"),
                    descuento_promo_pct: float = Query(0.0),
                    descuento_promo_nombre: str = Query("")):
    from datetime import datetime
    db_path = DATABASE_MLT if db == 'sw' else DATABASE

    # ── 1. Datos empresa ───────────────────────────────────────────────────────
    razon_soc = 'MICROBELL S.A.'
    dir_emp   = 'AV. MONROE 5088 PISO 2'
    tel_emp   = '+54 11 3988-0024'
    email_emp = 'info@microbellsa.com.ar'
    web_emp   = 'www.microbellsa.com'
    cuit_emp  = '30-70839018-2'
    iibb_emp  = 'CM 901-068199-0'
    fi_emp    = '2021-05-01'
    try:
        c_e = conn('WIN1252', db=db_path)
        cur_e = c_e.cursor()
        cur_e.execute(
            'SELECT RAZONSOCIAL, DIRECCION, TELEFONO, EMAIL, DIRECCIONWEB, '
            'CUIT, INGRESOSBRUTOS, FE_FECHAINC '
            'FROM "SUCURSALES" WHERE CODIGOSUCURSAL = ?', ('PRINCIPAL',)
        )
        emp = cur_e.fetchone()
        c_e.close()
        if emp:
            razon_soc = (emp[0] or razon_soc).strip()
            dir_emp   = (emp[1] or dir_emp).strip()
            tel_emp   = (emp[2] or tel_emp).strip()
            email_emp = (emp[3] or email_emp).strip()
            web_emp   = (emp[4] or web_emp).strip()
            cuit_emp  = (emp[5] or cuit_emp).strip()
            iibb_emp  = (emp[6] or iibb_emp).strip()
            fi_emp    = emp[7] if emp[7] else fi_emp
    except Exception:
        pass  # usa los valores hardcodeados

    # ── 2. Cabeza presupuesto ──────────────────────────────────────────────────
    try:
        c_h = conn('WIN1252', db=db_path)
        cur_h = c_h.cursor()
        cur_h.execute(
            'SELECT CODIGOCLIENTE, RAZONSOCIAL, FECHACOMPROBANTE, FECHAVENCIMIENTO, '
            'TOTAL, IVA1, IVA2, COMENTARIOS, CODIGOUSUARIO, CODIGOMULTIPLAZO, '
            'CODIGOTRANSPORTE, DIRECCION, TIPOIVA, TELEFONO, '
            'COALESCE(DESCUENTOPORCENTAJE, 0) '
            'FROM "CABEZAPRESUPUESTOS" WHERE NUMEROCOMPROBANTE = ?', (numero,)
        )
        cab = cur_h.fetchone()
        c_h.close()
    except Exception as ex:
        raise HTTPException(500, f"Cabeza: {ex}")

    if not cab:
        raise HTTPException(404, f"Presupuesto {numero} no encontrado")

    cod_cli, rs_cli, fec_comp, fec_vto, total_cab, iva1_cab, iva2_cab, \
    comentarios, cod_usu, cod_multi, cod_transp, dir_cli, tipo_iva, tel_cli, \
    dto_pct_cab = cab

    subtotal_cab  = float(total_cab or 0)  # neto después del descuento, sin IVA
    iva1_val      = float(iva1_cab or 0)
    iva2_val      = float(iva2_cab or 0)
    dto_gral_pdf  = float(dto_pct_cab or 0)
    total_final   = subtotal_cab + iva1_val + iva2_val

    # ── 3. Items ───────────────────────────────────────────────────────────────
    try:
        c_it = conn('WIN1252', db=db_path)
        cur_it = c_it.cursor()
        cur_it.execute(
            'SELECT COALESCE(NULLIF(TRIM(it.CODIGOPARTICULAR),\'\'), NULLIF(TRIM(a.CODIGOPARTICULAR),\'\'), TRIM(it.CODIGOARTICULO)), '
            'it.DESCRIPCION, it.CANTIDAD, it.PRECIOUNITARIO, '
            'it.BONIFICACION, it.PRECIOTOTAL, it.PORCENTAJEIVA '
            'FROM "CUERPOPRESUPUESTOS" it '
            'LEFT JOIN "ARTICULOS" a ON a.CODIGOARTICULO = it.CODIGOARTICULO '
            'WHERE it.NUMEROCOMPROBANTE = ? ORDER BY it.LINEA',
            (numero,)
        )
        items = cur_it.fetchall()
        c_it.close()
    except Exception as ex:
        raise HTTPException(500, f"Items: {ex}")

    # Recalcular subtotal desde ítems (TOTAL en CABEZA es IVA-inclusive para Flexxus)
    _dto_abs_pdf = abs(dto_gral_pdf)
    _dto_promo_pdf = max(0.0, min(float(descuento_promo_pct), 100.0))
    subtotal_cab = sum(float(it[5] or 0) for it in items) * (1 - _dto_abs_pdf / 100) * (1 - _dto_promo_pdf / 100)
    total_final  = subtotal_cab + iva1_val + iva2_val

    # ── 4. Datos cliente (CUIT, teléfono, vendedor) ───────────────────────────
    cuit_cli = ''
    vendedor_nombre = cod_usu or ''
    # tel_cli viene de CABEZAPRESUPUESTOS; si está vacío lo buscamos en CLIENTES
    tel_pdf = str(tel_cli or '').strip().lstrip('-').strip()
    try:
        c_cl = conn('WIN1252', db=db_path)
        cur_cl = c_cl.cursor()
        cur_cl.execute(
            'SELECT CUIT, TELEFONO, TELEFONOCELULAR FROM "CLIENTES" WHERE CODIGOCLIENTE = ?',
            (cod_cli,)
        )
        r_cl = cur_cl.fetchone()
        if r_cl:
            cuit_cli = (r_cl[0] or '').strip()
            if not tel_pdf:
                tel_pdf = (r_cl[1] or r_cl[2] or '').strip()
        cur_cl.execute(
            'SELECT RAZONSOCIAL FROM "USUARIOS" WHERE CODIGOUSUARIO = ?', (cod_usu,)
        )
        r_vend = cur_cl.fetchone()
        if r_vend:
            vendedor_nombre = (r_vend[0] or '').strip()
        c_cl.close()
    except Exception:
        pass

    # ── 5. Transporte ──────────────────────────────────────────────────────────
    transporte_desc = str(cod_transp or '').strip()
    if transporte_desc and transporte_desc != '0':
        for _tr_db in ([db_path, DATABASE] if db == 'sw' else [db_path]):
            try:
                c_tr = conn('WIN1252', db=_tr_db)
                cur_tr = c_tr.cursor()
                cur_tr.execute('SELECT DESCRIPCION FROM "TRANSPORTES" WHERE CODIGOTRANSPORTE = ?', (cod_transp,))
                r_tr = cur_tr.fetchone()
                c_tr.close()
                if r_tr and (r_tr[0] or '').strip():
                    transporte_desc = r_tr[0].strip()
                    break
            except Exception:
                pass
        else:
            transporte_desc = 'A CONVENIR'
    else:
        transporte_desc = 'A CONVENIR'

    # ── 6. Condición de pago ───────────────────────────────────────────────────
    cond_pago = str(cod_multi or '').strip()
    if cond_pago and cond_pago not in ('0', ''):
        try:
            c_mp = conn('WIN1252', db=db_path)
            cur_mp = c_mp.cursor()
            cur_mp.execute('SELECT DESCRIPCION FROM "MULTIPLAZOS" WHERE CODIGOMULTIPLAZO = ?', (cod_multi,))
            r_mp = cur_mp.fetchone()
            if r_mp:
                cond_pago = (r_mp[0] or '').strip()
            c_mp.close()
        except Exception:
            pass
    else:
        cond_pago = ''

    # ── 7. Construir PDF ───────────────────────────────────────────────────────
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=10*mm, rightMargin=10*mm,
        topMargin=4*mm, bottomMargin=12*mm
    )

    styles = getSampleStyleSheet()
    W = A4[0] - 20*mm  # ancho útil

    s_norm  = ParagraphStyle('norm',  fontSize=7,  leading=9,  fontName='Helvetica')
    s_bold  = ParagraphStyle('bold',  fontSize=7,  leading=9,  fontName='Helvetica-Bold')
    s_title = ParagraphStyle('title', fontSize=11, leading=13, fontName='Helvetica-Bold', alignment=TA_CENTER)
    s_small = ParagraphStyle('small', fontSize=6,  leading=7,  fontName='Helvetica')
    s_c     = ParagraphStyle('c',     fontSize=7,  leading=9,  fontName='Helvetica', alignment=TA_CENTER)
    s_r     = ParagraphStyle('r',     fontSize=7,  leading=9,  fontName='Helvetica', alignment=TA_RIGHT)
    s_rb    = ParagraphStyle('rb',    fontSize=7,  leading=9,  fontName='Helvetica-Bold', alignment=TA_RIGHT)

    GRIS   = colors.HexColor('#D9D9D9')
    OSCURO = colors.HexColor('#1F3864')
    AZUL   = colors.HexColor('#2E74B5')

    story = []

    # ── ENCABEZADO ─────────────────────────────────────────────────────────────
    # Logo
    logo_cell = ''
    if os.path.exists(LOGO_PATH):
        try:
            _logo_img = Image(LOGO_PATH, width=36*mm, height=36*0.32*mm)
            _logo_img.hAlign = 'CENTER'
            logo_cell = _logo_img
        except Exception:
            logo_cell = Paragraph(razon_soc, s_bold)
    else:
        logo_cell = Paragraph(razon_soc, s_bold)

    emp_info = (
        f"<b>{razon_soc}</b><br/>"
        f"{dir_emp}<br/>"
        f"Tel: {tel_emp}<br/>"
        f"{email_emp}<br/>"
        f"{web_emp}"
    )

    # Caja tipo comprobante "P" (Presupuesto)
    tipo_box = Table(
        [[Paragraph('<b>P</b>', ParagraphStyle('pb', fontSize=22, fontName='Helvetica-Bold', alignment=TA_CENTER, leading=26))]],
        colWidths=[15*mm], rowHeights=[15*mm]
    )
    tipo_box.setStyle(TableStyle([
        ('BOX',            (0,0),(0,0), 1.5, colors.black),
        ('VALIGN',         (0,0),(0,0), 'MIDDLE'),
        ('ALIGN',          (0,0),(0,0), 'CENTER'),
        ('TOPPADDING',     (0,0),(0,0), 6),
        ('BOTTOMPADDING',  (0,0),(0,0), 0),
        ('LEFTPADDING',    (0,0),(0,0), 0),
        ('RIGHTPADDING',   (0,0),(0,0), 0),
    ]))
    cod_info = Paragraph('COD.908', ParagraphStyle('cod', fontSize=8, fontName='Helvetica', alignment=TA_CENTER))

    nro_fmt = numero.zfill(8)
    doc_info = (
        f"<b>PRESUPUESTO</b><br/>"
        f"Nº 0001-{nro_fmt}<br/>"
        f"Fecha: {_d(fec_comp)}<br/>"
        f"CUIT: {cuit_emp}<br/>"
        f"IIBB: {iibb_emp}<br/>"
        f"F.Inicio: {_d(fi_emp)}<br/>"
        f"<font size='5'><b>DOCUMENTO NO VÁLIDO COMO FACTURA</b></font>"
    )

    header_data = [[
        logo_cell,
        Paragraph(emp_info, s_norm),
        Table([[tipo_box],[cod_info]], colWidths=[20*mm]),
        Paragraph(doc_info, s_norm),
    ]]
    header_table = Table(header_data, colWidths=[45*mm, 39*mm, 22*mm, None])
    header_table.setStyle(TableStyle([
        ('VALIGN',    (0,0),(-1,-1), 'TOP'),
        ('ALIGN',     (0,0),(0,0),   'CENTER'),
        ('LEFTPADDING',  (0,0),(-1,-1), 2),
        ('RIGHTPADDING', (0,0),(-1,-1), 2),
        ('TOPPADDING',   (0,0),(-1,-1), 0),
        ('BOTTOMPADDING',(0,0),(-1,-1), 0),
    ]))
    story.append(header_table)
    story.append(Spacer(1, 3*mm))

    # ── DATOS CLIENTE ──────────────────────────────────────────────────────────
    cli_left = (
        f"<b>Cliente:</b> {cod_cli} - {(rs_cli or '').strip()}<br/>"
        f"<b>Dirección:</b> {(dir_cli or '').strip()}<br/>"
        f"<b>Tel:</b> {tel_pdf}<br/>"
        f"<b>CUIT:</b> {cuit_cli}"
    )
    cli_right = (
        f"<b>Cond. IVA:</b> {(tipo_iva or '').strip()}<br/>"
        f"<b>Transporte:</b> {transporte_desc}<br/>"
        f"<b>Asistente:</b> {cod_usu or ''}<br/>"
        f"<b>Vendedor:</b> {vendedor_nombre}"
    )
    cli_table = Table(
        [[Paragraph(cli_left, s_norm), Paragraph(cli_right, s_norm)]],
        colWidths=[W*0.55, W*0.45]
    )
    cli_table.setStyle(TableStyle([
        ('BACKGROUND',   (0,0),(-1,-1), GRIS),
        ('BOX',          (0,0),(-1,-1), 0.5, colors.grey),
        ('LEFTPADDING',  (0,0),(-1,-1), 4),
        ('RIGHTPADDING', (0,0),(-1,-1), 4),
        ('TOPPADDING',   (0,0),(-1,-1), 3),
        ('BOTTOMPADDING',(0,0),(-1,-1), 3),
        ('VALIGN',       (0,0),(-1,-1), 'TOP'),
    ]))
    story.append(cli_table)
    story.append(Spacer(1, 2*mm))

    # ── TABLA ITEMS ────────────────────────────────────────────────────────────
    col_w = [22*mm, None, 18*mm, 28*mm, 26*mm, 28*mm]
    # cabecera
    hdr = ['Código', 'Descripción', 'Cantidad', 'Precio Unit.', 'Bonif.%', 'Subtotal']
    items_data = [[Paragraph(f'<b>{h}</b>', s_c) for h in hdr]]

    for it in items:
        cod_art, desc_art, cant, pu, bonif, ptotal, piva = it
        # Limpiar código: quitar espacios y el .0 si viene como float
        cod_str = str(cod_art or '').strip()
        if cod_str.endswith('.0'):
            cod_str = cod_str[:-2]
        try:
            cant_str = str(int(float(cant))) if float(cant) == int(float(cant)) else str(cant)
        except Exception:
            cant_str = str(cant or '')
        items_data.append([
            Paragraph(cod_str, s_c),
            Paragraph(str(desc_art or ''), s_norm),
            Paragraph(cant_str, s_c),
            Paragraph(_fmt(pu), s_r),
            Paragraph(f"{float(bonif or 0):.2f}%", s_c),
            Paragraph(_fmt(ptotal), s_r),
        ])

    # fila totales
    _dto_p_pr = max(0.0, min(float(descuento_promo_pct or 0), 100.0))
    s_dto = ParagraphStyle('sdto', fontSize=7, leading=9, fontName='Helvetica-Bold', alignment=TA_RIGHT, textColor=colors.HexColor('#b45309'))
    if _dto_p_pr > 0:
        # Descuento por promo combo
        _sub_bruto_pr = subtotal_cab / (1 - _dto_p_pr / 100) if _dto_p_pr < 100 else subtotal_cab
        _dto_m_pr = _sub_bruto_pr - subtotal_cab
        _lbl_pr = (descuento_promo_nombre.strip() or f'Desc. promo combo ({_dto_p_pr:g}%)')
        items_data.append(['', '', '', '', Paragraph('<b>SUBTOTAL S/DESC.</b>', s_rb), Paragraph(_fmt(_sub_bruto_pr), s_rb)])
        items_data.append(['', '', '', '', Paragraph(f'<b>{_lbl_pr}</b>', s_dto), Paragraph(f'<b>- {_fmt(_dto_m_pr)}</b>', s_dto)])
        items_data.append(['', '', '', '', Paragraph('<b>SUBTOTAL</b>', s_rb), Paragraph(_fmt(subtotal_cab), s_rb)])
    elif dto_gral_pdf != 0:
        # Descuento financiero por monto (dto_gral_pdf es negativo en DB → abs para cálculos)
        _dto_abs = abs(dto_gral_pdf)
        _sub_bruto_gral = subtotal_cab / (1 - _dto_abs / 100) if _dto_abs < 100 else subtotal_cab
        _dto_m_gral = _sub_bruto_gral - subtotal_cab
        _lbl_gral = f'Descuento {_dto_abs:g}%'
        items_data.append(['', '', '', '', Paragraph('<b>SUBTOTAL</b>', s_rb), Paragraph(_fmt(_sub_bruto_gral), s_rb)])
        items_data.append(['', '', '', '', Paragraph(f'<b>{_lbl_gral}</b>', s_dto), Paragraph(f'<b>- {_fmt(_dto_m_gral)}</b>', s_dto)])
        items_data.append(['', '', '', '', Paragraph('<b>SUBTOTAL C/DESC.</b>', s_rb), Paragraph(_fmt(subtotal_cab), s_rb)])
    else:
        items_data.append(['', '', '', '', Paragraph('<b>SUBTOTAL</b>', s_rb), Paragraph(_fmt(subtotal_cab), s_rb)])
    if iva1_val:
        items_data.append(['', '', '', '', Paragraph('<b>IVA 21%</b>', s_rb), Paragraph(_fmt(iva1_val), s_rb)])
    if iva2_val:
        items_data.append(['', '', '', '', Paragraph('<b>IVA 10.5%</b>', s_rb), Paragraph(_fmt(iva2_val), s_rb)])
    items_data.append(['', '', '', '', Paragraph('<b>TOTAL</b>', s_rb), Paragraph(_fmt(total_final), s_rb)])

    items_table = Table(items_data, colWidths=col_w, repeatRows=1)
    n_items = len(items) + 1  # +1 header
    ts = TableStyle([
        ('BACKGROUND',    (0,0),(-1,0),          AZUL),
        ('TEXTCOLOR',     (0,0),(-1,0),          colors.white),
        ('FONTNAME',      (0,0),(-1,0),          'Helvetica-Bold'),
        ('FONTSIZE',      (0,0),(-1,-1),         7),
        ('ROWBACKGROUNDS',(0,1),(-1,n_items-1),  [colors.white, colors.HexColor('#EEF3F8')]),
        ('GRID',          (0,0),(-1,n_items-1),  0.3, colors.grey),
        ('LINEABOVE',     (0,n_items),(-1,n_items), 0.8, colors.black),
        ('TOPPADDING',    (0,0),(-1,-1),         2),
        ('BOTTOMPADDING', (0,0),(-1,-1),         2),
        ('LEFTPADDING',   (0,0),(-1,-1),         3),
        ('RIGHTPADDING',  (0,0),(-1,-1),         3),
        ('VALIGN',        (0,0),(-1,-1),         'MIDDLE'),
    ])
    # línea separadora antes del TOTAL
    ts.add('LINEABOVE', (4,-1), (-1,-1), 1.2, colors.black)
    items_table.setStyle(ts)
    story.append(items_table)
    story.append(Spacer(1, 3*mm))

    # ── CONDICIONES DE VENTA ───────────────────────────────────────────────────
    cond_data = [
        [Paragraph('<b>Condiciones de Venta</b>', s_bold), ''],
        [Paragraph(f'<b>Cond. Pago:</b> {cond_pago}', s_norm),
         Paragraph(f'<b>Tipo Cambio:</b> $ 1,00', s_norm)],
        [Paragraph(f'<b>Domicilio Entrega:</b> {(dir_cli or "").strip()}', s_norm),
         Paragraph(f'<b>Fecha Venc.:</b> {_d(fec_vto)}', s_norm)],
        [Paragraph(f'<b>Observaciones:</b> {(comentarios or "").strip()}', s_norm), ''],
    ]
    cond_table = Table(cond_data, colWidths=[W*0.6, W*0.4])
    cond_table.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0), GRIS),
        ('SPAN',          (0,0),(-1,0)),
        ('SPAN',          (0,3),(-1,3)),
        ('BOX',           (0,0),(-1,-1), 0.5, colors.grey),
        ('INNERGRID',     (0,0),(-1,-1), 0.2, colors.lightgrey),
        ('LEFTPADDING',   (0,0),(-1,-1), 4),
        ('RIGHTPADDING',  (0,0),(-1,-1), 4),
        ('TOPPADDING',    (0,0),(-1,-1), 2),
        ('BOTTOMPADDING', (0,0),(-1,-1), 2),
        ('FONTSIZE',      (0,0),(-1,-1), 7),
    ]))
    story.append(cond_table)
    story.append(Spacer(1, 3*mm))

    # ── CUENTAS BANCARIAS (en flujo, centrada, angosta) ───────────────────────
    s_bh  = ParagraphStyle('bh',  fontSize=6, fontName='Helvetica-Bold', textColor=colors.white, alignment=TA_CENTER, leading=8)
    s_bno = ParagraphStyle('bno', fontSize=6, fontName='Helvetica', leading=8)
    s_bc  = ParagraphStyle('bc',  fontSize=6, fontName='Helvetica', leading=8, alignment=TA_CENTER)
    s_bct = ParagraphStyle('bct', fontSize=7, fontName='Helvetica-Bold', textColor=colors.white, alignment=TA_CENTER, leading=9)

    # Fila título "CUENTAS BANCARIAS" que abarca todas las columnas
    # Fila cabecera columnas
    bank_data = [
        [Paragraph('<b>CUENTAS BANCARIAS</b>', s_bct), '', '', ''],
        [Paragraph('<b>BANCO</b>',   s_bh),
         Paragraph('<b>CBU</b>',     s_bh),
         Paragraph('<b>SUCURSAL</b>',s_bh),
         Paragraph('<b>CTA. CTE.</b>',s_bh)],
    ]
    for nombre, cbu, suc, cta in _BANCOS:
        bank_data.append([
            Paragraph(nombre, s_bno), Paragraph(cbu, s_bno),
            Paragraph(suc, s_bc),     Paragraph(cta, s_bno),
        ])
    bank_data.append([
        Paragraph('<b>Mercado Pago</b>', ParagraphStyle('bmp', fontSize=6, fontName='Helvetica-Bold', leading=8)),
        Paragraph(f'{_MP_EMAIL}', s_bno),
        Paragraph('CVU', s_bc),
        Paragraph(_MP_CVU, s_bno),
    ])

    # Ancho total ~118mm, columnas: banco 26 + CBU 54 + suc 15 + cta 23
    BW = [26*mm, 54*mm, 15*mm, 23*mm]
    bank_table = Table(bank_data, colWidths=BW)
    bank_table.setStyle(TableStyle([
        # Título
        ('BACKGROUND',    (0,0),(-1,0),  OSCURO),
        ('SPAN',          (0,0),(-1,0)),
        ('ALIGN',         (0,0),(-1,0),  'CENTER'),
        ('TOPPADDING',    (0,0),(-1,0),  3),
        ('BOTTOMPADDING', (0,0),(-1,0),  3),
        # Cabecera columnas
        ('BACKGROUND',    (0,1),(-1,1),  OSCURO),
        # Filas datos
        ('ROWBACKGROUNDS',(0,2),(-1,-1), [colors.white, colors.HexColor('#EEF3F8')]),
        ('BOX',           (0,0),(-1,-1), 0.5, colors.grey),
        ('INNERGRID',     (0,1),(-1,-1), 0.2, colors.lightgrey),
        ('LEFTPADDING',   (0,0),(-1,-1), 3),
        ('RIGHTPADDING',  (0,0),(-1,-1), 3),
        ('TOPPADDING',    (0,1),(-1,-1), 2),
        ('BOTTOMPADDING', (0,1),(-1,-1), 2),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
    ]))
    bank_table.hAlign = 'CENTER'

    story.append(Spacer(1, 4*mm))
    story.append(bank_table)

    doc.build(story)
    buf.seek(0)
    fname = f"Presupuesto_{nro_fmt}.pdf"
    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{fname}"'}
    )

# ─── Detalle de comprobante ────────────────────────────────────────────────────
@app.get("/comprobantes/{tipo}/{numero}/detalle")
def detalle_comprobante(tipo: str, numero: str):
    sql = """
        SELECT CODIGOARTICULO, DESCRIPCION, CANTIDAD,
               PRECIOUNITARIO, PORCENTAJEIVA, DESCUENTO, PRECIOTOTAL
        FROM "CUERPOCOMPROBANTES"
        WHERE TIPOCOMPROBANTE = ? AND NUMEROCOMPROBANTE = ?
        ORDER BY LINEA
    """
    DB_PROD     = 'c:/flexxus/DB/DB-Microbell.gdb'
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    for db_path in [DATABASE, DATABASE_MLT, DB_PROD, DB_MLT_PROD]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            cur.execute(sql, (tipo, numero))
            rows = cur.fetchall()
            c.close()
            if rows:
                return [{
                    "codigo":      r[0], "descripcion": r[1],
                    "cantidad":    float(r[2]) if r[2] else 0,
                    "precio_unit": float(r[3]) if r[3] else 0,
                    "iva":         float(r[4]) if r[4] else 0,
                    "descuento":   float(r[5]) if r[5] else 0,
                    "total":       float(r[6]) if r[6] else 0,
                } for r in rows]
        except Exception:
            continue
    return []

# ─── PDF Comprobante (FA/FB/FCA/FE/NCA/NCB/NDA/NDB y Proforma SW) ────────────

_TIPO_NOMBRE = {
    'FA':'FACTURA A','FB':'FACTURA B','FCA':'FACTURA DE CREDITO A',
    'FCB':'FACTURA DE CREDITO B','FE':'FACTURA E',
    'NCA':'NOTA DE CREDITO A','NCB':'NOTA DE CREDITO B',
    'NCCA':'NOTA DE CREDITO DE CREDITO A','NCE':'NOTA DE CREDITO E',
    'NDA':'NOTA DE DEBITO A','NDB':'NOTA DE DEBITO B','NDE':'NOTA DE DEBITO E',
}
_TIPO_LETRA = {
    'FA':'A','FCA':'A','NCA':'A','NDA':'A','NCCA':'A',
    'FB':'B','FCB':'B','NCB':'B','NDB':'B',
    'FE':'E','NCE':'E','NDE':'E',
}
_BANCOS = [
    ('Santander Rio','0720131420000001149872','131','11498/7'),
    ('Prov. Bs. As.','0140004501400404115211','4004','041152/1'),
    ('HSBC','1500607500060732055732','607','607-3-205573'),
    ('Galicia','0070154520000005006724','154','5006-7-154/2'),
]
_CVU = 'E-mail: marketing@microbellsa.com.ar\nCVU: 000000310000004756934965'

def _fmt_num(numero: str):
    """4400014918.0 → ('0044','00014918','0044-00014918')"""
    try:
        n = int(float(numero))
        pv  = n // 100_000_000
        seq = n %  100_000_000
        return f'{pv:04d}', f'{seq:08d}', f'{pv:04d}-{seq:08d}'
    except Exception:
        return '0001', numero, numero

def _pesos(v): return f'$ {float(v or 0):,.2f}'.replace(',','X').replace('.',',').replace('X','.')

@app.get("/comprobantes/{tipo}/{numero}/pdf")
def comprobante_pdf_route(tipo: str, numero: str):
    from reportlab.platypus import KeepTogether
    from reportlab.platypus.flowables import Flowable

    class BottomSpacer(Flowable):
        """Empuja el contenido siguiente al pie de página."""
        def __init__(self, footer_h):
            Flowable.__init__(self)
            self._fh = footer_h
        def wrap(self, availWidth, availHeight):
            return availWidth, max(0, availHeight - self._fh)
        def draw(self):
            pass
    from reportlab.lib.utils import ImageReader
    DB_PROD     = 'c:/flexxus/DB/DB-Microbell.gdb'
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'

    # ── 1. Buscar cabeza ───────────────────────────────────────────────────────
    cab = None; items = []; found_db = None; is_mlt = False
    for db_path, mlt in [(DATABASE,False),(DB_PROD,False),(DATABASE_MLT,True),(DB_MLT_PROD,True)]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            cur.execute('SELECT * FROM "CABEZACOMPROBANTES" WHERE TIPOCOMPROBANTE=? AND NUMEROCOMPROBANTE=?',(tipo,numero))
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                cab = {k: (v.strip() if isinstance(v,str) else v) for k,v in zip(cols,row)}
                cur.execute(
                    'SELECT LINEA,CODIGOARTICULO,DESCRIPCION,CANTIDAD,'
                    'PRECIOUNITARIO,DESCUENTO,PORCENTAJEIVA,PRECIOTOTAL '
                    'FROM "CUERPOCOMPROBANTES" '
                    'WHERE TIPOCOMPROBANTE=? AND NUMEROCOMPROBANTE=? ORDER BY LINEA',
                    (tipo, numero)
                )
                items = cur.fetchall()
                found_db = db_path; is_mlt = mlt
                c.close(); break
            c.close()
        except Exception:
            pass
    if not cab:
        raise HTTPException(404, f"{tipo} {numero} no encontrado")

    # ── 2. CAE (solo Línea 1) ──────────────────────────────────────────────────
    cae = None; vto_cae = None
    if not is_mlt:
        for db_path in [found_db, DATABASE, DB_PROD]:
            try:
                c = conn('WIN1252', db=db_path)
                cur = c.cursor()
                cur.execute('SELECT CAE,VENCIMIENTOCAE FROM "CAEAFIP" WHERE TIPOCOMPROBANTE=? AND NUMEROCOMPROBANTE=?',(tipo,numero))
                r = cur.fetchone(); c.close()
                if r and r[0]:
                    cae = str(r[0]).strip()
                    vto_cae = r[1]; break
            except Exception:
                pass

    # ── 2b. Despachos (solo Línea 1) ──────────────────────────────────────────
    # Busca despachos por los códigos de artículo que componen la factura
    # usando STOCKXDESPACHO (CODIGOARTICULO → DESPACHO → ADUANA)
    despachos_lista = []  # lista de "nro - aduana" únicos
    if not is_mlt and items:
        codigos_articulos = list({str(it[1] or '').strip() for it in items if it[1]})
        if codigos_articulos:
            for db_path in [found_db, DATABASE, DB_PROD]:
                try:
                    c_d = conn('WIN1252', db=db_path)
                    cur_d = c_d.cursor()
                    placeholders = ','.join(['?' for _ in codigos_articulos])
                    # MAX(DESPACHO) por artículo = despacho más reciente de importación
                    cur_d.execute(
                        f'SELECT MAX(s.DESPACHO), MAX(s.ADUANA) '
                        f'FROM "STOCKXDESPACHO" s '
                        f'WHERE s.CODIGOARTICULO IN ({placeholders}) '
                        f"AND TRIM(s.DESPACHO) <> '' "
                        f'GROUP BY s.CODIGOARTICULO',
                        codigos_articulos
                    )
                    for r in cur_d.fetchall():
                        nrd    = str(r[0] or '').strip()
                        aduana = str(r[1] or '').strip()
                        # descartar despachos vacíos o con solo ceros
                        if not nrd or not nrd.replace('0','').strip():
                            continue
                        if not aduana or aduana == '-':
                            aduana = 'Aduana de Buenos Aires'
                        entry = f'{nrd} - {aduana}'
                        if entry not in despachos_lista:
                            despachos_lista.append(entry)
                    despachos_lista.sort()
                    c_d.close()
                    if despachos_lista:
                        break
                except Exception:
                    pass

    # ── 3. Datos cliente extra ─────────────────────────────────────────────────
    cli_ingbrutos = ''; cli_localidad = ''; cli_provincia = ''; cli_cp = ''
    try:
        c_cl = conn('WIN1252', db=DATABASE)
        cur_cl = c_cl.cursor()
        cur_cl.execute(
            'SELECT INGRESOSBRUTOS, LOCALIDAD, PROVINCIA, CP '
            'FROM "CLIENTES" WHERE CODIGOCLIENTE=?', (cab.get('CODIGOCLIENTE',''),)
        )
        r_cl = cur_cl.fetchone(); c_cl.close()
        if r_cl:
            cli_ingbrutos = str(r_cl[0] or '').strip()
            cli_localidad = str(r_cl[1] or '').strip()
            cli_provincia = str(r_cl[2] or '').strip()
            cli_cp        = str(r_cl[3] or '').strip()
    except Exception:
        pass

    # ── 4. Vendedor / asistente ────────────────────────────────────────────────
    cod_vend = str(cab.get('CODIGOUSUARIO','') or '').strip()
    cod_asist= str(cab.get('CODIGOUSUARIO2','') or '').strip()

    # ── 5. Condición de venta ──────────────────────────────────────────────────
    cond_venta = ''
    try:
        c_mp = conn('WIN1252', db=DATABASE)
        cur_mp = c_mp.cursor()
        cur_mp.execute('SELECT DESCRIPCION FROM "MULTIPLAZOS" WHERE CODIGOMULTIPLAZO=?',(cab.get('CODIGOMULTIPLAZO',''),))
        r_mp = cur_mp.fetchone(); c_mp.close()
        if r_mp: cond_venta = str(r_mp[0] or '').strip()
    except Exception:
        pass

    pv_str, seq_str, num_display = _fmt_num(numero)

    # ── 6. Generar PDF ─────────────────────────────────────────────────────────
    buf = BytesIO()
    W, H = A4
    m = 14*mm
    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=m, rightMargin=m,
                            topMargin=10*mm, bottomMargin=10*mm)

    AZUL    = colors.HexColor('#1a3a5c')
    CELESTE = colors.HexColor('#4A90D9')   # azul claro para tablas bancarias
    GRIS    = colors.HexColor('#f4f6f9')
    NEGRO   = colors.black
    ROJO    = colors.HexColor('#cc0000')

    base   = getSampleStyleSheet()
    sN     = ParagraphStyle('sN',  fontSize=7,  leading=9,  fontName='Helvetica')
    sNb    = ParagraphStyle('sNb', fontSize=7,  leading=9,  fontName='Helvetica-Bold')
    sNc    = ParagraphStyle('sNc', fontSize=7,  leading=9,  fontName='Helvetica', alignment=TA_CENTER)
    sNr    = ParagraphStyle('sNr', fontSize=7,  leading=9,  fontName='Helvetica', alignment=TA_RIGHT)
    sT     = ParagraphStyle('sT',  fontSize=9,  leading=11, fontName='Helvetica-Bold')
    sTc    = ParagraphStyle('sTc', fontSize=9,  leading=11, fontName='Helvetica-Bold', alignment=TA_CENTER)
    sCAE   = ParagraphStyle('sCAE',fontSize=8,  leading=10, fontName='Helvetica-Bold')
    sCAEv  = ParagraphStyle('sCAEv',fontSize=8, leading=10, fontName='Helvetica')

    cw = W - 2*m  # ancho útil

    story = []

    if is_mlt:
        # ══════════════════════════════════════════════════════════════════════
        # PROFORMA REMITO (SW / DATABASE_MLT)
        # ══════════════════════════════════════════════════════════════════════
        fecha_str = ''
        if cab.get('FECHACOMPROBANTE'):
            try: fecha_str = cab['FECHACOMPROBANTE'].strftime('%d/%m/%Y')
            except Exception: fecha_str = str(cab['FECHACOMPROBANTE'])[:10]

        # Cabeza: logo | badge A | "Proforma Remito / RN-N° / FECHA"
        logo_img = Image(LOGO_PATH, width=40*mm, height=14*mm) if os.path.exists(LOGO_PATH) else Paragraph('microbell S.A.', sT)

        badge_tbl = Table([[Paragraph('<b>A</b>',ParagraphStyle('ba',fontSize=28,fontName='Helvetica-Bold',alignment=TA_CENTER))]],
                          colWidths=[18*mm], rowHeights=[20*mm])
        badge_tbl.setStyle(TableStyle([('BOX',(0,0),(-1,-1),1.5,NEGRO),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('ALIGN',(0,0),(-1,-1),'CENTER')]))

        sRsw  = ParagraphStyle('sRsw',  fontSize=7, leading=9, fontName='Helvetica',      alignment=TA_RIGHT)
        sRbsw = ParagraphStyle('sRbsw', fontSize=7, leading=9, fontName='Helvetica-Bold', alignment=TA_RIGHT)
        right_hdr = [
            Paragraph('<b>Proforma Remito</b>',             ParagraphStyle('ph', fontSize=11,fontName='Helvetica-Bold',alignment=TA_RIGHT)),
            Paragraph('<b>DOCUMENTO NO VALIDO COMO FACTURA</b>', ParagraphStyle('phd',fontSize=7,fontName='Helvetica-Bold',textColor=ROJO,alignment=TA_RIGHT)),
            Paragraph(f'RN - N°: {num_display}',            ParagraphStyle('phn',fontSize=10,fontName='Helvetica-Bold',alignment=TA_RIGHT)),
            Paragraph(f'FECHA: {fecha_str}',                sRbsw),
            Paragraph(f'CUIT: 30-70839018-2',               sRsw),
            Paragraph(f'INGRESOS BRUTOS: CM 901-068199-0',  sRsw),
            Paragraph(f'Inicio de Actividades: 05/09/2005', sRsw),
        ]

        emp_left_sw = [
            logo_img,
            Paragraph('Dirección: AV. MONROE 5088 PISO 2', sN),
            Paragraph('C.A.B.A. CAPITAL FEDERAL C1431CAP', sN),
            Paragraph('Télefono: +54 11 3988-0024', sN),
            Paragraph('Email: info@microbellsa.com.ar  www.microbellsa.com', sN),
        ]

        hdr_tbl = Table([[emp_left_sw, badge_tbl, right_hdr]],
                        colWidths=[cw*0.35, 22*mm, cw*0.50])
        hdr_tbl.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(0,-1),4),('RIGHTPADDING',(0,0),(-1,-1),0)]))
        story.append(hdr_tbl)
        story.append(HRFlowable(width=cw, thickness=1.5, color=AZUL, spaceAfter=4))

        # Bloque cliente
        cod_cli  = str(cab.get('CODIGOCLIENTE','') or '').strip()
        rs_cli   = str(cab.get('RAZONSOCIAL','') or '').strip()
        dir_cli  = str(cab.get('DIRECCION','') or '').strip()
        cuit_cli = str(cab.get('CUIT','') or '').strip()
        tel_cli  = str(cab.get('TELEFONO','') or '').strip()

        cli_rows = [
            [Paragraph(f'<b>Cliente:</b> {cod_cli}  {rs_cli}', sNb), Paragraph(f'<b>CUIT:</b> {cuit_cli}', sN)],
            [Paragraph(f'<b>Dirección:</b> {dir_cli}', sN), Paragraph(f'<b>Cod.Vend.:</b> {cod_vend}', sN)],
            [Paragraph(f'<b>Localidad:</b> {cli_localidad}   <b>Provincia:</b> {cli_provincia}', sN), Paragraph(f'<b>Cod.Asist.:</b> {cod_asist}', sN)],
            [Paragraph(f'<b>Cond. de Venta:</b> {cond_venta}', sN), Spacer(1,1)],
        ]
        cli_tbl = Table(cli_rows, colWidths=[cw*0.62, cw*0.38])
        cli_tbl.setStyle(TableStyle([
            ('BOX',(0,0),(-1,-1),0.5,colors.grey),
            ('INNERGRID',(0,0),(-1,-1),0.3,colors.lightgrey),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),
            ('LEFTPADDING',(0,0),(-1,-1),6),
        ]))
        story.append(cli_tbl)
        story.append(Spacer(1,4))

        # Items SW
        hdrs_sw = ['CANT','CODIGO','ARTÍCULO','DTO','PRECIO UNITARIO','TOTAL']
        cws_sw  = [14*mm, 18*mm, cw-14*mm-18*mm-18*mm-32*mm-28*mm, 18*mm, 32*mm, 28*mm]
        it_rows = [[Paragraph(h, sNb) for h in hdrs_sw]]
        total_sw = 0.0
        for it in items:
            cant  = float(it[3] or 0)
            pu    = float(it[4] or 0)
            dto   = float(it[5] or 0)
            total_item = float(it[7] or 0)
            total_sw += total_item
            obs_it = ''
            it_rows.append([
                Paragraph(str(int(cant)) if cant == int(cant) else str(cant), sNc),
                Paragraph(str(it[1] or ''), sN),
                Paragraph(str(it[2] or ''), sN),
                Paragraph(f'{dto:.2f} %', sNr),
                Paragraph(_pesos(pu), sNr),
                Paragraph(_pesos(total_item), sNr),
            ])
        it_tbl = Table(it_rows, colWidths=cws_sw, repeatRows=1)
        it_tbl.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),CELESTE),('TEXTCOLOR',(0,0),(-1,0),colors.white),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,GRIS]),
            ('BOX',(0,0),(-1,-1),0.5,colors.grey),
            ('INNERGRID',(0,0),(-1,-1),0.3,colors.lightgrey),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),
            ('LEFTPADDING',(0,0),(-1,-1),4),
        ]))
        story.append(it_tbl)
        story.append(Spacer(1,6))

        # Observaciones + cuentas bancarias + total
        obs_sw = str(cab.get('COMENTARIOS','') or '').strip()
        if obs_sw:
            story.append(Paragraph(f'<b>Observaciones:</b> {obs_sw}', sN))
            story.append(Spacer(1,4))

        story.append(BottomSpacer(65*mm))   # empuja footer al pie

        # Tabla bancaria compacta (125mm) centrada
        bw = [26*mm, 46*mm, 18*mm, 26*mm]   # total ~116mm
        bk_data_sw = [
            [Paragraph('<b>CUENTAS BANCARIAS</b>', ParagraphStyle('bct',fontSize=7,fontName='Helvetica-Bold',textColor=colors.white,alignment=TA_CENTER,leading=9)),'','',''],
            [Paragraph('<b>BANCO</b>',sNb),Paragraph('<b>CBU</b>',sNb),Paragraph('<b>SUCURSAL</b>',sNb),Paragraph('<b>CTA. CTE.</b>',sNb)],
        ]
        for b,cbu,suc,cta in _BANCOS:
            bk_data_sw.append([Paragraph(b,sN),Paragraph(cbu,sN),Paragraph(suc,sNc),Paragraph(cta,sN)])
        bk_data_sw.append([Paragraph(_CVU,sN),'','',''])
        bk_tbl_sw = Table(bk_data_sw, colWidths=bw)
        bk_tbl_sw.setStyle(TableStyle([
            ('SPAN',(0,0),(-1,0)),
            ('BACKGROUND',(0,0),(-1,0),CELESTE),('TEXTCOLOR',(0,0),(-1,0),colors.white),
            ('BACKGROUND',(0,1),(-1,1),colors.HexColor('#d0e4f7')),
            ('ROWBACKGROUNDS',(0,2),(-1,-2),[colors.white,GRIS]),
            ('SPAN',(0,-1),(-1,-1)),
            ('BOX',(0,0),(-1,-1),0.5,colors.grey),('INNERGRID',(0,0),(-1,-1),0.3,colors.lightgrey),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('TOPPADDING',(0,0),(-1,-1),2),('BOTTOMPADDING',(0,0),(-1,-1),2),
            ('LEFTPADDING',(0,0),(-1,-1),4),
        ]))

        bk_total_sw = sum(bw)
        pad_sw = (cw - bk_total_sw) / 2   # centrado

        cotiz = float(cab.get('COTIZACION',1) or 1)
        tot_str = _pesos(total_sw)
        sTotal = ParagraphStyle('sTot', fontSize=12, fontName='Helvetica-Bold', alignment=TA_RIGHT)

        footer_sw = Table([
            [Spacer(1,1), bk_tbl_sw, Spacer(1,1)],
            ['', Paragraph(f'Tipo de Cambio: $ {cotiz:.0f}', sN), ''],
            ['', Paragraph(f'<b>TOTAL:  {tot_str}</b>', sTotal), ''],
        ], colWidths=[pad_sw, bk_total_sw, pad_sw])
        footer_sw.setStyle(TableStyle([
            ('VALIGN',(0,0),(-1,-1),'TOP'),
            ('ALIGN',(0,0),(-1,-1),'CENTER'),
        ]))
        story.append(KeepTogether([footer_sw]))

    else:
        # ══════════════════════════════════════════════════════════════════════
        # COMPROBANTE LÍNEA 1 (FA, NCA, NDA, FCA, FE, etc.)
        # ══════════════════════════════════════════════════════════════════════
        tipo_nombre = _TIPO_NOMBRE.get(tipo, tipo)
        letra       = _TIPO_LETRA.get(tipo, 'A')

        fecha_str = ''
        if cab.get('FECHACOMPROBANTE'):
            try: fecha_str = cab['FECHACOMPROBANTE'].strftime('%d/%m/%Y')
            except Exception: fecha_str = str(cab['FECHACOMPROBANTE'])[:10]

        # Logo
        logo_img = Image(LOGO_PATH, width=38*mm, height=13*mm) if os.path.exists(LOGO_PATH) else Paragraph('<b>microbell S.A.</b>', sT)

        emp_left = [
            logo_img,
            Paragraph('Dirección: AV. MONROE 5088 PISO 2', sN),
            Paragraph('C.A.B.A. CAPITAL FEDERAL C1431CAP', sN),
            Paragraph('Télefono: +54 11 3988-0024', sN),
            Paragraph('Email: info@microbellsa.com.ar  www.microbellsa.com', sN),
        ]

        badge_cell = Table([[Paragraph(f'<b>{letra}</b>', ParagraphStyle('ltr',fontSize=30,fontName='Helvetica-Bold',alignment=TA_CENTER))]],
                            colWidths=[20*mm], rowHeights=[22*mm])
        badge_cell.setStyle(TableStyle([('BOX',(0,0),(-1,-1),2,NEGRO),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('ALIGN',(0,0),(-1,-1),'CENTER')]))

        cod_pv_label = Paragraph(f'COD. N° {pv_str}', sNc)

        badge_full = Table([[badge_cell],[cod_pv_label]], colWidths=[22*mm])
        badge_full.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'MIDDLE'),('ALIGN',(0,0),(-1,-1),'CENTER')]))

        sR  = ParagraphStyle('sR',  fontSize=7, leading=9, fontName='Helvetica',      alignment=TA_RIGHT)
        sRb = ParagraphStyle('sRb', fontSize=7, leading=9, fontName='Helvetica-Bold', alignment=TA_RIGHT)
        emp_right = [
            Paragraph(f'<b>{tipo_nombre}</b>', ParagraphStyle('tn',fontSize=9,fontName='Helvetica-Bold',alignment=TA_RIGHT)),
            Paragraph(f'N° {num_display}',     ParagraphStyle('nn',fontSize=9,fontName='Helvetica-Bold',alignment=TA_RIGHT)),
            Paragraph(f'Fecha emisión: {fecha_str}',        sR),
            Paragraph(f'CUIT: 30-70839018-2',               sR),
            Paragraph(f'Ing. Brutos: CM 901-068199-0',       sR),
            Paragraph(f'Inic. Activ.: 05/09/2005',           sR),
            Paragraph(f'RESPONSABLE INSCRIPTO',              sRb),
        ]

        hdr_tbl = Table([[emp_left, badge_full, emp_right]],
                        colWidths=[cw*0.38, 26*mm, cw*0.50])
        hdr_tbl.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(0,-1),4),('RIGHTPADDING',(0,0),(-1,-1),0)]))
        story.append(hdr_tbl)
        story.append(HRFlowable(width=cw, thickness=2, color=AZUL, spaceAfter=4))

        # Bloque cliente
        cod_cli  = str(cab.get('CODIGOCLIENTE','') or '').strip()
        rs_cli   = str(cab.get('RAZONSOCIAL','') or '').strip()
        dir_cli  = str(cab.get('DIRECCION','') or '').strip()
        cuit_cli = str(cab.get('CUIT','') or '').strip()
        tipoiva  = str(cab.get('TIPOIVA','') or '').strip()
        tel_cli  = str(cab.get('TELEFONO','') or '').strip()

        localidad_full = cli_localidad
        if cli_provincia: localidad_full += f'  Provincia: {cli_provincia}'
        if cli_cp:        localidad_full += f'  C.P.: {cli_cp}'

        cli_data = [
            [Paragraph(f'<b>Cliente:</b>   {cod_cli}  {rs_cli}', sN),
             Paragraph(f'<b>C.U.I.T.:</b>  {cuit_cli}', sN)],
            [Paragraph(f'<b>Dirección:</b>  {dir_cli}', sN),
             Paragraph(f'<b>Ing. Brutos:</b>  {cli_ingbrutos}', sN)],
            [Paragraph(f'<b>Localidad:</b>  {localidad_full}', sN),
             Paragraph(f'<b>Cond. Vta.:</b>  {cond_venta}', sN)],
            [Paragraph(f'<b>I.V.A.:</b>  {tipoiva}   <b>C. P.:</b>  {cli_cp}', sN),
             Paragraph(f'<b>Cod. Vend.:</b>  {cod_vend}   <b>Cod. Asist.:</b>  {cod_asist}', sN)],
        ]
        cli_tbl = Table(cli_data, colWidths=[cw*0.55, cw*0.45])
        cli_tbl.setStyle(TableStyle([
            ('BOX',(0,0),(-1,-1),0.5,colors.grey),
            ('INNERGRID',(0,0),(-1,-1),0.3,colors.lightgrey),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),
            ('LEFTPADDING',(0,0),(-1,-1),6),
        ]))
        story.append(cli_tbl)
        story.append(Spacer(1,5))

        # Items
        hdrs_l1 = ['CODIGO','DESCRIPCIÓN','CANTIDAD','P. UNITARIO','DESCUENTO','IVA','TOTAL']
        cws_l1  = [18*mm, cw-18*mm-22*mm-26*mm-22*mm-14*mm-26*mm, 22*mm, 26*mm, 22*mm, 14*mm, 26*mm]
        it_rows = [[Paragraph(h, sNb) for h in hdrs_l1]]
        subtotal = 0.0; iva21_tot = 0.0; iva105_tot = 0.0
        total_neto = 0.0
        for it in items:
            cant  = float(it[3] or 0)
            pu    = float(it[4] or 0)
            dto   = float(it[5] or 0)
            piva  = float(it[6] or 0)
            ptot  = float(it[7] or 0)
            neto_item = pu * cant * (1 - dto/100)
            total_neto += neto_item
            iva_item = neto_item * piva / 100
            if piva >= 20: iva21_tot  += iva_item
            elif piva > 0: iva105_tot += iva_item
            subtotal += neto_item
            it_rows.append([
                Paragraph(str(it[1] or ''), sNc),
                Paragraph(str(it[2] or ''), sN),
                Paragraph(f'{cant:g}', sNc),
                Paragraph(_pesos(pu), sNr),
                Paragraph(f'{dto:.2f}%', sNc),
                Paragraph(f'{piva:.2f}%', sNr),
                Paragraph(_pesos(ptot), sNr),
            ])

        it_tbl = Table(it_rows, colWidths=cws_l1, repeatRows=1)
        it_tbl.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),CELESTE),('TEXTCOLOR',(0,0),(-1,0),colors.white),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,GRIS]),
            ('BOX',(0,0),(-1,-1),0.5,colors.grey),
            ('INNERGRID',(0,0),(-1,-1),0.3,colors.lightgrey),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),
            ('LEFTPADDING',(0,0),(-1,-1),4),
        ]))
        story.append(it_tbl)

        # Sección Despachos (antes del footer) — en línea separados por |
        if despachos_lista:
            story.append(Spacer(1,4))
            desp_txt = ' &nbsp;|&nbsp; '.join(despachos_lista)
            story.append(Paragraph(f'<b>Despachos:</b> {desp_txt}',
                                   ParagraphStyle('sDesp', fontSize=7, leading=9,
                                                  fontName='Helvetica', wordWrap='CJK')))

        story.append(BottomSpacer(95*mm))   # empuja footer al pie

        # Footer: cuentas bancarias centradas + totales IVA derecha
        obs   = str(cab.get('COMENTARIOS','') or '').strip()
        cotiz = float(cab.get('COTIZACION',1) or 1)
        total_cab = float(cab.get('TOTAL',0) or 0)
        iva1_cab  = float(cab.get('IVA1',0) or 0)
        iva2_cab  = float(cab.get('IVA2',0) or 0)
        gran_total = total_cab + iva1_cab + iva2_cab
        dto_total  = float(cab.get('DESCUENTOMONTO',0) or 0)

        bw_l1 = [26*mm, 46*mm, 18*mm, 26*mm]   # 116mm total — compacto, centrado
        bk_data = [[Paragraph('<b>CUENTAS BANCARIAS</b>', sTc), '', '', '']]
        bk_data.append([Paragraph('<b>BANCO</b>',sNb),Paragraph('<b>CBU</b>',sNb),Paragraph('<b>SUC.</b>',sNb),Paragraph('<b>CTA. CTE.</b>',sNb)])
        for b,cbu,suc,cta in _BANCOS:
            bk_data.append([Paragraph(b,sN),Paragraph(cbu,sN),Paragraph(suc,sNc),Paragraph(cta,sN)])
        bk_data.append([Paragraph(_CVU, sN), '', '', ''])
        bk_tbl = Table(bk_data, colWidths=bw_l1)
        bk_tbl.setStyle(TableStyle([
            ('SPAN',(0,0),(-1,0)), ('BACKGROUND',(0,0),(-1,0),CELESTE), ('TEXTCOLOR',(0,0),(-1,0),colors.white),
            ('BACKGROUND',(0,1),(-1,1),colors.HexColor('#d0e4f7')),
            ('BOX',(0,0),(-1,-1),0.5,colors.grey), ('INNERGRID',(0,0),(-1,-1),0.3,colors.lightgrey),
            ('SPAN',(0,-1),(-1,-1)),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'), ('TOPPADDING',(0,0),(-1,-1),2),
            ('BOTTOMPADDING',(0,0),(-1,-1),2), ('LEFTPADDING',(0,0),(-1,-1),3),
        ]))

        sTotL1 = ParagraphStyle('sTotL1', fontSize=12, fontName='Helvetica-Bold', alignment=TA_RIGHT)
        tot_rows = [
            [Paragraph('Neto gravado:', sN), Paragraph(_pesos(total_neto), sNr)],
            [Paragraph('Descuento %:', sN),  Paragraph(_pesos(dto_total), sNr)],
            [Paragraph('Subtotal:', sN),      Paragraph(_pesos(total_cab), sNr)],
            [Paragraph('Perc:', sN),          Paragraph('$ 0,00', sNr)],
            [Paragraph('I.V.A. 21%:', sN),    Paragraph(_pesos(iva1_cab), sNr)],
            [Paragraph('I.V.A. 10,5%:', sN),  Paragraph(_pesos(iva2_cab), sNr)],
            [Paragraph('<b>TOTAL:</b>', sTotL1), Paragraph(f'<b>{_pesos(gran_total)}</b>', sTotL1)],
        ]
        tot_tbl = Table(tot_rows, colWidths=[32*mm, 40*mm])
        tot_tbl.setStyle(TableStyle([
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'), ('TOPPADDING',(0,0),(-1,-1),2), ('BOTTOMPADDING',(0,0),(-1,-1),2),
            ('LINEABOVE',(0,-1),(-1,-1),1.5,CELESTE),
        ]))

        bk_total_l1 = sum(bw_l1)         # 116mm
        tot_total_l1 = 72*mm             # 32+40
        pad_l1 = (cw - bk_total_l1 - tot_total_l1) / 2  # centra banco dejando totales a la derecha

        footer_tbl = Table(
            [[Spacer(1,1), bk_tbl, Spacer(1,4), tot_tbl]],
            colWidths=[pad_l1, bk_total_l1, 4*mm, tot_total_l1]
        )
        footer_tbl.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'BOTTOM')]))
        footer_items = [footer_tbl]
        if obs:
            footer_items.append(Spacer(1,3))
            footer_items.append(Paragraph(f'Observaciones: {obs}', sN))
        footer_items.append(Paragraph(f'Tipo de Cambio: $ {cotiz:.2f}', sN))
        footer_items.append(Spacer(1,8))
        footer_items.append(HRFlowable(width=cw, thickness=0.5, color=colors.grey, spaceAfter=6))
        if cae:
            vto_cae_str = ''
            if vto_cae:
                try: vto_cae_str = vto_cae.strftime('%d/%m/%Y')
                except Exception: vto_cae_str = str(vto_cae)[:10]

            # Construir URL QR AFIP
            import json as _json, base64 as _b64
            _TIPO_AFIP_MAP = {
                'FA':1,'NDA':2,'NCA':3,'FB':6,'NDB':7,'NCB':8,
                'FCA':201,'FCB':206,'FE':19,'NCCA':203,'NDE':12,'NCE':13,
                'FE_AFIP':19,
            }
            gran_total_cae = float(cab.get('TOTAL',0) or 0) + float(cab.get('IVA1',0) or 0) + float(cab.get('IVA2',0) or 0)
            cuit_rec_raw   = str(cab.get('CUIT','0') or '0')
            cuit_rec_num   = int(''.join(c for c in cuit_rec_raw if c.isdigit()) or '0')
            pv_int         = int(pv_str)
            seq_int        = int(seq_str)
            fecha_afip     = ''
            if cab.get('FECHACOMPROBANTE'):
                try: fecha_afip = cab['FECHACOMPROBANTE'].strftime('%Y-%m-%d')
                except Exception: fecha_afip = str(cab['FECHACOMPROBANTE'])[:10]
            qr_data = {
                "ver":1, "fecha":fecha_afip,
                "cuit":30708390182, "ptoVta":pv_int,
                "tipoCmp":_TIPO_AFIP_MAP.get(tipo,1), "nroCmp":seq_int,
                "importe":round(gran_total_cae,2), "moneda":"PES", "ctz":1.0,
                "tipoDocRec":80, "nroDocRec":cuit_rec_num,
                "tipoCodAut":"E", "codAut":int(cae)
            }
            # Base64 estándar (sin padding) — mismo formato que ARCA emite
            import urllib.parse as _uparse
            _json_bytes = _json.dumps(qr_data, separators=(',',':')).encode()
            qr_b64 = _b64.b64encode(_json_bytes).decode().rstrip('=')
            # URL-encode el parámetro para que + y / no rompan la URL
            qr_afip_url = ('https://servicioscf.afip.gob.ar/publico/comprobantes/cae.aspx?p='
                           + _uparse.quote(qr_b64, safe=''))
            # URL sitio web empresa (derecho)
            qr_web_url = 'https://www.microbellsa.com'

            # Generar imágenes QR
            qr_img_l = qr_img_r = None
            try:
                import qrcode as _qrc
                # QR izquierdo: AFIP CAE — ERROR_CORRECT_L para menor densidad y mejor escaneo
                _qr_l = _qrc.QRCode(error_correction=_qrc.constants.ERROR_CORRECT_L, box_size=6, border=2)
                _qr_l.add_data(qr_afip_url); _qr_l.make(fit=True)
                _pil_l = _qr_l.make_image(fill_color='black', back_color='white')
                _buf_l = BytesIO(); _pil_l.save(_buf_l, format='PNG'); _buf_l.seek(0)
                qr_img_l = Image(_buf_l, width=32*mm, height=32*mm)
                # QR derecho: sitio web empresa — URL corta, baja densidad
                _qr_r = _qrc.QRCode(error_correction=_qrc.constants.ERROR_CORRECT_L, box_size=6, border=2)
                _qr_r.add_data(qr_web_url); _qr_r.make(fit=True)
                _pil_r = _qr_r.make_image(fill_color='black', back_color='white')
                _buf_r = BytesIO(); _pil_r.save(_buf_r, format='PNG'); _buf_r.seek(0)
                qr_img_r = Image(_buf_r, width=32*mm, height=32*mm)
            except Exception:
                qr_img_l = Paragraph('[ QR AFIP ]', sN)
                qr_img_r = Paragraph('[ QR Web ]', sN)

            sCAEc = ParagraphStyle('sCAEc', fontSize=8, leading=11, fontName='Helvetica')
            sCAEb = ParagraphStyle('sCAEb', fontSize=8, leading=11, fontName='Helvetica-Bold')
            cae_center = [
                Paragraph(f'Factura Electrónica / CAE:', sCAEc),
                Paragraph(f'<b>{cae}</b>', sCAEb),
                Spacer(1,4),
                Paragraph(f'Fecha Vencimiento CAE:', sCAEc),
                Paragraph(f'<b>{vto_cae_str}</b>', sCAEb),
            ]
            qr_w = 34*mm
            mid_w = cw - 2*qr_w
            cae_tbl = Table([[qr_img_l, cae_center, qr_img_r]],
                            colWidths=[qr_w, mid_w, qr_w])
            cae_tbl.setStyle(TableStyle([
                ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
                ('ALIGN',(0,0),(0,-1),'LEFT'),
                ('ALIGN',(2,0),(2,-1),'RIGHT'),
                ('ALIGN',(1,0),(1,-1),'LEFT'),
                ('TOPPADDING',(0,0),(-1,-1),2),('BOTTOMPADDING',(0,0),(-1,-1),2),
            ]))
            footer_items.append(cae_tbl)
        else:
            footer_items.append(Paragraph('Factura Electrónica / CAE: (pendiente)', sN))
        story.append(KeepTogether(footer_items))

    doc.build(story)
    buf.seek(0)
    pv_s, seq_s, _ = _fmt_num(numero)
    fname = f'{tipo}_{pv_s}-{seq_s}.pdf'
    return StreamingResponse(buf, media_type='application/pdf',
                             headers={'Content-Disposition': f'inline; filename="{fname}"'})

# ─── Debug despachos comprobante ──────────────────────────────────────────────
@app.get("/debug/despachos2/{tipo}/{numero}")
def debug_despachos2(tipo: str, numero: str):
    """Muestra contenido de DETALLEDESPACHOVENTAS para el comprobante dado,
       y los artículos del comprobante cruzados con STOCKXDESPACHO."""
    result = {}
    _DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    for db_path in [DATABASE, _DB_PROD]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()

            # 1. Artículos de la factura
            cur.execute(
                'SELECT LINEA,CODIGOARTICULO FROM "CUERPOCOMPROBANTES" '
                'WHERE TIPOCOMPROBANTE=? AND NUMEROCOMPROBANTE=? ORDER BY LINEA',
                (tipo, numero)
            )
            arts = [{'linea': r[0], 'articulo': str(r[1] or '').strip()} for r in cur.fetchall()]
            result['articulos'] = arts

            # 2. DETALLEDESPACHOVENTAS — raw con exactamente ese tipo/numero
            cur.execute(
                'SELECT FIRST 50 TIPOCOMPROBANTE, NUMEROCOMPROBANTE, LINEA, DESPACHO, CANTIDAD '
                'FROM "DETALLEDESPACHOVENTAS" '
                'WHERE TIPOCOMPROBANTE=? AND NUMEROCOMPROBANTE=? ORDER BY LINEA, DESPACHO',
                (tipo, numero)
            )
            rows_ddv = [{'tipo': str(r[0]).strip(), 'numero': str(r[1]).strip(),
                         'linea': r[2], 'despacho': str(r[3] or '').strip(), 'cantidad': r[4]}
                        for r in cur.fetchall()]
            result['DETALLEDESPACHOVENTAS_exacto'] = rows_ddv

            # 3. DETALLEDESPACHOVENTAS — busca con LIKE para ver si numero está en otro formato
            cur.execute(
                'SELECT FIRST 20 TIPOCOMPROBANTE, NUMEROCOMPROBANTE, LINEA, DESPACHO '
                'FROM "DETALLEDESPACHOVENTAS" '
                "WHERE TIPOCOMPROBANTE=? AND TRIM(DESPACHO)<>'' "
                "AND DESPACHO<>'000000000000000' ORDER BY NUMEROCOMPROBANTE DESC",
                (tipo,)
            )
            rows_sample = [{'tipo': str(r[0]).strip(), 'numero': str(r[1]).strip(),
                            'linea': r[2], 'despacho': str(r[3] or '').strip()}
                           for r in cur.fetchall()]
            result['DETALLEDESPACHOVENTAS_sample_mismo_tipo'] = rows_sample

            # 4. STOCKXDESPACHO por artículos de la factura
            codigos = [a['articulo'] for a in arts if a['articulo']]
            if codigos:
                ph = ','.join(['?' for _ in codigos])
                cur.execute(
                    f'SELECT CODIGOARTICULO, DESPACHO, ADUANA '
                    f'FROM "STOCKXDESPACHO" WHERE CODIGOARTICULO IN ({ph}) '
                    f"AND TRIM(DESPACHO)<>'' ORDER BY CODIGOARTICULO, DESPACHO",
                    codigos
                )
                result['STOCKXDESPACHO_por_articulo'] = [
                    {'articulo': str(r[0]).strip(), 'despacho': str(r[1] or '').strip(),
                     'aduana': str(r[2] or '').strip()}
                    for r in cur.fetchall()
                ]
            c.close()
            result['db_usado'] = db_path
            break
        except Exception as e:
            result[f'error_{db_path}'] = str(e)
    return result

# ─── Rubros / jerarquía ────────────────────────────────────────────────────────
@app.get("/debug/tablas-mlt")
def debug_tablas_mlt():
    """Lista todas las tablas de DB-MLT-Prueba.gdb"""
    try:
        c = conn('WIN1252', db=DATABASE_MLT)
        cur = c.cursor()
        cur.execute("SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=0 ORDER BY RDB$RELATION_NAME")
        tablas = [r[0] for r in cur.fetchall()]
        c.close()
        return {"tablas": tablas, "db": DATABASE_MLT}
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/tablas_rubro")
def debug_tablas_rubro():
    c = conn()
    cur = c.cursor()
    # Columnas de cada tabla de jerarquía
    result = {}
    for tabla in ['GRUPOSUPERRUBROS','SUPERRUBROS','RUBROS']:
        cur.execute(f"SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME='{tabla}' ORDER BY RDB$FIELD_POSITION")
        result[f'cols_{tabla}'] = [r[0] for r in cur.fetchall()]
        try:
            cur.execute(f'SELECT FIRST 2 * FROM "{tabla}"')
            result[f'muestra_{tabla}'] = [list(r) for r in cur.fetchall()]
        except Exception as e:
            result[f'muestra_{tabla}_err'] = str(e)
    c.close()
    return result

@app.get("/gruposuperrubros")
def get_gruposuperrubros():
    c = conn()
    cur = c.cursor()
    # Solo los que tienen artículos con remanente > 0
    cur.execute("""
        SELECT DISTINCT g.CODIGOGRUPOSUPERRUBRO, g.DESCRIPCION
        FROM "GRUPOSUPERRUBROS" g
        WHERE EXISTS (
            SELECT 1 FROM "ARTICULOS" a
            JOIN "RUBROS" r ON r.CODIGORUBRO = a.CODIGORUBRO
            JOIN "SUPERRUBROS" sr ON sr.CODIGOSUPERRUBRO = r.CODIGOSUPERRUBRO
            WHERE sr.CODIGOGRUPOSUPERRUBRO = g.CODIGOGRUPOSUPERRUBRO
              AND a.ACTIVO = '1'
        )
        ORDER BY g.DESCRIPCION
    """)
    rows = cur.fetchall()
    c.close()
    return [{"codigo": r[0], "descripcion": r[1]} for r in rows]

@app.get("/superrubros")
def get_superrubros(grupo: Optional[str] = None):
    c = conn()
    cur = c.cursor()
    if grupo:
        cur.execute("""
            SELECT DISTINCT sr.CODIGOSUPERRUBRO, sr.DESCRIPCION
            FROM "SUPERRUBROS" sr
            WHERE sr.CODIGOGRUPOSUPERRUBRO = ?
              AND EXISTS (
                SELECT 1 FROM "ARTICULOS" a
                JOIN "RUBROS" r ON r.CODIGORUBRO = a.CODIGORUBRO
                WHERE r.CODIGOSUPERRUBRO = sr.CODIGOSUPERRUBRO AND a.ACTIVO = '1'
              )
            ORDER BY sr.DESCRIPCION
        """, (grupo,))
    else:
        cur.execute('SELECT CODIGOSUPERRUBRO, DESCRIPCION, CODIGOGRUPOSUPERRUBRO FROM "SUPERRUBROS" ORDER BY DESCRIPCION')
    rows = cur.fetchall()
    c.close()
    return [{"codigo": r[0], "descripcion": r[1], "grupo": r[2] if len(r)>2 else None} for r in rows]

@app.get("/rubros")
def get_rubros(superrubro: Optional[str] = None, grupo: Optional[str] = None):
    c = conn()
    cur = c.cursor()
    params = []
    filtro_sr = ""
    if superrubro:
        filtro_sr = "AND r.CODIGOSUPERRUBRO = ?"
        params.append(superrubro)
    elif grupo:
        filtro_sr = "AND sr.CODIGOGRUPOSUPERRUBRO = ?"
        params.append(grupo)
    cur.execute(f"""
        SELECT DISTINCT r.CODIGORUBRO, r.DESCRIPCION, r.CODIGOSUPERRUBRO
        FROM "RUBROS" r
        JOIN "SUPERRUBROS" sr ON sr.CODIGOSUPERRUBRO = r.CODIGOSUPERRUBRO
        WHERE EXISTS (
            SELECT 1 FROM "ARTICULOS" a
            WHERE a.CODIGORUBRO = r.CODIGORUBRO AND a.ACTIVO = '1'
        )
        {filtro_sr}
        ORDER BY r.DESCRIPCION
    """, params)
    rows = cur.fetchall()
    c.close()
    return [{"codigo": r[0], "descripcion": r[1], "superrubro": r[2]} for r in rows]

# ─── DEBUG: verificar matching de reservas para un artículo ──────────────────
@app.get("/debug/reservas/{codigoparticular}")
def debug_reservas_articulo(codigoparticular: str, token: Optional[str] = None, request: Request = None):
    """Muestra reservas activas y cómo matchean con el artículo dado (solo admin)."""
    reservas = _get_reservas_activas()
    # Obtener datos del artículo desde Firebird
    c = conn()
    cur = c.cursor()
    cur.execute(
        'SELECT a.CODIGOARTICULO, a.CODIGOPARTICULAR, a.DESCRIPCION, a.CODIGOMARCA,'
        ' a.CODIGORUBRO, r.CODIGOSUPERRUBRO, sr.CODIGOGRUPOSUPERRUBRO'
        ' FROM "ARTICULOS" a'
        ' LEFT JOIN "RUBROS" r ON r.CODIGORUBRO = a.CODIGORUBRO'
        ' LEFT JOIN "SUPERRUBROS" sr ON sr.CODIGOSUPERRUBRO = r.CODIGOSUPERRUBRO'
        ' WHERE a.CODIGOPARTICULAR = ?',
        (codigoparticular,)
    )
    row = cur.fetchone()
    c.close()
    if not row:
        return {"error": f"Artículo {codigoparticular!r} no encontrado"}
    item = {
        "codigo":           str(row[0] or '').strip(),
        "codigoparticular": str(row[1] or '').strip(),
        "descripcion":      row[2],
        "marca":            str(row[3] or '').strip(),
        "codigo_rubro":     str(row[4] or '').strip(),
        "codigo_superrubro": str(row[5] or '').strip(),
        "codigo_gruposuperrubro": str(row[6] or '').strip(),
    }
    resultado = []
    for rv in reservas:
        rv_art  = str(rv.get('codigo_articulo')  or '').strip()
        rv_part = str(rv.get('codigo_particular') or '').strip()
        it_cod  = item["codigo"]
        it_part = item["codigoparticular"]
        matches = {
            "rv_art==it_cod":  rv_art  == it_cod  if rv_art  else None,
            "rv_part==it_part": rv_part == it_part if rv_part else None,
            "rv_art==it_part": rv_art  == it_part if rv_art  else None,
            "rv_part==it_cod": rv_part == it_cod  if rv_part else None,
        }
        applies = any(v for v in matches.values() if v is not None)
        resultado.append({
            "reserva_id":       rv.get('id'),
            "tipo":             rv.get('tipo'),
            "deposito":         rv.get('deposito'),
            "cantidad":         rv.get('cantidad'),
            "cantidad_utilizada": rv.get('cantidad_utilizada'),
            "motivo":           rv.get('motivo'),
            "rv_codigo_articulo":  rv_art or None,
            "rv_codigo_particular": rv_part or None,
            "matches":          matches,
            "aplica":           applies,
        })
    # Replicar EXACTAMENTE lo que hace /stock para este artículo:
    # FMA_STOCK(NULL, NULL, dep, 1, 1) — misma query bulk de producción
    codigo_interno = item['codigo']  # "03421"
    rem_prod = {}
    for dep in ['001', '003']:
        try:
            c2 = conn()
            cur2 = c2.cursor()
            cur2.execute(f'SELECT ID_ARTICULO, STOCKREMANENTE FROM "FMA_STOCK"(NULL, NULL, \'{dep}\', 1, 1)')
            rem_map = {str(r[0]).strip(): float(r[1] or 0) for r in cur2.fetchall()}
            c2.close()
            # buscar por codigo_interno con y sin strip
            val = rem_map.get(codigo_interno, rem_map.get(codigo_interno.strip(), None))
            # fallback: buscar por codigoparticular
            if val is None:
                val = rem_map.get(item['codigoparticular'], 0.0)
            rem_prod[dep] = val if val is not None else 0.0
        except Exception as e:
            rem_prod[dep] = f"ERROR: {e}"
    # Construir item igual que /stock y aplicar reservas
    test_item = {
        "codigo":           codigo_interno,
        "codigoparticular": item['codigoparticular'],
        "remanente":        (rem_prod.get('001', 0) or 0) + (rem_prod.get('003', 0) or 0),
        "remanente_001":    rem_prod.get('001', 0),
        "remanente_003":    rem_prod.get('003', 0),
        "marca":            item.get('marca', ''),
        "codigo_rubro":     item.get('codigo_rubro', ''),
        "codigo_superrubro": item.get('codigo_superrubro', ''),
        "codigo_gruposuperrubro": item.get('codigo_gruposuperrubro', ''),
    }
    _apply_reservas([test_item], _get_reservas_activas(), rem_key='remanente')
    return {
        "articulo": item,
        "reservas_activas": resultado,
        "simulacion_exacta_produccion": {
            "remanente_001_firebird_raw":  rem_prod.get('001'),
            "remanente_003_firebird_raw":  rem_prod.get('003'),
            "reservado_deposito_001":      (test_item.get("reservado_por_deposito") or {}).get("001", 0),
            "remanente_001_post_reserva":  test_item.get("remanente_001"),
            "remanente_003_post_reserva":  test_item.get("remanente_003"),
            "lo_que_ve_el_vendedor_001":   test_item.get("remanente_001"),
        }
    }

@app.get("/marcas")
def get_marcas():
    c = conn()
    cur = c.cursor()
    cur.execute("""
        SELECT DISTINCT m.CODIGOMARCA, m.DESCRIPCION
        FROM "MARCAS" m
        WHERE EXISTS (
            SELECT 1 FROM "ARTICULOS" a
            WHERE a.CODIGOMARCA = m.CODIGOMARCA AND a.ACTIVO = '1'
        )
        ORDER BY m.DESCRIPCION
    """)
    rows = cur.fetchall()
    c.close()
    return [{"codigo": r[0], "descripcion": r[1]} for r in rows]

# ─── Debug ────────────────────────────────────────────────────────────────────
@app.get("/debug/stock")
def debug_stock():
    try:
        c = conn()
        cur = c.cursor()
        resultado = {}

        # Columnas de STOCK
        cur.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME='STOCK' ORDER BY RDB$FIELD_POSITION")
        resultado["columnas_stock"] = [r[0] for r in cur.fetchall()]

        # Primeras 5 filas de STOCK (ver qué campos tiene)
        cur.execute('SELECT FIRST 5 * FROM "STOCK"')
        cols = [d[0] for d in cur.description]
        resultado["stock_muestra_cols"] = cols
        resultado["stock_muestra_rows"] = [list(r) for r in cur.fetchall()]

        # Valores distintos del campo que identifica depósito (CODIGODEPOSITO o CODIGOSUCURSAL)
        for campo in ["CODIGODEPOSITO", "CODIGOSUCURSAL", "DEPOSITO"]:
            try:
                cur.execute(f'SELECT DISTINCT "{campo}" FROM "STOCK"')
                resultado[f"distintos_{campo}"] = [r[0] for r in cur.fetchall()]
            except Exception:
                pass

        c.close()
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/presupuesto/{numero}")
def debug_presupuesto(numero: str):
    """Muestra los campos de aprobación y remisión tal como están en la DB."""
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute(
            'SELECT NUMEROCOMPROBANTE, FECHAAPROBADO, CODIGOUSUARIOAPROBACION, '
            'CODIGOOPERACION, ANULADA, CLASECOMPROBANTE, CODIGOMULTIPLAZO, '
            'COTIZACIONFIJA, LISTAPRECIO, NUMEROTRANSACCION, CODIGORESPONSABLE, '
            'FECHAVENCIMIENTO, CODIGOUSUARIO, CODIGOUSUARIO2 '
            'FROM "CABEZAPRESUPUESTOS" WHERE NUMEROCOMPROBANTE = ?', (numero,)
        )
        cab = cur.fetchone()
        cur.execute(
            'SELECT LINEA, CANTIDAD, CANTIDADREMITIDA '
            'FROM "CUERPOPRESUPUESTOS" WHERE NUMEROCOMPROBANTE = ? ORDER BY LINEA',
            (numero,)
        )
        items = cur.fetchall()
        # Revisar triggers sobre CABEZAPRESUPUESTOS (nombre + tipo + source)
        cur.execute(
            "SELECT TRIM(RDB$TRIGGER_NAME), RDB$TRIGGER_TYPE, RDB$TRIGGER_SOURCE "
            "FROM RDB$TRIGGERS "
            "WHERE RDB$RELATION_NAME = 'CABEZAPRESUPUESTOS' AND RDB$SYSTEM_FLAG = 0"
        )
        triggers = [{"name": r[0], "type": r[1], "source": str(r[2])[:800] if r[2] else None}
                    for r in cur.fetchall()]
        # Default del campo CODIGOUSUARIOAPROBACION
        cur.execute("""
            SELECT rf.RDB$DEFAULT_SOURCE
            FROM RDB$RELATION_FIELDS rf
            WHERE rf.RDB$RELATION_NAME = 'CABEZAPRESUPUESTOS'
              AND TRIM(rf.RDB$FIELD_NAME) = 'CODIGOUSUARIOAPROBACION'
        """)
        col_def = cur.fetchone()
        c.close()
        return {
            "cabeza": {
                "numero":                   cab[0],
                "fechaaprobado":            str(cab[1]),
                "codigousuarioaprobacion":  repr(cab[2]),
                "codigooperacion":          cab[3],
                "anulada":                  cab[4],
                "clasecomprobante":         cab[5],
                "codigomultiplazo":         cab[6],
                "cotizacionfija":           cab[7],
                "listaprecio":              cab[8],
                "numerotransaccion":        cab[9],
                "codigoresponsable":        repr(cab[10]),
                "fechavencimiento":         str(cab[11]),
                "codigousuario":            cab[12],
                "codigousuario2":           cab[13],
            } if cab else None,
            "items": [{"linea": r[0], "cantidad": r[1], "cantidadremitida": r[2]} for r in items],
            "triggers": triggers,
            "col_default_aprobacion": str(col_def[0]) if col_def and col_def[0] else None,
        }
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/debug/depositos")
def debug_depositos():
    try:
        c = conn()
        cur = c.cursor()
        resultado = {}

        for tabla in ["DEPOSITOS", "SUBDEPOSITOS", "WEB_STOCK", "STOCKPORUSUARIO", "STOCKPORUSUARIODETALLE"]:
            try:
                cur.execute(f"SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME='{tabla}' ORDER BY RDB$FIELD_POSITION")
                resultado[f"cols_{tabla}"] = [r[0] for r in cur.fetchall()]
            except Exception as ex:
                resultado[f"cols_{tabla}_error"] = str(ex)

        # Muestra los depósitos existentes
        try:
            cur.execute('SELECT FIRST 20 * FROM "DEPOSITOS"')
            cols = [d[0] for d in cur.description]
            resultado["depositos_cols"] = cols
            resultado["depositos_rows"] = [list(r) for r in cur.fetchall()]
        except Exception as ex:
            resultado["depositos_error"] = str(ex)

        # Muestra sample de WEB_STOCK
        try:
            cur.execute('SELECT FIRST 3 * FROM "WEB_STOCK"')
            cols = [d[0] for d in cur.description]
            resultado["web_stock_cols"] = cols
            resultado["web_stock_rows"] = [list(r) for r in cur.fetchall()]
        except Exception as ex:
            resultado["web_stock_error"] = str(ex)

        c.close()
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/articulos_monedas")
def debug_articulos_monedas():
    try:
        c = conn()
        cur = c.cursor()
        resultado = {}
        # Campos de ARTICULOS que tengan CODIGO o PARTICULAR
        cur.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME='ARTICULOS' AND (RDB$FIELD_NAME CONTAINING 'PARTICULAR' OR RDB$FIELD_NAME CONTAINING 'PRECIO') ORDER BY RDB$FIELD_POSITION")
        resultado["articulos_precio_particular"] = [r[0] for r in cur.fetchall()]
        # Muestra artículo 01315 - valores raw de precio
        cur.execute(
            "SELECT CODIGOARTICULO, CODIGOPARTICULAR, CODIGOMONEDA, "
            "PRECIOLISTA1, PRECIOLISTA2, PRECIOLISTA3 "
            "FROM \"ARTICULOS\" WHERE CODIGOARTICULO = '01315' OR CODIGOPARTICULAR = '01315'"
        )
        row = cur.fetchone()
        if row:
            resultado["art_01315"] = {
                "CODIGOARTICULO": row[0], "CODIGOPARTICULAR": row[1],
                "CODIGOMONEDA": row[2],
                "PRECIOLISTA1_raw": str(row[3]), "PRECIOLISTA1_float": float(row[3]) if row[3] else None,
                "PRECIOLISTA2_raw": str(row[4]), "PRECIOLISTA3_raw": str(row[5]),
            }
        # CAMBIO raw de MONEDAS para DOLARES
        cur.execute('SELECT CODIGOMONEDA, CAMBIO, CAST(CAMBIO AS VARCHAR(30)) FROM "MONEDAS" WHERE CODIGOMONEDA = \'DOLARES\'')
        rm = cur.fetchone()
        if rm:
            resultado["dolares_cambio"] = {"raw": str(rm[1]), "str": rm[2], "float": float(rm[1]) if rm[1] else None}
            if row and row[3] and rm[1]:
                resultado["calculo"] = {
                    "preciolista1_float": float(row[3]),
                    "cambio_float": float(rm[1]),
                    "resultado_sin_round": float(row[3]) * float(rm[1]),
                    "resultado_round2": round(float(row[3]) * float(rm[1]), 2),
                    "resultado_round4": round(float(row[3]) * float(rm[1]), 4),
                }
        # Estructura y datos de MONEDAS
        cur.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME='MONEDAS' ORDER BY RDB$FIELD_POSITION")
        resultado["monedas_cols"] = [r[0] for r in cur.fetchall()]
        # Columnas de CLIENTES (para identificar campo IVA)
        cur.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME='CLIENTES' ORDER BY RDB$FIELD_POSITION")
        all_cols = [r[0] for r in cur.fetchall()]
        resultado["clientes_cols"] = all_cols
        # Busca columnas relacionadas a IVA/condición fiscal
        resultado["clientes_cols_iva"] = [c for c in all_cols if any(k in c for k in ['IVA','CONDIC','CATEG','FISCAL','RESPON'])]
        # Buscar generadores/secuencias de Firebird (para numeración PR)
        cur.execute("SELECT TRIM(RDB$GENERATOR_NAME), RDB$GENERATOR_ID FROM RDB$GENERATORS WHERE RDB$SYSTEM_FLAG=0 ORDER BY RDB$GENERATOR_NAME")
        resultado["generators"] = [{"name": r[0], "id": r[1]} for r in cur.fetchall()]
        # Tablas que podrían tener el contador (buscar tablas con columnas NUMERO+TIPO)
        cur.execute("""
            SELECT DISTINCT TRIM(a.RDB$RELATION_NAME)
            FROM RDB$RELATION_FIELDS a
            JOIN RDB$RELATION_FIELDS b ON a.RDB$RELATION_NAME=b.RDB$RELATION_NAME
            WHERE (a.RDB$FIELD_NAME CONTAINING 'NUMERO' OR a.RDB$FIELD_NAME = 'NUMERO')
              AND (b.RDB$FIELD_NAME CONTAINING 'TIPO' OR b.RDB$FIELD_NAME CONTAINING 'COMPROBANTE' OR b.RDB$FIELD_NAME CONTAINING 'DOCUMENTO')
              AND a.RDB$RELATION_NAME NOT STARTING WITH 'RDB$'
              AND a.RDB$RELATION_NAME NOT CONTAINING 'CABEZA'
              AND a.RDB$RELATION_NAME NOT CONTAINING 'CUERPO'
            ORDER BY 1
        """)
        resultado["tablas_contador_candidatas"] = [r[0] for r in cur.fetchall()]

        # Buscar triggers sobre CABEZAPRESUPUESTOS
        try:
            cur.execute("""
                SELECT TRIM(t.RDB$TRIGGER_NAME), t.RDB$TRIGGER_TYPE,
                       CAST(t.RDB$TRIGGER_SOURCE AS VARCHAR(500))
                FROM RDB$TRIGGERS t
                WHERE t.RDB$RELATION_NAME = 'CABEZAPRESUPUESTOS'
                  AND t.RDB$SYSTEM_FLAG = 0
            """)
            resultado["triggers_cabezapresupuestos"] = [
                {"name": r[0], "type": r[1], "src_snippet": str(r[2])[:300] if r[2] else ""}
                for r in cur.fetchall()
            ]
        except Exception as ex:
            resultado["triggers_err"] = str(ex)

        # Valor actual del generador AUXILIAR (podría ser el contador PR)
        try:
            cur.execute("SELECT GEN_ID(AUXILIAR, 0) FROM RDB$DATABASE")
            resultado["gen_auxiliar_value"] = cur.fetchone()[0]
        except Exception as ex:
            resultado["gen_auxiliar_err"] = str(ex)

        c.close()
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/setup/fechaaprobado_nullable")
def setup_fechaaprobado_nullable():
    """Ejecutar una sola vez: quita NOT NULL de FECHAAPROBADO en CABEZAPRESUPUESTOS."""
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        # Quitar NOT NULL y DEFAULT de FECHAAPROBADO
        cur.execute("""
            UPDATE RDB$RELATION_FIELDS
            SET RDB$NULL_FLAG = NULL,
                RDB$DEFAULT_VALUE = NULL,
                RDB$DEFAULT_SOURCE = NULL
            WHERE RDB$RELATION_NAME = 'CABEZAPRESUPUESTOS'
              AND RDB$FIELD_NAME = 'FECHAAPROBADO'
        """)
        c.commit()
        # Verificar
        cur.execute("""
            SELECT RDB$NULL_FLAG, RDB$DEFAULT_SOURCE
            FROM RDB$RELATION_FIELDS
            WHERE RDB$RELATION_NAME = 'CABEZAPRESUPUESTOS'
              AND RDB$FIELD_NAME = 'FECHAAPROBADO'
        """)
        row = cur.fetchone()
        c.close()
        return {"ok": True,
                "RDB$NULL_FLAG": row[0] if row else '?',
                "RDB$DEFAULT_SOURCE": row[1] if row else '?',
                "msg": "Ambos deben ser None/null para que FECHAAPROBADO quede en NULL al insertar"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/debug/procedimientos")
def debug_procedimientos():
    try:
        c = conn()
        cur = c.cursor()
        resultado = {}
        # Procedimientos almacenados
        cur.execute("SELECT TRIM(RDB$PROCEDURE_NAME) FROM RDB$PROCEDURES WHERE RDB$SYSTEM_FLAG=0 ORDER BY RDB$PROCEDURE_NAME")
        procs = [r[0] for r in cur.fetchall()]
        resultado["procedimientos"] = [p for p in procs if any(x in p for x in ['STOCK','DEPO','REMANEN','SALDO'])]
        resultado["total_procedimientos"] = len(procs)
        # Probar si STOCK tiene datos recientes por depósito via CUERPOCOMPROBANTES
        # Artículo 02785 depósito 003
        cur.execute("""
            SELECT cc.CODIGODEPOSITO,
                   SUM(CASE WHEN cab.TIPOCOMPROBANTE IN ('FA','FE','FCA','FCE','RE','NC','NCA','SIV','FDI')
                             THEN cc.CANTIDAD ELSE 0 END) AS CANT_COMP,
                   COUNT(*) AS FILAS
            FROM "CUERPOCOMPROBANTES" cc
            JOIN "CABEZACOMPROBANTES" cab ON cab.TIPOCOMPROBANTE=cc.TIPOCOMPROBANTE AND cab.NUMEROCOMPROBANTE=cc.NUMEROCOMPROBANTE
            WHERE cc.CODIGOARTICULO='02785' AND cc.CODIGODEPOSITO IN ('001','003')
            GROUP BY cc.CODIGODEPOSITO
        """)
        cols = [d[0] for d in cur.description]
        resultado["movimientos_02785"] = [dict(zip(cols,list(r))) for r in cur.fetchall()]
        # Ver tipos de comprobante distintos en CUERPOCOMPROBANTES
        cur.execute("SELECT DISTINCT TIPOCOMPROBANTE FROM \"CUERPOCOMPROBANTES\" ORDER BY TIPOCOMPROBANTE")
        resultado["tipos_comprobante_cuerpo"] = [r[0] for r in cur.fetchall()]
        c.close()
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/procs_stock")
def debug_procs_stock():
    try:
        c = conn()
        cur = c.cursor()
        resultado = {}
        procs_interesantes = [
            'FMA_CALCULA_STOCKREMANENTE','FMA_CALCULASTOCKREMANENTE',
            'FMA_CALCULA_STOCKREAL','FMA_CALCULASTOCKREAL',
            'FMA_DETALLESTOCK','FMA_STOCK','FMA_DEPOSITOS'
        ]
        for proc in procs_interesantes:
            try:
                cur.execute("""
                    SELECT TRIM(p.RDB$PARAMETER_NAME), p.RDB$PARAMETER_TYPE,
                           TRIM(f.RDB$FIELD_TYPE), p.RDB$PARAMETER_NUMBER
                    FROM RDB$PROCEDURE_PARAMETERS p
                    JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = p.RDB$FIELD_SOURCE
                    WHERE p.RDB$PROCEDURE_NAME = ?
                    ORDER BY p.RDB$PARAMETER_TYPE, p.RDB$PARAMETER_NUMBER
                """, (proc,))
                params = [{"nombre": r[0], "tipo": "INPUT" if r[1]==0 else "OUTPUT", "campo": r[2]} for r in cur.fetchall()]
                if params:
                    resultado[proc] = params
            except Exception as ex:
                resultado[proc + "_error"] = str(ex)
        # Intentar llamar FMA_DETALLESTOCK con artículo conocido
        for call in [
            ("FMA_DETALLESTOCK", "EXECUTE PROCEDURE \"FMA_DETALLESTOCK\" '02785'"),
            ("FMA_STOCK", "EXECUTE PROCEDURE \"FMA_STOCK\" '02785'"),
        ]:
            try:
                cur.execute(call[1])
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                resultado[call[0]+"_result"] = [dict(zip(cols,list(r))) for r in rows[:5]]
            except Exception as ex:
                resultado[call[0]+"_call_error"] = str(ex)
        c.close()
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/stock_condiciones")
def debug_stock_condiciones():
    """Verifica OPERACIONES.COMPROMETESTOCK y CASILLEROS para entender por qué no descuenta stock"""
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        res = {}
        # 1. Ver COMPROMETESTOCK para cada OPERACION usada en NPs
        cur.execute('SELECT CODIGOOPERACION, DESCRIPCION, COMPROMETESTOCK FROM "OPERACIONES" ORDER BY CODIGOOPERACION')
        res['operaciones'] = [{'codigo': str(r[0]), 'descripcion': str(r[1]).strip(), 'comprometestock': r[2]} for r in cur.fetchall()]
        # 2. Ver si existen registros en CASILLEROS para artículo 00590 (el de la antena)
        cur.execute('SELECT CODIGOARTICULO, LOTE, CODIGODEPOSITO FROM "CASILLEROS" WHERE CODIGOARTICULO = ? ORDER BY LOTE', ('00590',))
        rows = cur.fetchall()
        res['casilleros_00590'] = [{'art': r[0], 'lote': str(r[1]), 'deposito': str(r[2])} for r in rows]
        # 3. Ver qué LOTE usamos en CUERPOPEDIDOS para ese artículo
        cur.execute("SELECT FIRST 3 LOTE, CODIGODEPOSITO, CANTIDAD, CANTIDADREMITIDA FROM \"CUERPOPEDIDOS\" WHERE CODIGOARTICULO = '00590' ORDER BY NUMEROCOMPROBANTE DESC")
        res['cuerpopedidos_00590'] = [{'lote': str(r[0]), 'deposito': str(r[1]), 'cantidad': r[2], 'remitida': r[3]} for r in cur.fetchall()]
        # 4. Ver cuántos casilleros existen en total (para saber si la tabla está poblada)
        cur.execute('SELECT COUNT(*) FROM "CASILLEROS"')
        res['total_casilleros'] = cur.fetchone()[0]
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/stock_profundo")
def debug_stock_profundo():
    """Fuente de FMA_CALCULA_STOCKPEDIDO + artículo real en CUERPOPEDIDOS + CASILLEROS sample"""
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        res = {}
        # 1. Fuente de FMA_CALCULA_STOCKPEDIDO (sin el filtro EXISTS)
        for sp in ['FMA_CALCULA_STOCKPEDIDO', 'FMA_STOCK']:
            cur.execute('SELECT RDB$PROCEDURE_SOURCE FROM RDB$PROCEDURES WHERE TRIM(RDB$PROCEDURE_NAME)=?', (sp,))
            row = cur.fetchone()
            res[f'src_{sp}'] = str(row[0])[:2000] if row and row[0] else 'NO ENCONTRADO'
        # 2. Buscar el artículo 00590 en ARTICULOS para ver su CODIGOARTICULO interno
        cur.execute("SELECT CODIGOARTICULO, CODIGOPARTICULAR, DESCRIPCION FROM \"ARTICULOS\" WHERE CODIGOPARTICULAR='00590' OR CODIGOARTICULO='00590'")
        rows = cur.fetchall()
        res['articulos_00590'] = [{'interno': r[0], 'particular': r[1], 'desc': str(r[2])[:40]} for r in rows]
        # 3. CUERPOPEDIDOS de los últimos 5 pedidos NP (cualquier artículo)
        cur.execute("SELECT FIRST 5 NUMEROCOMPROBANTE, CODIGOARTICULO, LOTE, CANTIDAD, CODIGODEPOSITO FROM \"CUERPOPEDIDOS\" WHERE TIPOCOMPROBANTE='NP' ORDER BY NUMEROCOMPROBANTE DESC")
        res['cuerpopedidos_ultimos'] = [{'num': str(r[0]), 'art': r[1], 'lote': str(r[2]), 'cant': r[3], 'dep': str(r[4])} for r in cur.fetchall()]
        # 4. Sample de CASILLEROS para ver qué lote usan
        cur.execute("SELECT FIRST 5 CODIGOARTICULO, LOTE, CODIGODEPOSITO FROM \"CASILLEROS\"")
        res['casilleros_sample'] = [{'art': r[0], 'lote': str(r[1]), 'dep': str(r[2])} for r in cur.fetchall()]
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/sp_remanente")
def debug_sp_remanente():
    """Lee el código fuente de FMA_CALCULASTOCKREMANENTE para ver qué tablas/condiciones usa"""
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        res = {}
        for sp in ['FMA_CALCULASTOCKREMANENTE', 'FMA_CALCULA_STOCKREMANENTE']:
            try:
                cur.execute("""
                    SELECT TRIM(RDB$PROCEDURE_NAME), RDB$PROCEDURE_SOURCE
                    FROM RDB$PROCEDURES
                    WHERE TRIM(RDB$PROCEDURE_NAME) = ?
                """, (sp,))
                row = cur.fetchone()
                res[sp] = str(row[1]) if row and row[1] else 'NO ENCONTRADO'
            except Exception as e2:
                res[sp] = str(e2)
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/fma_stock")
def debug_fma_stock():
    try:
        c = conn()
        cur = c.cursor()
        resultado = {}
        # Test FMA_STOCK con filtro de depósitos 001 y 003
        for depositos in ["001,003", "001|003", "001;003", "001 003"]:
            try:
                cur.execute(f"""
                    SELECT FIRST 3 ID_ARTICULO, CODIGO_PRODUCTO, STOCKREAL, STOCKREMANENTE
                    FROM "FMA_STOCK"(NULL, NULL, '{depositos}', 1, 1)
                    WHERE STOCKREAL > 0
                """)
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, list(r))) for r in cur.fetchall()]
                resultado[f"FMA_STOCK_depositos_{depositos}"] = rows
                break
            except Exception as ex:
                resultado[f"FMA_STOCK_depositos_{depositos}_error"] = str(ex)
        # Test FMA_CALCULASTOCKREMANENTE para artículo 02785
        for depo in ['001', '003']:
            try:
                cur.execute('EXECUTE PROCEDURE "FMA_CALCULASTOCKREMANENTE" ?, ?, ?', ('02785', '000', depo))
                row = cur.fetchone()
                resultado[f"remanente_02785_dep{depo}"] = float(row[0]) if row else None
            except Exception as ex:
                resultado[f"remanente_02785_dep{depo}_error"] = str(ex)
        # Test FMA_CALCULA_STOCKREAL para artículo 02785
        for depo in ['001', '003']:
            try:
                cur.execute('EXECUTE PROCEDURE "FMA_CALCULA_STOCKREAL" ?, ?, ?', ('02785', '000', depo))
                row = cur.fetchone()
                resultado[f"stockreal_02785_dep{depo}"] = float(row[0]) if row else None
            except Exception as ex:
                resultado[f"stockreal_02785_dep{depo}_error"] = str(ex)
        c.close()
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/cta/{codigo}")
def debug_cta(codigo: str):
    resultado = {}
    # 1. Lookup en CLIENTES
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute('SELECT CODIGOCLIENTE, CODIGOPARTICULAR, RAZONSOCIAL FROM "CLIENTES" WHERE CODIGOCLIENTE = ?', (codigo,))
        row = cur.fetchone()
        c.close()
        resultado['clientes'] = {"codigocliente": row[0], "codigoparticular": row[1], "razonsocial": row[2]} if row else None
    except Exception as e:
        resultado['clientes_error'] = str(e)

    DATABASE_MLT = 'c:/flexxus/db/DB-MLT-Microbell.gdb'
    # 2. BD principal - todos los comprobantes del cliente (sin filtro de saldo)
    for db_key, db_path in [('db_main', DATABASE), ('db_est', DATABASE_EST), ('db_mlt', DATABASE_MLT)]:
        try:
            c = conn('LATIN1', db=db_path)
            cur = c.cursor()
            # Buscar por codigo y por codigoparticular
            cp = resultado.get('clientes', {}) or {}
            codigos = list({codigo, cp.get('codigoparticular','')})
            codigos = [x for x in codigos if x]
            placeholders = ','.join(['?' for _ in codigos])
            cur.execute(f"""
                SELECT TIPOCOMPROBANTE, NUMEROCOMPROBANTE, FECHACOMPROBANTE,
                       CODIGOCLIENTE, TOTAL, IVA1, IVA2, PAGADO, CUENTACORRIENTE, ANULADA
                FROM "CABEZACOMPROBANTES"
                WHERE CODIGOCLIENTE IN ({placeholders})
                  AND TIPOCOMPROBANTE IN ('FA','FB','FE','FCA','FCB','DI','SIV')
                ORDER BY FECHACOMPROBANTE DESC
            """, tuple(codigos))
            rows = cur.fetchall()
            c.close()
            resultado[db_key] = [{"tipo":r[0],"numero":r[1],"fecha":str(r[2]),"cod_cli":r[3],
                                   "total":float(r[4] or 0),"iva1":float(r[5] or 0),"iva2":float(r[6] or 0),
                                   "pagado":float(r[7] or 0),"ctacte":r[8],"anulada":r[9]} for r in rows]
        except Exception as e:
            # Si falla, listar tablas disponibles en esa BD
            resultado[db_key+'_error'] = str(e)
            try:
                c2 = conn('LATIN1', db=db_path)
                cur2 = c2.cursor()
                cur2.execute("SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0 ORDER BY RDB$RELATION_NAME")
                tablas = [r[0] for r in cur2.fetchall()]
                c2.close()
                resultado[db_key+'_tablas'] = tablas
            except Exception as e2:
                resultado[db_key+'_tablas_error'] = str(e2)
    return resultado

@app.get("/debug/cta_detalle/{codigo}")
def debug_cta_detalle(codigo: str, vendedor: Optional[str] = None):
    """Diagnostica por qué un cliente no muestra movimientos en el detalle.
    Muestra: lookup CLIENTES, codigos resueltos, registros en CABEZACOMPROBANTES por tipo y cuentacorriente."""
    DB_PROD_D     = DATABASE
    DB_MLT_PROD_D = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    resultado = {"codigo_ingresado": codigo}

    # 1. Lookup en CLIENTES (igual que endpoint detalle)
    try:
        c = conn('WIN1252', db=DB_PROD_D)
        cur = c.cursor()
        cur.execute(
            'SELECT CODIGOCLIENTE, CODIGOPARTICULAR, RAZONSOCIAL, CODIGOVENDEDOR '
            'FROM "CLIENTES" WHERE CODIGOCLIENTE=? OR CODIGOPARTICULAR=?',
            (codigo, codigo)
        )
        rows_cli = cur.fetchall()
        c.close()
        resultado['clientes_rows'] = [
            {"codigocliente": r[0], "codigoparticular": r[1], "razonsocial": r[2], "codigovendedor": r[3]}
            for r in rows_cli
        ]
        # Armar codigos igual que el endpoint
        codigos_set = set()
        if rows_cli:
            cli = rows_cli[0]
            if cli[0] and str(cli[0]).strip(): codigos_set.add(str(cli[0]).strip())
            if cli[1] and str(cli[1]).strip(): codigos_set.add(str(cli[1]).strip())
        if not codigos_set:
            codigos_set.add(codigo)
        codigos = list(codigos_set)
        resultado['codigos_usados'] = codigos
    except Exception as e:
        resultado['clientes_error'] = str(e)
        codigos = [codigo]

    # 2. Para cada DB: contar por TIPOCOMPROBANTE + CUENTACORRIENTE + ANULADA
    _NC_TIPOS_D = ('NCA','NCB','NCCA','NCCB','NCE','NCCE','SIV','NDA','NDB','NDCA','NDCB')
    for db_key, db_path in [('DB_PROD', DB_PROD_D), ('DB_MLT_PROD', DB_MLT_PROD_D)]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            ph = ','.join(['?']*len(codigos))
            # a) Todo lo que existe para estos codigos
            cur.execute(f"""
                SELECT TIPOCOMPROBANTE, CUENTACORRIENTE, ANULADA,
                       CODIGOCLIENTE, CODIGOUSUARIO,
                       COUNT(*), SUM(TOTAL+IVA1+IVA2-PAGADO)
                FROM "CABEZACOMPROBANTES"
                WHERE CODIGOCLIENTE IN ({ph})
                GROUP BY TIPOCOMPROBANTE, CUENTACORRIENTE, ANULADA, CODIGOCLIENTE, CODIGOUSUARIO
                ORDER BY TIPOCOMPROBANTE
            """, tuple(codigos))
            rows_all = cur.fetchall()
            resultado[db_key+'_todos'] = [
                {"tipo": r[0], "ctacte": r[1], "anulada": r[2],
                 "codigocliente": r[3], "codigousuario": r[4],
                 "cant": r[5], "saldo_total": float(r[6] or 0)}
                for r in rows_all
            ]
            # b) Lo que pasaría con el filtro del detalle
            cur.execute(f"""
                SELECT TIPOCOMPROBANTE, NUMEROCOMPROBANTE, FECHACOMPROBANTE,
                       CODIGOCLIENTE, CODIGOUSUARIO,
                       TOTAL, IVA1, IVA2, PAGADO, CUENTACORRIENTE, COTIZACION, CODIGOMONEDA
                FROM "CABEZACOMPROBANTES"
                WHERE CODIGOCLIENTE IN ({ph})
                  AND ANULADA = '0'
                  AND TIPOCOMPROBANTE NOT IN ('RE','RI','INA')
                ORDER BY FECHACOMPROBANTE DESC
            """, tuple(codigos))
            rows_ctacte = cur.fetchall()
            resultado[db_key+'_sin_filtro_ctacte'] = [
                {"tipo": r[0], "num": r[1], "fecha": str(r[2]),
                 "cod_cli": r[3], "usuario": r[4],
                 "total": float(r[5] or 0), "iva1": float(r[6] or 0), "iva2": float(r[7] or 0),
                 "pagado": float(r[8] or 0), "ctacte": r[9],
                 "cotiz": float(r[10] or 1), "moneda": r[11]}
                for r in rows_ctacte
            ]
            c.close()
        except Exception as e:
            resultado[db_key+'_error'] = str(e)

    return resultado

@app.get("/debug/query_cta/{codigo}")
def debug_query_cta(codigo: str):
    """Corre _query_cta EXACTAMENTE como lo hace el endpoint de detalle y muestra resultado + errores."""
    DB_PROD_D     = DATABASE
    DB_MLT_PROD_D = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    resultado = {}

    # Lookup igual que el endpoint
    try:
        c = conn('WIN1252', db=DB_PROD_D)
        cur = c.cursor()
        cur.execute(
            'SELECT CODIGOCLIENTE, CODIGOPARTICULAR FROM "CLIENTES" WHERE CODIGOCLIENTE=? OR CODIGOPARTICULAR=?',
            (codigo, codigo)
        )
        cli = cur.fetchone()
        c.close()
        codigos_set = set()
        if cli:
            if cli[0] and str(cli[0]).strip(): codigos_set.add(str(cli[0]).strip())
            if cli[1] and str(cli[1]).strip(): codigos_set.add(str(cli[1]).strip())
        if not codigos_set:
            codigos_set.add(codigo)
        codigos = list(codigos_set)
        resultado['codigos'] = codigos
    except Exception as e:
        resultado['lookup_error'] = str(e)
        codigos = [codigo]

    # Correr _query_cta con captura de errores por fila
    for db_key, db_path in [('DB_PROD', DB_PROD_D), ('DB_MLT_PROD', DB_MLT_PROD_D)]:
        try:
            cambios = _get_cambios(db_path)
            resultado[db_key+'_cambios'] = cambios
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            ph = ', '.join(['?'] * len(codigos))
            params = list(codigos)
            _NC_T = "('NCA','NCB','NCCA','NCCB','NCE','NCCE','SIV','NDA','NDB','NDCA','NDCB')"
            sql = f"""
                SELECT FIRST 200 SKIP 0
                    TIPOCOMPROBANTE, NUMEROCOMPROBANTE, FECHACOMPROBANTE,
                    TOTAL, IVA1, IVA2, PAGADO, COTIZACION, CODIGOMONEDA,
                    FECHAVENCIMIENTO, CLASECOMPROBANTE
                FROM "CABEZACOMPROBANTES"
                WHERE CODIGOCLIENTE IN ({ph})
                  AND ANULADA = '0'
                  AND TIPOCOMPROBANTE NOT IN ('RE', 'RI', 'INA')
                  AND (CUENTACORRIENTE = '1' OR TIPOCOMPROBANTE IN {_NC_T})
                ORDER BY FECHAVENCIMIENTO ASC, FECHACOMPROBANTE ASC
            """
            cur.execute(sql, tuple(params))
            rows_raw = []
            row_errors = []
            while True:
                try:
                    r = cur.fetchone()
                    if r is None:
                        break
                    tipo=r[0]; num=r[1]; fecha=r[2]
                    total=float(r[3] or 0); iva1=float(r[4] or 0); iva2=float(r[5] or 0)
                    pagado=float(r[6] or 0); cotiz=float(r[7] or 1) or 1.0
                    moneda=str(r[8] or '').strip(); fvto=r[9]; clase=r[10]
                    neto=total+iva1+iva2; debe=neto-pagado
                    cambio=cambios.get(moneda,1.0) or 1.0
                    deuda=debe*cambio/cotiz
                    rows_raw.append({
                        "tipo":tipo,"num":str(num),"fecha":str(fecha),
                        "neto":round(neto,2),"pagado":round(pagado,2),
                        "deuda":round(deuda,2),"moneda":moneda,
                        "cotiz":cotiz,"cambio":cambio,"incluido":abs(deuda)>=0.01
                    })
                except Exception as row_e:
                    row_errors.append(str(row_e))
            c.close()
            resultado[db_key+'_filas'] = rows_raw
            resultado[db_key+'_filas_incluidas'] = sum(1 for r in rows_raw if r['incluido'])
            if row_errors:
                resultado[db_key+'_errores_fila'] = row_errors
        except Exception as e:
            resultado[db_key+'_error'] = str(e)

    return resultado

@app.get("/debug/esquema_docs")
def debug_esquema_docs():
    """Inspecciona columnas, NOT NULL, defaults y generators de pedidos/presupuestos."""
    try:
        c = conn('LATIN1')
        cur = c.cursor()
        resultado = {}

        # Columnas + NOT NULL + DEFAULT de cada tabla
        for tabla in ['CABEZAPEDIDOS', 'CUERPOPEDIDOS', 'CABEZAPRESUPUESTOS', 'CUERPOPRESUPUESTOS']:
            try:
                cur.execute(f"""
                    SELECT TRIM(rf.RDB$FIELD_NAME),
                           rf.RDB$NULL_FLAG,
                           TRIM(rf.RDB$DEFAULT_SOURCE)
                    FROM RDB$RELATION_FIELDS rf
                    WHERE rf.RDB$RELATION_NAME = '{tabla}'
                    ORDER BY rf.RDB$FIELD_POSITION
                """)
                resultado[f'cols_{tabla}'] = [
                    {"campo": r[0], "not_null": r[1]==1, "default": r[2]}
                    for r in cur.fetchall()
                ]
            except Exception as ex:
                resultado[f'cols_{tabla}_error'] = str(ex)

        # Muestra fila real de CABEZAPEDIDOS (para ver qué trae Flexxus)
        for tabla in ['CABEZAPEDIDOS', 'CABEZAPRESUPUESTOS']:
            try:
                cur.execute(f'SELECT FIRST 1 * FROM "{tabla}" ORDER BY NUMEROCOMPROBANTE DESC')
                cols = [d[0] for d in cur.description]
                row = cur.fetchone()
                resultado[f'muestra_{tabla}'] = dict(zip(cols, [str(v) if v is not None else None for v in row])) if row else None
            except Exception as ex:
                resultado[f'muestra_{tabla}_error'] = str(ex)

        # Generators relacionados con pedidos/presupuestos
        try:
            cur.execute("""
                SELECT TRIM(RDB$GENERATOR_NAME), RDB$GENERATOR_ID
                FROM RDB$GENERATORS
                WHERE RDB$SYSTEM_FLAG = 0
                ORDER BY RDB$GENERATOR_NAME
            """)
            todos = [{"nombre": r[0], "id": r[1]} for r in cur.fetchall()]
            resultado['generators'] = [g for g in todos if any(x in g['nombre'].upper() for x in ['PED','PRE','COMP','DOC','NUMERO','NP'])]
            resultado['generators_todos'] = todos
        except Exception as ex:
            resultado['generators_error'] = str(ex)

        # Triggers en CABEZAPEDIDOS (puede haber lógica de numeración)
        try:
            cur.execute("""
                SELECT TRIM(RDB$TRIGGER_NAME), RDB$TRIGGER_TYPE, TRIM(RDB$TRIGGER_SOURCE)
                FROM RDB$TRIGGERS
                WHERE RDB$RELATION_NAME IN ('CABEZAPEDIDOS','CABEZAPRESUPUESTOS')
                  AND RDB$SYSTEM_FLAG = 0
                ORDER BY RDB$TRIGGER_NAME
            """)
            resultado['triggers'] = [{"nombre": r[0], "tipo": r[1], "fuente": r[2]} for r in cur.fetchall()]
        except Exception as ex:
            resultado['triggers_error'] = str(ex)

        c.close()
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug/esquema_pedidos")
def debug_esquema_pedidos():
    """Columnas CABEZAPEDIDOS + valores OPERACION + muestra filas NP recientes"""
    res = {}
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        # Columnas de CABEZAPEDIDOS
        cur.execute("""
            SELECT TRIM(rf.RDB$FIELD_NAME), rf.RDB$FIELD_POSITION,
                   f.RDB$NULL_FLAG, f.RDB$DEFAULT_VALUE
            FROM RDB$RELATION_FIELDS rf
            JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
            WHERE rf.RDB$RELATION_NAME = 'CABEZAPEDIDOS'
            ORDER BY rf.RDB$FIELD_POSITION
        """)
        res['cols_cabezapedidos'] = [r[0] for r in cur.fetchall()]
        # Valores distintos de OPERACION en NPs
        cur.execute("SELECT DISTINCT OPERACION, COUNT(*) FROM \"CABEZAPEDIDOS\" WHERE TIPOCOMPROBANTE='NP' GROUP BY OPERACION ORDER BY OPERACION")
        res['operacion_values'] = [{'operacion': str(r[0]), 'count': r[1]} for r in cur.fetchall()]
        # Últimos 3 NPs con campos clave
        try:
            cur.execute("""
                SELECT FIRST 3 NUMEROCOMPROBANTE, OPERACION, FECHATERMINADA,
                       CODIGOUSUARIO, CODIGOUSUARIO2, CODIGOTECNICO, CLASECOMPROBANTE
                FROM "CABEZAPEDIDOS" WHERE TIPOCOMPROBANTE='NP' AND OPERACION IN ('2','4')
                ORDER BY NUMEROCOMPROBANTE DESC
            """)
            res['sample_np_terminados'] = [
                {'num': str(r[0]), 'operacion': str(r[1]), 'fechaterminada': str(r[2]),
                 'usuario': str(r[3]), 'usuario2': str(r[4]), 'tecnico': str(r[5]), 'clase': str(r[6])}
                for r in cur.fetchall()
            ]
        except Exception as e2:
            res['sample_np_extended_error'] = str(e2)
        c.close()
    except Exception as e:
        res['error'] = str(e)
    return res

@app.get("/debug/comparar_np/{num_a}/{num_b}")
def debug_comparar_np(num_a: str, num_b: str):
    """Compara campo a campo dos NPs en CABEZAPEDIDOS."""
    res = {}
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        cur.execute("SELECT * FROM \"CABEZAPEDIDOS\" WHERE NUMEROCOMPROBANTE IN (?,?) AND TIPOCOMPROBANTE='NP'", (num_a, num_b))
        cols = [d[0] for d in cur.description]
        rows = {str(r[cols.index('NUMEROCOMPROBANTE')]): dict(zip(cols, [str(v) if v is not None else None for v in r])) for r in cur.fetchall()}
        res['cabeza'] = rows

        # Diferencias entre los dos
        if num_a in rows and num_b in rows:
            diffs = {}
            for col in cols:
                va = rows[num_a].get(col)
                vb = rows[num_b].get(col)
                if va != vb:
                    diffs[col] = {num_a: va, num_b: vb}
            res['diferencias'] = diffs

        # Triggers sobre CABEZAPEDIDOS
        cur.execute("""
            SELECT TRIM(t.RDB$TRIGGER_NAME), t.RDB$TRIGGER_TYPE, t.RDB$TRIGGER_SOURCE
            FROM RDB$TRIGGERS t
            WHERE t.RDB$RELATION_NAME = 'CABEZAPEDIDOS' AND t.RDB$SYSTEM_FLAG = 0
            ORDER BY t.RDB$TRIGGER_SEQUENCE
        """)
        res['triggers'] = [{'nombre': r[0], 'tipo': r[1], 'source': (r[2] or '')[:300]} for r in cur.fetchall()]
        c.close()
    except Exception as e:
        res['error'] = str(e)
    return res

@app.get("/debug/operacion_np")
def debug_operacion_np():
    """Muestra OPERACION de los últimos 10 NPs y todos los valores distintos existentes."""
    res = {}
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        # Últimos 10 NPs: número, operacion, fechacomprobante
        cur.execute("""
            SELECT FIRST 10 NUMEROCOMPROBANTE, OPERACION, FECHACOMPROBANTE, CODIGOUSUARIO
            FROM "CABEZAPEDIDOS" WHERE TIPOCOMPROBANTE='NP'
            ORDER BY CAST(NUMEROCOMPROBANTE AS INTEGER) DESC
        """)
        res['ultimos_10'] = [{'num': str(r[0]), 'operacion': str(r[1]).strip(),
                               'fecha': str(r[2])[:10], 'usuario': str(r[3]).strip()}
                              for r in cur.fetchall()]
        # Todos los valores de OPERACION con conteo
        cur.execute("SELECT DISTINCT OPERACION, COUNT(*) FROM \"CABEZAPEDIDOS\" WHERE TIPOCOMPROBANTE='NP' GROUP BY OPERACION ORDER BY OPERACION")
        res['operacion_dist'] = [{'valor': repr(r[0]), 'count': r[1]} for r in cur.fetchall()]
        c.close()
    except Exception as e:
        res['error'] = str(e)
    return res

@app.get("/debug/comparar_pedidos")
def debug_comparar_pedidos():
    """Compara todos los campos de dos NPs: 100023473 (Flexxus) vs 100023547 (APP)"""
    res = {}
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        for num in ['100023473', '100023547']:
            cur.execute('SELECT * FROM "CABEZAPEDIDOS" WHERE TIPOCOMPROBANTE=\'NP\' AND NUMEROCOMPROBANTE=?', (num,))
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                res[num] = {cols[i]: str(row[i]) if row[i] is not None else None for i in range(len(cols))}
            else:
                res[num] = 'NO ENCONTRADO'
        c.close()
    except Exception as e:
        res['error'] = str(e)
    return res

@app.get("/debug/esquema_mlt")
def debug_esquema_mlt():
    """Columnas de CABEZACOMPROBANTES y CUERPOCOMPROBANTES en DB-MLT"""
    try:
        c = conn('LATIN1', db=DATABASE_MLT)
        cur = c.cursor()
        resultado = {}
        for tabla in ['CABEZACOMPROBANTES', 'CUERPOCOMPROBANTES']:
            cur.execute("""
                SELECT TRIM(f.RDB$FIELD_NAME),
                       TRIM(tp.RDB$TYPE_NAME),
                       f.RDB$NULL_FLAG,
                       f.RDB$DEFAULT_SOURCE
                FROM RDB$RELATION_FIELDS f
                JOIN RDB$FIELDS ff ON ff.RDB$FIELD_NAME = f.RDB$FIELD_SOURCE
                LEFT JOIN RDB$TYPES tp ON tp.RDB$TYPE = ff.RDB$FIELD_TYPE AND tp.RDB$FIELD_NAME = 'RDB$FIELD_TYPE'
                WHERE f.RDB$RELATION_NAME = ?
                ORDER BY f.RDB$FIELD_POSITION
            """, (tabla,))
            resultado[tabla] = [{"col": r[0], "tipo": r[1], "notnull": r[2], "default": r[3]} for r in cur.fetchall()]
        # Muestra de 1 fila para ver valores reales
        for tabla in ['CABEZACOMPROBANTES', 'CUERPOCOMPROBANTES']:
            try:
                cur.execute(f'SELECT FIRST 1 * FROM "{tabla}"')
                cols = [d[0] for d in cur.description]
                row = cur.fetchone()
                resultado[tabla+'_muestra'] = dict(zip(cols, [str(v) for v in row])) if row else {}
            except Exception as ex:
                resultado[tabla+'_muestra_err'] = str(ex)
        c.close()
        return resultado
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/mlt_cab/{numero}")
def debug_mlt_cab(numero: str):
    """Ver CABEZACOMPROBANTES de DATABASE_MLT para un número de comprobante."""
    try:
        c = conn('LATIN1', DATABASE_MLT)
        cur = c.cursor()
        cur.execute("SELECT FIRST 1 * FROM \"CABEZACOMPROBANTES\" "
                    "WHERE NUMEROCOMPROBANTE = ?", (numero,))
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        c.close()
        if row:
            return dict(zip(cols, [str(v) for v in row]))
        return {"error": "no encontrado", "numero": numero}
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/mlt_tablas")
def debug_mlt_tablas():
    """Lista todas las tablas de DATABASE_MLT."""
    try:
        c = conn('LATIN1', DATABASE_MLT)
        cur = c.cursor()
        cur.execute("SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS "
                    "WHERE RDB$SYSTEM_FLAG = 0 ORDER BY RDB$RELATION_NAME")
        tablas = [r[0] for r in cur.fetchall()]
        c.close()
        return {"tablas": tablas}
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/quevendi_mlt/{cliente}")
def debug_quevendi_mlt(cliente: str):
    """Diagnostica por qué que_vendi no trae resultados de DATABASE_MLT para un cliente."""
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    result = {"cliente": cliente, "pasos": []}

    # 1. Lookup en DB-MLT-Microbell.gdb (prod MLT tiene CLIENTES, DATABASE_MLT no)
    cod_mlt = cliente
    try:
        c = conn('WIN1252', DB_MLT_PROD)
        cur = c.cursor()
        cur.execute('SELECT CODIGOCLIENTE, CODIGOPARTICULAR FROM "CLIENTES" '
                    'WHERE CODIGOCLIENTE = ? OR CODIGOPARTICULAR = ?', (cliente, cliente))
        row = cur.fetchone()
        c.close()
        result["pasos"].append({"paso": "lookup_clientes_mlt_prod", "encontrado": row is not None,
                                "fila": [str(x) for x in row] if row else None})
        if row:
            cod_mlt = str(row[0]).strip()
    except Exception as e:
        result["pasos"].append({"paso": "lookup_clientes_mlt_prod", "error": str(e)})

    result["codigocliente_mlt"] = cod_mlt

    # 2. Verificar columnas de CUERPOCOMPROBANTES en DATABASE_MLT
    try:
        c = conn('LATIN1', DATABASE_MLT)
        cur = c.cursor()
        cur.execute("SELECT FIRST 1 * FROM \"CUERPOCOMPROBANTES\"")
        cols = [d[0] for d in cur.description]
        result["cuerpocomprobantes_cols"] = cols
        result["tiene_codigoparticular"] = "CODIGOPARTICULAR" in cols

        # 3. Buscar comprobantes del cliente
        cur.execute(
            'SELECT FIRST 5 cb.TIPOCOMPROBANTE, cb.NUMEROCOMPROBANTE, cb.FECHACOMPROBANTE '
            'FROM "CABEZACOMPROBANTES" cb '
            "WHERE cb.CODIGOCLIENTE = ? AND cb.ANULADA = '0' "
            "AND cb.TIPOCOMPROBANTE IN ('FA','FB','FE','FCA','FCB','FCE','FCCA','FCCB','FCCE','NCA','NCB','NCCA','NCCB') "
            'ORDER BY cb.FECHACOMPROBANTE DESC',
            (cod_mlt,)
        )
        rows_cab = cur.fetchall()
        result["cabeza_count"] = len(rows_cab)
        result["cabeza_sample"] = [[str(x) for x in r] for r in rows_cab]

        # 4. Si tiene CODIGOPARTICULAR: probar el SQL completo
        if "CODIGOPARTICULAR" in cols and rows_cab:
            try:
                cur.execute(
                    'SELECT FIRST 3 '
                    'COALESCE(NULLIF(TRIM(cu.CODIGOPARTICULAR),\'\'), TRIM(cu.CODIGOARTICULO)) AS COD_ART, '
                    'cu.DESCRIPCION, cb.TIPOCOMPROBANTE, cb.NUMEROCOMPROBANTE, '
                    'CAST(cu.CANTIDAD AS DOUBLE PRECISION) '
                    'FROM "CUERPOCOMPROBANTES" cu '
                    'JOIN "CABEZACOMPROBANTES" cb '
                    '  ON cb.TIPOCOMPROBANTE = cu.TIPOCOMPROBANTE '
                    ' AND cb.NUMEROCOMPROBANTE = cu.NUMEROCOMPROBANTE '
                    "WHERE cb.CODIGOCLIENTE = ? AND cb.ANULADA = '0' "
                    "AND cb.TIPOCOMPROBANTE IN ('FA','FB','FE','FCA','FCB','FCE','FCCA','FCCB','FCCE','NCA','NCB','NCCA','NCCB')",
                    (cod_mlt,)
                )
                rows_cuerpo = cur.fetchall()
                result["cuerpo_count"] = len(rows_cuerpo)
                result["cuerpo_sample"] = [[str(x) for x in r] for r in rows_cuerpo]
            except Exception as e2:
                result["cuerpo_error"] = str(e2)
        elif "CODIGOPARTICULAR" not in cols:
            result["nota"] = "CODIGOPARTICULAR no existe en CUERPOCOMPROBANTES de DATABASE_MLT"
        c.close()
    except Exception as e:
        result["pasos"].append({"paso": "query_mlt", "error": str(e)})

    return result


@app.get("/debug/quevendi_prod/{cliente}")
def debug_quevendi_prod(cliente: str):
    """Busca FA 100001668 y 100001684 en las 4 BDs y diagnostica que_vendi para un cliente."""
    DB_PROD     = 'c:/flexxus/DB/DB-Microbell.gdb'
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    result = {"cliente": cliente}

    # 1. Resolver codigos
    codigos = set([cliente])
    for db_label, db_path in [("DATABASE", DATABASE), ("DB_PROD", DB_PROD)]:
        try:
            c = conn('WIN1252', db_path)
            cur = c.cursor()
            cur.execute('SELECT CODIGOCLIENTE, CODIGOPARTICULAR FROM "CLIENTES" '
                        'WHERE CODIGOCLIENTE = ? OR CODIGOPARTICULAR = ?', (cliente, cliente))
            row = cur.fetchone()
            c.close()
            if row:
                for v in row:
                    if v is not None and str(v).strip():
                        codigos.add(str(v).strip())
            result[f"lookup_{db_label}"] = [str(x) for x in row] if row else None
        except Exception as e:
            result[f"lookup_{db_label}_error"] = str(e)

    codigos = list(codigos)
    result["codigos"] = codigos
    ph = ','.join('?' * len(codigos))

    # 2. Buscar FA 100001668 y 100001684 en las 4 BDs
    for db_label, db_path in [("DATABASE", DATABASE), ("DATABASE_MLT", DATABASE_MLT),
                               ("DB_PROD", DB_PROD), ("DB_MLT_PROD", DB_MLT_PROD)]:
        try:
            c = conn('LATIN1', db=db_path)
            cur = c.cursor()
            # Buscar las facturas objetivo
            cur.execute(
                'SELECT TIPOCOMPROBANTE, NUMEROCOMPROBANTE, CODIGOCLIENTE, FECHACOMPROBANTE '
                'FROM "CABEZACOMPROBANTES" WHERE NUMEROCOMPROBANTE IN (100001668, 100001684)'
            )
            rows_t = cur.fetchall()
            result[f"facturas_target_{db_label}"] = [[str(x) for x in r] for r in rows_t]

            # Contar comprobantes del cliente
            cur.execute(
                f'SELECT COUNT(*) FROM "CABEZACOMPROBANTES" '
                f"WHERE CODIGOCLIENTE IN ({ph}) AND ANULADA = '0' "
                f"AND TIPOCOMPROBANTE IN ('FA','FB','FE','FCA','FCB','FCE','FCCA','FCCB','FCCE','NCA','NCB','NCCA','NCCB')",
                codigos
            )
            result[f"count_{db_label}"] = cur.fetchone()[0]

            # Contar en CUERPOCOMPROBANTES (para verificar el JOIN)
            cur.execute(
                f'SELECT COUNT(*) FROM "CUERPOCOMPROBANTES" cu '
                f'JOIN "CABEZACOMPROBANTES" cb '
                f'  ON cb.TIPOCOMPROBANTE = cu.TIPOCOMPROBANTE '
                f' AND cb.NUMEROCOMPROBANTE = cu.NUMEROCOMPROBANTE '
                f"WHERE cb.CODIGOCLIENTE IN ({ph}) AND cb.ANULADA = '0' "
                f"AND cb.TIPOCOMPROBANTE IN ('FA','FB','FE','FCA','FCB','FCE','FCCA','FCCB','FCCE','NCA','NCB','NCCA','NCCB')",
                codigos
            )
            result[f"count_con_join_{db_label}"] = cur.fetchone()[0]
            c.close()
        except Exception as e:
            result[f"error_{db_label}"] = str(e)

    return result


@app.get("/debug/qv_errors")
def debug_qv_errors():
    """Muestra errores y conteo de filas por BD de la última llamada a que_vendi."""
    return {"errors": _QV_LAST_ERRORS, "counts": _QV_LAST_COUNTS}


@app.get("/debug/flexxus_deudas_schema")
def debug_flexxus_deudas_schema():
    """Busca tablas relevantes (saldo/deuda/vendedor/cuenta) y columnas de CABEZACOMPROBANTES en prod."""
    DB_PROD     = 'c:/flexxus/DB/DB-Microbell.gdb'
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    result = {}
    keywords = ['DEUDA', 'SALDO', 'VENDEDOR', 'CUENTA', 'COBRANZA', 'VENC']
    for label, db_path in [('DB_PROD', DB_PROD), ('DB_MLT_PROD', DB_MLT_PROD)]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            # Todas las tablas
            cur.execute("SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS "
                        "WHERE RDB$SYSTEM_FLAG = 0 ORDER BY RDB$RELATION_NAME")
            todas = [r[0] for r in cur.fetchall()]
            relevantes = [t for t in todas if any(k in t.upper() for k in keywords)]
            result[f'{label}_tablas_relevantes'] = relevantes
            # Columnas de CABEZACOMPROBANTES
            cur.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS "
                        "WHERE TRIM(RDB$RELATION_NAME) = 'CABEZACOMPROBANTES' "
                        "ORDER BY RDB$FIELD_POSITION")
            cols_cab = [r[0] for r in cur.fetchall()]
            result[f'{label}_cabeza_cols'] = cols_cab
            # Columnas de CUERPOCOMPROBANTES (solo las 20 primeras)
            cur.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS "
                        "WHERE TRIM(RDB$RELATION_NAME) = 'CUERPOCOMPROBANTES' "
                        "ORDER BY RDB$FIELD_POSITION")
            cols_cue = [r[0] for r in cur.fetchall()]
            result[f'{label}_cuerpo_cols'] = cols_cue
            c.close()
        except Exception as e:
            result[f'{label}_error'] = str(e)
    return result


@app.get("/debug/cliente_vendedor/{nombre}")
def debug_cliente_vendedor(nombre: str):
    """Muestra CODIGOVENDEDOR de un cliente en DB_PROD y sus comprobantes pendientes."""
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    result = {}
    try:
        c = conn('WIN1252', DB_PROD)
        cur = c.cursor()
        cur.execute(
            "SELECT CODIGOCLIENTE, CODIGOPARTICULAR, RAZONSOCIAL, CODIGOVENDEDOR, ACTIVO "
            'FROM "CLIENTES" WHERE UPPER(RAZONSOCIAL) CONTAINING UPPER(?)',
            (nombre,)
        )
        rows = cur.fetchall()
        result['clientes'] = [{'cod': r[0], 'part': r[1], 'razon': r[2], 'vendedor': r[3], 'activo': r[4]} for r in rows]
        # Para cada cliente encontrado, contar sus comprobantes pendientes
        for cli in result['clientes']:
            cod = (cli['cod'] or '').strip()
            if not cod: continue
            cur.execute(
                "SELECT COUNT(*) FROM \"CABEZACOMPROBANTES\" "
                "WHERE CODIGOCLIENTE=? AND CUENTACORRIENTE='1' AND ANULADA='0' "
                "AND TIPOCOMPROBANTE NOT IN ('RE','RI','INA')",
                (cod,)
            )
            cli['comprobantes_cta'] = cur.fetchone()[0]
        c.close()
    except Exception as e:
        result['error'] = str(e)
    return result


@app.get("/debug/cliente_en_mlt/{nombre}")
def debug_cliente_en_mlt(nombre: str):
    """Diagnostica la presencia de un cliente en DB_MLT_PROD buscando por nombre parcial en CABEZACOMPROBANTES."""
    DB_PROD     = 'c:/flexxus/DB/DB-Microbell.gdb'
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    result = {}

    # 1. Buscar en DB_PROD CLIENTES
    try:
        c = conn('WIN1252', DB_PROD)
        cur = c.cursor()
        cur.execute(
            "SELECT CODIGOCLIENTE, CODIGOPARTICULAR, RAZONSOCIAL, CODIGOVENDEDOR "
            'FROM "CLIENTES" WHERE UPPER(RAZONSOCIAL) CONTAINING UPPER(?) AND ACTIVO=\'1\'',
            (nombre,)
        )
        rows = cur.fetchall()
        c.close()
        result['DB_PROD_clientes'] = [
            {'cod': r[0], 'part': r[1], 'razon': r[2], 'vendedor': r[3]} for r in rows
        ]
    except Exception as e:
        result['DB_PROD_error'] = str(e)

    # 2. Buscar en DB_PROD CABEZACOMPROBANTES por RAZONSOCIAL (para verificar si la FA está ahí)
    try:
        c = conn('WIN1252', DB_PROD)
        cur = c.cursor()
        cur.execute(
            "SELECT FIRST 10 CODIGOCLIENTE, TIPOCOMPROBANTE, NUMEROCOMPROBANTE, RAZONSOCIAL, FECHACOMPROBANTE, CUENTACORRIENTE, ANULADA "
            'FROM "CABEZACOMPROBANTES" WHERE UPPER(RAZONSOCIAL) CONTAINING UPPER(?) '
            "AND CUENTACORRIENTE='1' AND ANULADA='0' ORDER BY FECHACOMPROBANTE DESC",
            (nombre,)
        )
        rows = cur.fetchall()
        c.close()
        result['DB_PROD_cabeza_por_razon'] = [
            {'cod': r[0], 'tipo': r[1], 'num': r[2], 'razon': r[3], 'fecha': str(r[4]), 'cta': r[5], 'anul': r[6]} for r in rows
        ]
    except Exception as e:
        result['DB_PROD_cabeza_error'] = str(e)

    # 3. Buscar en DB_MLT_PROD CABEZACOMPROBANTES por RAZONSOCIAL
    try:
        c = conn('WIN1252', DB_MLT_PROD)
        cur = c.cursor()
        cur.execute(
            "SELECT FIRST 5 CODIGOCLIENTE, TIPOCOMPROBANTE, NUMEROCOMPROBANTE, RAZONSOCIAL, FECHACOMPROBANTE "
            'FROM "CABEZACOMPROBANTES" WHERE UPPER(RAZONSOCIAL) CONTAINING UPPER(?)',
            (nombre,)
        )
        rows = cur.fetchall()
        c.close()
        result['DB_MLT_PROD_cabeza_por_razon'] = [
            {'cod': r[0], 'tipo': r[1], 'num': r[2], 'razon': r[3], 'fecha': str(r[4])} for r in rows
        ]
    except Exception as e:
        result['DB_MLT_PROD_cabeza_error'] = str(e)

    # 3. Si encontramos clientes en DB_PROD, buscar su CODIGOPARTICULAR en CUERPOCOMPROBANTES de DB_MLT_PROD
    for cli in result.get('DB_PROD_clientes', []):
        part = cli.get('part', '')
        cod  = cli.get('cod', '')
        key  = f"lookup_part_{part or cod}"
        if part:
            try:
                c = conn('LATIN1', DB_MLT_PROD)
                cur = c.cursor()
                cur.execute(
                    'SELECT FIRST 5 DISTINCT CODIGOCLIENTE FROM "CUERPOCOMPROBANTES" WHERE CODIGOPARTICULAR = ?',
                    (part,)
                )
                rows = cur.fetchall()
                c.close()
                result[key] = {'codigoscliente_en_mlt': [str(r[0]) for r in rows]}
            except Exception as e:
                result[key] = {'error': str(e)}

    return result


@app.get("/debug/vista_deuda_directo/{codigocliente}")
def debug_vista_deuda_directo(codigocliente: str):
    """Consulta directa de VISTADEUDACLIENTES + CABEZACOMPROBANTES para un CODIGOCLIENTE."""
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    result = {}
    # 1. VISTADEUDACLIENTES directo
    for charset in ['LATIN1', 'WIN1252']:
        try:
            c = conn(charset, DB_PROD)
            cur = c.cursor()
            cur.execute(
                'SELECT TIPOCOMPROBANTE, NUMEROCOMPROBANTE, PAGADO, NETO, DEBE, TOTALACTUALIZADO, CODIGOMONEDA, COTIZACION '
                'FROM "VISTADEUDACLIENTES" WHERE CODIGOCLIENTE = ?',
                (codigocliente,)
            )
            rows = cur.fetchall()
            c.close()
            result[f'vista_{charset}'] = [
                {'tipo': r[0], 'num': r[1], 'pagado': r[2], 'neto': r[3],
                 'debe': r[4], 'total_act': r[5], 'moneda': r[6], 'cotiz': r[7]}
                for r in rows
            ]
            result[f'vista_{charset}_suma'] = sum(float(r[5] or 0) for r in rows)
        except Exception as e:
            result[f'vista_{charset}_error'] = str(e)
    # 2. CABEZACOMPROBANTES directo (sin MONEDAS) para ver registros raw
    try:
        c = conn('WIN1252', DB_PROD)
        cur = c.cursor()
        cur.execute(
            "SELECT TIPOCOMPROBANTE, NUMEROCOMPROBANTE, TOTAL, IVA1, IVA2, PAGADO, CODIGOMONEDA, COTIZACION, ANULADA, CUENTACORRIENTE "
            'FROM "CABEZACOMPROBANTES" WHERE CODIGOCLIENTE = ? '
            "AND CUENTACORRIENTE='1' AND ANULADA='0' "
            "AND TIPOCOMPROBANTE NOT IN ('RE','RI','INA') "
            "AND ABS(CAST(TOTAL AS DOUBLE PRECISION)+CAST(IVA1 AS DOUBLE PRECISION)+CAST(IVA2 AS DOUBLE PRECISION)-CAST(PAGADO AS DOUBLE PRECISION)) >= 0.01 "
            "ORDER BY FECHACOMPROBANTE DESC",
            (codigocliente,)
        )
        rows = cur.fetchall()
        c.close()
        result['cabeza_raw'] = [
            {'tipo': r[0], 'num': r[1], 'total': r[2], 'iva1': r[3], 'iva2': r[4],
             'pagado': r[5], 'moneda': r[6], 'cotiz': r[7], 'anul': r[8], 'cta': r[9]}
            for r in rows
        ]
        result['cabeza_raw_suma_simple'] = sum(
            float((r[2] or 0)) + float((r[3] or 0)) + float((r[4] or 0)) - float((r[5] or 0))
            for r in rows
        )
    except Exception as e:
        result['cabeza_raw_error'] = str(e)
    return result


@app.get("/debug/vista_deuda_clientes")
def debug_vista_deuda_clientes():
    """Inspecciona VISTADEUDACLIENTES: columnas, definición SQL y muestra de datos."""
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    result = {}
    try:
        c = conn('WIN1252', db=DB_PROD)
        cur = c.cursor()
        # Columnas de la vista
        cur.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS "
                    "WHERE TRIM(RDB$RELATION_NAME) = 'VISTADEUDACLIENTES' "
                    "ORDER BY RDB$FIELD_POSITION")
        cols = [r[0] for r in cur.fetchall()]
        result['columnas'] = cols
        # Definición SQL de la vista
        cur.execute("SELECT RDB$VIEW_SOURCE FROM RDB$RELATIONS "
                    "WHERE TRIM(RDB$RELATION_NAME) = 'VISTADEUDACLIENTES'")
        row = cur.fetchone()
        result['sql_vista'] = str(row[0]) if row else None
        # Muestra de 3 filas
        try:
            cur.execute('SELECT FIRST 3 * FROM "VISTADEUDACLIENTES"')
            sample_cols = [d[0] for d in cur.description]
            sample_rows = cur.fetchall()
            result['muestra'] = [dict(zip(sample_cols, [str(v) if v is not None else None for v in r])) for r in sample_rows]
        except Exception as e2:
            result['muestra_error'] = str(e2)
        # Total deuda en la vista (para comparar con Flexxus)
        try:
            deuda_col = next((col for col in cols if 'DEUDA' in col.upper() or 'SALDO' in col.upper()), None)
            if deuda_col:
                cur.execute(f'SELECT COUNT(*), SUM(CAST("{deuda_col}" AS DOUBLE PRECISION)) FROM "VISTADEUDACLIENTES"')
                r = cur.fetchone()
                result[f'total_filas'] = r[0]
                result[f'suma_{deuda_col}'] = float(r[1] or 0)
        except Exception as e3:
            result['suma_error'] = str(e3)
        c.close()
    except Exception as e:
        result['error'] = str(e)
    return result


@app.get("/debug/cta_vendedor/{vendedor}")
def debug_cta_vendedor(vendedor: str):
    """Diagnóstico: suma deuda directo desde CABEZACOMPROBANTES con CODIGOVENDEDOR si existe."""
    DB_PROD     = 'c:/flexxus/DB/DB-Microbell.gdb'
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    result = {}
    for label, db_path in [('DB_PROD', DB_PROD), ('DB_MLT_PROD', DB_MLT_PROD)]:
        try:
            c = conn('LATIN1', db=db_path)
            cur = c.cursor()
            # Verificar si CABEZACOMPROBANTES tiene CODIGOVENDEDOR
            cur.execute("SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS "
                        "WHERE TRIM(RDB$RELATION_NAME) = 'CABEZACOMPROBANTES' "
                        "AND TRIM(RDB$FIELD_NAME) CONTAINING 'VENDEDOR'")
            cols_vend = [r[0] for r in cur.fetchall()]
            result[f'{label}_cols_vendedor_en_cabeza'] = cols_vend
            if cols_vend:
                col = cols_vend[0]
                cur.execute(f"""
                    SELECT COUNT(*),
                           SUM(CAST(TOTAL AS DOUBLE PRECISION) + CAST(IVA1 AS DOUBLE PRECISION) + CAST(IVA2 AS DOUBLE PRECISION)),
                           SUM(CAST(TOTAL AS DOUBLE PRECISION) + CAST(IVA1 AS DOUBLE PRECISION) + CAST(IVA2 AS DOUBLE PRECISION) - CAST(PAGADO AS DOUBLE PRECISION))
                    FROM "CABEZACOMPROBANTES"
                    WHERE UPPER({col}) = UPPER(?)
                      AND CUENTACORRIENTE = '1'
                      AND ANULADA = '0'
                      AND TIPOCOMPROBANTE IN ('FA','FB','FE','FCA','FCB','DI','SIV','NCA','NCB','NDA','NDB','NCAE','NDAE')
                      AND ABS(CAST(TOTAL AS DOUBLE PRECISION) + CAST(IVA1 AS DOUBLE PRECISION) + CAST(IVA2 AS DOUBLE PRECISION) - CAST(PAGADO AS DOUBLE PRECISION)) > 0.01
                """, (vendedor.upper(),))
                row = cur.fetchone()
                result[f'{label}_directo_count'] = row[0]
                result[f'{label}_directo_total_bruto'] = float(row[1] or 0)
                result[f'{label}_directo_deuda'] = float(row[2] or 0)
            c.close()
        except Exception as e:
            result[f'{label}_error'] = str(e)
    return result


@app.get("/debug/camping_query/{cod}")
def debug_camping_query(cod: str):
    """Debug exacto de _query_cta para un cliente específico (ej: 348 = CAMPING LA PLATA)."""
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    out = {"cod": cod, "db_prod": {}, "db_mlt": {}}
    for db_key, db_path in [("db_prod", DB_PROD), ("db_mlt", DB_MLT_PROD)]:
        res = {"charset_test": {}, "fetchone_loop": [], "errors": []}
        # Test 1: conectar y contar sin filtros extra
        for charset in ['WIN1252', 'LATIN1']:
            try:
                c = conn(charset, db=db_path)
                cur = c.cursor()
                cur.execute('SELECT COUNT(*) FROM "CABEZACOMPROBANTES" WHERE CODIGOCLIENTE = ?', (cod,))
                row = cur.fetchone()
                c.close()
                res["charset_test"][charset] = {"count_total": row[0] if row else None}
            except Exception as e:
                res["charset_test"][charset] = {"error": str(e)}

        # Test 2: SIN FILTROS — ver todos los registros para detectar debe>0 excluidos
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            cur.execute("""
                SELECT FIRST 500 SKIP 0
                    TIPOCOMPROBANTE, NUMEROCOMPROBANTE, FECHACOMPROBANTE,
                    TOTAL, IVA1, IVA2, PAGADO, COTIZACION, CODIGOMONEDA,
                    FECHAVENCIMIENTO, CLASECOMPROBANTE, CUENTACORRIENTE, ANULADA
                FROM "CABEZACOMPROBANTES"
                WHERE CODIGOCLIENTE = ?
                ORDER BY FECHACOMPROBANTE ASC
            """, (cod,))
            all_rows = []
            while True:
                try:
                    r = cur.fetchone()
                    if r is None: break
                    total=float(r[3] or 0); iva1=float(r[4] or 0); iva2=float(r[5] or 0)
                    pagado=float(r[6] or 0); neto=total+iva1+iva2; debe=neto-pagado
                    if abs(debe) >= 0.01:
                        all_rows.append({"tipo":str(r[0]),"num":str(r[1]),"fecha":str(r[2]),
                            "neto":round(neto,2),"pagado":round(pagado,2),"debe":round(debe,2),
                            "cotiz":float(r[7] or 1),"moneda":str(r[8] or '').strip(),
                            "ctacte":str(r[11]),"anulada":str(r[12])})
                except Exception as e:
                    all_rows.append({"row_error": str(e)})
                    break
            res["sin_filtros_con_saldo"] = all_rows
            c.close()
        except Exception as exec_e:
            res["sin_filtros_error"] = str(exec_e)

        # Test 3: fetchone loop con WIN1252 — misma query que _query_cta
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            cur.execute("""
                SELECT FIRST 500 SKIP 0
                    TIPOCOMPROBANTE, NUMEROCOMPROBANTE, FECHACOMPROBANTE,
                    TOTAL, IVA1, IVA2, PAGADO, COTIZACION, CODIGOMONEDA,
                    FECHAVENCIMIENTO, CLASECOMPROBANTE
                FROM "CABEZACOMPROBANTES"
                WHERE CODIGOCLIENTE = ?
                  AND CUENTACORRIENTE = '1'
                  AND ANULADA = '0'
                  AND TIPOCOMPROBANTE NOT IN ('RE', 'RI', 'INA')
                ORDER BY FECHAVENCIMIENTO ASC, FECHACOMPROBANTE ASC
            """, (cod,))
            n = 0
            while True:
                try:
                    r = cur.fetchone()
                    if r is None:
                        break
                    n += 1
                    # intento conversión
                    try:
                        total = float(r[3] or 0); iva1 = float(r[4] or 0); iva2 = float(r[5] or 0)
                        pagado = float(r[6] or 0); cotiz = float(r[7] or 1) or 1.0
                        neto = total + iva1 + iva2; debe = neto - pagado
                        res["fetchone_loop"].append({
                            "n": n, "tipo": str(r[0]), "num": str(r[1]),
                            "fecha": str(r[2]), "neto": neto, "debe": debe,
                            "cotiz": cotiz, "moneda": str(r[8] or '').strip()
                        })
                    except Exception as conv_e:
                        res["fetchone_loop"].append({"n": n, "conv_error": str(conv_e), "raw": [str(x) for x in r]})
                except Exception as fetch_e:
                    res["errors"].append({"at_row": n, "fetch_error": str(fetch_e)})
                    break
            res["total_fetched"] = n
            c.close()
        except Exception as exec_e:
            res["execute_error"] = str(exec_e)

        out[db_key] = res
    return out

@app.get("/debug/deuda_por_bd/{vendedor}")
def debug_deuda_por_bd(vendedor: str):
    """Muestra por cada cliente del vendedor: resultado de DB_PROD vs DB_MLT_PROD por separado."""
    DB_PROD     = 'c:/flexxus/DB/DB-Microbell.gdb'
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    try:
        c = conn('WIN1252', db=DB_PROD)
        cur = c.cursor()
        cur.execute(
            'SELECT CODIGOCLIENTE, RAZONSOCIAL FROM "CLIENTES" '
            'WHERE ACTIVO=? AND UPPER(CODIGOVENDEDOR)=? ORDER BY RAZONSOCIAL',
            ('1', vendedor.upper())
        )
        clientes = cur.fetchall()
        c.close()
    except Exception as e:
        return {"error_clientes": str(e)}

    resultado = []
    for cod_raw, razon in clientes:
        cod = (cod_raw or '').strip()
        if not cod: continue
        d_prod = d_mlt = 0.0
        err_prod = err_mlt = None
        try:
            rows = _query_cta(DB_PROD, [cod], 500, 0)
            d_prod = sum(float(r[5] or 0) for r in rows)
        except Exception as e:
            err_prod = str(e)
        try:
            rows = _query_cta(DB_MLT_PROD, [cod], 500, 0)
            d_mlt = sum(float(r[5] or 0) for r in rows)
        except Exception as e:
            err_mlt = str(e)
        total = d_prod + d_mlt
        if abs(total) >= 0.01 or err_prod or err_mlt:
            entry = {
                "cod": cod, "razon": razon,
                "db_prod": round(d_prod, 2), "db_mlt": round(d_mlt, 2),
                "total": round(total, 2)
            }
            if err_prod: entry["err_prod"] = err_prod
            if err_mlt:  entry["err_mlt"]  = err_mlt
            resultado.append(entry)

    total_general = sum(r["total"] for r in resultado)
    return {"total_general": round(total_general, 2), "clientes": len(resultado), "detalle": resultado}

@app.get("/debug/listar_dbs")
def listar_dbs():
    import glob
    archivos = glob.glob('c:/flexxus/**/*.gdb', recursive=True) + \
               glob.glob('c:/flexxus/**/*.FDB', recursive=True)
    return {"archivos": sorted(archivos)}

@app.get("/debug/generators")
def debug_generators():
    """Lista generators y su valor actual en ambas BDs"""
    resultado = {}
    for nombre, db_path in [('oficial', DATABASE), ('sw', DATABASE_MLT)]:
        try:
            c = conn('LATIN1', db=db_path)
            cur = c.cursor()
            cur.execute(
                "SELECT TRIM(RDB$GENERATOR_NAME), RDB$GENERATOR_ID "
                "FROM RDB$GENERATORS WHERE RDB$SYSTEM_FLAG = 0 "
                "ORDER BY RDB$GENERATOR_NAME"
            )
            gens = cur.fetchall()
            vals = {}
            for g in gens:
                try:
                    cur.execute(f'SELECT GEN_ID("{g[0]}", 0) FROM RDB$DATABASE')
                    vals[g[0]] = cur.fetchone()[0]
                except Exception:
                    vals[g[0]] = None
            resultado[nombre] = vals
            c.close()
        except Exception as e:
            resultado[nombre] = {"error": str(e)}
    return resultado

@app.get("/debug/tablas_mlt")
def debug_tablas_mlt():
    """Lista todas las tablas de usuario en DB-MLT-Microbell.gdb"""
    try:
        c = conn('LATIN1', db=DATABASE_MLT)
        cur = c.cursor()
        cur.execute(
            "SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS "
            "WHERE RDB$SYSTEM_FLAG = 0 AND RDB$VIEW_BLR IS NULL "
            "ORDER BY RDB$RELATION_NAME"
        )
        tablas = [r[0] for r in cur.fetchall()]
        c.close()
        return {"tablas": tablas}
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/esquema_clientes")
def debug_esquema_clientes():
    """Muestra todas las columnas de la tabla CLIENTES en DB-Prueba.gdb"""
    try:
        c = conn()
        cur = c.cursor()
        cur.execute(
            "SELECT TRIM(RDB$FIELD_NAME), RDB$FIELD_POSITION "
            "FROM RDB$RELATION_FIELDS "
            "WHERE TRIM(RDB$RELATION_NAME) = 'CLIENTES' "
            "ORDER BY RDB$FIELD_POSITION"
        )
        cols = [{"pos": r[1], "nombre": r[0]} for r in cur.fetchall()]
        c.close()
        return {"columnas": cols}
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/muestra_clientes")
def debug_muestra_clientes(vendedor: str = "KRAFFT"):
    """Muestra 3 clientes del vendedor para ver valores de columnas IVA"""
    try:
        c = conn()
        cur = c.cursor()
        cur.execute(
            'SELECT FIRST 3 CODIGOCLIENTE, RAZONSOCIAL, CODIGOPARTICULAR '
            'FROM "CLIENTES" WHERE ACTIVO = ? AND CODIGOVENDEDOR = ? ORDER BY RAZONSOCIAL',
            ('1', vendedor.upper())
        )
        rows = cur.fetchall()
        c.close()
        return [{"codigo": r[0], "razonsocial": r[1], "codigoparticular": r[2]} for r in rows]
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/tablas_of")
def debug_tablas_of():
    """Lista todas las tablas de DB-Prueba.gdb"""
    try:
        c = conn('LATIN1', db=DATABASE)
        cur = c.cursor()
        cur.execute("SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=0 ORDER BY RDB$RELATION_NAME")
        rows = cur.fetchall()
        c.close()
        return [r[0].strip() for r in rows]
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/contadores_of")
def debug_contadores_of():
    """Muestra todos los contadores de la BD oficial (DB-Prueba.gdb)"""
    try:
        c = conn('LATIN1', db=DATABASE)
        cur = c.cursor()
        cur.execute('SELECT CODIGOCONTADOR, DESCRIPCION, VALOR FROM "CONTADORES" ORDER BY CODIGOCONTADOR')
        rows = cur.fetchall()
        c.close()
        return [{"codigo": r[0], "descripcion": (r[1] or "").strip(), "valor": r[2]} for r in rows]
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/contadores_sw")
def debug_contadores_sw():
    """Muestra todos los contadores de la BD SW (DB-MLT-Prueba.gdb)"""
    try:
        c = conn('LATIN1', db=DATABASE_MLT)
        cur = c.cursor()
        cur.execute('SELECT CODIGOCONTADOR, DESCRIPCION, VALOR FROM "CONTADORES" ORDER BY CODIGOCONTADOR')
        rows = cur.fetchall()
        c.close()
        return [{"codigo": r[0], "descripcion": (r[1] or "").strip(), "valor": r[2]} for r in rows]
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/info_bd")
def debug_info_bd():
    """Muestra las rutas de BD configuradas y cuenta de registros clave"""
    result = {
        "DATABASE": DATABASE,
        "DATABASE_MLT": DATABASE_MLT,
    }
    try:
        c = conn()
        cur = c.cursor()
        cur.execute('SELECT COUNT(*) FROM "CABEZAPRESUPUESTOS"')
        result["presupuestos_total"] = cur.fetchone()[0]
        cur.execute('SELECT MAX(CAST(NUMEROCOMPROBANTE AS INTEGER)) FROM "CABEZAPRESUPUESTOS"')
        result["presupuesto_max"] = cur.fetchone()[0]
        cur.execute('SELECT COUNT(*) FROM "CABEZAPEDIDOS"')
        result["pedidos_total"] = cur.fetchone()[0]
        cur.execute('SELECT MAX(CAST(NUMEROCOMPROBANTE AS INTEGER)) FROM "CABEZAPEDIDOS"')
        result["pedido_max"] = cur.fetchone()[0]
        c.close()
    except Exception as e:
        result["error"] = str(e)
    return result

@app.get("/debug/config_prueba")
def debug_config_prueba():
    """Lee configuracion.ini del perfil Prueba"""
    import os
    result = {}
    paths = [
        'c:/flexxus/FlexxusERP/Prueba/bin/configuracion.ini',
        'c:/flexxus/FlexxusERP/BIN/FlexxusServer.ini',
    ]
    for p in paths:
        try:
            with open(p, 'r', encoding='latin1', errors='replace') as f:
                result[p] = f.read()
        except Exception as e:
            result[p] = f"Error: {e}"
    return result

@app.get("/debug/config_flexxus")
def debug_config_flexxus():
    """Busca archivos de configuración de Flexxus que definen la BD por empresa"""
    import glob, os
    result = {"archivos": [], "contenido": {}}
    patterns = [
        'c:/flexxus/**/*.ini', 'c:/flexxus/**/*.cfg',
        'c:/flexxus/**/*.config', 'c:/flexxus/**/Empresas*.xml',
        'c:/flexxus/**/empresas*.ini', 'c:/flexxus/**/conexion*.ini',
        'c:/flexxus/FlexxusERP/*.ini', 'c:/flexxus/FlexxusERP/*.cfg',
        'c:/flexxus/FlexxusERP/*.xml',
    ]
    for p in patterns:
        for f in glob.glob(p, recursive=True):
            result["archivos"].append(f)
            try:
                with open(f, 'r', encoding='latin1', errors='replace') as fp:
                    content = fp.read(3000)
                if 'DB-' in content or 'Prueba' in content or 'Microbell' in content or 'gdb' in content.lower():
                    result["contenido"][f] = content
            except Exception as e:
                result["contenido"][f] = f"Error: {e}"
    return result

@app.get("/debug/comparar_bases")
def debug_comparar_bases():
    """Compara presupuesto MAX en DB-Prueba vs DB-Microbell"""
    result = {}
    for nombre, path in [
        ("DB-Prueba", "c:/flexxus/DB/DB-Prueba.gdb"),
        ("DB-Microbell", "c:/flexxus/DB/DB-Microbell.gdb"),
    ]:
        try:
            c = firebirdsql.connect(host=HOST, port=PORT, database=path,
                                    user=DB_USER, password=DB_PASS, charset='LATIN1')
            cur = c.cursor()
            cur.execute('SELECT MAX(CAST(NUMEROCOMPROBANTE AS INTEGER)), COUNT(*) FROM "CABEZAPRESUPUESTOS"')
            row = cur.fetchone()
            cur.execute('SELECT MAX(CAST(NUMEROCOMPROBANTE AS INTEGER)), COUNT(*) FROM "CABEZAPEDIDOS"')
            row2 = cur.fetchone()
            c.close()
            result[nombre] = {
                "presupuesto_max": row[0], "presupuestos_total": row[1],
                "pedido_max": row2[0], "pedidos_total": row2[1]
            }
        except Exception as e:
            result[nombre] = {"error": str(e)}
    return result

@app.get("/debug/listar_gdbs")
def debug_listar_gdbs():
    """Lista todos los archivos .gdb y .fdb en c:/flexxus/"""
    import glob, os
    result = []
    for pattern in ['c:/flexxus/**/*.gdb', 'c:/flexxus/**/*.fdb',
                    'c:/Flexxus/**/*.gdb', 'c:/Flexxus/**/*.fdb']:
        for f in glob.glob(pattern, recursive=True):
            try:
                size = os.path.getsize(f)
                mtime = os.path.getmtime(f)
                from datetime import datetime
                result.append({
                    "path": f,
                    "size_mb": round(size / 1024 / 1024, 1),
                    "modificado": datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
                })
            except Exception:
                result.append({"path": f})
    return sorted(result, key=lambda x: x.get("modificado",""), reverse=True)

@app.get("/debug/esquema_cuerpos")
def debug_esquema_cuerpos():
    """Columnas reales de CUERPOPRESUPUESTOS y CUERPOPEDIDOS en DB-Prueba.gdb"""
    result = {}
    c = conn()
    cur = c.cursor()
    for tabla in ['CUERPOPRESUPUESTOS', 'CUERPOPEDIDOS']:
        cur.execute(
            "SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS "
            "WHERE TRIM(RDB$RELATION_NAME) = ? ORDER BY RDB$FIELD_POSITION",
            (tabla,))
        result[tabla] = [r[0] for r in cur.fetchall()]
    c.close()
    return result

@app.get("/debug/esquema_cabezas")
def debug_esquema_cabezas():
    """Columnas de CABEZAPRESUPUESTOS y CABEZAPEDIDOS"""
    result = {}
    c = conn()
    cur = c.cursor()
    for tabla in ['CABEZAPRESUPUESTOS', 'CABEZAPEDIDOS']:
        cur.execute(
            "SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS "
            "WHERE TRIM(RDB$RELATION_NAME) = ? ORDER BY RDB$FIELD_POSITION",
            (tabla,))
        result[tabla] = [r[0] for r in cur.fetchall()]
    c.close()
    return result

@app.get("/debug/muestra_presupuesto/{numero}")
def debug_muestra_presupuesto(numero: str):
    """Muestra cabeza + cuerpo de un presupuesto específico"""
    try:
        c = conn()
        cur = c.cursor()
        cur.execute('SELECT * FROM "CABEZAPRESUPUESTOS" WHERE NUMEROCOMPROBANTE = ?', (numero,))
        cols_cab = [d[0] for d in cur.description]
        row = cur.fetchone()
        cabeza = dict(zip(cols_cab, [str(v) if v is not None else None for v in row])) if row else None
        cur.execute('SELECT * FROM "CUERPOPRESUPUESTOS" WHERE NUMEROCOMPROBANTE = ? ORDER BY LINEA', (numero,))
        cols_cue = [d[0] for d in cur.description]
        cuerpos = [dict(zip(cols_cue, [str(v) if v is not None else None for v in r])) for r in cur.fetchall()]
        c.close()
        return {"cabeza": cabeza, "cuerpos": cuerpos}
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/codigos_presupuesto/{numero}")
def debug_codigos_presupuesto(numero: str):
    """Compara CODIGOARTICULO vs CODIGOPARTICULAR en CUERPOPRESUPUESTOS."""
    try:
        c = conn()
        cur = c.cursor()
        cur.execute(
            "SELECT LINEA, CODIGOARTICULO, CODIGOPARTICULAR, DESCRIPCION "
            "FROM \"CUERPOPRESUPUESTOS\" WHERE NUMEROCOMPROBANTE = ? ORDER BY LINEA",
            (numero,)
        )
        rows = cur.fetchall()
        c.close()
        return [{"linea": r[0], "codigoarticulo": r[1], "codigoparticular": r[2], "descripcion": r[3]} for r in rows]
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/ultimos_presupuestos")
def debug_ultimos_presupuestos():
    """Últimos 5 presupuestos en DB-Prueba.gdb"""
    try:
        c = conn()
        cur = c.cursor()
        cur.execute(
            'SELECT FIRST 5 TIPOCOMPROBANTE, NUMEROCOMPROBANTE, CODIGOCLIENTE, '
            'RAZONSOCIAL, FECHACOMPROBANTE, TOTAL, CODIGOUSUARIO '
            'FROM "CABEZAPRESUPUESTOS" ORDER BY FECHAMODIFICACION DESC')
        rows = cur.fetchall()
        c.close()
        return [{"tipo": r[0], "numero": r[1], "cliente": r[2],
                 "razon": r[3], "fecha": str(r[4]), "total": r[5], "usuario": r[6]}
                for r in rows]
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/comparar/{num_bueno}/{num_malo}")
def debug_comparar(num_bueno: str, num_malo: str):
    """
    Compara TODAS las columnas de dos presupuestos.
    Uso: /debug/comparar/7849/7857
    Resalta las columnas que difieren.
    """
    try:
        c = conn()
        cur = c.cursor()

        def leer(numero):
            cur.execute('SELECT * FROM "CABEZAPRESUPUESTOS" WHERE NUMEROCOMPROBANTE = ?', (numero,))
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            if not row:
                return None, cols
            return dict(zip(cols, [str(v) if v is not None else '__NULL__' for v in row])), cols

        datos_bueno, cols = leer(num_bueno)
        datos_malo, _    = leer(num_malo)

        # Defaults del dominio para cada columna
        cur.execute("""
            SELECT TRIM(rf.RDB$FIELD_NAME), rf.RDB$NULL_FLAG, rf.RDB$DEFAULT_SOURCE
            FROM RDB$RELATION_FIELDS rf
            WHERE rf.RDB$RELATION_NAME = 'CABEZAPRESUPUESTOS'
            ORDER BY rf.RDB$FIELD_POSITION
        """)
        defaults = {r[0].strip(): {"not_null": r[1]==1, "default": str(r[2]).strip() if r[2] else None}
                    for r in cur.fetchall()}

        # Todos los triggers (INSERT + UPDATE)
        cur.execute("""
            SELECT TRIM(RDB$TRIGGER_NAME), RDB$TRIGGER_TYPE, CAST(RDB$TRIGGER_SOURCE AS VARCHAR(4000))
            FROM RDB$TRIGGERS
            WHERE RDB$RELATION_NAME = 'CABEZAPRESUPUESTOS' AND RDB$SYSTEM_FLAG = 0
        """)
        triggers = [{"name": r[0], "type": r[1],
                     "type_desc": {1:"BEFORE INSERT",2:"AFTER INSERT",3:"BEFORE UPDATE",4:"AFTER UPDATE",
                                   5:"BEFORE DELETE",6:"AFTER DELETE"}.get(r[1],"?"),
                     "source": r[2]} for r in cur.fetchall()]

        c.close()

        if not datos_bueno:
            return {"error": f"No encontrado: {num_bueno}"}
        if not datos_malo:
            return {"error": f"No encontrado: {num_malo}"}

        diferencias = {}
        iguales = {}
        for col in cols:
            v1 = datos_bueno.get(col)
            v2 = datos_malo.get(col)
            if v1 != v2:
                diferencias[col] = {
                    f"bueno_{num_bueno}": v1,
                    f"malo_{num_malo}": v2,
                    "default_info": defaults.get(col.strip())
                }
            else:
                iguales[col] = v1

        return {
            "diferencias": diferencias,
            "total_diferencias": len(diferencias),
            "iguales": iguales,
            "triggers": triggers,
            "defaults_columnas": defaults,
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/setup/restaurar_defaults_aprobacion")
def restaurar_defaults_aprobacion():
    """
    Restaura el DEFAULT original de FECHAAPROBADO ('1900-01-01') y CODIGOUSUARIOAPROBACION ('')
    que fue eliminado por setup/fechaaprobado_nullable.
    Ejecutar UNA SOLA VEZ.
    """
    try:
        import struct
        c = conn('WIN1252')
        cur = c.cursor()

        # Restaurar DEFAULT '1900-01-01 00:00:00' en FECHAAPROBADO
        # En Firebird, el DEFAULT_SOURCE es texto SQL, y DEFAULT_VALUE es BLR binario.
        # Actualizamos solo DEFAULT_SOURCE (texto); Firebird lo recompila al arrancar.
        cur.execute("""
            UPDATE RDB$RELATION_FIELDS
            SET RDB$DEFAULT_SOURCE = 'DEFAULT ''1900-01-01 00:00:00'''
            WHERE RDB$RELATION_NAME = 'CABEZAPRESUPUESTOS'
              AND TRIM(RDB$FIELD_NAME) = 'FECHAAPROBADO'
        """)
        # Restaurar DEFAULT '' en CODIGOUSUARIOAPROBACION
        cur.execute("""
            UPDATE RDB$RELATION_FIELDS
            SET RDB$DEFAULT_SOURCE = 'DEFAULT '''''
            WHERE RDB$RELATION_NAME = 'CABEZAPRESUPUESTOS'
              AND TRIM(RDB$FIELD_NAME) = 'CODIGOUSUARIOAPROBACION'
        """)
        c.commit()

        # Verificar
        cur.execute("""
            SELECT TRIM(RDB$FIELD_NAME), RDB$NULL_FLAG, RDB$DEFAULT_SOURCE
            FROM RDB$RELATION_FIELDS
            WHERE RDB$RELATION_NAME = 'CABEZAPRESUPUESTOS'
              AND TRIM(RDB$FIELD_NAME) IN ('FECHAAPROBADO', 'CODIGOUSUARIOAPROBACION')
        """)
        rows = cur.fetchall()
        c.close()
        return {
            "ok": True,
            "columnas": [{"campo": r[0], "not_null": r[1], "default": str(r[2]) if r[2] else None} for r in rows],
            "instruccion": "Reiniciar Firebird para que tome los nuevos defaults"
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/debug/parametros_np")
def debug_parametros_np():
    """Verifica el valor de NP en PARAMETROS y hace prueba de TRIM."""
    res = {}
    try:
        c = conn('LATIN1')
        cur = c.cursor()
        # Buscar con TRIM
        cur.execute("SELECT TIPODOCUMENTO, VALOR FROM \"PARAMETROS\" WHERE TRIM(TIPODOCUMENTO) = 'NP'")
        row = cur.fetchone()
        res['con_trim'] = {'tipodocumento': repr(row[0]), 'valor': row[1]} if row else None
        # Buscar sin TRIM para ver el valor crudo
        cur.execute("SELECT TIPODOCUMENTO, VALOR FROM \"PARAMETROS\" WHERE TIPODOCUMENTO LIKE '%NP%'")
        rows = cur.fetchall()
        res['like_NP'] = [{'tipodocumento': repr(r[0]), 'valor': r[1]} for r in rows]
        # Ver todas las filas con comprobante tipo 2 chars
        cur.execute("SELECT TIPODOCUMENTO, VALOR FROM \"PARAMETROS\" ORDER BY TIPODOCUMENTO")
        all_rows = cur.fetchall()
        res['primeros_20'] = [{'tipodocumento': repr(r[0]), 'valor': r[1]} for r in all_rows[:20]]
        c.close()
    except Exception as e:
        res['error'] = str(e)
    return res

@app.get("/debug/mlt_tablas_pedidos")
def debug_mlt_tablas_pedidos():
    """Verifica si DB-MLT tiene CABEZAPEDIDOS y cuántos registros NP hay en cada tabla."""
    res = {"db": DATABASE_MLT}
    try:
        c = conn('WIN1252', db=DATABASE_MLT)
        cur = c.cursor()
        # Tablas que existen
        cur.execute("SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=0 AND RDB$RELATION_NAME CONTAINING 'CABEZA'")
        res["tablas_cabeza"] = [r[0] for r in cur.fetchall()]
        # Contar NP en CABEZACOMPROBANTES
        try:
            cur.execute("SELECT COUNT(*) FROM \"CABEZACOMPROBANTES\" WHERE TIPOCOMPROBANTE='NP'")
            res["cabezacomprobantes_NP"] = cur.fetchone()[0]
        except Exception as e:
            res["cabezacomprobantes_NP_error"] = str(e)
        # Contar NP en CABEZAPEDIDOS si existe
        try:
            cur.execute("SELECT COUNT(*) FROM \"CABEZAPEDIDOS\" WHERE TIPOCOMPROBANTE='NP'")
            res["cabezapedidos_NP"] = cur.fetchone()[0]
        except Exception as e:
            res["cabezapedidos_NP_error"] = str(e)
        c.close()
    except Exception as e:
        res["error"] = str(e)
    return res

@app.get("/debug/fix_cuentacorriente_sw")
def debug_fix_cuentacorriente_sw(numero: str = Query(...)):
    """Corrige CUENTACORRIENTE=1 en CABEZACOMPROBANTES de DB-MLT para un pedido SW."""
    try:
        c = conn('WIN1252', db=DATABASE_MLT)
        cur = c.cursor()
        cur.execute(
            'UPDATE "CABEZACOMPROBANTES" SET CUENTACORRIENTE=1 '
            'WHERE NUMEROCOMPROBANTE=? AND TIPOCOMPROBANTE=?',
            (numero, 'NP')
        )
        affected = cur.rowcount
        c.commit()
        c.close()
        return {"ok": True, "numero": numero, "filas_actualizadas": affected}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/debug/inspect_sw_pedido")
def debug_inspect_sw_pedido(numero: str = Query(...)):
    """Devuelve todos los campos de CABEZACOMPROBANTES en DB-MLT para un número dado,
    y también el último NP nativo (creado por Flexxus) para comparar."""
    try:
        c = conn('WIN1252', db=DATABASE_MLT)
        cur = c.cursor()
        # Registro del pedido pedido
        cur.execute('SELECT * FROM "CABEZACOMPROBANTES" WHERE NUMEROCOMPROBANTE=? AND TIPOCOMPROBANTE=?', (numero, 'NP'))
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        target = dict(zip(cols, [str(v) for v in row])) if row else None
        # Último NP que NO sea el nuestro (para comparar)
        cur.execute(
            'SELECT FIRST 1 * FROM "CABEZACOMPROBANTES" WHERE TIPOCOMPROBANTE=? AND NUMEROCOMPROBANTE<>? ORDER BY FECHACOMPROBANTE DESC',
            ('NP', numero)
        )
        row2 = cur.fetchone()
        nativo = dict(zip(cols, [str(v) for v in row2])) if row2 else None
        c.close()
        # Diferencias
        diffs = {}
        if target and nativo:
            for k in cols:
                if target.get(k) != nativo.get(k):
                    diffs[k] = {"nuestro": target.get(k), "nativo": nativo.get(k)}
        return {"pedido": numero, "encontrado": target is not None, "diferencias_con_nativo": diffs, "nuestro": target, "nativo": nativo}
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/check_dbs")
def debug_check_dbs():
    """Muestra qué bases de datos está usando la API."""
    import os
    return {
        "DATABASE": DATABASE,
        "DATABASE_MLT": DATABASE_MLT,
        "DB_L1_env": os.getenv('DB_L1', '(no seteado)'),
        "DB_MLT_env": os.getenv('DB_MLT', '(no seteado)'),
    }

@app.get("/debug/comparar_sw_produccion")
def debug_comparar_sw_produccion(n_prod: str = Query(...), n_prueba: str = Query(...)):
    """Compara un pedido nativo de DB-MLT-Microbell.gdb (producción SW)
    contra uno creado por la API en DB-MLT-Prueba.gdb."""
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    def _get(db, numero):
        try:
            c = conn('WIN1252', db=db)
            cur = c.cursor()
            cur.execute('SELECT * FROM "CABEZACOMPROBANTES" WHERE NUMEROCOMPROBANTE=? AND TIPOCOMPROBANTE=?', (numero, 'NP'))
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            c.close()
            return dict(zip(cols, [str(v) for v in row])) if row else None
        except Exception as e:
            return {"error": str(e)}
    prod  = _get(DB_MLT_PROD, n_prod)
    prueba = _get(DATABASE_MLT, n_prueba)
    diffs = {}
    if prod and prueba and "error" not in prod and "error" not in prueba:
        for k in prod:
            if k in prueba and prod[k] != prueba[k]:
                diffs[k] = {"produccion_nativo": prod[k], "prueba_api": prueba[k]}
    return {"n_prod": n_prod, "n_prueba": n_prueba,
            "produccion_encontrado": prod is not None and "error" not in (prod or {}),
            "prueba_encontrado": prueba is not None and "error" not in (prueba or {}),
            "diferencias": diffs}

@app.get("/debug/tablas_mlt_prod")
def debug_tablas_mlt_prod():
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    try:
        c = conn('WIN1252', db=DB_MLT_PROD)
        cur = c.cursor()
        cur.execute("SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=0 AND RDB$RELATION_NAME CONTAINING 'CABEZA' ORDER BY 1")
        tablas = [r[0] for r in cur.fetchall()]
        # Buscar el pedido en todas las tablas CABEZA*
        encontrado = {}
        for t in tablas:
            try:
                cur.execute(f'SELECT COUNT(*) FROM "{t}" WHERE CAST(NUMEROCOMPROBANTE AS VARCHAR(20))=? AND TIPOCOMPROBANTE=?', ('100023558', 'NP'))
                n = cur.fetchone()[0]
                if n > 0:
                    encontrado[t] = n
            except Exception:
                pass
        c.close()
        return {"tablas_cabeza": tablas, "pedido_100023558_en": encontrado}
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/mlt_prod_np")
def debug_mlt_prod_np():
    """Muestra los últimos 5 NP en CABEZACOMPROBANTES de DB-MLT-Microbell.gdb y total de registros."""
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    try:
        c = conn('WIN1252', db=DB_MLT_PROD)
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM \"CABEZACOMPROBANTES\"")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM \"CABEZACOMPROBANTES\" WHERE TIPOCOMPROBANTE='NP'")
        total_np = cur.fetchone()[0]
        cur.execute("SELECT FIRST 5 TIPOCOMPROBANTE, NUMEROCOMPROBANTE, CODIGOCLIENTE, RAZONSOCIAL, FECHACOMPROBANTE FROM \"CABEZACOMPROBANTES\" WHERE TIPOCOMPROBANTE='NP' ORDER BY FECHACOMPROBANTE DESC")
        rows = cur.fetchall()
        c.close()
        return {
            "db": DB_MLT_PROD,
            "total_comprobantes": total,
            "total_NP": total_np,
            "ultimos_5_NP": [{"tipo": r[0], "numero": str(r[1]), "cliente": r[2], "razon": r[3], "fecha": str(r[4])} for r in rows]
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/mlt_prod_parametros")
def debug_mlt_prod_parametros():
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    try:
        c = conn('WIN1252', db=DB_MLT_PROD)
        cur = c.cursor()
        cur.execute("SELECT TIPODOCUMENTO, VALOR FROM \"PARAMETROS\" WHERE TRIM(TIPODOCUMENTO) IN ('NP','PP')")
        params = {str(r[0]).strip(): str(r[1]) for r in cur.fetchall()}
        # Max número en CABEZACOMPROBANTES
        cur.execute("SELECT MAX(NUMEROCOMPROBANTE) FROM \"CABEZACOMPROBANTES\" WHERE TIPOCOMPROBANTE='NP'")
        max_np = str(cur.fetchone()[0])
        c.close()
        return {"db": DB_MLT_PROD, "parametros_NP_PP": params, "max_NP_en_tabla": max_np}
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/comparar_l1_vs_sw_nativo")
def debug_comparar_l1_vs_sw_nativo(n_sw: str = Query(...), n_l1: str = Query(...)):
    """Compara un pedido SW nativo vs uno L1 nativo, ambos en CABEZAPEDIDOS de DB-Prueba."""
    try:
        c = conn('WIN1252', db=DATABASE)
        cur = c.cursor()
        def _get(numero):
            cur.execute('SELECT * FROM "CABEZAPEDIDOS" WHERE NUMEROCOMPROBANTE=? AND TIPOCOMPROBANTE=?', (numero, 'NP'))
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, [str(v) for v in row])) if row else None
        sw  = _get(n_sw)
        l1  = _get(n_l1)
        c.close()
        diffs = {}
        if sw and l1:
            for k in sw:
                if sw[k] != l1[k]:
                    diffs[k] = {"sw_nativo": sw[k], "l1_nativo": l1[k]}
        return {"diferencias": diffs, "sw": sw, "l1": l1}
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/buscar_pedido")
def debug_buscar_pedido(numero: str = Query(...)):
    """Busca un pedido en L1 (CABEZAPEDIDOS) y SW (CABEZACOMPROBANTES) y reporta en cuál está."""
    resultado = {"numero": numero, "l1": None, "sw": None}
    # L1
    try:
        c1 = conn('WIN1252', db=DATABASE)
        cur1 = c1.cursor()
        cur1.execute(
            'SELECT NUMEROCOMPROBANTE, CODIGOCLIENTE, RAZONSOCIAL, TOTAL, FECHACOMPROBANTE '
            'FROM "CABEZAPEDIDOS" WHERE NUMEROCOMPROBANTE = ? AND TIPOCOMPROBANTE = ?',
            (numero, 'NP'))
        r = cur1.fetchone()
        c1.close()
        resultado["l1"] = {"encontrado": r is not None, "db": DATABASE,
                           "fila": [str(x) for x in r] if r else None}
    except Exception as e:
        resultado["l1"] = {"error": str(e)}
    # SW
    try:
        c2 = conn('WIN1252', db=DATABASE_MLT)
        cur2 = c2.cursor()
        cur2.execute(
            'SELECT NUMEROCOMPROBANTE, CODIGOCLIENTE, RAZONSOCIAL, TOTAL, FECHACOMPROBANTE '
            'FROM "CABEZACOMPROBANTES" WHERE NUMEROCOMPROBANTE = ? AND TIPOCOMPROBANTE = ?',
            (numero, 'NP'))
        r2 = cur2.fetchone()
        c2.close()
        resultado["sw"] = {"encontrado": r2 is not None, "db": DATABASE_MLT,
                           "fila": [str(x) for x in r2] if r2 else None}
    except Exception as e:
        resultado["sw"] = {"error": str(e)}
    return resultado

@app.get("/debug/ultimos_pedidos")
def debug_ultimos_pedidos():
    """Últimos 5 pedidos en DB-Prueba.gdb"""
    try:
        c = conn()
        cur = c.cursor()
        cur.execute(
            'SELECT FIRST 5 TIPOCOMPROBANTE, NUMEROCOMPROBANTE, CODIGOCLIENTE, '
            'RAZONSOCIAL, FECHACOMPROBANTE, TOTAL, CODIGOUSUARIO '
            'FROM "CABEZAPEDIDOS" ORDER BY FECHAMODIFICACION DESC')
        rows = cur.fetchall()
        c.close()
        return [{"tipo": r[0], "numero": r[1], "cliente": r[2],
                 "razon": r[3], "fecha": str(r[4]), "total": r[5], "usuario": r[6]}
                for r in rows]
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/test_mlt")
def debug_test_mlt():
    """Verifica conectividad, tablas y datos de DB-MLT-Microbell.gdb."""
    DB_MLT_PROD = 'c:/flexxus/DB/DB-MLT-Microbell.gdb'
    result = {"db_path": DB_MLT_PROD}

    def _fresh(sql, params=None):
        c2 = firebirdsql.connect(host=HOST, port=PORT, database=DB_MLT_PROD,
                                  user=DB_USER, password=DB_PASS, charset='WIN1252')
        cur2 = c2.cursor()
        if params:
            cur2.execute(sql, params)
        else:
            cur2.execute(sql)
        rows = cur2.fetchall()
        c2.close()
        return rows

    try:
        result["tablas_count"] = len(_fresh(
            "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=0"))
        result["conexion"] = "OK"
    except Exception as e:
        result["conexion_error"] = str(e)
        return result

    for label, sql, params in [
        ("total",            'SELECT COUNT(*) FROM "CABEZACOMPROBANTES"', None),
        ("cta_1",            'SELECT COUNT(*) FROM "CABEZACOMPROBANTES" WHERE CUENTACORRIENTE=?', ('1',)),
        ("cta_0",            'SELECT COUNT(*) FROM "CABEZACOMPROBANTES" WHERE CUENTACORRIENTE=?', ('0',)),
        ("anulada_0",        'SELECT COUNT(*) FROM "CABEZACOMPROBANTES" WHERE ANULADA=?', ('0',)),
        ("anulada_1",        'SELECT COUNT(*) FROM "CABEZACOMPROBANTES" WHERE ANULADA=?', ('1',)),
        ("filtros_cta",      'SELECT COUNT(*) FROM "CABEZACOMPROBANTES" '
                             "WHERE CUENTACORRIENTE='1' AND ANULADA='0' "
                             "AND TIPOCOMPROBANTE NOT IN ('RE','RI','INA')", None),
    ]:
        try:
            result[f"count_{label}"] = _fresh(sql, params)[0][0]
        except Exception as e:
            result[f"count_{label}_error"] = str(e)

    # Tipos de comprobante
    try:
        rows = _fresh('SELECT TIPOCOMPROBANTE, COUNT(*) FROM "CABEZACOMPROBANTES" '
                      'GROUP BY TIPOCOMPROBANTE ORDER BY 2 DESC')
        result["dist_tipo"] = {str(r[0]).strip(): r[1] for r in rows}
    except Exception as e:
        result["dist_tipo_error"] = str(e)

    # Suma bruta de debe con filtros _query_cta
    try:
        rows = _fresh(
            'SELECT COUNT(*), SUM(TOTAL+IVA1+IVA2-PAGADO) FROM "CABEZACOMPROBANTES" '
            "WHERE CUENTACORRIENTE='1' AND ANULADA='0' "
            "AND TIPOCOMPROBANTE NOT IN ('RE','RI','INA') "
            "AND (TOTAL+IVA1+IVA2-PAGADO) > 0"
        )
        result["registros_con_debe"] = rows[0][0]
        result["suma_debe_bruta"] = float(rows[0][1] or 0)
    except Exception as e:
        result["suma_debe_error"] = str(e)

    # Muestra últimas 5 filas
    try:
        rows = _fresh("SELECT FIRST 5 CODIGOCLIENTE, TIPOCOMPROBANTE, TOTAL, PAGADO, FECHACOMPROBANTE "
                      'FROM "CABEZACOMPROBANTES" ORDER BY FECHACOMPROBANTE DESC')
        result["muestra"] = [
            {"cod": str(r[0]).strip(), "tipo": str(r[1]).strip(),
             "total": float(r[2] or 0), "pagado": float(r[3] or 0), "fecha": str(r[4])}
            for r in rows
        ]
    except Exception as e:
        result["muestra_error"] = str(e)

    return result

@app.get("/debug/fa_en_todas_las_bds/{numero}")
def debug_fa_en_todas_las_bds(numero: str):
    """
    Busca un comprobante por NUMEROCOMPROBANTE en TODAS las BDs disponibles.
    Muestra TOTAL, IVA1, IVA2, PAGADO, CODIGOCLIENTE para comparar qué BD usa Flexxus.
    Ejemplo: /debug/fa_en_todas_las_bds/4400014878
    """
    bds = [
        ('DB-Prueba',    'c:/flexxus/DB/DB-Prueba.gdb'),
        ('DB-Microbell', 'c:/flexxus/DB/DB-Microbell.gdb'),
        ('DB-MLT-Prueba',    'c:/flexxus/DB/DB-MLT-Prueba.gdb'),
        ('DB-MLT-Microbell', 'c:/flexxus/DB/DB-MLT-Microbell.gdb'),
    ]
    result = {}
    for nombre, path in bds:
        for charset in ['WIN1252', 'LATIN1']:
            try:
                c2 = firebirdsql.connect(host=HOST, port=PORT, database=path,
                                          user=DB_USER, password=DB_PASS, charset=charset)
                cur2 = c2.cursor()
                cur2.execute(
                    'SELECT TIPOCOMPROBANTE, NUMEROCOMPROBANTE, CODIGOCLIENTE, '
                    'TOTAL, IVA1, IVA2, PAGADO, COTIZACION, CODIGOMONEDA, CUENTACORRIENTE, ANULADA '
                    'FROM "CABEZACOMPROBANTES" WHERE NUMEROCOMPROBANTE = ?',
                    (numero,)
                )
                rows = cur2.fetchall()
                c2.close()
                if rows:
                    result[nombre] = [{
                        "tipo": str(r[0]).strip(), "num": str(r[1]).strip(),
                        "cliente": str(r[2]).strip(),
                        "total": float(r[3] or 0), "iva1": float(r[4] or 0),
                        "iva2": float(r[5] or 0), "pagado": float(r[6] or 0),
                        "neto": float(r[3] or 0)+float(r[4] or 0)+float(r[5] or 0),
                        "debe": float(r[3] or 0)+float(r[4] or 0)+float(r[5] or 0)-float(r[6] or 0),
                        "cotiz": float(r[7] or 1), "moneda": str(r[8] or '').strip(),
                        "cta_corriente": str(r[9] or '').strip(), "anulada": r[10],
                    } for r in rows]
                else:
                    result[nombre] = "no_encontrado"
                break
            except Exception as e:
                result[f"{nombre}_{charset}"] = str(e)
    return result

@app.get("/debug/cliente_en_bds/{codigo}")
def debug_cliente_en_bds(codigo: str):
    """Muestra registro CLIENTES y todos sus comprobantes en DB-Prueba y DB-MLT-Prueba."""
    result = {}
    for nombre, db_path in [('L1_Prueba', DATABASE), ('SW_MLT', DATABASE_MLT)]:
        try:
            c = conn('WIN1252', db=db_path)
            cur = c.cursor()
            # Registro en CLIENTES
            cur.execute(
                'SELECT CODIGOCLIENTE, CODIGOPARTICULAR, RAZONSOCIAL, CODIGOVENDEDOR, ACTIVO '
                'FROM "CLIENTES" WHERE CODIGOCLIENTE=? OR CODIGOPARTICULAR=?',
                (codigo, codigo)
            )
            cli = cur.fetchone()
            result[nombre] = {'cliente': dict(zip(
                ['cod', 'part', 'razon', 'vendedor', 'activo'], cli
            )) if cli else None}
            # Comprobantes bajo ese código
            codigos = []
            if cli:
                if cli[0] and str(cli[0]).strip(): codigos.append(str(cli[0]).strip())
                if cli[1] and str(cli[1]).strip(): codigos.append(str(cli[1]).strip())
            if not codigos: codigos = [codigo]
            ph = ','.join(['?']*len(codigos))
            cur.execute(
                f'SELECT TIPOCOMPROBANTE, NUMEROCOMPROBANTE, CODIGOCLIENTE, TOTAL, PAGADO, '
                f'CUENTACORRIENTE, ANULADA FROM "CABEZACOMPROBANTES" '
                f'WHERE CODIGOCLIENTE IN ({ph}) ORDER BY NUMEROCOMPROBANTE',
                tuple(codigos)
            )
            rows = cur.fetchall()
            c.close()
            result[nombre]['comprobantes_total'] = len(rows)
            result[nombre]['comprobantes'] = [{
                'tipo': str(r[0]).strip(), 'num': str(r[1]).strip(),
                'cliente': str(r[2]).strip(),
                'total': float(r[3] or 0), 'pagado': float(r[4] or 0),
                'debe': float(r[3] or 0) - float(r[4] or 0),
                'cta': str(r[5] or '').strip(), 'anulada': r[6]
            } for r in rows]
        except Exception as e:
            result[nombre] = {'error': str(e)}
    return result

@app.get("/debug/tablas_usuarios")
def debug_tablas_usuarios():
    """Lista tablas que podrían contener usuarios/operadores y muestra sus columnas."""
    candidatas = ['USUARIOS', 'OPERADORES', 'VENDEDORES', 'USERS', 'EMPLEADOS',
                  'OPERADOR', 'USUARIO', 'AGENTES', 'PERSONAL']
    result = {}
    try:
        c = conn('WIN1252')
        cur = c.cursor()
        # Todas las tablas del sistema
        cur.execute("""
            SELECT RDB$RELATION_NAME FROM RDB$RELATIONS
            WHERE RDB$SYSTEM_FLAG = 0 AND RDB$VIEW_BLR IS NULL
            ORDER BY RDB$RELATION_NAME
        """)
        todas = [r[0].strip() for r in cur.fetchall()]
        result['todas_las_tablas'] = todas
        # Buscar candidatas
        for tabla in candidatas:
            if tabla in todas:
                try:
                    cur.execute(f'SELECT FIRST 3 * FROM "{tabla}"')
                    cols = [d[0] for d in cur.description]
                    rows = cur.fetchall()
                    result[tabla] = {
                        'columnas': cols,
                        'muestra': [dict(zip(cols, [str(v) for v in r])) for r in rows]
                    }
                except Exception as e:
                    result[tabla] = {'error': str(e)}
        c.close()
    except Exception as e:
        result['error'] = str(e)
    return result

@app.get("/debug/cotizacion_fa_akrafft")
def debug_cotizacion_fa_akrafft():
    """
    Muestra distribución de COTIZACION en FA con deuda para AKRAFFT.
    Si COTIZACION != 1 en algunos FA de pesos, _query_cta da distinto al SQL crudo.
    """
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'

    def _fresh(sql, params=None):
        c2 = firebirdsql.connect(host=HOST, port=PORT, database=DB_PROD,
                                  user=DB_USER, password=DB_PASS, charset='WIN1252')
        cur2 = c2.cursor()
        if params: cur2.execute(sql, params)
        else: cur2.execute(sql)
        rows = cur2.fetchall()
        c2.close()
        return rows

    try:
        rows = _fresh('SELECT CODIGOCLIENTE FROM "CLIENTES" WHERE ACTIVO=? AND UPPER(CODIGOVENDEDOR)=?',
                      ('1', 'AKRAFFT'))
        codigos = [str(r[0]).strip() for r in rows if (r[0] or '').strip()]
    except Exception as e:
        return {"error": str(e)}

    ph = ', '.join(['?'] * len(codigos))

    # Distribución de COTIZACION en registros con debe>0
    try:
        rows = _fresh(
            f'SELECT COTIZACION, CODIGOMONEDA, COUNT(*), SUM(TOTAL+IVA1+IVA2-PAGADO) '
            f'FROM "CABEZACOMPROBANTES" WHERE CODIGOCLIENTE IN ({ph}) '
            f"AND CUENTACORRIENTE='1' AND ANULADA='0' "
            f"AND TIPOCOMPROBANTE NOT IN ('RE','RI','INA') "
            f"AND (TOTAL+IVA1+IVA2-PAGADO) > 0 "
            f'GROUP BY COTIZACION, CODIGOMONEDA ORDER BY 4 DESC',
            tuple(codigos)
        )
        dist = [{"cotiz": float(r[0] or 1), "moneda": str(r[1]).strip(),
                 "count": r[2], "suma_bruta": float(r[3] or 0)} for r in rows]
    except Exception as e:
        return {"error_dist": str(e)}

    # Calcular lo que daría _query_cta para cada grupo (cambio PESOS=1, DOLARES=1475)
    cambios = {"PESOS": 1.0, "DOLARES": 1475.0}
    total_app = 0.0
    for d in dist:
        cambio = cambios.get(d["moneda"], 1.0)
        cotiz  = d["cotiz"] or 1.0
        # Cada FA tiene un COTIZACION diferente, pero aproximamos con la suma del grupo
        deuda_convertida = d["suma_bruta"] * cambio / cotiz
        d["deuda_convertida_aprox"] = round(deuda_convertida, 2)
        total_app += deuda_convertida

    return {
        "total_sql_bruto": round(sum(d["suma_bruta"] for d in dist), 2),
        "total_app_aprox": round(total_app, 2),
        "diferencia": round(sum(d["suma_bruta"] for d in dist) - total_app, 2),
        "dist_cotizacion": dist
    }

@app.get("/debug/gap2_akrafft")
def debug_gap2_akrafft():
    """
    Investigación de gap Parte 2:
    - Compara suma con/sin filtro CUENTACORRIENTE
    - Muestra valores reales de MONEDAS
    - Muestra distribución CUENTACORRIENTE y ANULADA para registros AKRAFFT
    """
    DB_PROD = 'c:/flexxus/DB/DB-Microbell.gdb'

    def _fresh(sql, params=None):
        c2 = firebirdsql.connect(host=HOST, port=PORT, database=DB_PROD,
                                  user=DB_USER, password=DB_PASS, charset='WIN1252')
        cur2 = c2.cursor()
        if params:
            cur2.execute(sql, params)
        else:
            cur2.execute(sql)
        rows = cur2.fetchall()
        c2.close()
        return rows

    result = {}

    # Códigos AKRAFFT
    try:
        rows = _fresh('SELECT CODIGOCLIENTE FROM "CLIENTES" WHERE ACTIVO=? AND UPPER(CODIGOVENDEDOR)=?',
                      ('1', 'AKRAFFT'))
        codigos = [str(r[0]).strip() for r in rows if (r[0] or '').strip()]
    except Exception as e:
        return {"error_clientes": str(e)}

    ph = ', '.join(['?'] * len(codigos))

    # 1. Distribución CUENTACORRIENTE para registros AKRAFFT
    try:
        rows = _fresh(
            f'SELECT CUENTACORRIENTE, COUNT(*), SUM(TOTAL+IVA1+IVA2-PAGADO) '
            f'FROM "CABEZACOMPROBANTES" WHERE CODIGOCLIENTE IN ({ph}) '
            f"AND TIPOCOMPROBANTE NOT IN ('RE','RI','INA') AND ANULADA='0' "
            f'GROUP BY CUENTACORRIENTE',
            tuple(codigos)
        )
        result["dist_cuentacorriente"] = [
            {"cta": str(r[0]).strip() if r[0] is not None else "NULL",
             "count": r[1], "suma_debe": float(r[2] or 0)}
            for r in rows
        ]
    except Exception as e:
        result["error_dist_cta"] = str(e)

    # 2. Suma SIN filtro de CUENTACORRIENTE (todo tipo no RE/RI/INA, no anulado, debe>0)
    try:
        rows = _fresh(
            f'SELECT COUNT(*), SUM(TOTAL+IVA1+IVA2-PAGADO) FROM "CABEZACOMPROBANTES" '
            f"WHERE CODIGOCLIENTE IN ({ph}) AND ANULADA='0' "
            f"AND TIPOCOMPROBANTE NOT IN ('RE','RI','INA') "
            f"AND (TOTAL+IVA1+IVA2-PAGADO) > 0",
            tuple(codigos)
        )
        result["sin_filtro_cta_count"] = rows[0][0]
        result["sin_filtro_cta_suma"] = float(rows[0][1] or 0)
    except Exception as e:
        result["error_sin_cta"] = str(e)

    # 3. Suma SIN filtro de ANULADA (solo CUENTACORRIENTE='1', debe>0)
    # 3. Suma bruta de clientes activos AKRAFFT (sin conversión de moneda)
    try:
        rows = _fresh('SELECT CODIGOCLIENTE FROM "CLIENTES" WHERE ACTIVO=? AND UPPER(CODIGOVENDEDOR)=?',
                      ('1', 'AKRAFFT'))
        codigos_activos = [str(r[0]).strip() for r in rows if (r[0] or '').strip()]
        ph = ', '.join(['?'] * len(codigos_activos))
        rows = _fresh(
            f'SELECT COUNT(*), SUM(TOTAL+IVA1+IVA2-PAGADO) FROM "CABEZACOMPROBANTES" '
            f"WHERE CODIGOCLIENTE IN ({ph}) AND CUENTACORRIENTE='1' AND ANULADA='0' "
            f"AND TIPOCOMPROBANTE NOT IN ('RE','RI','INA') AND (TOTAL+IVA1+IVA2-PAGADO)>0",
            tuple(codigos_activos)
        )
        result["activos_count_con_debe"] = rows[0][0]
        result["activos_suma_debe_bruta_sin_conversion"] = float(rows[0][1] or 0)
    except Exception as e:
        result["error_activos_debe"] = str(e)

    return result
