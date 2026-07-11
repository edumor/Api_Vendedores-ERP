-- Esquema de admin.db (solo estructura de tablas, SIN datos)
-- No contiene informacion sensible del negocio (vendedores, ventas, tokens, etc.)

CREATE TABLE admin_audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT DEFAULT (datetime('now','localtime')),
        usuario TEXT NOT NULL,
        metodo TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        ip TEXT
    , accion TEXT, detalle TEXT, seccion TEXT);

CREATE TABLE catalog_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        catalog_id INTEGER NOT NULL,
        profile_id INTEGER NOT NULL,
        FOREIGN KEY (catalog_id) REFERENCES catalogs(id) ON DELETE CASCADE,
        FOREIGN KEY (profile_id) REFERENCES vendor_profiles(id) ON DELETE CASCADE,
        UNIQUE(catalog_id, profile_id)
    );

CREATE TABLE catalogos (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre      TEXT NOT NULL,
        descripcion TEXT DEFAULT '',
        filename    TEXT NOT NULL,
        token       TEXT NOT NULL UNIQUE,
        subido_por  TEXT NOT NULL,
        fecha       TEXT DEFAULT (datetime('now','localtime')),
        activo      INTEGER DEFAULT 1
    , email_enviado INTEGER DEFAULT 0, wa_enviado INTEGER DEFAULT 0, perfiles_texto TEXT DEFAULT '', email_count INTEGER DEFAULT 0, wa_count INTEGER DEFAULT 0, push_enviado INTEGER DEFAULT 0, push_count INTEGER DEFAULT 0);

CREATE TABLE catalogs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        descripcion TEXT,
        url TEXT NOT NULL,
        activo INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now'))
    );

CREATE TABLE feature_flags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigousuario TEXT,
        feature TEXT NOT NULL,
        enabled INTEGER DEFAULT 1,
        UNIQUE(codigousuario, feature)
    );

CREATE TABLE multiplazos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        dias TEXT NOT NULL,
        activo INTEGER DEFAULT 1
    );

CREATE TABLE offer_conditions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        condicion_comercial TEXT NOT NULL,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE,
        UNIQUE(offer_id, condicion_comercial)
    );

CREATE TABLE offer_financial_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        porcentaje REAL NOT NULL,
        orden INTEGER DEFAULT 0,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE
    );

CREATE TABLE offer_product_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        codigo_producto TEXT,
        descuento_pct REAL DEFAULT 0,
        bonificacion_pct REAL DEFAULT 0, descripcion TEXT DEFAULT '', cantidad REAL DEFAULT 1,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE
    );

CREATE TABLE offer_vendors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        offer_id INTEGER NOT NULL,
        codigousuario TEXT,
        FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE,
        UNIQUE(offer_id, codigousuario)
    );

CREATE TABLE offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        tipo TEXT NOT NULL,
        descripcion TEXT,
        fecha_desde TEXT,
        fecha_hasta TEXT,
        activo INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now'))
    , deposito TEXT DEFAULT '', cupo INTEGER DEFAULT 0, usos INTEGER DEFAULT 0, tipo_financiero TEXT DEFAULT 'descuento_total', monto_minimo REAL DEFAULT 0, financial_escalones TEXT);

CREATE TABLE sqlite_sequence(name,seq);

CREATE TABLE stock_ajuste_backup (
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

CREATE TABLE stock_ajuste_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT DEFAULT (datetime('now','localtime')),
        usuario TEXT NOT NULL,
        deposito TEXT NOT NULL,
        filtro_desc TEXT,
        total_articulos INTEGER DEFAULT 0,
        con_pendientes INTEGER DEFAULT 0,
        estado TEXT DEFAULT 'ok',
        detalle TEXT
    , deposito_nombre TEXT);

CREATE TABLE vendedores_contacto (
        codigo      TEXT PRIMARY KEY,
        nombre      TEXT NOT NULL,
        mail        TEXT DEFAULT '',
        celular     TEXT DEFAULT '',
        apikey_wa   TEXT DEFAULT '',
        activo      INTEGER DEFAULT 1
    );

CREATE TABLE vendor_multiplazos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigousuario TEXT,
        multiplazo_id INTEGER NOT NULL,
        FOREIGN KEY (multiplazo_id) REFERENCES multiplazos(id) ON DELETE CASCADE,
        UNIQUE(codigousuario, multiplazo_id)
    );

CREATE TABLE vendor_multiplazos_fb (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigousuario TEXT NOT NULL,
        codigo_multiplazo TEXT NOT NULL,
        UNIQUE(codigousuario, codigo_multiplazo)
    );

CREATE TABLE vendor_profile_assignments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigousuario TEXT NOT NULL,
        profile_id INTEGER NOT NULL,
        FOREIGN KEY (profile_id) REFERENCES vendor_profiles(id) ON DELETE CASCADE,
        UNIQUE(codigousuario, profile_id)
    );

CREATE TABLE vendor_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT UNIQUE NOT NULL,
        nombre TEXT NOT NULL,
        activo INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now'))
    );

