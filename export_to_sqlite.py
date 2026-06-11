import firebirdsql
import sqlite3
import os

HOST = '190.111.231.86'
PORT = 3050
DATABASE = 'c:/flexxus/db/DB-Microbell.gdb'
USER = 'SYSDBA'
PASSWORD = '3122414422'
SQLITE_FILE = 'Microbell.db'

def new_conn(charset='WIN1252'):
    return firebirdsql.connect(host=HOST, port=PORT, database=DATABASE,
                               user=USER, password=PASSWORD, charset=charset)

def get_tables():
    con = new_conn()
    cur = con.cursor()
    cur.execute("""
        SELECT RDB$RELATION_NAME FROM RDB$RELATIONS
        WHERE RDB$SYSTEM_FLAG = 0 AND RDB$VIEW_BLR IS NULL
        ORDER BY RDB$RELATION_NAME
    """)
    tables = [row[0].strip() for row in cur.fetchall()]
    con.close()
    return tables

# Tablas con caracteres raros — conectar con LATIN1
LATIN1_TABLES = {'CABEZACOMPROBANTES', 'CABEZAPEDIDOS', 'CUERPOCOMPRAS',
                 'CUERPOCOTIZACION', 'CUERPOORDENESCOMPRAS'}

if os.path.exists(SQLITE_FILE):
    os.remove(SQLITE_FILE)

sqlite_con = sqlite3.connect(SQLITE_FILE)
sqlite_con.execute("PRAGMA journal_mode=WAL")
sqlite_con.execute("PRAGMA synchronous=NORMAL")

tables = get_tables()
print(f"Tablas encontradas: {len(tables)}")

exported = []
errors = []

for i, table in enumerate(tables, 1):
    charset = 'LATIN1' if table in LATIN1_TABLES else 'WIN1252'
    fb_con = new_conn(charset)
    fb_cur = fb_con.cursor()
    try:
        fb_cur.execute(f'SELECT * FROM "{table}"')
        cols = [desc[0] for desc in fb_cur.description]

        # Crear tabla en SQLite
        col_defs = ', '.join(f'"{c}" TEXT' for c in cols)
        sqlite_con.execute(f'DROP TABLE IF EXISTS "{table}"')
        sqlite_con.execute(f'CREATE TABLE "{table}" ({col_defs})')

        # Insertar filas
        placeholders = ', '.join('?' for _ in cols)
        rows_out = []
        skipped = 0
        for row in fb_cur:
            try:
                clean = []
                for val in row:
                    if val is None:
                        clean.append(None)
                    elif isinstance(val, (bytes, bytearray)):
                        clean.append('[BLOB]')
                    else:
                        clean.append(str(val))
                rows_out.append(clean)
            except Exception:
                skipped += 1

        sqlite_con.executemany(f'INSERT INTO "{table}" VALUES ({placeholders})', rows_out)
        sqlite_con.commit()
        msg = f"({len(rows_out)} filas" + (f", {skipped} omitidas" if skipped else "") + ")"
        print(f"  OK  [{i}/{len(tables)}] {table} {msg}")
        exported.append(table)
    except Exception as e:
        print(f"  ERR [{i}/{len(tables)}] {table}: {str(e)[:70]}")
        errors.append(table)
    finally:
        try:
            fb_con.close()
        except Exception:
            pass

sqlite_con.close()

size_mb = os.path.getsize(SQLITE_FILE) / 1024 / 1024
print(f"\nListo: {len(exported)} tablas exportadas, {len(errors)} errores.")
print(f"Archivo: {SQLITE_FILE} ({size_mb:.1f} MB)")
if errors:
    print(f"Errores: {', '.join(errors)}")
