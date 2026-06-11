"""
Debug: busca pedido NP en ambas bases Firebird y muestra flags.
Uso: python debug_pedido.py 100023592
"""
import sys, os, subprocess

NUMERO = sys.argv[1] if len(sys.argv) > 1 else '100023592'

# Leer variables de entorno (las mismas que usa la API)
DB_L1   = os.getenv('DB_L1',  'c:/flexxus/DB/DB-Prueba.gdb')
DB_MLT  = os.getenv('DB_MLT', 'c:/flexxus/DB/DB-MLT-Prueba.gdb')
FB_HOST = os.getenv('FB_HOST', '127.0.0.1')
FB_PORT = int(os.getenv('FB_PORT', '3050'))
FB_USER = os.getenv('FB_USER', 'SYSDBA')
FB_PASS = os.getenv('FB_PASS', '')

print(f'DB_L1  = {DB_L1}')
print(f'DB_MLT = {DB_MLT}')
print(f'User   = {FB_USER}  Pass = "{FB_PASS}"')
print()

import firebirdsql

def buscar(db, tabla, numero):
    try:
        c = firebirdsql.connect(
            host=FB_HOST, port=FB_PORT, database=db,
            user=FB_USER, password=FB_PASS, charset='WIN1252'
        )
        cur = c.cursor()
        col_usuario = 'CODIGOUSUARIO2' if tabla == 'CABEZACOMPROBANTES' else 'CODIGOUSUARIO'
        cur.execute(
            f'SELECT NUMEROCOMPROBANTE, CODIGOCLIENTE, RAZONSOCIAL, TOTAL, {col_usuario} '
            f'FROM "{tabla}" WHERE NUMEROCOMPROBANTE=? AND TIPOCOMPROBANTE=?',
            (numero, 'NP')
        )
        row = cur.fetchone()
        c.close()
        return row
    except Exception as e:
        return f'ERROR: {e}'

print(f'=== Pedido NP {NUMERO} ===')
r1 = buscar(DB_L1,  'CABEZAPEDIDOS',      NUMERO)
r2 = buscar(DB_MLT, 'CABEZACOMPROBANTES', NUMERO)

print(f'L1  (DB-Prueba.gdb)    CABEZAPEDIDOS      -> {r1}')
print(f'SW  (DB-MLT-Prueba.gdb) CABEZACOMPROBANTES -> {r2}')
print()

# Si ambos dan error de auth, intentar leer credenciales del .env del servidor
if 'ERROR' in str(r1) and 'password' in str(r1).lower():
    print('Error de credenciales. Buscando en el entorno del proceso uvicorn...')
    # Intentar leer del archivo .env si existe
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        print(f'Archivo .env encontrado: {env_file}')
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith('FB_') or line.startswith('DB_'):
                    print(f'  {line}')
    else:
        print('No hay archivo .env. Las credenciales se pasan por variables de entorno al proceso uvicorn.')
        print('Intentá correr este script desde el mismo entorno que uvicorn:')
        print('  cd C:\\api_vendedores && python debug_pedido.py ' + NUMERO)

print()
# Flags admin.db
try:
    import sqlite3
    admin_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'admin.db')
    conn = sqlite3.connect(admin_db)
    rows = conn.execute(
        "SELECT codigousuario, feature, enabled FROM feature_flags "
        "WHERE codigousuario IN ('ECOMMERCE','RBOCHOR','BOCHOR') AND feature IN ('pedidos','pedidos_sw')"
    ).fetchall()
    conn.close()
    print('Flags relevantes en admin.db:')
    for row in rows:
        print(f'  usuario={row[0]}  feature={row[1]}  enabled={row[2]}')
except Exception as e:
    print(f'admin.db error: {e}')
