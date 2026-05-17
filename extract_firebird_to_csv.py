import firebirdsql
import csv
import os
import zipfile

HOST = '190.111.231.86'
PORT = 3050
DATABASE = 'c:/flexxus/db/DB-Microbell.gdb'
USER = 'SYSDBA'
PASSWORD = '3122414422'
CHARSET = 'WIN1252'
OUTPUT_DIR = 'Microbell_CSV'
ZIP_FILE = 'Microbell_CSV.zip'

def new_conn():
    return firebirdsql.connect(host=HOST, port=PORT, database=DATABASE,
                               user=USER, password=PASSWORD, charset=CHARSET)

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

def export_table(table):
    con = new_conn()
    cur = con.cursor()
    try:
        cur.execute(f'SELECT * FROM "{table}"')
        cols = [desc[0] for desc in cur.description]
        col_types = [desc[1] for desc in cur.description]

        rows_out = []
        for row in cur.fetchall():
            clean = []
            for val, typ in zip(row, col_types):
                if val is None:
                    clean.append('')
                elif isinstance(val, (bytes, bytearray)):
                    clean.append('[BLOB]')
                else:
                    try:
                        clean.append(str(val))
                    except Exception:
                        clean.append('[ERR]')
            rows_out.append(clean)

        filepath = os.path.join(OUTPUT_DIR, f"{table}.csv")
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows_out)
        return len(rows_out), None
    except Exception as e:
        return 0, str(e)
    finally:
        try:
            con.close()
        except Exception:
            pass

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Obteniendo lista de tablas...")
tables = get_tables()
print(f"Tablas encontradas: {len(tables)}")

exported = []
errors = []

for i, table in enumerate(tables, 1):
    count, err = export_table(table)
    if err:
        print(f"  ERR [{i}/{len(tables)}] {table}: {err[:60]}")
        errors.append(table)
    else:
        print(f"  OK  [{i}/{len(tables)}] {table} ({count} filas)")
        exported.append(table)

print(f"\nCreando {ZIP_FILE}...")
with zipfile.ZipFile(ZIP_FILE, 'w', zipfile.ZIP_DEFLATED) as zf:
    for table in exported:
        filepath = os.path.join(OUTPUT_DIR, f"{table}.csv")
        zf.write(filepath, f"{table}.csv")

print(f"\nListo: {len(exported)} exportadas, {len(errors)} errores.")
if errors:
    print(f"Tablas con error: {', '.join(errors)}")
