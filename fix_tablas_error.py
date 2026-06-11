import firebirdsql
import csv
import os
import zipfile

HOST = '190.111.231.86'
PORT = 3050
DATABASE = 'c:/flexxus/db/DB-Microbell.gdb'
USER = 'SYSDBA'
PASSWORD = '3122414422'
OUTPUT_DIR = 'Microbell_CSV'
ZIP_FILE = 'Microbell_CSV.zip'

TABLAS = ['CABEZACOMPROBANTES', 'CABEZAPEDIDOS', 'CUERPOCOMPRAS',
          'CUERPOCOTIZACION', 'CUERPOORDENESCOMPRAS']

def export_table_safe(table):
    con = firebirdsql.connect(host=HOST, port=PORT, database=DATABASE,
                              user=USER, password=PASSWORD, charset='LATIN1')
    cur = con.cursor()
    try:
        cur.execute(f'SELECT * FROM "{table}"')
        cols = [desc[0] for desc in cur.description]
        filepath = os.path.join(OUTPUT_DIR, f"{table}.csv")
        count = 0
        skipped = 0
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            for row in cur:
                try:
                    clean = []
                    for val in row:
                        if val is None:
                            clean.append('')
                        elif isinstance(val, (bytes, bytearray)):
                            clean.append('[BLOB]')
                        else:
                            clean.append(str(val).encode('utf-8', errors='replace').decode('utf-8'))
                    writer.writerow(clean)
                    count += 1
                except Exception:
                    skipped += 1
        return count, skipped
    finally:
        try:
            con.close()
        except Exception:
            pass

exported_fix = []
for table in TABLAS:
    count, skipped = export_table_safe(table)
    print(f"  OK  {table} ({count} filas, {skipped} filas omitidas)")
    exported_fix.append(table)

# Actualizar ZIP agregando las tablas reparadas
print(f"\nActualizando {ZIP_FILE}...")
with zipfile.ZipFile(ZIP_FILE, 'a', zipfile.ZIP_DEFLATED) as zf:
    for table in exported_fix:
        filepath = os.path.join(OUTPUT_DIR, f"{table}.csv")
        zf.write(filepath, f"{table}.csv")

print("Listo.")
