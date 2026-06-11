"""
Normaliza los celulares en admin.db: quita el + del inicio.
Ejecutar en el servidor: python fix_celulares.py
"""
import sqlite3, os

db_path = os.path.join(os.path.dirname(__file__), 'admin.db')
c = sqlite3.connect(db_path)

rows = c.execute("SELECT codigo, celular FROM vendedores_contacto").fetchall()
print("Antes:")
for r in rows:
    print(f"  {r[0]}: {r[1]}")

c.execute("UPDATE vendedores_contacto SET celular = REPLACE(celular, '+', '') WHERE celular LIKE '+%'")
c.commit()

rows = c.execute("SELECT codigo, celular FROM vendedores_contacto").fetchall()
print("\nDespues:")
for r in rows:
    print(f"  {r[0]}: {r[1]}")

c.close()
print("\nListo.")
