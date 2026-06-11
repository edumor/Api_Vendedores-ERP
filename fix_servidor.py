"""
Script de fix para el servidor.
Ejecutar: C:\Python311\python.exe fix_servidor.py
"""
import re

path = r"C:\api_vendedores\main.py"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Eliminar cualquier bloque if __name__ existente (roto o no)
content = re.sub(r"\n+if __name__\s*==\s*['\"]__main__['\"].*", "", content, flags=re.DOTALL)
content = content.rstrip()

# Agregar el bloque correcto
block = '\n\nif __name__ == "__main__":\n    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)\n'
content += block

with open(path, "w", encoding="utf-8") as f:
    f.write(content)

# Verificar sintaxis
import ast
try:
    ast.parse(content)
    print("OK - sintaxis correcta. Ahora corre ./reiniciar")
except SyntaxError as e:
    print(f"ERROR de sintaxis en linea {e.lineno}: {e.msg}")
