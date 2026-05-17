import firebirdsql

print("Probando conexion a Firebird...")

for user in ['SYSDBA', 'ADMIN']:
    try:
        c = firebirdsql.connect(
            host='190.111.231.86',
            port=3050,
            database='c:/flexxus/db/DB-Microbell.gdb',
            user=user,
            password='3122414422',
            charset='WIN1252'
        )
        print(f'CONECTADO como {user} — PASSWORD CORRECTO: 3122414422')
        c.close()
    except Exception as e:
        print(f'Fallo {user}: {e}')
