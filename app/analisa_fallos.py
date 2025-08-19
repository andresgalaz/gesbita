import json
import pymysql
import os
import sys
import re

# Configura tu conexión a MySQL
print(
    {
        "host": os.environ["DB_HOST"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "database": os.environ["DB_NAME"],
    }
)

if not os.getenv("DB_HOST"):
    raise ValueError("DB_HOST is not set in the environment variables.")


errores_acumulados = {}

lnNumber = 0
erNumber = 0
for linea in sys.stdin:
    lnNumber = lnNumber + 1
    if lnNumber % 1000 == 0:
        print(f"Procesnado línea {lnNumber} {erNumber}")

    if '{"success": false' in linea:
        simple = "'"
        doble = '"'
        linea = re.sub(
            r'("message": \\"")(.*?)("\\")',
            lambda m: f"\"message\": '{m.group(2).replace(doble, simple)}'",
            linea,
        )
        if '"message": \\""' in linea:
            linea = linea.replace('"message": \\""', '"message": "')
            linea = linea.replace('"\\"', '"')

        # Separa la fecha y el JSON
        try:
            fecha_str, json_str = linea.strip().split(" ", 1)
            # Si la fecha incluye hora, ajusta el split
            # Ejemplo: '2024-06-20 12:34:56'
            if len(fecha_str) >= 10:  # solo fecha
                fecha_str = fecha_str[:10]

            # Carga el JSON
            data = json.loads(json_str)

            # Acumula los errores en un diccionario
            clave = (fecha_str, data.get("message"), data.get("errno"))
            if clave in errores_acumulados:
                errores_acumulados[clave] += 1
            else:
                errores_acumulados[clave] = 1

            erNumber = erNumber + 1
            if erNumber % 1000 == 0:
                print(f"Procesnado línea {lnNumber} {erNumber}")

        except Exception as e:
            print(f"Error procesando línea: {linea.strip()}\n{e}")
            raise e

conn = pymysql.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
)
cursor = conn.cursor()

# Crea la tabla si no existe
cursor.execute("""
CREATE TABLE IF NOT EXISTS log_fallos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fecha DATETIME,
    mensaje VARCHAR(255),
    errno VARCHAR(255),
    cantidad INT DEFAULT 0
)
""")

# Inserta los errores acumulados en la base de datos
for clave, cantidad in errores_acumulados.items():
    cursor.execute(
        """
        INSERT INTO log_fallos (fecha, mensaje, errno, cantidad)
        VALUES (%s, %s, %s, %s)
        """,
        (clave[0], clave[1], clave[2], cantidad),
    )
# Inserta en la base de datos
conn.commit()
cursor.close()
conn.close()
