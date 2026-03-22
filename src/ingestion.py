import requests
import sqlite3
import pandas as pd
import os

def ejecutar_ingesta():
    # 1. Configuración de rutas (relativas a la raíz del proyecto)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, 'src', 'db', 'ingestion.db')
    xlsx_path = os.path.join(base_dir, 'src', 'xlsx', 'ingestion.xlsx')
    audit_path = os.path.join(base_dir, 'src', 'static', 'auditoria', 'ingestion.txt')

    print("--- Iniciando proceso de ingesta ---")

    # 2. Lectura de datos desde el API
    api_url = "https://jsonplaceholder.typicode.com/posts"
    try:
        response = requests.get(api_url)
        response.raise_for_status() # Lanza error si la descarga falla
        datos_api = response.json()
        print(f"Éxito: Se extrajeron {len(datos_api)} registros del API.")
    except Exception as e:
        print(f"Error al conectar con el API: {e}")
        return

    # 3. Almacenamiento en SQLite
    try:
        conn = sqlite3.connect(db_path)
        df = pd.DataFrame(datos_api)
        # Guardamos en la tabla 'posts'
        df.to_sql('posts', conn, if_exists='replace', index=False)
        conn.close()
        print(f"Éxito: Datos guardados en SQLite ({db_path}).")
    except Exception as e:
        print(f"Error en base de datos: {e}")
        return

    # 4. Generación de Evidencias (Pandas -> Excel)
    try:
        # Tomamos una muestra de los primeros 10 registros
        muestra = df.head(10)
        muestra.to_excel(xlsx_path, index=False)
        print(f"Éxito: Muestra guardada en Excel ({xlsx_path}).")
    except Exception as e:
        print(f"Error al generar Excel: {e}")

    # 5. Archivo de Auditoría (.txt)
    try:
        # Volvemos a leer de la DB para comparar (Integridad)
        conn = sqlite3.connect(db_path)
        df_db = pd.read_sql_query("SELECT * FROM posts", conn)
        conn.close()

        registros_api = len(datos_api)
        registros_db = len(df_db)

        with open(audit_path, "w", encoding="utf-8") as f:
            f.write("REPORTE DE AUDITORÍA DE INGESTA\n")
            f.write("-" * 30 + "\n")
            f.write(f"Registros obtenidos del API: {registros_api}\n")
            f.write(f"Registros almacenados en DB: {registros_db}\n")
            if registros_api == registros_db:
                f.write("Resultado: Sincronización Exitosa.\n")
            else:
                f.write("Resultado: Discrepancia detectada.\n")
        
        print(f"Éxito: Reporte de auditoría generado ({audit_path}).")
    except Exception as e:
        print(f"Error en auditoría: {e}")

if __name__ == "__main__":
    ejecutar_ingesta()