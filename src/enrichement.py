import pandas as pd
import sqlite3
import os

def enrich_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Rutas de archivos
    db_path = os.path.join(base_dir, 'src', 'db', 'ingestion.db')
    json_path = os.path.join(base_dir, 'src', 'data_sources', 'usuarios.json')
    csv_path = os.path.join(base_dir, 'src', 'data_sources', 'categorias.csv')
    
    output_xlsx = os.path.join(base_dir, 'src', 'xlsx', 'enriched_data.xlsx')
    report_path = os.path.join(base_dir, 'src', 'static', 'auditoria', 'enriched_report.txt')

    print("--- Iniciando Enriquecimiento de Datos ---")

    # 1. Cargar Dataset Base (de la Actividad 2)
    conn = sqlite3.connect(db_path)
    df_base = pd.read_sql_query("SELECT * FROM posts", conn)
    conn.close()

    # 2. Cargar Fuentes Adicionales
    df_json = pd.read_json(json_path)
    df_csv = pd.read_csv(csv_path)

    # 3. Operaciones de Integración (Merging/Joins)
    # Cruzamos por 'userId' para traer el nombre del autor y país
    df_enriched = pd.merge(df_base, df_json, on='userId', how='left')
    
    # Cruzamos por 'id' para traer la categoría
    df_enriched = pd.merge(df_enriched, df_csv, on='id', how='left')

    # 4. Transformaciones Adicionales
    df_enriched['nombre_autor'] = df_enriched['nombre_autor'].fillna("Autor Anónimo")
    df_enriched['categoria'] = df_enriched['categoria'].fillna("General")

    # 5. Exportar Dataset Enriquecido
    df_enriched.to_excel(output_xlsx, index=False)
    print(f"Dataset enriquecido guardado en: {output_xlsx}")

    # 6. Generar Reporte de Auditoría
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("REPORTE DE ENRIQUECIMIENTO DE DATOS\n")
        f.write("="*40 + "\n")
        f.write(f"Registros en dataset base: {len(df_base)}\n")
        f.write(f"Registros en dataset enriquecido: {len(df_enriched)}\n")
        f.write("\nFUENTES INTEGRADAS:\n")
        f.write("- usuarios.json: Información de autores (nombre, país)\n")
        f.write("- categorias.csv: Clasificación por ID de post\n")
        f.write("\nOPERACIONES:\n")
        f.write(f"- Merge 'Left Join' sobre userId: {df_enriched['nombre_autor'].notnull().sum()} coincidencias.\n")
        f.write("- Imputación de valores para registros sin match (Autor Anónimo).\n")
        f.write("-" * 40 + "\n")
        f.write("Estado: INTEGRACIÓN EXITOSA PARA MODELADO FINAL\n")

if __name__ == "__main__":
    enrich_data()