## EA2: Preprocesamiento y Limpieza (Simulación Cloud)
En esta etapa se procesaron los datos crudos de la base de datos `ingestion.db`:
- **Carga de Datos:** Se simuló un entorno cloud cargando el dataset mediante Pandas.
- **Limpieza:** - Se eliminaron registros duplicados.
  - Se imputaron valores nulos en campos de texto.
  - Se normalizaron los títulos (conversión a MAYÚSCULAS).
- **Auditoría:** Se generó el archivo `cleaning_report.txt` con las estadísticas antes/después.
- **Automatización:** El pipeline ejecuta secuencialmente `ingestion.py` y `cleaning.py`.