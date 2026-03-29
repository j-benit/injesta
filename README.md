## EA2: Preprocesamiento y Limpieza (Simulación Cloud)
En esta etapa se procesaron los datos crudos de la base de datos `ingestion.db`:
- **Carga de Datos:** Se simuló un entorno cloud cargando el dataset mediante Pandas.
- **Limpieza:** - Se eliminaron registros duplicados.
  - Se imputaron valores nulos en campos de texto.
  - Se normalizaron los títulos (conversión a MAYÚSCULAS).
- **Auditoría:** Se generó el archivo `cleaning_report.txt` con las estadísticas antes/después.
- **Automatización:** El pipeline ejecuta secuencialmente `ingestion.py` y `cleaning.py`.

- ## EA3: Enriquecimiento de Datos (Multi-fuente)
En esta fase se integraron diversas fuentes para aportar valor al dataset base:
- **Fuentes Adicionales:** Se incorporaron archivos en formato JSON (`usuarios.json`) y CSV (`categorias.csv`).
- **Lógica de Integración:**
  - Se realizó un *Left Join* con los datos de usuarios para añadir campos de `Autor` y `País`.
  - Se integró una clasificación por `Categoría` basada en el ID del post.
- **Auditoría de Enriquecimiento:** El archivo `enriched_report.txt` detalla la cantidad de registros cruzados exitosamente y las transformaciones realizadas.
- **Automatización:** El workflow de GitHub Actions ahora ejecuta el pipeline completo: Ingesta -> Limpieza -> Enriquecimiento.
