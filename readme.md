## Proyecto sobre calidad del aire en Valencia

![Raimundo][imagen/raimundo.jpg]
 
Este proyecto automatiza la ingesta, limpieza y análisis de datos de calidad del aire obtenidos desde la API de OpenData Valencia
.
Genera informes actuales y históricos con gráficos y documentos Word (.docx).

## Funcionalidades
- Conexión automática con la API de estaciones de contaminación atmosférica de Valencia.
- Ingesta y limpieza de datos en PostgreSQL.
- Generación de archivos CSV históricos y actuales.
- Creación de reportes con gráficos de NO₂, PM₁₀ y PM₂.₅.
- Exportación de informes en Word (.docx) con imágenes embebidas.

## Requisitos-
1. Instalación de dependencias

Ejecuta:

pip install -r requirements.txt


Contenido sugerido del archivo requirements.txt:

pandas
requests
sqlalchemy
psycopg2-binary
matplotlib
python-docx
argparse

2️. Configuración de PostgreSQL

Asegúrate de tener una base de datos PostgreSQL activa y accesible.
Por defecto, el script usa la conexión:

postgresql://postgres:mysecretpassword@localhost:5432/postgres


Puedes modificar esta línea en el script:

churro = 'postgresql://postgres:mysecretpassword@localhost:5432/postgres'

🗂 Estructura del Proyecto
project/
├── app.py
├── data/
│   └── raw/
│       └── ultimo_YYYY-MM-DDTHH-MM-SS.csv
├── output/
│   ├── actual/
│   │   ├── no2_por_estacion.png
│   │   ├── pm10_por_estacion.png
│   │   ├── pm25_por_estacion.png
│   │   ├── tabla_actual.csv
│   │   └── Reporte_Actual.docx
│   └── historico/
│       ├── registro_historico.csv
│       └── NO2_historico.png
└── requirements.txt

🚀 Uso
📥 Ingesta de datos + Informe Actual
python app/app.py --modo actual


Genera:

CSV con los últimos datos (data/raw/ultimo_*.csv)

Gráficos de NO₂, PM₁₀ y PM₂.₅ por estación

Informe Word (output/actual/Reporte_Actual.docx)

⏳ Ingesta de datos + Informe Histórico
python app/app.py --modo historico


Opcionalmente, puedes filtrar por fecha o estación:

python app/app.py --modo historico --since "2025-10-01" --estacion "A05_POLITECNIC_60m"


Genera:

Filtro de datos según los parámetros

Gráfico histórico de NO₂ por estación (NO2_historico.png)

## Parámetros disponibles
Parámetro	Descripción	Ejemplo
--modo	Modo de ejecución (actual o historico)	--modo actual
--since	Fecha de inicio (formato YYYY-MM-DD)	--since "2025-10-01"
--estacion	ID de estación FIWARE	--estacion "A05_POLITECNIC_60m"
## Salidas esperadas
Reporte Actual:

Gráficos de promedio de contaminantes por estación.
Incluye:

NO2

PM10

PM25

Reporte Histórico:

Evolución temporal del NO₂ en cada estación seleccionada.
Notas

El script elimina el archivo registro_historico.csv y lo reemplaza tras cada ejecución.

Los gráficos se guardan automáticamente en las carpetas output/actual y output/historico.

Si no existen datos nuevos en la API, el script notificará "No hay nuevos records".

