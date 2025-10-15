import requests
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# get data
url = "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas/records"
try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()  # dict con keys como 'results', 'total_count'
except:
    print("Error en la conexion")

# df w/ important info
df = pd.DataFrame(data["results"])
cleaned_df = pd.DataFrame(df[["objectid", "nombre","direccion", "tipozona","no2", "pm10", "pm25", "tipoemisio", "fecha_carg", "calidad_am", "fiwareid"]])

# connect to postgresql
churro = 'postgresql://postgres:mysecretpassword@localhost:5432/postgres'
engine = create_engine(churro)
cleaned_df.to_sql("cleaned_df", engine, if_exists="replace", index=False)

# ultimo valor por estacion
query = "SELECT fiwareid FROM cleaned_df ORDER BY fecha_carg desc"
pd.read_sql(query, engine)