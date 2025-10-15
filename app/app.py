import requests
import pandas as pd
from sqlalchemy import create_engine
import os
import argparse
from datetime import datetime

def check_updates(data, engine):
    # Get the latest records for each station
    query = "SELECT fiwareid, MAX(fecha_carg) as latest_fecha FROM cleaned_df GROUP BY fiwareid"
    latest_times = pd.read_sql(query, engine)

    # Extract data
    df = pd.DataFrame(data["results"])
    df["fecha_carg"] = pd.to_datetime(df["fecha_carg"], format="%Y-%m-%dT%H:%M:%S%z")

    # Merge the API data with the latest timestamps
    new_data = pd.merge(df, latest_times, on="fiwareid", how="left", suffixes=("", "_latest"))

    # Filter out older records
    new_data = new_data[new_data["fecha_carg"] > new_data["latest_fecha"]]

    if not new_data.empty:
        print(f"{len(new_data)} nuevos registros encontrados")
        return new_data
    else:
        print("No hay nuevos registros")
        return None

def save_raw_csv(data, engine):
    # Process the data
    df = pd.DataFrame(data["results"])
    df["fecha_carg"] = pd.to_datetime(df["fecha_carg"], format="%Y-%m-%dT%H:%M:%S%z")
    cleaned_df = pd.DataFrame(df[["objectid", "nombre", "direccion", "tipozona", "no2", "pm10", "pm25", "tipoemisio", "fecha_carg", "calidad_am", "fiwareid"]])

    # Save to PostgreSQL
    cleaned_df.to_sql("cleaned_df", engine, if_exists="replace", index=False)
    print("Datos cargados a PostgreSQL")

    # Save the CSV 
    os.makedirs("../data/raw", exist_ok=True)
    timestamp = pd.to_datetime("now").strftime("%Y-%m-%dT%H-%M-%SZ")
    cleaned_df["fecha_carg"] = cleaned_df["fecha_carg"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    cleaned_df.to_csv(f"../data/raw/ultimo_{timestamp}.csv", index=False)
    print("CSV guardado")

def parse_args():
    p = argparse.ArgumentParser(description="Reports")
    p.add_argument("--mode", choices=["actual", "historico"], required=True, help="Modo: 'actual' o 'historico'")
    p.add_argument("--since", type=str, help="Fecha de inicio (formato: YYYY-MM-DD)")
    p.add_argument("--estacion", type=str, help="ID de estacion")
    return p.parse_args()

def generate_actual_report():
    print("x")

def generate_historo_report():
    print("x")

def main():
    args = parse_args()
    print(args)

    # Get data from API
    url = "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas/records"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()  # dict with keys like 'results', 'total_count'
        print("Conexión exitosa a la API")
    except requests.exceptions.RequestException as e:
        print(f"Error en la conexión: {e}")
        return

    # Connect to PostgreSQL
    churro = 'postgresql://postgres:mysecretpassword@localhost:5432/postgres'
    engine = create_engine(churro)

    # Check for updates
    new_data = check_updates(data, engine)

    if new_data is not None:
        save_raw_csv(data, engine)

    if args.mode == "historico":
        pass
    elif args.mode == "actual":
        pass

if __name__ == "__main__":
    main()