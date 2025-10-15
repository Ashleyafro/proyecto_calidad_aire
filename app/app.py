import requests
import pandas as pd
from sqlalchemy import create_engine
import os
import argparse
from datetime import datetime
import matplotlib.pyplot as plt


def check_updates(data, engine):
    # Get the latest records for each station
    query = "SELECT fiwareid, MAX(fecha_carg) as latest_fecha FROM cleaned_df GROUP BY fiwareid ORDER BY latest_fecha desc"
    latest_times = pd.read_sql(query, engine)

    # Extract data
    df = pd.DataFrame(data["results"])
    df["fecha_carg"] = pd.to_datetime(df["fecha_carg"], format="%Y-%m-%dT%H:%M:%S%z")

    # Merge the API data with the latest timestamps
    new_data = pd.merge(df, latest_times, on="fiwareid", how="left", suffixes=("", "_latest"))

    # Filter out older records
    new_data = new_data[new_data["fecha_carg"] > new_data["latest_fecha"]]

def save_raw_csv(data, engine):
    # conseguir datos
    df = pd.DataFrame(data["results"])
    df["fecha_carg"] = pd.to_datetime(df["fecha_carg"], format="%Y-%m-%dT%H:%M:%S%z")
    cleaned_df = pd.DataFrame(df[["objectid", "nombre", "direccion", "tipozona", "no2", "pm10", "pm25", "tipoemisio", "fecha_carg", "calidad_am", "fiwareid"]])

    # Save in PostgreSQL
    cleaned_df.to_sql("cleaned_df", engine, if_exists="replace", index=False)
    print("Datos cargados a PostgreSQL")
    
    # Guardar el CSV
    os.makedirs("../data/raw", exist_ok=True)
    timestamp = pd.to_datetime("now").strftime("%Y-%m-%dT%H-%M-%SZ")
    cleaned_df["fecha_carg"] = cleaned_df["fecha_carg"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    csv_filepath = os.path.join("../data/raw", f"ultimo_{timestamp}.csv")

    try:
        cleaned_df.to_csv(csv_filepath, index=False)
        print(f"CSV guardado en: {csv_filepath}")
    except Exception as e:
        print(f"Error al guardar el archivo CSV: {e}")
        return

def parse_args():
    p = argparse.ArgumentParser(description="Reports")
    p.add_argument("--modo", choices=["actual", "historico"], required=True, help="Modo: 'actual' o 'historico'")
    p.add_argument("--since", type=str, help="Fecha de inicio (formato: YYYY-MM-DD)")
    p.add_argument("--estacion", type=str, help="ID de estacion")
    return p.parse_args()

def generate_actual_report():
    # Get latest csv
    csv_folder = "../data/raw/"
    file = os.listdir(csv_folder)[0]
    latest_csv = os.path.join(csv_folder, file)
    data = pd.read_csv(latest_csv)

    # Create summary tables
    summary = data.groupby("fiwareid")[["no2", "pm10", "pm25"]].agg("mean")
    summary["estacion"] = data.groupby("fiwareid")["nombre"].first()
    os.makedirs("../output/actual", exist_ok=True)
    print("Tabla de resumen guardada")

    # Generate graph
    summary["no2"].plot(kind="bar")
    plt.title("NO2 por estacion")
    plt.xlabel("Estacion")
    plt.ylabel("Nivel NO2") 

    summary["pm10"].plot(kind="bar")
    plt.title("pm10 por estacion")
    plt.xlabel("Estacion")
    plt.ylabel("Nivel pm10")

    summary["pm25"].plot(kind="bar")
    plt.title("pm25 por estacion")
    plt.xlabel("Estacion")
    plt.ylabel("Nivel pm25")

    # Export them
    output_dir = os.path.join(os.getcwd(), "output", "actual")
    os.makedirs(output_dir, exist_ok=True)

    summary.to_csv(os.path.join(output_dir, "tabla_actual.csv"))
    plt.savefig(os.path.join(output_dir, "no2_por_estacion.png"))
    plt.savefig(os.path.join(output_dir, "pm10_por_estacion.png"))
    plt.savefig(os.path.join(output_dir, "pm25_por_estacion.png"))

    print("Graficos actuales guardados")

def generate_historico_report(since, engine):
    if since:
        query = f"SELECT * from cleaned_df WHERE fecha_carg >= {since} ORDER BY fecha_carg"
    else:
        query = f"SELECT * from cleaned_df ORDER BY fecha_carg"

    historial = pd.read_sql(query, engine)

    for fiwareid in historial["fiwareid"].unique():
        ## add smth
        plt.title("NO2 por estacion")
        plt.xlabel("Estacion")
        plt.ylabel("Nivel NO2") 
    output_dir = os.path.join(os.getcwd(), "output", "historico")
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, "NO2_historico.png"))

    print("Graficos historicos guardados")


def main():
    args = parse_args()

    # Get data from API
    url = "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas/records"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()  # dict with keys like 'results', 'total_count'
        print("Conexión exitosa a la API")
    except:
        print("Error en la conexión")
        return

    # Connect to PostgreSQL
    churro = 'postgresql://postgres:mysecretpassword@localhost:5432/postgres'
    engine = create_engine(churro)

    # Check for updates
    new_data = check_updates(data, engine)

    if new_data is not None:
        save_raw_csv(data, engine)

    if args.modo == "historico":
        generate_historico_report(args.since, engine)
    elif args.modo == "actual":
        generate_actual_report()

if __name__ == "__main__":
    main()