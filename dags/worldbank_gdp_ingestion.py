import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

PROJECT_ID = "worldbank-513921"
BUCKET_NAME = f"{PROJECT_ID}-worldbank-raw"
DATASET = "raw_worldbank"
TABLE = "indicators"

COUNTRIES = ["MAR", "TUN", "DZA", "EGY", "FRA"]
INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
    "SP.POP.TOTL": "Population, total",
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of total labor force)",
    "SP.DYN.LE00.IN": "Life expectancy at birth, total (years)"
}

def extract_and_upload_to_gcs(**kwargs):
    ds = kwargs['ds']
    all_data = []
    
    for country in COUNTRIES:
        for indicator_code, indicator_label in INDICATORS.items():
            url = f"http://api.worldbank.org/v2/country/{country}/indicator/{indicator_code}?format=json&per_page=1000"
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            
            # Détection du piège de l'API (HTTP 200 avec message d'erreur)
            if not isinstance(data, list) or len(data) == 0:
                raise ValueError(f"Réponse API invalide pour {country}/{indicator_code}")
            
            if len(data) == 1 and "message" in data[0]:
                error_msg = data[0]["message"]
                raise ValueError(f"API World Bank a rejeté la requête pour {country}/{indicator_code}: {error_msg}")
            
            if len(data) >= 2 and isinstance(data[1], list):
                for entry in data[1]:
                    if entry.get("value") is not None:
                        all_data.append({
                            "country_code": country,
                            "country_name": entry["country"]["value"],
                            "indicator_code": indicator_code,
                            "indicator_label": indicator_label,
                            "year": int(entry["date"]),
                            "value": float(entry["value"]),
                            "load_date": ds
                        })
    
    if not all_data:
        raise ValueError("Aucune donnée extraite.")
    
    df = pd.DataFrame(all_data)
    local_csv_path = "/tmp/indicators.csv"
    df.to_csv(local_csv_path, index=False)
    
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"worldbank/{ds}/indicators.csv")
    blob.upload_from_filename(local_csv_path)
    
    print(f"Uploadé {len(df)} lignes vers gs://{BUCKET_NAME}/{blob.name}")

default_args = {
    'owner': 'data-eng-bootcamp',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="worldbank_gdp_ingestion",
    description="Pipeline d'ingestion World Bank : API -> GCS -> BigQuery",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["module6", "worldbank"],
) as dag:

    extract_and_upload = PythonOperator(
        task_id="extract_and_upload",
        python_callable=extract_and_upload_to_gcs,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["worldbank/{{ ds }}/indicators.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.{TABLE}",
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "country_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "indicator_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "indicator_label", "type": "STRING", "mode": "NULLABLE"},
            {"name": "year", "type": "INT64", "mode": "NULLABLE"},
            {"name": "value", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "load_date", "type": "DATE", "mode": "NULLABLE"},
        ],
        time_partitioning={"type": "DAY", "field": "load_date"},
        cluster_fields=["country_code"],
    )

    check_quality = BigQueryCheckOperator(
        task_id="check_quality",
        sql=f"""
            SELECT COUNT(DISTINCT country_code) 
            FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
            WHERE load_date = '{{{{ ds }}}}'
        """,
        use_legacy_sql=False,
    )

    extract_and_upload >> load_to_bigquery >> check_quality