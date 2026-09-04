import csv
import json
import sys
from datetime import datetime, timezone

import requests
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import bigquery, storage
from google.oauth2 import service_account

sys.path.insert(0, "/opt/spark/pyspark")

from schema_config import (
    API_BASE_URL,
    BIGQUERY_DATASET,
    BIGQUERY_KEYFILE,
    BUCKET_NAME,
    BUSINESS_DOMAIN,
    DAGS_FOLDER,
    GCS_KEYFILE,
    LOCATION,
    PROJECT_ID,
    TABLE_CONFIG,
    TRANSFORMER_FILE,
)

DATA = "order_items"
TABLE = TABLE_CONFIG[DATA]
API_URL = f"{API_BASE_URL}/{TABLE['api_path']}/"


def _extract_data():
    response = requests.get(API_URL, timeout=60)
    response.raise_for_status()
    records = response.json()
    columns = [name for name, _ in TABLE["schema"]]
    output_file = DAGS_FOLDER / f"{DATA}.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for record in records:
            writer.writerow({column: record.get(column) for column in columns})

    print(f"Extracted {len(records)} rows -> {output_file}")


def _load_data_to_gcs():
    with open(GCS_KEYFILE, "r", encoding="utf-8") as f:
        credentials = service_account.Credentials.from_service_account_info(json.load(f))

    client = storage.Client(project=PROJECT_ID, credentials=credentials)
    file_path = DAGS_FOLDER / f"{DATA}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{TABLE['raw_path']}"
    client.bucket(BUCKET_NAME).blob(destination_blob_name).upload_from_filename(
        str(file_path)
    )


def _load_data_from_gcs_to_bigquery():
    with open(BIGQUERY_KEYFILE, "r", encoding="utf-8") as f:
        credentials = service_account.Credentials.from_service_account_info(json.load(f))

    client = bigquery.Client(project=PROJECT_ID, credentials=credentials, location=LOCATION)
    source_path = (
        f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/"
        f"{TABLE['cleaned_path']}*.parquet"
    )
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{DATA}"
    schema = [
        bigquery.SchemaField(column_name, data_type)
        for column_name, data_type in TABLE["schema"]
    ]
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )
    client.load_table_from_uri(
        source_path,
        table_id,
        job_config=job_config,
        location=LOCATION,
    ).result()


with DAG(
    dag_id="greenery_order_items_data_pipeline",
    start_date=datetime(2021, 2, 9, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    tags=["DEB", "Skooldio", "greenery", "order_items"],
) as dag:
    extract_data = PythonOperator(task_id="extract_data", python_callable=_extract_data)
    load_data_to_gcs = PythonOperator(task_id="load_data_to_gcs", python_callable=_load_data_to_gcs)
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application=str(TRANSFORMER_FILE),
        application_args=["--table", DATA],
        conn_id="my_spark",
        name="greenery_order_items_transform",
        verbose=True,
    )
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
    )

    extract_data >> load_data_to_gcs >> transform_data >> load_data_from_gcs_to_bigquery
