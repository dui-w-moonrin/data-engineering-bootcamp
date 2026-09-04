import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DAGS_FOLDER = Path("/opt/airflow/dags")
PYSPARK_FOLDER = Path("/opt/spark/pyspark")
AIRFLOW_CONFIG_FOLDER = Path("/opt/airflow/config")

GCS_KEYFILE = PYSPARK_FOLDER / "deb-load-data-to-gcs.json"
BIGQUERY_KEYFILE = AIRFLOW_CONFIG_FOLDER / "deb-loading-data-to-bigquery-secured.json"
TRANSFORMER_FILE = PYSPARK_FOLDER / "transformers.py"

sys.path.insert(0, str(PYSPARK_FOLDER))

from schema_config import (
    PROJECT_ID,
    LOCATION,
    BUCKET_NAME,
    BUSINESS_DOMAIN,
    BIGQUERY_DATASET,
    TABLE_CONFIG,
)


DATA = "order_items"
TABLE = TABLE_CONFIG[DATA]
API_URL = f"http://34.87.139.82:8000/{DATA}/"


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
            writer.writerow({
                column: record.get(column)
                for column in columns
            })

    print(f"Extracted {len(records)} rows -> {output_file}")


def _load_data_to_gcs():
    with open(GCS_KEYFILE, "r", encoding="utf-8") as f:
        service_account_info = json.load(f)

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )

    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials,
    )

    bucket = storage_client.bucket(BUCKET_NAME)

    file_path = DAGS_FOLDER / f"{DATA}.csv"
    destination_blob_name = (
        f"raw/{BUSINESS_DOMAIN}/{TABLE['raw_path']}"
    )

    bucket.blob(destination_blob_name).upload_from_filename(
        str(file_path)
    )

    print(
        f"Uploaded -> "
        f"gs://{BUCKET_NAME}/{destination_blob_name}"
    )


def _load_data_from_gcs_to_bigquery():
    with open(BIGQUERY_KEYFILE, "r", encoding="utf-8") as f:
        service_account_info = json.load(f)

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )

    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials,
        location=LOCATION,
    )

    source_path = (
        f"gs://{BUCKET_NAME}/"
        f"cleaned/{BUSINESS_DOMAIN}/"
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

    job = bigquery_client.load_table_from_uri(
        source_path,
        table_id,
        job_config=job_config,
        location=LOCATION,
    )

    job.result()

    table = bigquery_client.get_table(table_id)

    print(
        f"Loaded {table.num_rows} rows "
        f"and {len(table.schema)} columns "
        f"to {table_id}"
    )


with DAG(
    dag_id="greenery_order_items_data_pipeline",
    start_date=datetime(2021, 2, 9, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    tags=["DEB", "Skooldio", "greenery", "order_items"],
) as dag:

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
    )

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

    (
        extract_data
        >> load_data_to_gcs
        >> transform_data
        >> load_data_from_gcs_to_bigquery
    )