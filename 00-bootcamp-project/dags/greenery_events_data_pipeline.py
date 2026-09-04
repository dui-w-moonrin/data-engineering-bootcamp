import csv
import json
import sys
from datetime import datetime, timezone

import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
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

DATA = "events"
TABLE = TABLE_CONFIG[DATA]
API_URL = f"{API_BASE_URL}/{TABLE['api_path']}/"


def _extract_data(ds):
    response = requests.get(API_URL, params={"created_at": ds}, timeout=60)
    response.raise_for_status()
    records = response.json()
    if not records:
        raise AirflowSkipException(f"No {DATA} data found for {ds}")

    columns = [name for name, _ in TABLE["schema"]]
    output_file = DAGS_FOLDER / f"{DATA}-{ds}.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for record in records:
            writer.writerow({column: record.get(column) for column in columns})


def _load_data_to_gcs(ds):
    with open(GCS_KEYFILE, "r", encoding="utf-8") as f:
        credentials = service_account.Credentials.from_service_account_info(json.load(f))
    client = storage.Client(project=PROJECT_ID, credentials=credentials)
    destination = f"raw/{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    client.bucket(BUCKET_NAME).blob(destination).upload_from_filename(
        str(DAGS_FOLDER / f"{DATA}-{ds}.csv")
    )


def _load_data_from_gcs_to_bigquery(ds):
    with open(BIGQUERY_KEYFILE, "r", encoding="utf-8") as f:
        credentials = service_account.Credentials.from_service_account_info(json.load(f))
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials, location=LOCATION)
    source = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{DATA}/{ds}/*.parquet"
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{DATA}"
    schema = [bigquery.SchemaField(name, dtype) for name, dtype in TABLE["schema"]]
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )
    client.load_table_from_uri(source, table_id, job_config=config, location=LOCATION).result()


with DAG(
    dag_id="greenery_events_data_pipeline",
    start_date=datetime(2021, 2, 10, tzinfo=timezone.utc),
    end_date=datetime(2021, 2, 10, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["DEB", "Skooldio", "greenery", "events"],
) as dag:
    extract_data = PythonOperator(task_id="extract_data", python_callable=_extract_data)
    load_data_to_gcs = PythonOperator(task_id="load_data_to_gcs", python_callable=_load_data_to_gcs)
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application=str(TRANSFORMER_FILE),
        application_args=["--table", DATA, "--date", "{{ ds }}"],
        conn_id="my_spark",
        name="greenery_events_transform",
        verbose=True,
    )
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
    )
    extract_data >> load_data_to_gcs >> transform_data >> load_data_from_gcs_to_bigquery
