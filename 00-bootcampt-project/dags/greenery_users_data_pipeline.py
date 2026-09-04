import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

from airflow import DAG
from airflow.exceptions import AirflowSkipException
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


DATA = "users"
TABLE = TABLE_CONFIG[DATA]
API_URL = f"http://34.87.139.82:8000/{DATA}/"

FIRST_DATE = "2020-01-05"


def _extract_data(ds):
    response = requests.get(
        API_URL,
        params={"created_at": ds},
        timeout=60,
    )
    response.raise_for_status()

    records = response.json()

    if not records:
        raise AirflowSkipException(
            f"No {DATA} data found for {ds}"
        )

    columns = [name for name, _ in TABLE["schema"]]

    output_file = DAGS_FOLDER / f"{DATA}-{ds}.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=columns,
        )

        writer.writeheader()

        for record in records:
            writer.writerow({
                column: record.get(column)
                for column in columns
            })

    print(
        f"Extracted {len(records)} rows "
        f"for {ds} -> {output_file}"
    )


def _load_data_to_gcs(ds):
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

    file_path = DAGS_FOLDER / f"{DATA}-{ds}.csv"

    destination_blob_name = (
        f"raw/{BUSINESS_DOMAIN}/"
        f"{DATA}/{ds}/{DATA}.csv"
    )

    bucket.blob(
        destination_blob_name
    ).upload_from_filename(
        str(file_path)
    )

    print(
        f"Uploaded -> "
        f"gs://{BUCKET_NAME}/{destination_blob_name}"
    )


def _load_data_from_gcs_to_bigquery(ds):
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
        f"{DATA}/{ds}/*.parquet"
    )

    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{DATA}"

    schema = [
        bigquery.SchemaField(column_name, data_type)
        for column_name, data_type in TABLE["schema"]
    ]

    if ds == FIRST_DATE:
        write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    else:
        write_disposition = (
            bigquery.WriteDisposition.WRITE_APPEND
        )

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
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
        f"Loaded partition {ds}. "
        f"Table now contains {table.num_rows} rows "
        f"and {len(table.schema)} columns "
        f"in {table_id}"
    )


with DAG(
    dag_id="greenery_users_data_pipeline",
    start_date=datetime(
        2020,
        1,
        5,
        tzinfo=timezone.utc,
    ),
    end_date=datetime(
        2020,
        12,
        26,
        tzinfo=timezone.utc,
    ),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["DEB", "Skooldio", "greenery", "users"],
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
        application_args=[
            "--table",
            DATA,
            "--date",
            "{{ ds }}",
        ],
        conn_id="my_spark",
        name="greenery_users_transform",
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