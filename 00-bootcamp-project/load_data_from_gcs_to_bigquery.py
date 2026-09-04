# load_data_from_gcs_to_bigquery.py

import json
import sys
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account


# ============================================================
# Project Paths
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent
PYSPARK_FOLDER = PROJECT_ROOT / "pyspark"

# Allow this script to import schema_config.py from pyspark/
sys.path.insert(0, str(PYSPARK_FOLDER))


# ============================================================
# Shared Pipeline Configuration
# ============================================================

from schema_config import (
    PROJECT_ID,
    LOCATION,
    BUCKET_NAME,
    BUSINESS_DOMAIN,
    BIGQUERY_DATASET,
    TABLE_CONFIG,
)


# ============================================================
# BigQuery Credential
# ============================================================

KEYFILE_BIGQUERY = (
    PROJECT_ROOT / "deb-loading-data-to-bigquery-secured.json"
)


# ============================================================
# Helper Function:
# Convert Shared Schema -> BigQuery Schema
# ============================================================

def get_bigquery_schema(table_name):

    schema_config = TABLE_CONFIG[table_name]["schema"]

    return [
        bigquery.SchemaField(
            column_name,
            data_type,
        )
        for column_name, data_type in schema_config
    ]


# ============================================================
# Load BigQuery Credentials
# ============================================================

with open(KEYFILE_BIGQUERY, "r") as f:
    service_account_info_bigquery = json.load(f)


credentials_bigquery = (
    service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )
)


# ============================================================
# Create BigQuery Client
# ============================================================

bigquery_client = bigquery.Client(
    project=PROJECT_ID,
    credentials=credentials_bigquery,
    location=LOCATION,
)


# ============================================================
# Load All Tables
# GCS Cleaned Parquet -> BigQuery
# ============================================================

for table_name, table_config in TABLE_CONFIG.items():

    # --------------------------------------------------------
    # Build GCS Source Path
    # --------------------------------------------------------

    source_data_in_gcs = (
        f"gs://{BUCKET_NAME}/"
        f"cleaned/{BUSINESS_DOMAIN}/"
        f"{table_config['cleaned_path']}"
        f"*.parquet"
    )

    # --------------------------------------------------------
    # Build BigQuery Target Table
    # --------------------------------------------------------

    table_id = (
        f"{PROJECT_ID}."
        f"{BIGQUERY_DATASET}."
        f"{table_name}"
    )

    print()
    print("=" * 80)
    print(f"Loading table: {table_name}")
    print(f"Source : {source_data_in_gcs}")
    print(f"Target : {table_id}")
    print("=" * 80)

    # --------------------------------------------------------
    # Get Schema from Shared Configuration
    # --------------------------------------------------------

    bigquery_schema = get_bigquery_schema(table_name)

    print("Schema:")

    for field in bigquery_schema:
        print(
            f"  - {field.name}: {field.field_type}"
        )

    # --------------------------------------------------------
    # Configure BigQuery Load Job
    # --------------------------------------------------------

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=bigquery_schema,
    )

    # --------------------------------------------------------
    # Load Parquet from GCS -> BigQuery
    # --------------------------------------------------------

    load_job = bigquery_client.load_table_from_uri(
        source_data_in_gcs,
        table_id,
        job_config=job_config,
        location=LOCATION,
    )

    print()
    print("Waiting for BigQuery load job...")

    # Wait until job finishes
    load_job.result()

    # --------------------------------------------------------
    # Verify Loaded Table
    # --------------------------------------------------------

    table = bigquery_client.get_table(table_id)

    print()
    print(f"SUCCESS: {table_name}")
    print(f"Rows   : {table.num_rows}")
    print(f"Columns: {len(table.schema)}")
    print(f"Target : {table_id}")


# ============================================================
# Complete
# ============================================================

print()
print("=" * 80)
print("ALL TABLES LOADED TO BIGQUERY SUCCESSFULLY")
print("=" * 80)