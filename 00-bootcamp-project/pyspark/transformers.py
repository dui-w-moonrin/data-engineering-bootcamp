import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)

from schema_config import (
    BUCKET_NAME,
    BUSINESS_DOMAIN,
    TABLE_CONFIG,
)


# ============================================================
# Configuration
# ============================================================

KEYFILE_PATH = "/opt/spark/pyspark/deb-load-data-to-gcs.json"

PARTITIONED_TABLES = {
    "events",
    "orders",
    "users",
}


# ============================================================
# Arguments
# ============================================================

parser = argparse.ArgumentParser()

parser.add_argument(
    "--table",
    required=True,
    choices=TABLE_CONFIG.keys(),
)

parser.add_argument(
    "--date",
    required=False,
)

args = parser.parse_args()

table_name = args.table
partition_date = args.date
table_config = TABLE_CONFIG[table_name]


# ============================================================
# Validate Arguments
# ============================================================

if table_name in PARTITIONED_TABLES and not partition_date:
    raise ValueError(
        f"--date is required for partitioned table: {table_name}"
    )


# ============================================================
# Spark Data Type Mapping
# ============================================================

SPARK_TYPE_MAP = {
    "STRING": StringType(),
    "INT64": LongType(),
    "FLOAT64": DoubleType(),
    "TIMESTAMP": TimestampType(),
}


# ============================================================
# Helper: Convert Shared Schema -> Spark StructType
# ============================================================

def get_spark_schema(table_name):

    schema_config = TABLE_CONFIG[table_name]["schema"]

    fields = [
        StructField(
            column_name,
            SPARK_TYPE_MAP[data_type],
            True,
        )
        for column_name, data_type in schema_config
    ]

    return StructType(fields)


# ============================================================
# Helper: Build Input / Output Paths
# ============================================================

def build_paths(
    table_name,
    table_config,
    partition_date=None,
):

    if table_name in PARTITIONED_TABLES:

        input_path = (
            f"gs://{BUCKET_NAME}/"
            f"raw/{BUSINESS_DOMAIN}/"
            f"{table_name}/{partition_date}/"
            f"{table_name}.csv"
        )

        output_path = (
            f"gs://{BUCKET_NAME}/"
            f"cleaned/{BUSINESS_DOMAIN}/"
            f"{table_name}/{partition_date}/"
        )

    else:

        input_path = (
            f"gs://{BUCKET_NAME}/"
            f"raw/{BUSINESS_DOMAIN}/"
            f"{table_config['raw_path']}"
        )

        output_path = (
            f"gs://{BUCKET_NAME}/"
            f"cleaned/{BUSINESS_DOMAIN}/"
            f"{table_config['cleaned_path']}"
        )

    return input_path, output_path


# ============================================================
# Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName(f"greenery-transform-{table_name}")
    .config(
        "spark.memory.offHeap.enabled",
        "true",
    )
    .config(
        "spark.memory.offHeap.size",
        "5g",
    )
    .config(
        "fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    .config(
        "google.cloud.auth.service.account.enable",
        "true",
    )
    .config(
        "google.cloud.auth.service.account.json.keyfile",
        KEYFILE_PATH,
    )
    .getOrCreate()
)


# ============================================================
# Build Paths
# ============================================================

input_path, output_path = build_paths(
    table_name=table_name,
    table_config=table_config,
    partition_date=partition_date,
)


print()
print("=" * 80)
print(f"Processing table: {table_name}")

if partition_date:
    print(f"Partition date : {partition_date}")

print(f"Input  : {input_path}")
print(f"Output : {output_path}")
print("=" * 80)


# ============================================================
# Extract
# ============================================================

spark_schema = get_spark_schema(table_name)

df = (
    spark.read
    .option("header", True)
    .schema(spark_schema)
    .csv(input_path)
)


# ============================================================
# Validate / Display
# ============================================================

print()
print(f"Schema: {table_name}")

df.printSchema()


print()
print(f"Sample: {table_name}")

df.show(
    5,
    truncate=False,
)


# ============================================================
# Transform
#
# Currently no business transformation.
# Data types are enforced by Spark schema above.
# ============================================================

result = df


# ============================================================
# Load to Cleaned Zone as Parquet
# ============================================================

(
    result.write
    .mode("overwrite")
    .parquet(output_path)
)


print()
print(f"SUCCESS: {table_name}")
print(f"Written to: {output_path}")


# ============================================================
# Complete
# ============================================================

print()
print("=" * 80)
print(f"DATASET COMPLETED: {table_name}")
print("=" * 80)


spark.stop()