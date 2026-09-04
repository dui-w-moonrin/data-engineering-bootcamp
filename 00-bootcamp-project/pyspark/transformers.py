import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from schema_config import (
    BUCKET_NAME,
    BUSINESS_DOMAIN,
    GCS_KEYFILE,
    PARTITIONED_TABLES,
    TABLE_CONFIG,
)


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

if table_name in PARTITIONED_TABLES and not partition_date:
    raise ValueError(
        f"--date is required for partitioned table: {table_name}"
    )


SPARK_TYPE_MAP = {
    "STRING": StringType(),
    "INT64": LongType(),
    "FLOAT64": DoubleType(),
    "TIMESTAMP": TimestampType(),
}


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


def build_paths(table_name, table_config, partition_date=None):
    if table_name in PARTITIONED_TABLES:
        input_path = (
            f"gs://{BUCKET_NAME}/"
            f"raw/{BUSINESS_DOMAIN}/"
            f"{table_name}/{partition_date}/{table_name}.csv"
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


spark = (
    SparkSession.builder
    .appName(f"greenery-transform-{table_name}")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "5g")
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
        str(GCS_KEYFILE),
    )
    .getOrCreate()
)

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

spark_schema = get_spark_schema(table_name)

df = (
    spark.read
    .option("header", True)
    .schema(spark_schema)
    .csv(input_path)
)

df.printSchema()
df.show(5, truncate=False)

result = df

(
    result.write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"SUCCESS: {table_name}")
print(f"Written to: {output_path}")

spark.stop()
