import json

from google.cloud import storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"

project_id = "YOUR_PROJECT_ID"
bucket_name = "deb-bootcamp-033-dui"

# ----------------------------------------
# Dataset configuration
# ----------------------------------------

non_partitioned_data = [
    "addresses",
    "order_items",
    "products",
    "promos",
]

partitioned_data = [
    {
        "table": "events",
        "date": "2021-02-10",
    },
    {
        "table": "orders",
        "date": "2021-02-10",
    },
    {
        "table": "users",
        "date": "2020-10-23",
    },
]


# ----------------------------------------
# Load GCP credentials
# ----------------------------------------

keyfile_gcs = "deb-load-data-to-gcs.json"

with open(keyfile_gcs) as f:
    service_account_info_gcs = json.load(f)

credentials_gcs = service_account.Credentials.from_service_account_info(
    service_account_info_gcs
)

storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)

bucket = storage_client.bucket(bucket_name)


# ----------------------------------------
# Upload non-partitioned datasets
# ----------------------------------------

for table in non_partitioned_data:

    file_path = f"{DATA_FOLDER}/{table}.csv"

    destination_blob_name = (
        f"raw/{BUSINESS_DOMAIN}/{table}/{table}.csv"
    )

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    print(f"Uploaded: {file_path}")
    print(f"       -> gs://{bucket_name}/{destination_blob_name}")

# -----------------------------
# Upload partitioned
# -----------------------------
for dataset in partitioned_data:

    table = dataset["table"]
    date = dataset["date"]

    # Local file ไม่มี partition folder
    file_path = f"{DATA_FOLDER}/{table}.csv"

    # แต่สร้าง partition path ตอน upload ไป GCS
    destination_blob_name = (
        f"raw/{BUSINESS_DOMAIN}/{table}/{date}/{table}.csv"
    )

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    print(f"Uploaded: {file_path}")
    print(f"       -> gs://{bucket_name}/{destination_blob_name}")