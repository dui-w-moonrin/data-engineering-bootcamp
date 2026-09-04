from pathlib import Path


PROJECT_ID = "dui-bootcamp"
LOCATION = "asia-southeast1"
BUCKET_NAME = "deb-bootcamp-033-dui"
BUSINESS_DOMAIN = "greenery"
BIGQUERY_DATASET = "deb_bootcamp"

API_BASE_URL = "http://34.87.139.82:8000"

DAGS_FOLDER = Path("/opt/airflow/dags")
PYSPARK_FOLDER = Path("/opt/spark/pyspark")
AIRFLOW_CONFIG_FOLDER = Path("/opt/airflow/config")

GCS_KEYFILE = PYSPARK_FOLDER / "deb-load-data-to-gcs.json"
BIGQUERY_KEYFILE = (
    AIRFLOW_CONFIG_FOLDER
    / "deb-loading-data-to-bigquery-secured.json"
)
TRANSFORMER_FILE = PYSPARK_FOLDER / "transformers.py"

PARTITIONED_TABLES = {"events", "orders", "users"}


TABLE_CONFIG = {
    "addresses": {
        "api_path": "addresses",
        "partitioned": False,
        "raw_path": "addresses/addresses.csv",
        "cleaned_path": "addresses/",
        "schema": [
            ("address_id", "STRING"),
            ("address", "STRING"),
            ("zipcode", "STRING"),
            ("state", "STRING"),
            ("country", "STRING"),
        ],
    },
    "events": {
        "api_path": "events",
        "partitioned": True,
        "partition_field": "created_at",
        "source_fields": {
            "user_id": "user",
            "order_id": "order",
            "product_id": "product",
        },
        "schema": [
            ("event_id", "STRING"),
            ("session_id", "STRING"),
            ("page_url", "STRING"),
            ("created_at", "TIMESTAMP"),
            ("event_type", "STRING"),
            ("user_id", "STRING"),
            ("order_id", "STRING"),
            ("product_id", "STRING"),
        ],
    },
    "order_items": {
        "api_path": "order-items",
        "partitioned": False,
        "source_fields": {
            "order_id": "order",
            "product_id": "product",
        },
        "raw_path": "order_items/order_items.csv",
        "cleaned_path": "order_items/",
        "schema": [
            ("order_id", "STRING"),
            ("product_id", "STRING"),
            ("quantity", "INT64"),
        ],
    },
    "orders": {
        "api_path": "orders",
        "partitioned": True,
        "partition_field": "created_at",
        "source_fields": {
            "user_id": "user",
            "promo_id": "promo",
            "address_id": "address",
        },
        "schema": [
            ("order_id", "STRING"),
            ("created_at", "TIMESTAMP"),
            ("order_cost", "FLOAT64"),
            ("shipping_cost", "FLOAT64"),
            ("order_total", "FLOAT64"),
            ("tracking_id", "STRING"),
            ("shipping_service", "STRING"),
            ("estimated_delivery_at", "TIMESTAMP"),
            ("delivered_at", "TIMESTAMP"),
            ("status", "STRING"),
            ("user_id", "STRING"),
            ("promo_id", "STRING"),
            ("address_id", "STRING"),
        ],
    },
    "products": {
        "api_path": "products",
        "partitioned": False,
        "raw_path": "products/products.csv",
        "cleaned_path": "products/",
        "schema": [
            ("product_id", "STRING"),
            ("name", "STRING"),
            ("price", "FLOAT64"),
            ("inventory", "INT64"),
        ],
    },
    "promos": {
        "api_path": "promos",
        "partitioned": False,
        "raw_path": "promos/promos.csv",
        "cleaned_path": "promos/",
        "schema": [
            ("promo_id", "STRING"),
            ("discount", "FLOAT64"),
            ("status", "STRING"),
        ],
    },
    "users": {
        "api_path": "users",
        "partitioned": True,
        "partition_field": "created_at",
        "source_fields": {
            "address_id": "address",
        },
        "schema": [
            ("user_id", "STRING"),
            ("first_name", "STRING"),
            ("last_name", "STRING"),
            ("email", "STRING"),
            ("phone_number", "STRING"),
            ("created_at", "TIMESTAMP"),
            ("updated_at", "TIMESTAMP"),
            ("address_id", "STRING"),
        ],
    },
}
