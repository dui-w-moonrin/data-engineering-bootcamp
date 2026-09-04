# ============================================================
# Greenery Pipeline - Shared Configuration
# ============================================================

PROJECT_ID = "dui-bootcamp"
LOCATION = "asia-southeast1"

BUCKET_NAME = "deb-bootcamp-033-dui"
BUSINESS_DOMAIN = "greenery"

BIGQUERY_DATASET = "deb_bootcamp"


# ============================================================
# Table Configuration
#
# Data type convention:
# STRING    -> Spark StringType     -> BigQuery STRING
# INT64     -> Spark LongType       -> BigQuery INT64
# FLOAT64   -> Spark DoubleType     -> BigQuery FLOAT64
# TIMESTAMP -> Spark TimestampType  -> BigQuery TIMESTAMP
# ============================================================

TABLE_CONFIG = {

    "addresses": {
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
        "raw_path": "events/2021-02-10/events.csv",
        "cleaned_path": "events/2021-02-10/",
        "schema": [
            ("event_id", "STRING"),
            ("session_id", "STRING"),
            ("page_url", "STRING"),
            ("created_at", "TIMESTAMP"),
            ("event_type", "STRING"),
            ("user", "STRING"),
            ("order", "STRING"),
            ("product", "STRING"),
        ],
    },

    "order_items": {
        "raw_path": "order_items/order_items.csv",
        "cleaned_path": "order_items/",
        "schema": [
            ("order_id", "STRING"),
            ("product_id", "STRING"),
            ("quantity", "INT64"),
        ],
    },

    "orders": {
        "raw_path": "orders/2021-02-10/orders.csv",
        "cleaned_path": "orders/2021-02-10/",
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
            ("user", "STRING"),
            ("promo", "STRING"),
            ("address", "STRING"),
        ],
    },

    "products": {
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
        "raw_path": "promos/promos.csv",
        "cleaned_path": "promos/",
        "schema": [
            ("promo_id", "STRING"),
            ("discount", "FLOAT64"),
            ("status", "STRING"),
        ],
    },

    "users": {
        "raw_path": "users/2020-10-23/users.csv",
        "cleaned_path": "users/2020-10-23/",
        "schema": [
            ("user_id", "STRING"),
            ("first_name", "STRING"),
            ("last_name", "STRING"),
            ("email", "STRING"),
            ("phone_number", "STRING"),
            ("created_at", "TIMESTAMP"),
            ("updated_at", "TIMESTAMP"),
            ("address", "STRING"),
        ],
    },
}