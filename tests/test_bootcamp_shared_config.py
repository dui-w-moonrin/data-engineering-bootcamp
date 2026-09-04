from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


CONFIG_PATH = (
    Path(__file__).resolve().parents[1]
    / "00-bootcamp-project"
    / "pyspark"
    / "schema_config.py"
)


def load_config():
    spec = spec_from_file_location("bootcamp_schema_config", CONFIG_PATH)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_shared_environment_paths_and_credentials_are_centralized():
    config = load_config()

    assert str(config.DAGS_FOLDER) == "/opt/airflow/dags"
    assert str(config.PYSPARK_FOLDER) == "/opt/spark/pyspark"
    assert str(config.AIRFLOW_CONFIG_FOLDER) == "/opt/airflow/config"
    assert str(config.GCS_KEYFILE) == "/opt/spark/pyspark/deb-load-data-to-gcs.json"
    assert str(config.BIGQUERY_KEYFILE) == (
        "/opt/airflow/config/deb-loading-data-to-bigquery-secured.json"
    )
    assert str(config.TRANSFORMER_FILE) == "/opt/spark/pyspark/transformers.py"


def test_table_metadata_centralizes_api_paths_and_partitioning():
    config = load_config()

    assert config.API_BASE_URL == "http://34.87.139.82:8000"
    assert config.TABLE_CONFIG["order_items"]["api_path"] == "order-items"

    assert config.PARTITIONED_TABLES == {"events", "orders", "users"}

    for table_name in config.PARTITIONED_TABLES:
        assert config.TABLE_CONFIG[table_name]["partitioned"] is True
        assert config.TABLE_CONFIG[table_name]["partition_field"] == "created_at"

    for table_name in {"addresses", "order_items", "products", "promos"}:
        assert config.TABLE_CONFIG[table_name]["partitioned"] is False


def test_api_source_fields_map_to_canonical_output_columns():
    config = load_config()

    expected_mappings = {
        "order_items": {
            "order_id": "order",
            "product_id": "product",
        },
        "events": {
            "user_id": "user",
            "order_id": "order",
            "product_id": "product",
        },
        "orders": {
            "user_id": "user",
            "promo_id": "promo",
            "address_id": "address",
        },
        "users": {
            "address_id": "address",
        },
    }

    for table_name, expected in expected_mappings.items():
        assert config.TABLE_CONFIG[table_name]["source_fields"] == expected

    assert [name for name, _ in config.TABLE_CONFIG["events"]["schema"]][-3:] == [
        "user_id",
        "order_id",
        "product_id",
    ]
    assert [name for name, _ in config.TABLE_CONFIG["orders"]["schema"]][-3:] == [
        "user_id",
        "promo_id",
        "address_id",
    ]
    assert config.TABLE_CONFIG["users"]["schema"][-1][0] == "address_id"
