from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "00-bootcamp-project" / "dags"
PARTITIONED_DAGS = {
    "events": DAGS_DIR / "greenery_events_data_pipeline.py",
    "orders": DAGS_DIR / "greenery_orders_data_pipeline.py",
    "users": DAGS_DIR / "greenery_users_data_pipeline.py",
}


def test_partitioned_dags_branch_when_a_date_has_no_data():
    for table_name, dag_path in PARTITIONED_DAGS.items():
        source = dag_path.read_text(encoding="utf-8")

        assert "BranchPythonOperator" in source, table_name
        assert 'return "load_data_to_gcs"' in source, table_name
        assert 'return "do_nothing"' in source, table_name
        assert 'task_id="do_nothing"' in source, table_name
        assert 'trigger_rule="one_success"' in source, table_name


def test_partitioned_dags_write_only_the_target_bigquery_partition():
    for table_name, dag_path in PARTITIONED_DAGS.items():
        source = dag_path.read_text(encoding="utf-8")

        assert 'partition = ds.replace("-", "")' in source, table_name
        assert '${partition}' in source, table_name
        assert "bigquery.TimePartitioning(" in source, table_name
        assert 'TABLE["partition_field"]' in source, table_name
        assert "WRITE_TRUNCATE" in source, table_name
        assert "WRITE_APPEND" not in source, table_name


def test_partitioned_dags_use_manual_backfill_friendly_schedule_bounds():
    expected_start_dates = {
        "events": "datetime(2021, 2, 9, tzinfo=timezone.utc)",
        "orders": "datetime(2021, 2, 9, tzinfo=timezone.utc)",
        "users": "datetime(2020, 1, 4, tzinfo=timezone.utc)",
    }

    for table_name, dag_path in PARTITIONED_DAGS.items():
        source = dag_path.read_text(encoding="utf-8")

        assert expected_start_dates[table_name] in source
        assert "end_date=" not in source
        assert "catchup=False" in source
