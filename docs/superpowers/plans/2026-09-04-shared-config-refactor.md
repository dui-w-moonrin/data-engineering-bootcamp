# Shared Config Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Centralize shared Greenery pipeline configuration and credential paths while preserving the user's current module-00 implementation for later comparison with `zkan/deb6`.

**Architecture:** `pyspark/schema_config.py` is the single source of truth for environment values, table metadata, schemas, API paths, container paths, and credential paths. Each DAG retains only its table identity and task logic, while `transformers.py` consumes the same config.

**Tech Stack:** Python, Apache Airflow 3.3, Spark/PySpark, Google Cloud Storage, BigQuery, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-04-shared-config-refactor-design.md`

## Global Constraints

- Do not commit Google service-account JSON contents.
- Do not modify modules 01/03/04.
- Preserve the snapshot branch `pre-deb6-refactor` unchanged.
- Work only on `shared-config-refactor`.
- Keep `DATA` local to each DAG.
- Defer teacher-specific branching/BigQuery partition behavior to a later comparison commit.

---

### Task 1: Correct the module-00 directory name

**Files:**
- Rename: `00-bootcampt-project/` -> `00-bootcamp-project/`
- Modify: `.gitignore`

- [ ] Rename the Git tree without changing file contents.
- [ ] Change the runtime CSV ignore path to `00-bootcamp-project/dags/*.csv`.
- [ ] Commit the rename independently.

### Task 2: Add a failing shared-config contract test

**Files:**
- Create: `tests/test_bootcamp_shared_config.py`

- [ ] Assert shared path constants exist and have the expected container locations.
- [ ] Assert `order_items` maps to API path `order-items`.
- [ ] Assert `events`, `orders`, and `users` are marked partitioned.
- [ ] Run the test against the pre-refactor config and confirm it fails because the new contract is not implemented yet.

### Task 3: Refactor the shared configuration

**Files:**
- Modify: `00-bootcamp-project/pyspark/schema_config.py`

**Produces:** `API_BASE_URL`, `DAGS_FOLDER`, `PYSPARK_FOLDER`, `AIRFLOW_CONFIG_FOLDER`, `GCS_KEYFILE`, `BIGQUERY_KEYFILE`, `TRANSFORMER_FILE`, `PARTITIONED_TABLES`, and table `api_path`/`partitioned` metadata.

- [ ] Add the shared constants without embedding credential contents.
- [ ] Normalize table metadata and remove hard-coded dated raw/cleaned paths for partitioned tables.
- [ ] Preserve existing schemas and project-specific values.

### Task 4: Make DAGs consume only the shared config

**Files:**
- Modify all seven `00-bootcamp-project/dags/greenery_*_data_pipeline.py` files.

- [ ] Remove duplicated container path and credential-path declarations.
- [ ] Import those values from `schema_config.py`.
- [ ] Build API URLs using `API_BASE_URL` and table `api_path`.
- [ ] Keep each DAG's `DATA` local.
- [ ] Preserve current task behavior in this commit.

### Task 5: Make the generic transformer consume the shared config

**Files:**
- Modify: `00-bootcamp-project/pyspark/transformers.py`

- [ ] Import `GCS_KEYFILE` and `PARTITIONED_TABLES` from shared config.
- [ ] Remove duplicate credential-path and partitioned-table constants.
- [ ] Preserve `--table` and `--date` behavior.

### Task 6: Verify and commit

- [ ] Run the shared-config contract test and confirm PASS.
- [ ] Parse all modified Python files with Python AST/compile checks.
- [ ] Confirm no credential JSON file is present in the commit tree.
- [ ] Commit the refactor.
- [ ] Compare this refactored result with `zkan/deb6` in a separate follow-up pass.
