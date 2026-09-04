# Shared Config Refactor Design

## Goal

Refactor the bootcamp project so environment-wide values and credential file paths have one source of truth, while preserving the current pipeline structure and keeping personal Google service-account JSON files out of Git.

## Scope

- Rename `00-bootcampt-project` to `00-bootcamp-project`.
- Keep each DAG's `DATA` constant local to that DAG because it identifies the pipeline.
- Move shared values to `00-bootcamp-project/pyspark/schema_config.py`: project, region, bucket, business domain, BigQuery dataset, API base URL, container paths, credential paths, transformer path, partition metadata, API path metadata, and schemas.
- Keep the generic `transformers.py`; do not return to seven duplicated transformer files.
- Use `api_path="order-items"` for `order_items` so the known 404 is fixed centrally.
- Do not commit credential contents. JSON credentials remain local and are only referenced by path.
- Do not modify modules 01/03/04 or other unrelated local work.

## Deferred to the comparison pass

Teacher-solution behavior such as BranchPythonOperator flow and BigQuery partition-decorator loading will be compared and adopted, if appropriate, in a separate commit after this refactor. This keeps the shared-config refactor reviewable on its own.

## Verification

- A small standard-library test validates the shared-config contract.
- All modified Python files must parse successfully.
- The final diff must be limited to the renamed module-00 project, `.gitignore`, tests, and design/plan docs.
