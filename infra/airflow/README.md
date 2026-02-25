# Airflow Orchestration

This directory contains Airflow DAGs for production workflow orchestration.

## DAGs

- [batch_etl_orchestration.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/batch_etl_orchestration.py)
  - Dataset-aware daily batch orchestration (triggered by raw Bronze dataset updates)
  - Provider-native operators (`SparkSubmitOperator`, sensors, `BashOperator`)
  - Dependency checks before ETL execution
  - SLA miss callback notifications
  - Gold output validation + data-quality scan task
  - Run metadata publication (`all_done`) for auditability across success/failure paths

- [batch_etl_backfill.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/batch_etl_backfill.py)
  - Dedicated manual backfill workflow (separate from daily DAG)
  - Date-range validation and bounded backfill windows
  - Dynamic triggering of daily DAG runs per ingestion date
  - Backfill run metadata publication for auditability

- [environment_promotion_workflow.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/environment_promotion_workflow.py)
  - Controlled `dev -> stage -> prod` promotion workflow
  - Dependency artifact check
  - dbt build gate using governance selector scope
  - dbt source freshness gate
  - dbt docs/lineage artifact generation gate
  - Soda quality scan gate
  - Promotion record publishing

- [cost_performance_optimization.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/cost_performance_optimization.py)
  - Dataset-aware Phase 3 optimization run
  - Policy artifact validation gate
  - Provider-native Spark compaction execution
  - Run metadata publication

Shared callbacks:
- [notifications.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/common/notifications.py)
- [run_metadata.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/common/run_metadata.py)

## Key Environment Variables

- `AIRFLOW_REPO_ROOT`
- `AIRFLOW_PYTHON_BIN`
- `AIRFLOW_DBT_SELECTOR`
- `AIRFLOW_OUTPUT_BASE_PATH`
- `AIRFLOW_RAW_INPUT_PATH_TEMPLATE`
- `AIRFLOW_DATASET_RAW_TRANSACTIONS`
- `AIRFLOW_DATASET_GOLD_DAILY_REVENUE`
- `AIRFLOW_DATASET_GOLD_TOP_PRODUCTS`
- `AIRFLOW_DATASET_GOLD_CUSTOMER_LTV`
- `AIRFLOW_PROMOTION_ARTIFACT_TEMPLATE`
- `AIRFLOW_PROMOTION_RECORD_TEMPLATE`
- `AIRFLOW_COMPACTION_DATASETS`
- `AIRFLOW_COMPACTION_TARGET_FILE_SIZE_MB`
- `AIRFLOW_BACKFILL_MAX_DAYS`
- `SPARK_WORKLOAD_PROFILE`
- `AIRFLOW_RUN_METADATA_PATH_TEMPLATE`
- `AIRFLOW_ALERT_WEBHOOK_URL`
- `AIRFLOW_ALERT_SNS_TOPIC_ARN`
- `OPENLINEAGE_ENABLED`
- `OPENLINEAGE_URL`
- `OPENLINEAGE_NAMESPACE`
- `DEPLOYMENT_GIT_SHA`
- `DEPLOYMENT_RELEASE_VERSION`

Environment template:
- [airflow.env.example](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/config/airflow.env.example)

## Local Validation

Validate DAG syntax:

```bash
make airflow-dag-validate
```

## Notes

- These DAGs are intentionally environment-driven; credentials are not hardcoded.
- Alert callback routes to webhook and/or SNS when configured.
- Run metadata records can be written to local storage or `s3://` paths.
