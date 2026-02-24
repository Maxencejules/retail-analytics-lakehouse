# Airflow Orchestration

This directory contains Airflow DAGs for production workflow orchestration.

## DAGs

- [batch_etl_orchestration.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/batch_etl_orchestration.py)
  - Daily batch orchestration
  - Retries + backfill support (`catchup=True`)
  - Dependency checks before ETL execution
  - SLA miss callback notifications
  - Gold output validation + data-quality scan task
  - Run metadata publication (`all_done`) for auditability across success/failure paths

- [environment_promotion_workflow.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/environment_promotion_workflow.py)
  - Controlled `dev -> stage -> prod` promotion workflow
  - Dependency artifact check
  - dbt build gate
  - Soda quality scan gate
  - Promotion record publishing

Shared callbacks:
- [notifications.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/common/notifications.py)
- [run_metadata.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/common/run_metadata.py)

## Key Environment Variables

- `AIRFLOW_REPO_ROOT`
- `AIRFLOW_PYTHON_BIN`
- `AIRFLOW_OUTPUT_BASE_PATH`
- `AIRFLOW_RAW_INPUT_PATH_TEMPLATE`
- `AIRFLOW_PROMOTION_ARTIFACT_TEMPLATE`
- `AIRFLOW_PROMOTION_RECORD_TEMPLATE`
- `AIRFLOW_RUN_METADATA_PATH_TEMPLATE`
- `AIRFLOW_ALERT_WEBHOOK_URL`
- `AIRFLOW_ALERT_SNS_TOPIC_ARN`

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
