# Platform Evolution: Orchestration, Modeling, and Observability

## Objective

Strengthen production readiness by introducing controlled orchestration, transformation discipline, unified observability, and explicit data-quality alerting.

## Delivery Roadmap

### Phase 1: Orchestration + Run Metadata + Alerting Baseline

Scope:
- Airflow-managed batch orchestration with retries, backfills, dependency checks, and SLA callbacks.
- Environment promotion workflow with controlled `dev -> stage -> prod` transitions.
- Run metadata capture and publication for traceable promotions and execution auditability.
- Alerting baseline through Airflow callbacks, Prometheus alert rules, and webhook routing.

Exit criteria:
- Every production batch run has an auditable run record.
- Promotion is blocked on failed gates (dependency, dbt, quality scan).
- On-call receives actionable failures within SLA windows.

### Phase 2: dbt Semantic and Testing Layer + Lineage and Governance Controls

Scope:
- dbt model ownership for staging, marts, and metric-serving layers.
- Contracted tests for referential integrity, freshness, uniqueness, and metric sanity.
- Semantic consistency through model documentation and generated lineage artifacts.
- Governance controls: environment isolation, review gates, and deployment discipline.

Exit criteria:
- Warehouse logic is centralized in dbt with reproducible runs per environment.
- Lineage and docs are generated and published for every release.
- Data governance checks are enforceable in CI/CD and promotion workflows.

### Phase 3: Cost/Performance Automation (Compaction, Adaptive Scaling, Workload Policies)

Scope:
- Automated file compaction and layout optimization for Gold/Silver datasets.
- Adaptive compute scaling policies for Spark and warehouse workloads.
- Workload management policies for query isolation, concurrency, and spend protection.
- Cost guardrails using usage telemetry, retention policies, and budget-alert thresholds.

Exit criteria:
- Small-file and skew-related regressions are auto-remediated on schedule.
- Compute scales to demand with bounded SLA and spend variance.
- Workload policies prevent noisy-neighbor impact and runaway query cost.

## 1. Airflow Orchestration

Artifacts:
- [batch_etl_orchestration.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/batch_etl_orchestration.py)
- [environment_promotion_workflow.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/environment_promotion_workflow.py)
- [run_metadata.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/common/run_metadata.py)

Implemented capabilities:
- Dataset-aware scheduling for daily batch ETL via Airflow `Dataset` dependencies.
- Provider-native orchestration operators (Spark submit, sensors, Bash).
- Upstream dependency checks before compute execution.
- SLA miss and task-failure notifications via webhook callback.
- Dedicated backfill DAG separated from daily orchestration with bounded date-range controls.
- Promotion workflow with environment ordering gates (`dev -> stage -> prod`).
- Promotion gates require dbt governance contracts, source+target freshness, source+target snapshots, and Soda quality checks.
- Run metadata publication for both batch and promotion DAG runs on success/failure paths.

## 2. dbt Warehouse Layer

Artifacts:
- [dbt_project.yml](C:/Users/USER/retail-analytics-lakehouse/warehouse/dbt/dbt_project.yml)
- [models](C:/Users/USER/retail-analytics-lakehouse/warehouse/dbt/models)
- [semantic_models.yml](C:/Users/USER/retail-analytics-lakehouse/warehouse/dbt/models/semantic/semantic_models.yml)
- [exposures.yml](C:/Users/USER/retail-analytics-lakehouse/warehouse/dbt/models/exposures.yml)
- [selectors.yml](C:/Users/USER/retail-analytics-lakehouse/warehouse/dbt/selectors.yml)
- [validate_dbt_governance.py](C:/Users/USER/retail-analytics-lakehouse/scripts/validate_dbt_governance.py)

Implemented capabilities:
- Staging normalization models (`stg_*`) and star-schema marts (`dim_*`, `fact_sales`).
- SCD2 customer history snapshots (`snp_dim_customer_history`) and a curated `dim_customer_scd2` mart.
- Metric-serving models for executive analytics.
- Semantic model contracts and reusable metrics for governed downstream consumption.
- Automated tests for uniqueness, referential integrity, freshness, accepted values, and measure sanity.
- Exposure lineage mapping from marts to executive dashboard assets.
- Selector-based governance scope (`phase2_governed_models`) for controlled promotion.
- CI governance contract validation for semantic assets, exposures, tags, and model ownership metadata.
- State-aware slim CI selection workflow (`state:modified+`) for targeted dbt validation.
- Environment-based targets for deployment discipline and controlled promotion.

## 3. Unified Monitoring Stack

Artifacts:
- [docker-compose.monitoring.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/docker-compose.monitoring.yml)
- [prometheus.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/prometheus/prometheus.yml)
- [alerts.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/prometheus/alerts.yml)

Implemented capabilities:
- CloudWatch agent configuration for infrastructure and pipeline logs.
- Prometheus scraping and alert rules for platform health.
- Alertmanager routing for centralized alert fanout.
- Exporter coverage for Kafka and warehouse Postgres telemetry.
- SLO alerting for freshness, latency, quality, and cost objectives.
- Grafana provisioning for dashboards and data sources.
- OpenLineage configuration for Airflow/Spark lineage export to Marquez.

## 4. Data-Quality Observability

Artifacts:
- [gold_quality.yml](C:/Users/USER/retail-analytics-lakehouse/quality/soda/checks/gold_quality.yml)
- [run_soda_scan.py](C:/Users/USER/retail-analytics-lakehouse/scripts/run_soda_scan.py)

Implemented capabilities:
- Soda checks for Gold fact/dimension integrity.
- Pipeline-integrated scan execution (Airflow + local Make target).
- Alert routing to webhook and optional SNS topic.
- Fail-fast behavior on quality violations.

## 5. Operating Commands

```bash
make airflow-dag-validate
make dbt-build DBT_TARGET=dev
make phase3-policy-validate
make compact-lakehouse
make soda-scan TARGET_ENV=dev
make monitoring-up
```

## 6. Promotion Control Model

1. Run ingestion and ETL in source environment.
2. Pass dbt build/tests in target environment.
3. Pass Soda quality scan in target environment.
4. Publish promotion record artifact with timestamp and release version.

## 7. Cost/Performance Automation (Phase 3)

Artifacts:
- [compact_tables.py](C:/Users/USER/retail-analytics-lakehouse/spark/optimization/compact_tables.py)
- [performance.py](C:/Users/USER/retail-analytics-lakehouse/spark/common/performance.py)
- [cost_performance_optimization.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/cost_performance_optimization.py)
- [adaptive-scaling-policy.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/spark/adaptive-scaling-policy.example.json)
- [workload-management.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/redshift/workload-management.example.json)
- [lakehouse-lifecycle-policy.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/s3/lifecycle/lakehouse-lifecycle-policy.example.json)
- [budget-alerts.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/cost/budget-alerts.example.json)
- [validate_phase3_policies.py](C:/Users/USER/retail-analytics-lakehouse/scripts/validate_phase3_policies.py)

Implemented capabilities:
- Automated compaction workflow for Silver/Gold to reduce small-file inefficiency.
- Adaptive Spark scaling profiles (`cost_saver`, `balanced`, `high_throughput`) with environment overrides.
- Redshift workload policy templates for queue isolation, concurrency control, and runaway query handling.
- S3 lifecycle templates for storage tiering and retention enforcement across Bronze/Silver/Gold.
- Budget and log-retention templates with CI validation guardrails.
- Airflow-scheduled optimization DAG with policy validation, compaction execution, and run metadata publication.
