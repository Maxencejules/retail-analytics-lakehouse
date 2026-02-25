# Chaos Engineering Runbook

## Objective

Validate resilience of orchestration and ETL workflows by injecting controlled failures.

## SLA/SLO Targets

- ETL job availability SLA: **99.9% monthly uptime** for scheduled batch ETL DAG runs.
- Batch ETL recovery SLO: failed jobs should auto-recover and complete within **30 minutes**.
- Data freshness SLO: `fact_sales` lag should remain <= **30 minutes**.

Interpretation:

- For 99.9% monthly uptime, allowable ETL unavailability is ~43.2 minutes/month.
- Chaos experiments should prove alerting and recovery before this budget is exhausted.

## Experiment Catalog

1. `airflow_network_partition`
   - Fault model: isolate Airflow container from network.
   - Expected behavior: DAG retries trigger and alert callbacks fire.
2. `spark_node_crash`
   - Fault model: terminate Spark container mid-workflow.
   - Expected behavior: orchestration retries and Spark service restart path restores pipeline.

## Local Drill Commands

Airflow partition:

```bash
make chaos-airflow-partition CHAOS_DURATION_SECONDS=60 CHAOS_TARGET_CONTAINER=airflow-webserver
```

Spark crash:

```bash
make chaos-spark-crash CHAOS_DURATION_SECONDS=60
```

Dry-run (no destructive action):

```bash
make chaos-run CHAOS_EXPERIMENT=airflow_network_partition CHAOS_DRY_RUN=true
```

## Gremlin Integration

Use `gremlin-http` mode when running in Gremlin-managed environments:

```bash
make chaos-run \
  CHAOS_EXPERIMENT=airflow_network_partition \
  CHAOS_MODE=gremlin-http \
  CHAOS_GREMLIN_ENDPOINT=$GREMLIN_API_ENDPOINT \
  CHAOS_PAYLOAD_FILE=chaos/payloads/airflow_network_partition.gremlin.example.json \
  CHAOS_TARGET_CONTAINER=airflow-webserver
```

Artifacts:

- Script: [chaos/run_experiment.py](C:/Users/USER/retail-analytics-lakehouse/chaos/run_experiment.py)
- Payload templates:
  - [airflow payload](C:/Users/USER/retail-analytics-lakehouse/chaos/payloads/airflow_network_partition.gremlin.example.json)
  - [spark payload](C:/Users/USER/retail-analytics-lakehouse/chaos/payloads/spark_node_crash.gremlin.example.json)
- Execution reports: `chaos/results/*.json`

## Post-Experiment Checklist

1. Confirm service recovery (`docker compose ps` and Airflow/Spark health checks).
2. Confirm alert routing fired (webhook/SNS/Alertmanager).
3. Confirm SLA impact budget consumed is within monthly threshold.
4. Record experiment ID, fault window, and remediation notes in operations log.
