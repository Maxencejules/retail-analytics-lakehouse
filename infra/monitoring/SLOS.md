# Lakehouse SLOs

## Freshness SLO

- Objective: `fact_sales` freshness lag <= 30 minutes.
- Metric: `lakehouse_data_freshness_timestamp_seconds{dataset="fact_sales"}`.
- Alert: `DataFreshnessSLOBreached`.

## Latency SLO

- Objective: P95 pipeline latency <= 5 minutes.
- Metric: `lakehouse_pipeline_latency_seconds_bucket`.
- Alert: `PipelineLatencySLOBreached`.

## Quality SLO

- Objective: zero critical data quality failures over rolling 15 minutes.
- Metric: `lakehouse_data_quality_failures_total`.
- Alert: `DataQualitySLOBreached`.

## Cost SLO

- Objective: budget utilization ratio <= 0.85.
- Metric: `lakehouse_cost_budget_utilization_ratio`.
- Alert: `CostBudgetSLOBreached`.

## Notes

- SLO alerts are defined in [slo-alerts.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/prometheus/slo-alerts.yml).
- Metrics should be emitted from orchestration, quality, and cost telemetry pipelines into Prometheus.
