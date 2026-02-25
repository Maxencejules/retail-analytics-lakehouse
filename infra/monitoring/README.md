# Monitoring and Observability Stack

Unified observability baseline:

- CloudWatch (infrastructure + log ingestion)
- Prometheus + Alertmanager + Grafana (metrics, dashboards, alert rules)
- OpenLineage + Marquez (data lineage)

## Local Stack

Compose file:
- [docker-compose.monitoring.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/docker-compose.monitoring.yml)

Start:

```bash
docker compose -f infra/monitoring/docker-compose.monitoring.yml up -d
```

Or use Make targets:

```bash
make monitoring-up
make monitoring-down
```

## Configuration Assets

- Prometheus scrape + alerts:
  - [prometheus.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/prometheus/prometheus.yml)
  - [alerts.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/prometheus/alerts.yml)
  - [slo-alerts.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/prometheus/slo-alerts.yml)
- Alertmanager routing:
  - [alertmanager.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/alertmanager/alertmanager.yml)
- Grafana provisioning + dashboard:
  - [datasources.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/grafana/provisioning/datasources/datasources.yml)
  - [dashboards.yml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/grafana/provisioning/dashboards/dashboards.yml)
  - [lakehouse-overview.json](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/grafana/dashboards/lakehouse-overview.json)
- OpenLineage configs:
  - [airflow_openlineage.yaml](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/openlineage/airflow_openlineage.yaml)
  - [spark-openlineage.conf](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/openlineage/spark-openlineage.conf)
- CloudWatch agent config:
  - [agent-config.json](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/cloudwatch/agent-config.json)

## Alert Routing

- Prometheus rules define infrastructure, platform, and SLO alerts (freshness, latency, quality, cost).
- Alertmanager routes all Prometheus alerts to configured receivers.
- Replace the default webhook endpoint in `infra/monitoring/alertmanager/alertmanager.yml` with your incident router.
- Airflow failure/SLA callbacks use `AIRFLOW_ALERT_WEBHOOK_URL`.
- Soda quality failures route to `SODA_ALERT_WEBHOOK_URL` and/or `SODA_ALERT_SNS_TOPIC_ARN`.
- SLO definitions are documented in [SLOS.md](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/SLOS.md).

## Exporter Coverage

- Airflow metrics endpoint (`airflow-metrics` scrape job)
- Spark executor metrics endpoint (`spark-metrics` scrape job)
- Kafka exporter (`kafka-exporter` service)
- Warehouse Postgres exporter (`warehouse-exporter` service)

Environment knobs:
- `KAFKA_EXPORTER_SERVER` (default `kafka:9092`)
- `WAREHOUSE_EXPORTER_DSN` (Postgres DSN for warehouse exporter)
