# Monitoring and Observability Stack

Unified observability baseline:

- CloudWatch (infrastructure + log ingestion)
- Prometheus + Grafana (metrics, dashboards, alert rules)
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

- Prometheus rules define infrastructure and platform alerts.
- Airflow failure/SLA callbacks use `AIRFLOW_ALERT_WEBHOOK_URL`.
- Soda quality failures route to `SODA_ALERT_WEBHOOK_URL` and/or `SODA_ALERT_SNS_TOPIC_ARN`.
