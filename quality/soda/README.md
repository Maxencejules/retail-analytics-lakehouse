# Soda Data Quality Observability

This directory defines Soda checks and alert-routing templates for warehouse quality monitoring.

## Files

- Soda datasource config:
  - [configuration.yml](C:/Users/USER/retail-analytics-lakehouse/quality/soda/configuration.yml)
- Gold quality checks:
  - [gold_quality.yml](C:/Users/USER/retail-analytics-lakehouse/quality/soda/checks/gold_quality.yml)
- Alert routing template:
  - [alert-routing.example.yml](C:/Users/USER/retail-analytics-lakehouse/quality/soda/alerts/alert-routing.example.yml)

## Run a Scan

```bash
pip install -r requirements-quality.txt
python scripts/run_soda_scan.py --checks quality/soda/checks/gold_quality.yml
```

Target-specific scan:

```bash
python scripts/run_soda_scan.py --checks quality/soda/checks/gold_quality.yml --target stage
```

## Alert Routing

- Set `SODA_ALERT_WEBHOOK_URL` to route failures to a webhook receiver (Slack, Teams, or incident router).
- Optional SNS routing can be enabled via `SODA_ALERT_SNS_TOPIC_ARN`.
- Script entrypoint:
  - [run_soda_scan.py](C:/Users/USER/retail-analytics-lakehouse/scripts/run_soda_scan.py)
