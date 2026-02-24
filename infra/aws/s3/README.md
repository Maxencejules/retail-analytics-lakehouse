# S3 Lake Structure

Recommended bucket prefix structure:

```text
s3://<lakehouse-bucket>/
  bronze/
  silver/
  gold/
  _checkpoints/
  _quarantine/
```

Layer intent:
- `bronze/`: raw append-only ingestion outputs.
- `silver/`: validated and conformed datasets.
- `gold/`: business-ready aggregates and marts.

This repository includes local placeholders for:
- `infra/aws/s3/bronze/`
- `infra/aws/s3/silver/`
- `infra/aws/s3/gold/`

