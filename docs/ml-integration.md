# PyTorch ML Integration Guide

## Objective

Add an ML path that trains and retrains a simple neural net for sales prediction
using Gold-layer data (`daily_revenue_by_store`).

## Components

- Model scripts:
  - [models/train_sales_predictor.py](C:/Users/USER/retail-analytics-lakehouse/models/train_sales_predictor.py)
  - [models/score_sales_predictor.py](C:/Users/USER/retail-analytics-lakehouse/models/score_sales_predictor.py)
  - [models/sales_features.py](C:/Users/USER/retail-analytics-lakehouse/models/sales_features.py)
- Airflow retraining DAG:
  - [ml_sales_retraining.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/ml_sales_retraining.py)
- Optional ML dependency set:
  - [requirements-ml.txt](C:/Users/USER/retail-analytics-lakehouse/requirements-ml.txt)

## Install Dependencies

```bash
pip install -r requirements-ml.txt
```

## Local Training Example

```bash
make ml-train \
  ML_GOLD_DAILY_REVENUE_PATH=data/lakehouse/gold/daily_revenue_by_store \
  ML_MODEL_OUTPUT_DIR=artifacts/models/sales_revenue_predictor \
  ML_MODEL_EPOCHS=30 \
  ML_MODEL_BATCH_SIZE=64
```

Outputs:

- `artifacts/models/sales_revenue_predictor/<run_id>/model.pt`
- `artifacts/models/sales_revenue_predictor/<run_id>/metrics.json`
- `artifacts/models/sales_revenue_predictor/latest_model_path.txt`

## Local Scoring Example

```bash
make ml-score \
  ML_GOLD_DAILY_REVENUE_PATH=data/lakehouse/gold/daily_revenue_by_store \
  ML_MODEL_OUTPUT_DIR=artifacts/models/sales_revenue_predictor \
  ML_PREDICTIONS_OUTPUT=artifacts/models/predictions/sales_predictions.jsonl
```

## Airflow Automated Retraining

DAG: `retail_ml_sales_retraining`

Trigger model:

- Schedule: dataset-aware on Gold daily revenue dataset updates.
- Tasks:
  1. Validate Gold training data exists
  2. Train PyTorch model
  3. Score model on available examples
  4. Publish run metadata

Relevant Airflow env vars:

- `AIRFLOW_ML_GOLD_DAILY_REVENUE_PATH`
- `AIRFLOW_ML_MODEL_OUTPUT_DIR`
- `AIRFLOW_ML_PREDICTION_OUTPUT_FILE`
- `AIRFLOW_ML_TRAIN_EPOCHS`
- `AIRFLOW_ML_TRAIN_BATCH_SIZE`
- `AIRFLOW_ML_TRAIN_LEARNING_RATE`
- `AIRFLOW_ML_TRAIN_VALIDATION_RATIO`
- `AIRFLOW_DATASET_ML_SALES_MODEL`

## Notes

- The model is intentionally simple (MLP regression baseline) for operational
  integration, not final forecasting quality.
- Use this as a foundation for feature store integration, model registry, and
  drift monitoring in later phases.
