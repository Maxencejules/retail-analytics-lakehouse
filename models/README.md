# PyTorch ML Integrations

Train and score with `--currency CAD|USD|GBP` (CAD default). The Gold loader reads Hive event-date partitions and selects one currency; new checkpoints record currency and scoring rejects mismatched or unlabelled legacy checkpoints. Rebuild legacy Gold without a currency column and retrain old checkpoints. The focused batch proof checks loading/aggregation; it does not validate PyTorch training quality or chronological/generalization claims.

This directory contains PyTorch-based ML utilities built on Gold-layer data.

## Available Scripts

- `train_sales_predictor.py`: trains a neural net regressor from `gold/daily_revenue_by_store`.
- `score_sales_predictor.py`: scores historical examples with the latest (or specified) model.
- `sales_features.py`: Gold-layer feature engineering helpers shared by train/score flows.

## Install ML Dependencies

```bash
pip install -r requirements-ml.txt
```

## Train Locally

```bash
python models/train_sales_predictor.py \
  --gold-path data/lakehouse/gold/daily_revenue_by_store \
  --output-dir artifacts/models/sales_revenue_predictor \
  --epochs 30 \
  --batch-size 64
```

## Score Locally

```bash
python models/score_sales_predictor.py \
  --gold-path data/lakehouse/gold/daily_revenue_by_store \
  --model-root artifacts/models/sales_revenue_predictor \
  --output-file artifacts/models/predictions/sales_predictions.jsonl
```

## Artifacts

- Trained model checkpoint: `artifacts/models/sales_revenue_predictor/<run_id>/model.pt`
- Training metrics/history: `metrics.json`, `history.json`, `summary.json`
- Latest pointer: `artifacts/models/sales_revenue_predictor/latest_model_path.txt`
