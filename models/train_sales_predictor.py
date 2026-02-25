"""Train a simple PyTorch neural network on Gold-layer sales data."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import random
from typing import Any

import numpy as np

if __package__ in {None, ""}:
    import sys

    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from models.sales_features import (
    FEATURE_NAMES,
    build_lagged_sales_examples,
    load_gold_daily_revenue,
)

LOGGER = logging.getLogger("models.train_sales_predictor")


def _load_torch() -> Any:
    try:
        import torch
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "PyTorch is required. Install ML dependencies with "
            "`pip install -r requirements-ml.txt`."
        ) from exc
    return torch


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train a sales prediction neural net from Gold-layer data."
    )
    parser.add_argument(
        "--gold-path",
        default="data/lakehouse/gold/daily_revenue_by_store",
        help="Path to Gold daily_revenue_by_store parquet dataset.",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/models/sales_revenue_predictor",
        help="Directory for model artifacts.",
    )
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--validation-ratio", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--min-samples", type=int, default=64)
    parser.add_argument("--run-tag", default="")
    parser.add_argument(
        "--device",
        choices=("auto", "cpu"),
        default="auto",
        help="Training device selection.",
    )
    return parser


def _set_seed(seed: int, torch_module: Any) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch_module.manual_seed(seed)


def _train_validation_split(
    features: np.ndarray,
    targets: np.ndarray,
    validation_ratio: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    if not 0.0 < validation_ratio < 0.9:
        raise ValueError("--validation-ratio must be between 0 and 0.9")

    total_rows = len(features)
    split_index = int(total_rows * (1.0 - validation_ratio))
    split_index = max(1, min(split_index, total_rows - 1))

    return (
        features[:split_index],
        targets[:split_index],
        features[split_index:],
        targets[split_index:],
    )


def _regression_metrics(
    actual: np.ndarray,
    predicted: np.ndarray,
) -> dict[str, float]:
    error = predicted - actual
    mae = float(np.mean(np.abs(error)))
    rmse = float(np.sqrt(np.mean(np.square(error))))
    mse = float(np.mean(np.square(error)))
    return {"mae": mae, "rmse": rmse, "mse": mse}


def _run_training(args: argparse.Namespace) -> tuple[dict[str, Any], Path]:
    torch = _load_torch()
    _set_seed(args.seed, torch)

    records = load_gold_daily_revenue(args.gold_path)
    features, targets, _, _ = build_lagged_sales_examples(records)

    if len(features) < args.min_samples:
        raise ValueError(
            f"Insufficient training samples ({len(features)}). "
            f"Need at least --min-samples={args.min_samples}."
        )

    train_x, train_y, val_x, val_y = _train_validation_split(
        features,
        targets,
        args.validation_ratio,
    )

    feature_mean = train_x.mean(axis=0)
    feature_std = train_x.std(axis=0)
    feature_std = np.where(feature_std < 1e-6, 1.0, feature_std)

    train_x_norm = (train_x - feature_mean) / feature_std
    val_x_norm = (val_x - feature_mean) / feature_std

    device_name = "cpu"
    if args.device == "auto" and torch.cuda.is_available():
        device_name = "cuda"
    device = torch.device(device_name)

    train_x_tensor = torch.tensor(train_x_norm, dtype=torch.float32, device=device)
    train_y_tensor = torch.tensor(train_y, dtype=torch.float32, device=device)
    val_x_tensor = torch.tensor(val_x_norm, dtype=torch.float32, device=device)
    val_y_tensor = torch.tensor(val_y, dtype=torch.float32, device=device)

    model = torch.nn.Sequential(
        torch.nn.Linear(train_x_tensor.shape[1], 32),
        torch.nn.ReLU(),
        torch.nn.Linear(32, 16),
        torch.nn.ReLU(),
        torch.nn.Linear(16, 1),
    ).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=args.learning_rate)
    loss_fn = torch.nn.MSELoss()

    history: list[dict[str, float]] = []
    best_val_mse = float("inf")
    best_state: dict[str, Any] | None = None

    for epoch in range(1, args.epochs + 1):
        model.train()
        permutation = torch.randperm(train_x_tensor.shape[0], device=device)
        epoch_loss = 0.0

        for start in range(0, train_x_tensor.shape[0], args.batch_size):
            indices = permutation[start : start + args.batch_size]
            batch_x = train_x_tensor[indices]
            batch_y = train_y_tensor[indices]

            predictions = model(batch_x).squeeze(-1)
            loss = loss_fn(predictions, batch_y)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            epoch_loss += float(loss.item()) * float(indices.numel())

        train_mse = epoch_loss / float(train_x_tensor.shape[0])
        model.eval()
        with torch.no_grad():
            val_predictions = model(val_x_tensor).squeeze(-1)
            val_mse = float(loss_fn(val_predictions, val_y_tensor).item())
            val_pred_np = val_predictions.detach().cpu().numpy()

        epoch_metrics = _regression_metrics(val_y, val_pred_np)
        epoch_record = {
            "epoch": float(epoch),
            "train_mse": train_mse,
            "val_mse": val_mse,
            "val_mae": epoch_metrics["mae"],
            "val_rmse": epoch_metrics["rmse"],
        }
        history.append(epoch_record)

        LOGGER.info(
            "epoch=%s train_mse=%.4f val_mse=%.4f val_mae=%.4f val_rmse=%.4f",
            epoch,
            train_mse,
            val_mse,
            epoch_metrics["mae"],
            epoch_metrics["rmse"],
        )

        if val_mse < best_val_mse:
            best_val_mse = val_mse
            best_state = {
                key: value.detach().cpu().clone()
                for key, value in model.state_dict().items()
            }

    if best_state is None:
        raise RuntimeError("Training did not produce a valid model state.")

    model.load_state_dict(best_state)
    model.eval()
    with torch.no_grad():
        final_val_predictions = model(val_x_tensor).squeeze(-1).cpu().numpy()
    final_metrics = _regression_metrics(val_y, final_val_predictions)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_tag = args.run_tag.strip()
    run_name = f"{timestamp}-{run_tag}" if run_tag else timestamp

    output_root = Path(args.output_dir)
    run_dir = output_root / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    model_path = run_dir / "model.pt"
    checkpoint = {
        "state_dict": best_state,
        "feature_names": list(FEATURE_NAMES),
        "feature_mean": feature_mean.tolist(),
        "feature_std": feature_std.tolist(),
        "model_config": {"input_dim": train_x_tensor.shape[1], "hidden_dims": [32, 16]},
        "training_config": {
            "epochs": args.epochs,
            "batch_size": args.batch_size,
            "learning_rate": args.learning_rate,
            "validation_ratio": args.validation_ratio,
            "seed": args.seed,
            "gold_path": args.gold_path,
        },
        "metrics": final_metrics,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    torch.save(checkpoint, model_path)

    history_path = run_dir / "history.json"
    history_path.write_text(json.dumps(history, indent=2), encoding="utf-8")

    metrics_path = run_dir / "metrics.json"
    metrics_record = {
        "samples": {
            "total": int(len(features)),
            "train": int(len(train_x)),
            "validation": int(len(val_x)),
        },
        "final_validation_metrics": final_metrics,
    }
    metrics_path.write_text(json.dumps(metrics_record, indent=2), encoding="utf-8")

    latest_pointer = output_root / "latest_model_path.txt"
    latest_pointer.parent.mkdir(parents=True, exist_ok=True)
    latest_pointer.write_text(str(model_path), encoding="utf-8")

    summary = {
        "run_name": run_name,
        "model_path": str(model_path),
        "metrics_path": str(metrics_path),
        "history_path": str(history_path),
        "samples_total": int(len(features)),
        "validation_mae": final_metrics["mae"],
        "validation_rmse": final_metrics["rmse"],
    }
    return summary, run_dir


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    summary, run_dir = _run_training(args)

    summary_path = Path(run_dir) / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOGGER.info("training_complete model_path=%s", summary["model_path"])
    LOGGER.info(
        "validation_mae=%.4f validation_rmse=%.4f",
        summary["validation_mae"],
        summary["validation_rmse"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
