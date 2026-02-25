"""Score Gold-layer sales examples with a trained PyTorch model artifact."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any

import numpy as np

if __package__ in {None, ""}:
    import sys

    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from models.sales_features import build_lagged_sales_examples, load_gold_daily_revenue

LOGGER = logging.getLogger("models.score_sales_predictor")


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
        description="Run batch scoring for the sales prediction model."
    )
    parser.add_argument(
        "--gold-path",
        default="data/lakehouse/gold/daily_revenue_by_store",
        help="Path to Gold daily_revenue_by_store parquet dataset.",
    )
    parser.add_argument(
        "--model-root",
        default="artifacts/models/sales_revenue_predictor",
        help="Directory containing model run folders.",
    )
    parser.add_argument(
        "--model-artifact",
        default="",
        help="Optional explicit path to model.pt artifact.",
    )
    parser.add_argument(
        "--output-file",
        default="artifacts/models/predictions/sales_predictions.jsonl",
        help="Path for scored predictions output.",
    )
    parser.add_argument(
        "--device",
        choices=("auto", "cpu"),
        default="auto",
        help="Inference device selection.",
    )
    return parser


def _resolve_model_path(args: argparse.Namespace) -> Path:
    if args.model_artifact.strip():
        candidate = Path(args.model_artifact)
        if candidate.is_dir():
            candidate = candidate / "model.pt"
        if not candidate.exists():
            raise FileNotFoundError(f"Model artifact not found: {candidate}")
        return candidate

    model_root = Path(args.model_root)
    latest_file = model_root / "latest_model_path.txt"
    if latest_file.exists():
        pointer_target = Path(latest_file.read_text(encoding="utf-8").strip())
        if pointer_target.exists():
            return pointer_target

    candidates = sorted(
        model_root.glob("**/model.pt"),
        key=lambda value: value.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            "No model artifact found. Train first with models/train_sales_predictor.py."
        )
    return candidates[0]


def _build_model(torch_module: Any, input_dim: int) -> Any:
    return torch_module.nn.Sequential(
        torch_module.nn.Linear(input_dim, 32),
        torch_module.nn.ReLU(),
        torch_module.nn.Linear(32, 16),
        torch_module.nn.ReLU(),
        torch_module.nn.Linear(16, 1),
    )


def _regression_metrics(
    actual: np.ndarray,
    predicted: np.ndarray,
) -> dict[str, float]:
    diff = predicted - actual
    mae = float(np.mean(np.abs(diff)))
    rmse = float(np.sqrt(np.mean(np.square(diff))))
    mse = float(np.mean(np.square(diff)))
    return {"mae": mae, "rmse": rmse, "mse": mse}


def run_scoring(args: argparse.Namespace) -> tuple[Path, Path]:
    torch = _load_torch()
    model_path = _resolve_model_path(args)

    checkpoint = torch.load(model_path, map_location="cpu")
    if not isinstance(checkpoint, dict):
        raise ValueError(f"Invalid checkpoint format in {model_path}")

    feature_mean = np.asarray(checkpoint["feature_mean"], dtype=np.float32)
    feature_std = np.asarray(checkpoint["feature_std"], dtype=np.float32)

    records = load_gold_daily_revenue(args.gold_path)
    features, targets, store_ids, event_dates = build_lagged_sales_examples(records)
    features_norm = (features - feature_mean) / feature_std

    device_name = "cpu"
    if args.device == "auto" and torch.cuda.is_available():
        device_name = "cuda"
    device = torch.device(device_name)

    model = _build_model(torch, features_norm.shape[1]).to(device)
    model.load_state_dict(checkpoint["state_dict"])
    model.eval()

    with torch.no_grad():
        feature_tensor = torch.tensor(features_norm, dtype=torch.float32, device=device)
        predicted = model(feature_tensor).squeeze(-1).cpu().numpy()

    metrics = _regression_metrics(targets, predicted)

    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for idx in range(len(predicted)):
            record = {
                "store_id": store_ids[idx],
                "event_date": event_dates[idx].isoformat(),
                "actual_daily_revenue": float(targets[idx]),
                "predicted_daily_revenue": float(predicted[idx]),
                "error": float(predicted[idx] - targets[idx]),
            }
            handle.write(json.dumps(record) + "\n")

    summary_path = output_path.with_suffix(".summary.json")
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_path": str(model_path),
        "output_path": str(output_path),
        "rows_scored": int(len(predicted)),
        "metrics": metrics,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOGGER.info(
        "scoring_complete output=%s rows=%s mae=%.4f rmse=%.4f",
        output_path,
        len(predicted),
        metrics["mae"],
        metrics["rmse"],
    )
    return output_path, summary_path


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    run_scoring(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
