"""Validate dbt governance contracts for Phase 2."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any

import yaml


def _load_yaml(path: Path) -> dict[str, Any]:
    content = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(content, dict):
        raise ValueError(f"Expected mapping content in {path}")
    return content


def _validate_model_contracts(
    schema_doc: dict[str, Any],
    *,
    required_tag: str,
    path_label: str,
    require_contract_name: bool = True,
) -> list[str]:
    errors: list[str] = []
    models = schema_doc.get("models", [])
    if not isinstance(models, list) or not models:
        errors.append(f"{path_label}: no models declared.")
        return errors

    for model in models:
        if not isinstance(model, dict):
            errors.append(f"{path_label}: invalid model entry type.")
            continue

        model_name = str(model.get("name", "unknown_model"))
        config = model.get("config", {})
        if not isinstance(config, dict):
            errors.append(f"{path_label}:{model_name}: missing config mapping.")
            continue

        tags = config.get("tags", [])
        if not isinstance(tags, list) or required_tag not in tags:
            errors.append(
                f"{path_label}:{model_name}: tag '{required_tag}' is required."
            )

        meta = config.get("meta", {})
        if not isinstance(meta, dict):
            errors.append(f"{path_label}:{model_name}: missing meta mapping.")
            continue

        owner = str(meta.get("owner", "")).strip()
        if not owner:
            errors.append(f"{path_label}:{model_name}: meta.owner is required.")

        if require_contract_name:
            contract_name = str(meta.get("contract_name", "")).strip()
            if not contract_name:
                errors.append(
                    f"{path_label}:{model_name}: meta.contract_name is required."
                )

    return errors


def _validate_semantic_assets(semantic_doc: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    semantic_models = semantic_doc.get("semantic_models", [])
    metrics = semantic_doc.get("metrics", [])
    if not isinstance(semantic_models, list) or not semantic_models:
        errors.append("semantic_models.yml: at least one semantic model is required.")
    if not isinstance(metrics, list) or not metrics:
        errors.append("semantic_models.yml: at least one metric is required.")
    return errors


def _validate_exposure(exposure_doc: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    exposures = exposure_doc.get("exposures", [])
    if not isinstance(exposures, list) or not exposures:
        return ["exposures.yml: at least one exposure is required."]

    dashboard = next(
        (
            exposure
            for exposure in exposures
            if isinstance(exposure, dict)
            and exposure.get("name") == "executive_retail_dashboard"
        ),
        None,
    )
    if dashboard is None:
        errors.append("exposures.yml: executive_retail_dashboard exposure is required.")
        return errors

    depends_on = dashboard.get("depends_on", [])
    owner = dashboard.get("owner", {})
    if not isinstance(depends_on, list) or len(depends_on) < 1:
        errors.append(
            "exposures.yml: executive_retail_dashboard must depend on models."
        )
    if not isinstance(owner, dict) or not str(owner.get("email", "")).strip():
        errors.append(
            "exposures.yml: executive_retail_dashboard owner email is required."
        )
    return errors


def run_validation(root: Path) -> list[str]:
    marts_schema = _load_yaml(
        root / "warehouse" / "dbt" / "models" / "marts" / "schema.yml"
    )
    metrics_schema = _load_yaml(
        root / "warehouse" / "dbt" / "models" / "metrics" / "schema.yml"
    )
    semantic_doc = _load_yaml(
        root / "warehouse" / "dbt" / "models" / "semantic" / "semantic_models.yml"
    )
    exposure_doc = _load_yaml(root / "warehouse" / "dbt" / "models" / "exposures.yml")

    errors: list[str] = []
    errors.extend(
        _validate_model_contracts(
            marts_schema,
            required_tag="governed",
            path_label="marts/schema.yml",
        )
    )
    errors.extend(
        _validate_model_contracts(
            metrics_schema,
            required_tag="governed",
            path_label="metrics/schema.yml",
        )
    )
    errors.extend(_validate_semantic_assets(semantic_doc))
    errors.extend(_validate_exposure(exposure_doc))
    return errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate dbt Phase 2 governance assets."
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root directory.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = Path(args.repo_root).resolve()
    errors = run_validation(root)
    if errors:
        print("dbt governance validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("dbt governance validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
