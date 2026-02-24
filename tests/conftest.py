"""Test harness configuration."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

spark_init = REPO_ROOT / "spark" / "__init__.py"
spark_spec = importlib.util.spec_from_file_location(
    "spark",
    spark_init,
    submodule_search_locations=[str(REPO_ROOT / "spark")],
)
if spark_spec is not None and spark_spec.loader is not None:
    spark_module = importlib.util.module_from_spec(spark_spec)
    sys.modules["spark"] = spark_module
    spark_spec.loader.exec_module(spark_module)
