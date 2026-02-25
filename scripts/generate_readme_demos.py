"""Generate README demo screenshots from a real local run."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import date
import gzip
import html
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from playwright.sync_api import sync_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate real terminal/dashboard demo screenshots for README."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=400,
        help="Synthetic rows to generate for the dashboard snapshot.",
    )
    parser.add_argument(
        "--output-dir",
        default="docs-site/assets/screenshots",
        help="Directory where README demo screenshots are written.",
    )
    parser.add_argument(
        "--work-dir",
        default=".tmp/readme-demo",
        help="Scratch directory for generated demo inputs.",
    )
    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=8512,
        help="Streamlit port used for screenshot capture.",
    )
    return parser


def _run_command(
    command: list[str],
    *,
    env: dict[str, str],
    transcript: list[str],
) -> None:
    transcript.append(f"$ {' '.join(command)}")
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    merged_output = f"{completed.stdout}\n{completed.stderr}".strip()
    output_lines = [line.rstrip() for line in merged_output.splitlines() if line.strip()]
    max_lines = 24
    for line in output_lines[:max_lines]:
        transcript.append(line)
    if len(output_lines) > max_lines:
        transcript.append(f"... ({len(output_lines) - max_lines} additional lines omitted)")

    transcript.append(f"[exit {completed.returncode}]")
    transcript.append("")

    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {completed.returncode}: {' '.join(command)}"
        )


def _write_gold_datasets(csv_path: Path, gold_root: Path) -> tuple[int, int]:
    daily_store: dict[tuple[date, str], dict[str, float]] = defaultdict(
        lambda: {
            "daily_revenue": 0.0,
            "units_sold": 0.0,
            "transaction_count": 0.0,
        }
    )
    by_product: dict[tuple[date, str], dict[str, float]] = defaultdict(
        lambda: {"daily_revenue": 0.0, "transaction_count": 0.0}
    )

    with gzip.open(csv_path, mode="rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            event_date = date.fromisoformat(row["ts_utc"][:10])
            store_id = row["store_id"]
            product_id = row["product_id"]
            quantity = float(row["quantity"])
            unit_price = float(row["unit_price"])
            revenue = quantity * unit_price

            store_key = (event_date, store_id)
            daily_store[store_key]["daily_revenue"] += revenue
            daily_store[store_key]["units_sold"] += quantity
            daily_store[store_key]["transaction_count"] += 1.0

            product_key = (event_date, product_id)
            by_product[product_key]["daily_revenue"] += revenue
            by_product[product_key]["transaction_count"] += 1.0

    daily_rows = []
    for (event_date, store_id), metrics in sorted(daily_store.items()):
        daily_rows.append(
            {
                "event_date": event_date,
                "store_id": store_id,
                "daily_revenue": float(round(metrics["daily_revenue"], 2)),
                "units_sold": float(metrics["units_sold"]),
                "transaction_count": float(metrics["transaction_count"]),
            }
        )

    products_by_date: dict[date, list[dict[str, float | str | date]]] = defaultdict(list)
    for (event_date, product_id), metrics in by_product.items():
        products_by_date[event_date].append(
            {
                "event_date": event_date,
                "product_id": product_id,
                "daily_revenue": float(round(metrics["daily_revenue"], 2)),
                "transaction_count": float(metrics["transaction_count"]),
            }
        )

    top_product_rows = []
    for event_date, product_rows in products_by_date.items():
        ranked = sorted(product_rows, key=lambda item: item["daily_revenue"], reverse=True)[:10]
        top_product_rows.extend(ranked)

    if gold_root.exists():
        shutil.rmtree(gold_root)

    daily_path = gold_root / "daily_revenue_by_store"
    top_products_path = gold_root / "top_10_products_by_day"
    daily_path.mkdir(parents=True, exist_ok=True)
    top_products_path.mkdir(parents=True, exist_ok=True)

    # Keep event_date as an in-file column so DuckDB read_parquet globs work out-of-the-box.
    pq.write_table(pa.Table.from_pylist(daily_rows), daily_path / "part-00001.parquet")
    pq.write_table(
        pa.Table.from_pylist(top_product_rows),
        top_products_path / "part-00001.parquet",
    )
    return len(daily_rows), len(top_product_rows)


def _wait_for_http(url: str, timeout_seconds: int = 120) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status < 500:
                    return
        except Exception:
            pass
        time.sleep(1.0)
    raise TimeoutError(f"Timed out waiting for dashboard URL: {url}")


def _capture_terminal_image(lines: list[str], output_path: Path) -> None:
    transcript = "\n".join(lines)
    content = f"""
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <style>
      body {{
        margin: 0;
        background: #111111;
        font-family: "Cascadia Code", "Consolas", monospace;
      }}
      .window {{
        width: 1280px;
        height: 720px;
        border-radius: 10px;
        overflow: hidden;
        border: 1px solid #2a2a2a;
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.45);
      }}
      .bar {{
        height: 44px;
        background: #1f1f1f;
        display: flex;
        align-items: center;
        justify-content: space-between;
        border-bottom: 1px solid #2a2a2a;
      }}
      .bar-left {{
        display: flex;
        align-items: center;
        gap: 8px;
        padding-left: 10px;
      }}
      .tab {{
        height: 30px;
        min-width: 300px;
        background: #2b2b2b;
        border: 1px solid #3a3a3a;
        border-bottom-color: #2b2b2b;
        border-radius: 6px 6px 0 0;
        color: #f3f3f3;
        display: flex;
        align-items: center;
        padding: 0 12px;
        font-size: 13px;
      }}
      .tab-icon {{
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 2px;
        background: #3b82f6;
        margin-right: 8px;
      }}
      .tab-close {{
        margin-left: auto;
        color: #a3a3a3;
      }}
      .tab-add {{
        color: #d4d4d4;
        font-size: 18px;
        line-height: 1;
        margin-left: 2px;
      }}
      .controls {{
        display: flex;
        align-items: stretch;
      }}
      .control {{
        width: 46px;
        height: 44px;
        color: #d4d4d4;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 14px;
      }}
      .control.close {{
        background: #c42b1c;
        color: #ffffff;
      }}
      pre {{
        margin: 0;
        padding: 18px 20px;
        color: #f5f5f5;
        font-size: 16px;
        line-height: 1.4;
        white-space: pre-wrap;
        word-break: break-word;
        background: #0c0c0c;
        height: calc(720px - 44px);
        box-sizing: border-box;
      }}
    </style>
  </head>
  <body>
    <div class="window">
      <div class="bar">
        <div class="bar-left">
          <div class="tab">
            <span class="tab-icon"></span>
            Windows PowerShell
            <span class="tab-close">x</span>
          </div>
          <span class="tab-add">+</span>
        </div>
        <div class="controls">
          <span class="control">-</span>
          <span class="control">[]</span>
          <span class="control close">x</span>
        </div>
      </div>
      <pre>{html.escape(transcript)}</pre>
    </div>
  </body>
</html>
""".strip()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 720})
        page.set_content(content, wait_until="domcontentloaded")
        page.screenshot(path=str(output_path))
        browser.close()


def _capture_dashboard_image(url: str, output_path: Path) -> None:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 980})
        page.goto(url, wait_until="networkidle", timeout=120000)
        page.wait_for_function(
            """() => {
                const text = document.body ? document.body.innerText : "";
                return (
                    text.includes("Retail Performance Cockpit") ||
                    text.includes("Retail Loyalty Executive Dashboard")
                );
            }""",
            timeout=120000,
        )
        page.wait_for_timeout(2500)
        page.screenshot(path=str(output_path), full_page=True)
        browser.close()


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    output_dir = (REPO_ROOT / args.output_dir).resolve()
    work_dir = (REPO_ROOT / args.work_dir).resolve()
    runtime_tmp = work_dir / "tmp"
    generated_dir = work_dir / "generated"
    gold_dir = work_dir / "gold"

    output_dir.mkdir(parents=True, exist_ok=True)
    runtime_tmp.mkdir(parents=True, exist_ok=True)
    generated_dir.mkdir(parents=True, exist_ok=True)

    os.environ["TMP"] = str(runtime_tmp)
    os.environ["TEMP"] = str(runtime_tmp)
    os.environ["TMPDIR"] = str(runtime_tmp)

    runtime_env = os.environ.copy()
    runtime_env["TMP"] = str(runtime_tmp)
    runtime_env["TEMP"] = str(runtime_tmp)
    runtime_env["TMPDIR"] = str(runtime_tmp)

    transcript: list[str] = [
        "$ python --version",
        sys.version.split()[0],
        "[exit 0]",
        "",
    ]

    _run_command(
        [
            sys.executable,
            "ingestion/generator/generate.py",
            "--mode",
            "batch",
            "--rows",
            str(args.rows),
            "--seed",
            "42",
            "--output-dir",
            str(generated_dir),
        ],
        env=runtime_env,
        transcript=transcript,
    )

    csv_path = generated_dir / "transactions.csv.gz"
    daily_rows, top_product_rows = _write_gold_datasets(csv_path, gold_dir)

    transcript.append("$ python -m streamlit --version")
    streamlit_version = subprocess.run(
        [sys.executable, "-m", "streamlit", "--version"],
        cwd=REPO_ROOT,
        env=runtime_env,
        capture_output=True,
        text=True,
        check=False,
    )
    streamlit_output = (streamlit_version.stdout + streamlit_version.stderr).strip()
    transcript.append(streamlit_output or "streamlit version output unavailable")
    transcript.append(f"[exit {streamlit_version.returncode}]")
    transcript.append("")
    transcript.append(
        f"Gold demo dataset ready: {daily_rows} daily rows, {top_product_rows} top-product rows."
    )
    transcript.append("")

    dashboard_url = f"http://127.0.0.1:{args.dashboard_port}"
    dashboard_env = runtime_env.copy()
    dashboard_env["DASHBOARD_DATA_SOURCE"] = "gold"
    dashboard_env["GOLD_BASE_PATH"] = str(gold_dir)

    dashboard_process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "dashboard/app.py",
            "--server.headless",
            "true",
            "--browser.gatherUsageStats",
            "false",
            "--server.port",
            str(args.dashboard_port),
        ],
        cwd=REPO_ROOT,
        env=dashboard_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        _wait_for_http(dashboard_url)
        dashboard_output = output_dir / "quickstart-dashboard.png"
        _capture_dashboard_image(dashboard_url, dashboard_output)
    finally:
        _stop_process(dashboard_process)

    transcript.append(f"Dashboard captured from: {dashboard_url}")
    transcript.append(f"Dashboard image: {output_dir / 'quickstart-dashboard.png'}")

    terminal_output = output_dir / "quickstart-terminal.png"
    _capture_terminal_image(transcript, terminal_output)

    print(f"Generated: {terminal_output}")
    print(f"Generated: {output_dir / 'quickstart-dashboard.png'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
