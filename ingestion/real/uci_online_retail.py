"""Normalize UCI Online Retail rows into the canonical transaction schema."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import logging
from pathlib import Path
import re
import sys
import uuid
from typing import Mapping, Sequence

if __package__ in {None, ""}:
    # Allows: python ingestion/real/uci_online_retail.py.
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from ingestion.generator.logging_utils import configure_logging
from ingestion.generator.models import TRANSACTION_FIELD_ORDER

ALLOWED_CURRENCIES: tuple[str, ...] = ("CAD", "USD")
ALLOWED_PAYMENT_METHODS: tuple[str, ...] = (
    "credit_card",
    "debit_card",
    "cash",
    "mobile_wallet",
)
ALLOWED_CHANNELS: tuple[str, ...] = ("online", "store")

TRANSACTION_NAMESPACE = uuid.UUID("f41b0646-90c7-4f64-8f32-4de18d76d5f6")

SOURCE_REQUIRED_COLUMNS: tuple[str, ...] = (
    "invoice_no",
    "stock_code",
    "quantity",
    "invoice_ts",
    "unit_price",
    "customer_id",
    "country",
)

SOURCE_HEADER_CANDIDATES: dict[str, tuple[str, ...]] = {
    "invoice_no": ("invoice", "invoiceno", "invoice_no"),
    "stock_code": ("stockcode", "stock_code"),
    "quantity": ("quantity", "qty"),
    "invoice_ts": ("invoicedate", "invoice_date", "invoice_ts"),
    "unit_price": ("price", "unitprice", "unit_price"),
    "customer_id": ("customerid", "customer_id"),
    "country": ("country",),
}

TIMESTAMP_FORMATS: tuple[str, ...] = (
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
)


@dataclass(slots=True, frozen=True)
class ConversionStats:
    """Summary metrics for a normalization run."""

    input_rows: int
    output_rows: int
    dropped_rows: int


def _normalize_header(header: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", header.strip().lower())


def _sanitize_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", value.strip().upper())


def _parse_integer(value: str) -> int | None:
    cleaned = value.strip()
    if not cleaned:
        return None

    try:
        parsed = float(cleaned)
    except ValueError:
        return None

    if not parsed.is_integer():
        return None
    return int(parsed)


def _parse_float(value: str) -> float | None:
    cleaned = value.strip()
    if not cleaned:
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_invoice_timestamp(value: str) -> datetime | None:
    cleaned = value.strip()
    if not cleaned:
        return None

    for fmt in TIMESTAMP_FORMATS:
        try:
            parsed = datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)

    return None


def _normalize_customer_id(value: str) -> str | None:
    cleaned = value.strip()
    if not cleaned:
        return None

    parsed = _parse_float(cleaned)
    if parsed is not None:
        if parsed <= 0:
            return None
        if parsed.is_integer():
            return f"CUST-{int(parsed):08d}"
        digits = re.sub(r"[^0-9]+", "", cleaned)
        if digits:
            return f"CUST-{digits}"
        return None

    token = _sanitize_token(cleaned)
    if not token:
        return None
    return f"CUST-{token}"


def _normalize_product_id(value: str) -> str | None:
    token = _sanitize_token(value)
    if not token:
        return None
    return f"PROD-{token}"


def _normalize_store_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", value.strip().upper()).strip("_")
    token = cleaned if cleaned else "UNKNOWN"
    return f"STORE-{token[:32]}"


def build_transaction_id(
    *,
    invoice_no: str,
    stock_code: str,
    invoice_ts_utc: datetime,
    customer_id: str,
    quantity: int,
    unit_price: float,
) -> str:
    """Build a deterministic UUID for each normalized transaction row."""
    key = "|".join(
        [
            invoice_no.strip().upper(),
            stock_code.strip().upper(),
            invoice_ts_utc.isoformat(),
            customer_id,
            str(quantity),
            f"{unit_price:.4f}",
        ]
    )
    return str(uuid.uuid5(TRANSACTION_NAMESPACE, key))


def resolve_column_mapping(fieldnames: Sequence[str]) -> dict[str, str]:
    """Resolve source headers to canonical Online Retail fields."""
    normalized_to_original: dict[str, str] = {}
    for field in fieldnames:
        normalized = _normalize_header(field)
        if normalized and normalized not in normalized_to_original:
            normalized_to_original[normalized] = field

    mapping: dict[str, str] = {}
    missing: list[str] = []
    for canonical in SOURCE_REQUIRED_COLUMNS:
        candidates = SOURCE_HEADER_CANDIDATES[canonical]
        matched = next(
            (
                normalized_to_original[name]
                for name in candidates
                if name in normalized_to_original
            ),
            None,
        )
        if matched is None:
            missing.append(canonical)
            continue
        mapping[canonical] = matched

    if missing:
        raise ValueError(f"missing required columns: {', '.join(missing)}")
    return mapping


def map_uci_row(
    row: Mapping[str, str],
    column_mapping: Mapping[str, str],
    *,
    currency: str,
    payment_method: str,
    channel: str,
) -> dict[str, object] | None:
    """Map one source row into the canonical transaction schema."""
    invoice_no = row[column_mapping["invoice_no"]].strip()
    if not invoice_no or invoice_no.upper().startswith("C"):
        return None

    quantity = _parse_integer(row[column_mapping["quantity"]])
    unit_price = _parse_float(row[column_mapping["unit_price"]])
    invoice_ts_utc = _parse_invoice_timestamp(row[column_mapping["invoice_ts"]])

    if quantity is None or unit_price is None or invoice_ts_utc is None:
        return None
    if quantity <= 0 or unit_price <= 0:
        return None

    customer_id = _normalize_customer_id(row[column_mapping["customer_id"]])
    product_id = _normalize_product_id(row[column_mapping["stock_code"]])
    if customer_id is None or product_id is None:
        return None

    country = row[column_mapping["country"]]
    store_id = _normalize_store_id(country)

    transaction_id = build_transaction_id(
        invoice_no=invoice_no,
        stock_code=row[column_mapping["stock_code"]],
        invoice_ts_utc=invoice_ts_utc,
        customer_id=customer_id,
        quantity=quantity,
        unit_price=unit_price,
    )

    return {
        "transaction_id": transaction_id,
        "ts_utc": invoice_ts_utc.isoformat().replace("+00:00", "Z"),
        "store_id": store_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": round(unit_price, 2),
        "currency": currency,
        "payment_method": payment_method,
        "channel": channel,
        "promo_id": None,
    }


def convert_uci_csv(
    *,
    input_path: Path,
    output_path: Path,
    input_encoding: str = "ISO-8859-1",
    currency: str = "USD",
    payment_method: str = "credit_card",
    channel: str = "online",
) -> ConversionStats:
    """Normalize a UCI CSV export into a compressed canonical transaction CSV."""
    currency_value = currency.upper()
    payment_method_value = payment_method.lower()
    channel_value = channel.lower()

    if currency_value not in ALLOWED_CURRENCIES:
        raise ValueError(f"currency must be one of {sorted(ALLOWED_CURRENCIES)}")
    if payment_method_value not in ALLOWED_PAYMENT_METHODS:
        raise ValueError(
            f"payment_method must be one of {sorted(ALLOWED_PAYMENT_METHODS)}"
        )
    if channel_value not in ALLOWED_CHANNELS:
        raise ValueError(f"channel must be one of {sorted(ALLOWED_CHANNELS)}")

    with input_path.open(mode="r", encoding=input_encoding, newline="") as input_handle:
        reader = csv.DictReader(input_handle)
        if reader.fieldnames is None:
            raise ValueError("input CSV has no header row")

        column_mapping = resolve_column_mapping(reader.fieldnames)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        input_rows = 0
        output_rows = 0
        with gzip.open(
            output_path, mode="wt", encoding="utf-8", newline=""
        ) as output_handle:
            writer = csv.DictWriter(
                output_handle,
                fieldnames=list(TRANSACTION_FIELD_ORDER),
            )
            writer.writeheader()

            for row in reader:
                input_rows += 1
                mapped = map_uci_row(
                    row,
                    column_mapping,
                    currency=currency_value,
                    payment_method=payment_method_value,
                    channel=channel_value,
                )
                if mapped is None:
                    continue
                writer.writerow(mapped)
                output_rows += 1

    if output_rows == 0:
        raise ValueError("no valid transaction rows were produced from input")

    return ConversionStats(
        input_rows=input_rows,
        output_rows=output_rows,
        dropped_rows=input_rows - output_rows,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize UCI Online Retail CSV rows into canonical transactions."
    )
    parser.add_argument(
        "--input-path",
        required=True,
        help="Path to the source UCI CSV export.",
    )
    parser.add_argument(
        "--output-path",
        default="data/generated/transactions.csv.gz",
        help="Destination path for normalized transactions.csv.gz.",
    )
    parser.add_argument(
        "--input-encoding",
        default="ISO-8859-1",
        help="Input CSV encoding (Online Retail exports are often ISO-8859-1).",
    )
    parser.add_argument(
        "--currency",
        default="USD",
        choices=ALLOWED_CURRENCIES,
        help="Output currency label. Prices are not FX-converted.",
    )
    parser.add_argument(
        "--payment-method",
        default="credit_card",
        choices=ALLOWED_PAYMENT_METHODS,
        help="Default payment method for source rows that do not provide one.",
    )
    parser.add_argument(
        "--channel",
        default="online",
        choices=ALLOWED_CHANNELS,
        help="Default sales channel for source rows that do not provide one.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Log verbosity level.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)

    logger = logging.getLogger("ingestion.real.uci_online_retail")
    stats = convert_uci_csv(
        input_path=Path(args.input_path),
        output_path=Path(args.output_path),
        input_encoding=args.input_encoding,
        currency=args.currency,
        payment_method=args.payment_method,
        channel=args.channel,
    )
    logger.info(
        "uci_online_retail_conversion_complete",
        extra={
            "context": {
                "input_path": args.input_path,
                "output_path": args.output_path,
                "input_rows": stats.input_rows,
                "output_rows": stats.output_rows,
                "dropped_rows": stats.dropped_rows,
                "currency": args.currency,
                "payment_method": args.payment_method,
                "channel": args.channel,
            }
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
