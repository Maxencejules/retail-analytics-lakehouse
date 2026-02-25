"""Tests for UCI Online Retail normalization into canonical transactions."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import gzip
from pathlib import Path
import uuid

import pytest

from ingestion.real.uci_online_retail import (
    build_transaction_id,
    convert_uci_csv,
    map_uci_row,
    resolve_column_mapping,
)


def _uci_headers() -> list[str]:
    return [
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Customer ID",
        "Country",
    ]


def _uci_row(**overrides: str) -> dict[str, str]:
    row = {
        "Invoice": "536365",
        "StockCode": "85123A",
        "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
        "Quantity": "6",
        "InvoiceDate": "12/1/2010 08:26",
        "Price": "2.55",
        "Customer ID": "17850.0",
        "Country": "United Kingdom",
    }
    row.update(overrides)
    return row


def test_resolve_column_mapping_handles_common_header_variants() -> None:
    mapping = resolve_column_mapping(
        [
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        ]
    )

    assert mapping["invoice_no"] == "InvoiceNo"
    assert mapping["unit_price"] == "UnitPrice"
    assert mapping["customer_id"] == "CustomerID"


def test_map_uci_row_returns_canonical_payload() -> None:
    mapping = resolve_column_mapping(_uci_headers())
    payload = map_uci_row(
        _uci_row(),
        mapping,
        currency="USD",
        payment_method="credit_card",
        channel="online",
    )

    assert payload is not None
    assert payload["store_id"] == "STORE-UNITED_KINGDOM"
    assert payload["customer_id"] == "CUST-00017850"
    assert payload["product_id"] == "PROD-85123A"
    assert payload["currency"] == "USD"
    assert payload["payment_method"] == "credit_card"
    assert payload["channel"] == "online"
    assert payload["promo_id"] is None
    assert payload["ts_utc"].endswith("Z")


@pytest.mark.parametrize(
    "override",
    [
        {"Invoice": "C536365"},
        {"Quantity": "0"},
        {"Price": "-1.00"},
        {"Customer ID": ""},
        {"InvoiceDate": "not-a-date"},
    ],
)
def test_map_uci_row_drops_invalid_or_return_rows(override: dict[str, str]) -> None:
    mapping = resolve_column_mapping(_uci_headers())
    payload = map_uci_row(
        _uci_row(**override),
        mapping,
        currency="USD",
        payment_method="credit_card",
        channel="online",
    )
    assert payload is None


def test_build_transaction_id_is_deterministic_uuid() -> None:
    kwargs = {
        "invoice_no": "536365",
        "stock_code": "85123A",
        "invoice_ts_utc": datetime(2010, 12, 1, 8, 26, tzinfo=timezone.utc),
        "customer_id": "CUST-00017850",
        "quantity": 6,
        "unit_price": 2.55,
    }
    first = build_transaction_id(**kwargs)
    second = build_transaction_id(**kwargs)

    assert first == second
    assert str(uuid.UUID(first)) == first


def test_convert_uci_csv_writes_gzip_with_canonical_header(tmp_path: Path) -> None:
    source_path = tmp_path / "uci.csv"
    output_path = tmp_path / "transactions.csv.gz"

    with source_path.open(mode="w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_uci_headers())
        writer.writeheader()
        writer.writerow(_uci_row())
        writer.writerow(_uci_row(Quantity="-1"))

    stats = convert_uci_csv(
        input_path=source_path,
        output_path=output_path,
        input_encoding="utf-8",
        currency="CAD",
        payment_method="debit_card",
        channel="online",
    )

    assert stats.input_rows == 2
    assert stats.output_rows == 1
    assert stats.dropped_rows == 1

    with gzip.open(output_path, mode="rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert len(rows) == 1
    assert rows[0]["currency"] == "CAD"
    assert rows[0]["payment_method"] == "debit_card"
