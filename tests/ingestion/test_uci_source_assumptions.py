import csv
import gzip

import pytest

from ingestion.real.uci_online_retail import (
    convert_uci_csv,
    map_uci_row,
    resolve_column_mapping,
)


def source():
    return {
        "Invoice": "536365",
        "StockCode": "85123A",
        "Quantity": "6",
        "InvoiceDate": "12/1/2010 08:26",
        "Price": "2.55",
        "Customer ID": "17850",
        "Country": "United Kingdom",
    }


@pytest.mark.parametrize("customer", ["NaN", "Infinity", "17850.5", "abc"])
def test_uci_invalid_customer_never_becomes_a_fabricated_id(customer):
    row = {**source(), "Customer ID": customer}
    assert (
        map_uci_row(
            row,
            resolve_column_mapping(list(row)),
            currency="GBP",
            payment_method="credit_card",
            channel="online",
        )
        is None
    )


def test_calendar_order_is_explicit_instead_of_guessed():
    row = source()
    kwargs = dict(currency="GBP", payment_method="credit_card", channel="online")
    month_first = map_uci_row(row, resolve_column_mapping(list(row)), **kwargs)
    day_first = map_uci_row(
        row,
        resolve_column_mapping(list(row)),
        timestamp_format="%d/%m/%Y %H:%M",
        **kwargs,
    )
    assert month_first["ts_utc"] == "2010-12-01T08:26:00Z"
    assert day_first["ts_utc"] == "2010-01-12T08:26:00Z"


def test_converter_defaults_to_actual_sterling_currency(tmp_path):
    path, output = tmp_path / "source.csv", tmp_path / "normalized.csv.gz"
    row = source()
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    convert_uci_csv(input_path=path, output_path=output, input_encoding="utf-8")
    with gzip.open(output, "rt", encoding="utf-8") as handle:
        normalized = next(csv.DictReader(handle))
    assert normalized["currency"] == "GBP" and normalized["unit_price"] == "2.55"
