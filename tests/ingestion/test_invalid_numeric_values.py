import math
import pytest

from ingestion.generator.generator import TransactionGenerator
from ingestion.generator.models import validate_transaction_payload
from ingestion.real.uci_online_retail import map_uci_row, resolve_column_mapping


@pytest.mark.parametrize("quantity", [True, "1.9", 1.9, "NaN", "Infinity", 2147483648])
def test_payload_does_not_truncate_or_accept_nonfinite_quantity(quantity):
    payload = TransactionGenerator().generate_event().to_serializable_dict()
    payload["quantity"] = quantity
    with pytest.raises(ValueError, match="quantity"):
        validate_transaction_payload(payload)


@pytest.mark.parametrize("price", [math.nan, math.inf, -math.inf])
def test_payload_requires_finite_price(price):
    payload = TransactionGenerator().generate_event().to_serializable_dict()
    payload["unit_price"] = price
    with pytest.raises(ValueError, match="unit_price"):
        validate_transaction_payload(payload)


@pytest.mark.parametrize("field", ["store_id", "customer_id", "product_id", "currency"])
def test_null_identifier_is_not_stringified_to_none(field):
    payload = TransactionGenerator().generate_event().to_serializable_dict()
    payload[field] = None
    with pytest.raises(ValueError, match=field):
        validate_transaction_payload(payload)


@pytest.mark.parametrize("price", ["NaN", "Infinity", "-Infinity"])
def test_uci_rejects_nonfinite_prices(price):
    row = {
        "Invoice": "536365",
        "StockCode": "85123A",
        "Quantity": "6",
        "InvoiceDate": "12/1/2010 08:26",
        "Price": price,
        "Customer ID": "17850",
        "Country": "United Kingdom",
    }
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
