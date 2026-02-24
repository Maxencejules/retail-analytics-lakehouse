"""Schema validation tests for generated transaction events."""

from __future__ import annotations

from datetime import datetime, timezone
import uuid

import pytest

from ingestion.generator.models import TransactionEvent, validate_transaction_payload


def _valid_payload() -> dict[str, object]:
    return {
        "transaction_id": str(uuid.uuid4()),
        "ts_utc": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
        "store_id": "STORE-0001",
        "customer_id": "CUST-00000001",
        "product_id": "PROD-000001",
        "quantity": 2,
        "unit_price": 19.99,
        "currency": "CAD",
        "payment_method": "credit_card",
        "channel": "online",
        "promo_id": None,
    }


def test_valid_payload_creates_transaction_event() -> None:
    event = validate_transaction_payload(_valid_payload())

    assert isinstance(event, TransactionEvent)
    assert event.quantity == 2
    assert event.unit_price == 19.99
    assert event.channel == "online"


def test_invalid_uuid_is_rejected() -> None:
    payload = _valid_payload()
    payload["transaction_id"] = "not-a-uuid"

    with pytest.raises(ValueError, match="transaction_id"):
        validate_transaction_payload(payload)


def test_non_utc_timestamp_is_rejected() -> None:
    payload = _valid_payload()
    payload["ts_utc"] = datetime(2025, 1, 1, 12, 0)

    with pytest.raises(ValueError, match="timezone-aware"):
        validate_transaction_payload(payload)


def test_quantity_must_be_positive() -> None:
    payload = _valid_payload()
    payload["quantity"] = 0

    with pytest.raises(ValueError, match="quantity must be > 0"):
        validate_transaction_payload(payload)


def test_unit_price_must_be_positive() -> None:
    payload = _valid_payload()
    payload["unit_price"] = 0

    with pytest.raises(ValueError, match="unit_price must be > 0"):
        validate_transaction_payload(payload)


def test_invalid_channel_is_rejected() -> None:
    payload = _valid_payload()
    payload["channel"] = "call_center"

    with pytest.raises(ValueError, match="channel"):
        validate_transaction_payload(payload)


def test_invalid_payment_method_is_rejected() -> None:
    payload = _valid_payload()
    payload["payment_method"] = "crypto"

    with pytest.raises(ValueError, match="payment_method"):
        validate_transaction_payload(payload)

