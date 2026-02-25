"""Transaction event schema and validation utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from uuid import UUID

ALLOWED_CHANNELS: frozenset[str] = frozenset({"online", "store"})
ALLOWED_PAYMENT_METHODS: frozenset[str] = frozenset(
    {"credit_card", "debit_card", "cash", "mobile_wallet"}
)

TRANSACTION_FIELD_ORDER: tuple[str, ...] = (
    "transaction_id",
    "ts_utc",
    "store_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "currency",
    "payment_method",
    "channel",
    "promo_id",
)


def _ensure_non_empty_string(name: str, value: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")

    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{name} must not be empty")
    return stripped


def _parse_iso_utc(ts_value: str) -> datetime:
    ts_clean = ts_value.strip()
    if ts_clean.endswith("Z"):
        ts_clean = f"{ts_clean[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(ts_clean)
    except ValueError as exc:
        raise ValueError("ts_utc must be a valid ISO-8601 timestamp") from exc
    return parsed


@dataclass(slots=True)
class TransactionEvent:
    """Canonical transaction event used by batch and streaming generation."""

    transaction_id: str
    ts_utc: datetime
    store_id: str
    customer_id: str
    product_id: str
    quantity: int
    unit_price: float
    currency: str
    payment_method: str
    channel: str
    promo_id: str | None = None

    def __post_init__(self) -> None:
        self._validate_transaction_id()
        self._validate_timestamp()
        self.store_id = _ensure_non_empty_string("store_id", self.store_id)
        self.customer_id = _ensure_non_empty_string("customer_id", self.customer_id)
        self.product_id = _ensure_non_empty_string("product_id", self.product_id)
        self.currency = _ensure_non_empty_string("currency", self.currency).upper()
        self.payment_method = _ensure_non_empty_string(
            "payment_method", self.payment_method
        ).lower()
        self.channel = _ensure_non_empty_string("channel", self.channel).lower()

        if self.channel not in ALLOWED_CHANNELS:
            raise ValueError(f"channel must be one of {sorted(ALLOWED_CHANNELS)}")

        if self.payment_method not in ALLOWED_PAYMENT_METHODS:
            raise ValueError(
                f"payment_method must be one of {sorted(ALLOWED_PAYMENT_METHODS)}"
            )

        if not isinstance(self.quantity, int):
            raise ValueError("quantity must be an integer")
        if self.quantity <= 0:
            raise ValueError("quantity must be > 0")

        if self.unit_price <= 0:
            raise ValueError("unit_price must be > 0")

        if self.promo_id is not None:
            self.promo_id = _ensure_non_empty_string("promo_id", self.promo_id)

    def _validate_transaction_id(self) -> None:
        try:
            UUID(self.transaction_id)
        except (TypeError, ValueError) as exc:
            raise ValueError("transaction_id must be a valid UUID string") from exc

    def _validate_timestamp(self) -> None:
        if not isinstance(self.ts_utc, datetime):
            raise ValueError("ts_utc must be a datetime value")

        if self.ts_utc.tzinfo is None:
            raise ValueError("ts_utc must be timezone-aware in UTC")
        if self.ts_utc.utcoffset() != timedelta(0):
            raise ValueError("ts_utc must be in UTC timezone")

        self.ts_utc = self.ts_utc.astimezone(timezone.utc)

    def as_dict(self) -> dict[str, Any]:
        """Return typed values suitable for Parquet and in-memory processing."""
        return {
            "transaction_id": self.transaction_id,
            "ts_utc": self.ts_utc,
            "store_id": self.store_id,
            "customer_id": self.customer_id,
            "product_id": self.product_id,
            "quantity": self.quantity,
            "unit_price": self.unit_price,
            "currency": self.currency,
            "payment_method": self.payment_method,
            "channel": self.channel,
            "promo_id": self.promo_id,
        }

    def to_serializable_dict(self) -> dict[str, Any]:
        """Return JSON/CSV-safe values."""
        payload = self.as_dict()
        payload["ts_utc"] = self.ts_utc.isoformat().replace("+00:00", "Z")
        return payload


def validate_transaction_payload(payload: Mapping[str, Any]) -> TransactionEvent:
    """Validate and normalize a payload into a typed TransactionEvent."""
    required_fields = set(TRANSACTION_FIELD_ORDER) - {"promo_id"}
    missing = sorted(field for field in required_fields if field not in payload)
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")

    ts_raw = payload["ts_utc"]
    if isinstance(ts_raw, datetime):
        ts_utc = ts_raw
    elif isinstance(ts_raw, str):
        ts_utc = _parse_iso_utc(ts_raw)
    else:
        raise ValueError("ts_utc must be datetime or ISO-8601 string")

    promo_raw = payload.get("promo_id")
    promo_id: str | None
    if promo_raw is None:
        promo_id = None
    elif isinstance(promo_raw, str) and not promo_raw.strip():
        promo_id = None
    elif isinstance(promo_raw, str):
        promo_id = promo_raw
    else:
        raise ValueError("promo_id must be a string or null")

    try:
        quantity = int(payload["quantity"])
    except (TypeError, ValueError) as exc:
        raise ValueError("quantity must be an integer") from exc

    try:
        unit_price = float(payload["unit_price"])
    except (TypeError, ValueError) as exc:
        raise ValueError("unit_price must be a float") from exc

    return TransactionEvent(
        transaction_id=str(payload["transaction_id"]),
        ts_utc=ts_utc,
        store_id=str(payload["store_id"]),
        customer_id=str(payload["customer_id"]),
        product_id=str(payload["product_id"]),
        quantity=quantity,
        unit_price=unit_price,
        currency=str(payload["currency"]),
        payment_method=str(payload["payment_method"]),
        channel=str(payload["channel"]),
        promo_id=promo_id,
    )
