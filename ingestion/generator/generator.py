"""Deterministic retail transaction event generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import random
from uuid import UUID

from ingestion.generator.models import TransactionEvent

DEFAULT_CURRENCIES: tuple[str, ...] = ("CAD", "USD")
DEFAULT_PAYMENT_METHODS: tuple[str, ...] = (
    "credit_card",
    "debit_card",
    "cash",
    "mobile_wallet",
)
DEFAULT_CHANNELS: tuple[str, ...] = ("online", "store")


@dataclass(slots=True)
class TransactionGenerator:
    """Generates realistic synthetic retail transactions."""

    seed: int = 42
    base_ts_utc: datetime = field(
        default_factory=lambda: datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    store_count: int = 250
    customer_count: int = 500_000
    product_count: int = 25_000
    promo_rate: float = 0.25

    def __post_init__(self) -> None:
        if self.store_count <= 0:
            raise ValueError("store_count must be > 0")
        if self.customer_count <= 0:
            raise ValueError("customer_count must be > 0")
        if self.product_count <= 0:
            raise ValueError("product_count must be > 0")
        if not 0 <= self.promo_rate <= 1:
            raise ValueError("promo_rate must be between 0 and 1")

        if self.base_ts_utc.tzinfo is None:
            raise ValueError("base_ts_utc must be timezone-aware in UTC")
        self.base_ts_utc = self.base_ts_utc.astimezone(timezone.utc)

        self._rng = random.Random(self.seed)

    _rng: random.Random = field(init=False, repr=False)

    def generate_event(self) -> TransactionEvent:
        """Generate one deterministic transaction event."""
        transaction_id = str(UUID(int=self._rng.getrandbits(128), version=4))

        ts_utc = self.base_ts_utc + timedelta(
            seconds=self._rng.randint(0, 30 * 24 * 60 * 60),
            microseconds=self._rng.randint(0, 999_999),
        )

        quantity = self._rng.randint(1, 8)
        unit_price = round(self._rng.uniform(1.99, 699.99), 2)
        channel = self._rng.choices(DEFAULT_CHANNELS, weights=(0.35, 0.65), k=1)[0]
        promo_id = (
            f"PROMO-{self._rng.randint(1, 9999):04d}"
            if self._rng.random() < self.promo_rate
            else None
        )

        return TransactionEvent(
            transaction_id=transaction_id,
            ts_utc=ts_utc,
            store_id=f"STORE-{self._rng.randint(1, self.store_count):04d}",
            customer_id=f"CUST-{self._rng.randint(1, self.customer_count):08d}",
            product_id=f"PROD-{self._rng.randint(1, self.product_count):06d}",
            quantity=quantity,
            unit_price=unit_price,
            currency=self._rng.choices(DEFAULT_CURRENCIES, weights=(0.9, 0.1), k=1)[0],
            payment_method=self._rng.choices(
                DEFAULT_PAYMENT_METHODS, weights=(0.5, 0.3, 0.1, 0.1), k=1
            )[0],
            channel=channel,
            promo_id=promo_id,
        )

    def generate_events(self, rows: int) -> list[TransactionEvent]:
        """Generate a list of events for batch output."""
        if rows <= 0:
            raise ValueError("rows must be > 0")
        return [self.generate_event() for _ in range(rows)]
