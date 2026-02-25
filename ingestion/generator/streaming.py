"""Kafka streaming publisher for generated transactions."""

from __future__ import annotations

import json
import logging
import signal
import threading
import time
from typing import Any, Sequence

from ingestion.generator.generator import TransactionGenerator
from ingestion.generator.models import TransactionEvent

LOGGER = logging.getLogger("ingestion.generator.streaming")


class KafkaTransactionPublisher:
    """Kafka producer wrapper for transaction events."""

    def __init__(self, bootstrap_servers: Sequence[str], topic: str) -> None:
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers cannot be empty")
        if not topic.strip():
            raise ValueError("topic cannot be empty")

        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError(
                "kafka-python is required for stream mode. Install with: pip install kafka-python"
            ) from exc

        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=list(bootstrap_servers),
            acks="all",
            retries=5,
            linger_ms=50,
            value_serializer=lambda payload: json.dumps(
                payload, separators=(",", ":"), sort_keys=True
            ).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
        )

    def publish(self, event: TransactionEvent) -> None:
        payload = event.to_serializable_dict()
        key = payload["customer_id"]
        self._producer.send(topic=self._topic, key=key, value=payload)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


def stream_transactions(
    *,
    rate_per_second: int,
    seed: int,
    bootstrap_servers: str,
    topic: str = "transactions",
    log_every: int = 1000,
) -> None:
    """Continuously publish generated transactions to Kafka until stopped."""
    if rate_per_second <= 0:
        raise ValueError("rate_per_second must be > 0")
    if log_every <= 0:
        raise ValueError("log_every must be > 0")

    server_list = [
        server.strip() for server in bootstrap_servers.split(",") if server.strip()
    ]
    if not server_list:
        raise ValueError("bootstrap_servers must include at least one host:port entry")

    generator = TransactionGenerator(seed=seed)
    publisher = KafkaTransactionPublisher(bootstrap_servers=server_list, topic=topic)
    stop_event = threading.Event()

    def _handle_signal(signum: int, _: Any) -> None:
        LOGGER.info(
            "shutdown_signal_received",
            extra={"context": {"signal": signum}},
        )
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_signal)
        except ValueError:
            # Signal registration may fail outside main thread in some environments.
            LOGGER.warning(
                "signal_handler_registration_skipped",
                extra={"context": {"signal": sig.value}},
            )

    interval = 1.0 / rate_per_second
    published = 0
    next_deadline = time.perf_counter()
    LOGGER.info(
        "stream_start",
        extra={
            "context": {
                "topic": topic,
                "rate_per_second": rate_per_second,
                "bootstrap_servers": server_list,
            }
        },
    )

    try:
        while not stop_event.is_set():
            event = generator.generate_event()
            publisher.publish(event)
            published += 1

            if published % log_every == 0:
                publisher.flush()
                LOGGER.info(
                    "stream_progress",
                    extra={"context": {"published": published, "topic": topic}},
                )

            next_deadline += interval
            delay = next_deadline - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            else:
                next_deadline = time.perf_counter()
    except KeyboardInterrupt:
        LOGGER.info("keyboard_interrupt_received")
    finally:
        publisher.close()
        LOGGER.info(
            "stream_stopped",
            extra={"context": {"published": published, "topic": topic}},
        )
