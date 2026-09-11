"""Shared configuration and Avro schema loading.

Every knob the demo needs is an environment variable with a sane default, so
the same code runs unchanged inside docker compose (kafka:29092) and from a
plain terminal on the host (localhost:9092).
"""

from __future__ import annotations

import os
from pathlib import Path

# --- Connection ------------------------------------------------------------

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# --- Topics ----------------------------------------------------------------

ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders.DLQ")
STATS_TOPIC = os.getenv("STATS_TOPIC", "orders.stats")

# One record per order that reached a terminal state. This is what makes
# retry behaviour visible to anything other than the consumer's console.
EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "orders.events")

TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
TOPIC_REPLICATION = int(os.getenv("TOPIC_REPLICATION", "1"))

CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "order-processor")

# --- Producer behaviour ----------------------------------------------------

# How many orders to send (0 = run forever until Ctrl+C).
MESSAGE_COUNT = int(os.getenv("MESSAGE_COUNT", "0"))
PRODUCE_INTERVAL_SEC = float(os.getenv("PRODUCE_INTERVAL_SEC", "1.0"))

PRODUCTS = os.getenv("PRODUCTS", "Item1,Item2,Item3,Item4,Item5").split(",")
PRICE_MIN = float(os.getenv("PRICE_MIN", "5.0"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "500.0"))

# Fraction of orders deliberately produced with an invalid price so that the
# consumer's validation path (permanent failure -> straight to DLQ) can be
# demonstrated live.
POISON_RATE = float(os.getenv("POISON_RATE", "0.10"))

# --- Consumer behaviour ----------------------------------------------------

# Probability that processing an otherwise valid order raises a *transient*
# error. This stands in for a flaky downstream (database timeout, HTTP 503...)
# and is what the retry logic exists for.
TRANSIENT_FAILURE_RATE = float(os.getenv("TRANSIENT_FAILURE_RATE", "0.20"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))          # attempts after the first
RETRY_BASE_DELAY_SEC = float(os.getenv("RETRY_BASE_DELAY_SEC", "0.5"))
RETRY_MAX_DELAY_SEC = float(os.getenv("RETRY_MAX_DELAY_SEC", "8.0"))
RETRY_JITTER = float(os.getenv("RETRY_JITTER", "0.3"))    # +/- 30% jitter

# Business rule used by the validation step.
MAX_ALLOWED_PRICE = float(os.getenv("MAX_ALLOWED_PRICE", "10000.0"))

# Deterministic runs for reproducible screenshots: set RANDOM_SEED=42.
_seed = os.getenv("RANDOM_SEED")
RANDOM_SEED = int(_seed) if _seed else None

# --- Schemas ---------------------------------------------------------------

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"


def load_schema(filename: str) -> str:
    """Return the raw Avro schema text for ``schemas/<filename>``."""
    return (SCHEMA_DIR / filename).read_text(encoding="utf-8")


ORDER_SCHEMA = load_schema("order.avsc")
FAILED_ORDER_SCHEMA = load_schema("failed_order.avsc")
ORDER_STATS_SCHEMA = load_schema("order_stats.avsc")
PROCESSING_EVENT_SCHEMA = load_schema("processing_event.avsc")
