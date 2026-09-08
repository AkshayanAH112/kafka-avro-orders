"""Dead Letter Queue inspector.

Reads ``orders.DLQ`` from the beginning and prints every dead letter with the
reason it was parked there, plus a breakdown by failure type. This is the
evidence half of the DLQ: the consumer writes, this reads.

    python -m src.dlq_consumer            # follow the DLQ until Ctrl+C
    DLQ_ONCE=1 python -m src.dlq_consumer # drain what is there, then exit
"""

from __future__ import annotations

import os
import signal
import sys
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src import config

_running = True
ONCE = os.getenv("DLQ_ONCE", "").lower() in {"1", "true", "yes"}
IDLE_EXIT_POLLS = 5          # in --once mode: quit after this many empty polls


def _stop(_signum, _frame) -> None:
    global _running
    _running = False


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")


def main() -> int:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(schema_registry, config.FAILED_ORDER_SCHEMA)
    ctx = SerializationContext(config.DLQ_TOPIC, MessageField.VALUE)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        # A dedicated read-only group so inspecting the DLQ never disturbs the
        # main processing group's offsets.
        "group.id": "dlq-viewer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.DLQ_TOPIC])

    print("=" * 96)
    print(f"  DEAD LETTER QUEUE  --  {config.DLQ_TOPIC}")
    print("=" * 96)
    header = (f"{'TIME':<10}{'ORDER':<9}{'PRODUCT':<9}{'PRICE':>12}  "
              f"{'TYPE':<21}{'TRY':>4}  REASON")
    print(header)
    print("-" * 96)

    by_type: Counter[str] = Counter()
    total = 0
    idle = 0

    try:
        while _running:
            msg = consumer.poll(1.0)

            if msg is None:
                idle += 1
                if ONCE and idle >= IDLE_EXIT_POLLS:
                    break
                continue
            idle = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                rec = deserializer(msg.value(), ctx)
            except Exception as exc:  # noqa: BLE001
                print(f"  <could not decode DLQ record @{msg.offset()}: {exc}>")
                continue

            total += 1
            by_type[rec["failureType"]] += 1
            print(f"{fmt_ts(rec['failedAt']):<10}{rec['orderId']:<9}"
                  f"{rec['product']:<9}{rec['price']:>12.2f}  "
                  f"{rec['failureType']:<21}{rec['attempts']:>4}  "
                  f"{rec['errorMessage']}")
    except KafkaException as exc:
        print(f"FATAL Kafka error: {exc}")
        return 1
    finally:
        consumer.close()
        print("-" * 96)
        print(f"  total dead letters: {total}")
        for failure_type, n in sorted(by_type.items(), key=lambda kv: -kv[1]):
            share = n / total * 100 if total else 0
            print(f"    {failure_type:<22}{n:>5}  ({share:.0f}%)")
        print("=" * 96)

    return 0


if __name__ == "__main__":
    sys.exit(main())
