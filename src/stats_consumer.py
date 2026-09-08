"""Reader for the aggregation topic.

Consumes ``orders.stats`` from the beginning and keeps the latest snapshot per
key. Because the topic is log-compacted, replaying it from offset 0 rebuilds
the current running average for every key -- which is the point of publishing
the aggregate to Kafka instead of leaving it inside the consumer process.

    python -m src.stats_consumer              # live dashboard, refresh on change
    STATS_ONCE=1 python -m src.stats_consumer # rebuild state, print once, exit
"""

from __future__ import annotations

import os
import signal
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src import config

_running = True
ONCE = os.getenv("STATS_ONCE", "").lower() in {"1", "true", "yes"}
IDLE_EXIT_POLLS = 5


def _stop(_signum, _frame) -> None:
    global _running
    _running = False


def fmt_ts(value) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%H:%M:%S")
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime("%H:%M:%S")


def render(latest: dict[str, dict]) -> None:
    header = (f"{'KEY':<10}{'COUNT':>8}{'SUM':>14}{'AVG':>12}"
              f"{'MIN':>10}{'MAX':>10}  {'UPDATED':<10}")
    print("\n" + "=" * len(header))
    print(f"  RUNNING AVERAGES  --  rebuilt from {config.STATS_TOPIC}")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    # "ALL" first, then products alphabetically.
    for key in sorted(latest, key=lambda k: (k != "ALL", k)):
        r = latest[key]
        print(f"{r['windowKey']:<10}{r['count']:>8}{r['sum']:>14.2f}"
              f"{r['avgPrice']:>12.2f}{r['minPrice']:>10.2f}{r['maxPrice']:>10.2f}"
              f"  {fmt_ts(r['updatedAt']):<10}")
    print("=" * len(header))


def main() -> int:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(schema_registry, config.ORDER_STATS_SCHEMA)
    ctx = SerializationContext(config.STATS_TOPIC, MessageField.VALUE)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": "stats-viewer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.STATS_TOPIC])

    latest: dict[str, dict] = {}
    idle = 0
    dirty = False

    try:
        while _running:
            msg = consumer.poll(1.0)

            if msg is None:
                # Caught up: show what we have, then wait for the next change.
                if dirty:
                    render(latest)
                    dirty = False
                idle += 1
                if ONCE and idle >= IDLE_EXIT_POLLS:
                    break
                continue
            idle = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            rec = deserializer(msg.value(), ctx)
            if rec is None:            # tombstone written by compaction
                latest.pop(msg.key().decode("utf-8"), None)
            else:
                latest[rec["windowKey"]] = rec
            dirty = True
    except KafkaException as exc:
        print(f"FATAL Kafka error: {exc}")
        return 1
    finally:
        consumer.close()
        if latest:
            render(latest)
        else:
            print(f"No aggregates yet on {config.STATS_TOPIC}.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
