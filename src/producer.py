"""Avro order producer.

Generates random ``Order`` records and publishes them to the ``orders`` topic,
serialised with the Confluent Avro serialiser: every payload on the wire is
``<magic byte 0x00><4-byte schema id><avro binary>`` and the writer schema is
registered once in the Schema Registry under the subject ``orders-value``.

A configurable fraction of orders is produced deliberately *invalid* (a
negative or absurd price). Those are the messages the consumer will reject
permanently and route to the Dead Letter Queue, which is what makes the DLQ
observable during the live demo.

    python -m src.producer                 # 1 order/second until Ctrl+C
    MESSAGE_COUNT=50 python -m src.producer
"""

from __future__ import annotations

import random
import signal
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from src import config

_running = True


def _stop(_signum, _frame) -> None:
    global _running
    _running = False
    print("\nStopping producer (flushing outstanding messages)...")


def delivery_report(err, msg) -> None:
    """Called once per message from ``poll``/``flush`` with the broker ack."""
    if err is not None:
        print(f"  DELIVERY FAILED  {err}")
        return
    key = msg.key().decode("utf-8") if msg.key() else "-"
    print(f"  ack  key={key:<6} -> {msg.topic()}[{msg.partition()}]@{msg.offset()}")


def make_order(order_id: int, rng: random.Random) -> tuple[dict, str]:
    """Build one order. Returns the record and a human label for the console."""
    product = rng.choice(config.PRODUCTS)

    if rng.random() < config.POISON_RATE:
        # Two flavours of permanently-bad data, so the DLQ shows more than one
        # validation reason.
        if rng.random() < 0.5:
            price = round(rng.uniform(-200.0, -1.0), 2)      # negative price
        else:
            price = round(rng.uniform(50_000.0, 99_999.0), 2)  # absurd price
        label = "POISON"
    else:
        price = round(rng.uniform(config.PRICE_MIN, config.PRICE_MAX), 2)
        label = "ok"

    return {"orderId": str(order_id), "product": product, "price": price}, label


def main() -> int:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    rng = random.Random(config.RANDOM_SEED)

    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry,
        config.ORDER_SCHEMA,
        conf={"auto.register.schemas": True},
    )
    key_serializer = StringSerializer("utf_8")

    producer = Producer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "client.id": "order-producer",
        # Durability: wait for all in-sync replicas, retry transient broker
        # errors, and keep per-key ordering while doing so.
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 20,
        "compression.type": "snappy",
    })

    print("=" * 72)
    print("  ORDER PRODUCER (Avro)")
    print(f"  brokers          : {config.BOOTSTRAP_SERVERS}")
    print(f"  schema registry  : {config.SCHEMA_REGISTRY_URL}")
    print(f"  topic            : {config.ORDERS_TOPIC}")
    print(f"  poison rate      : {config.POISON_RATE:.0%}  (these end up in the DLQ)")
    limit = config.MESSAGE_COUNT or "unlimited"
    print(f"  messages         : {limit} @ {config.PRODUCE_INTERVAL_SEC}s interval")
    print("=" * 72)

    ctx = SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)
    order_id = 1001
    sent = 0

    try:
        while _running:
            if config.MESSAGE_COUNT and sent >= config.MESSAGE_COUNT:
                break

            order, label = make_order(order_id, rng)
            marker = "!!" if label == "POISON" else "  "
            print(f"{marker} send #{sent + 1:<4} "
                  f"orderId={order['orderId']} product={order['product']:<6} "
                  f"price={order['price']:>10.2f}  [{label}]")

            try:
                producer.produce(
                    topic=config.ORDERS_TOPIC,
                    # Keying by product keeps all orders for one product on one
                    # partition, so per-product aggregation stays ordered.
                    key=key_serializer(order["product"]),
                    value=avro_serializer(order, ctx),
                    on_delivery=delivery_report,
                )
            except BufferError:
                # Local queue full: drain it and retry this order once.
                producer.poll(1.0)
                producer.produce(
                    topic=config.ORDERS_TOPIC,
                    key=key_serializer(order["product"]),
                    value=avro_serializer(order, ctx),
                    on_delivery=delivery_report,
                )

            producer.poll(0)          # serve delivery callbacks
            order_id += 1
            sent += 1
            time.sleep(config.PRODUCE_INTERVAL_SEC)
    finally:
        remaining = producer.flush(15)
        if remaining:
            print(f"WARNING: {remaining} message(s) were not delivered.")
        print(f"Producer finished. {sent} order(s) sent.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
