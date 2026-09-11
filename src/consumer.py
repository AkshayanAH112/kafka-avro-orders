"""Avro order consumer with retry logic, a Dead Letter Queue and real-time
aggregation of the running average price.

Per message the flow is:

    poll -> Avro deserialise ---(fails)---------------> DLQ  [DESERIALIZATION]
              |
            validate ---------(fails)-----------------> DLQ  [VALIDATION]
              |
            process  --(TransientError)--> retry with exponential backoff
              |                              |  budget exhausted
              |                              +-------> DLQ  [TRANSIENT_EXHAUSTED]
            success
              |
        update running average -> emit OrderStats -> commit offset

Offsets are committed manually and only after the message has reached a
terminal state (processed, or safely parked in the DLQ). Combined with
``enable.auto.offset.store=false`` this gives at-least-once delivery: nothing
is skipped because of a crash mid-retry.

    python -m src.consumer
"""

from __future__ import annotations

import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from src import config
from src.aggregator import Aggregator
from src.errors import PermanentError, TransientError

_running = True
STATS_EVERY = 10          # print the aggregate table every N processed orders


def _stop(_signum, _frame) -> None:
    global _running
    _running = False
    print("\nStopping consumer (committing offsets)...")


def now_utc() -> datetime:
    """Current UTC time.

    Avro ``timestamp-millis`` round-trips as a timezone-aware ``datetime`` --
    fastavro encodes one on write and hands one back on read -- so the whole
    pipeline uses ``datetime`` for these fields rather than raw epoch ints.
    """
    return datetime.now(tz=timezone.utc)


# --------------------------------------------------------------------------
# Business logic
# --------------------------------------------------------------------------

def validate(order: dict) -> None:
    """Business rules. Any breach is permanent -- retrying cannot fix data."""
    if not order.get("orderId"):
        raise PermanentError("orderId is empty")
    if not order.get("product"):
        raise PermanentError("product is empty")

    price = order.get("price")
    if price is None:
        raise PermanentError("price is missing")
    if price < 0:
        raise PermanentError(f"price must be >= 0, got {price:.2f}")
    if price > config.MAX_ALLOWED_PRICE:
        raise PermanentError(
            f"price {price:.2f} exceeds the maximum allowed "
            f"{config.MAX_ALLOWED_PRICE:.2f}"
        )


def process(order: dict, rng: random.Random) -> None:
    """Stand-in for the real downstream side effect (DB write, HTTP call...).

    It fails transiently with probability ``TRANSIENT_FAILURE_RATE`` so the
    retry path is exercised on every run of the demo.
    """
    if rng.random() < config.TRANSIENT_FAILURE_RATE:
        raise TransientError("downstream service unavailable (simulated 503)")


def backoff_delay(attempt: int, rng: random.Random) -> float:
    """Exponential backoff with full-range jitter, capped at RETRY_MAX_DELAY.

    attempt 1 -> ~0.5s, 2 -> ~1s, 3 -> ~2s ... jitter spreads out retries so
    a whole consumer group does not hammer a recovering downstream in lockstep.
    """
    raw = config.RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1))
    capped = min(raw, config.RETRY_MAX_DELAY_SEC)
    jitter = capped * config.RETRY_JITTER * (2 * rng.random() - 1)
    return max(0.0, capped + jitter)


def process_with_retry(order: dict, rng: random.Random) -> int:
    """Validate + process an order, retrying only transient failures.

    Returns the number of attempts made. Raises ``PermanentError`` (bad data,
    or the retry budget exhausted) when the message must be dead-lettered.
    """
    validate(order)                       # permanent by construction: no retry

    total_attempts = config.MAX_RETRIES + 1
    last_error: Exception | None = None

    for attempt in range(1, total_attempts + 1):
        try:
            process(order, rng)
            if attempt > 1:
                print(f"     recovered on attempt {attempt}/{total_attempts}")
            return attempt
        except TransientError as exc:
            last_error = exc
            if attempt == total_attempts:
                break
            delay = backoff_delay(attempt, rng)
            print(f"     transient failure on attempt {attempt}/{total_attempts}: "
                  f"{exc} -> retrying in {delay:.2f}s")
            time.sleep(delay)

    raise PermanentError(
        f"retry budget exhausted after {total_attempts} attempts: {last_error}"
    ) from last_error


# --------------------------------------------------------------------------
# Dead Letter Queue
# --------------------------------------------------------------------------

class DeadLetterQueue:
    """Publishes failed orders to ``orders.DLQ`` as Avro ``FailedOrder``s.

    The original bytes are also attached as a Kafka header so a replay tool can
    re-inject the exact payload without re-encoding it.
    """

    def __init__(self, producer: Producer, serializer: AvroSerializer) -> None:
        self._producer = producer
        self._serializer = serializer
        self._key_serializer = StringSerializer("utf_8")
        self._ctx = SerializationContext(config.DLQ_TOPIC, MessageField.VALUE)
        self.count = 0

    def send(self, msg, order: dict | None, failure_type: str,
             error_message: str, attempts: int) -> None:
        record = {
            "orderId": (order or {}).get("orderId", "<undecodable>"),
            "product": (order or {}).get("product", ""),
            "price": float((order or {}).get("price", -1.0)),
            "failureType": failure_type,
            "errorMessage": error_message[:1000],
            "attempts": attempts,
            "sourceTopic": msg.topic(),
            "sourcePartition": msg.partition(),
            "sourceOffset": msg.offset(),
            "failedAt": now_utc(),
        }

        headers = [
            ("dlq-failure-type", failure_type.encode("utf-8")),
            ("dlq-error", error_message[:900].encode("utf-8", "replace")),
            ("dlq-attempts", str(attempts).encode("utf-8")),
            ("dlq-source", f"{msg.topic()}[{msg.partition()}]@{msg.offset()}".encode()),
            ("dlq-original-value", msg.value() or b""),
        ]

        self._producer.produce(
            topic=config.DLQ_TOPIC,
            key=self._key_serializer(record["orderId"]),
            value=self._serializer(record, self._ctx),
            headers=headers,
        )
        self._producer.poll(0)
        self.count += 1
        print(f"  >> DLQ  orderId={record['orderId']} type={failure_type} "
              f"attempts={attempts} reason={error_message}")

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)


# --------------------------------------------------------------------------
# Processing events
# --------------------------------------------------------------------------

class EventStream:
    """Publishes one record per order that reached a terminal state.

    Without this, a successful retry leaves no trace anywhere except the
    consumer's own stdout: the order lands in the running average looking
    exactly like one that succeeded first time. That makes the retry logic
    impossible to observe from outside the process, which is a problem for any
    dashboard and for anyone trying to tell a healthy pipeline from a
    struggling one.

    Events are deliberately fire and forget. They are telemetry, not a record
    of truth, so a failure to publish one must never stop an order from being
    processed. The DLQ remains the durable evidence.
    """

    def __init__(self, producer: Producer, serializer: AvroSerializer) -> None:
        self._producer = producer
        self._serializer = serializer
        self._key_serializer = StringSerializer("utf_8")
        self._ctx = SerializationContext(config.EVENTS_TOPIC, MessageField.VALUE)

    def emit(self, order: dict | None, outcome: str, attempts: int,
             failure_type: str | None = None) -> None:
        record = {
            "orderId": (order or {}).get("orderId", "<undecodable>"),
            "product": (order or {}).get("product", ""),
            "price": float((order or {}).get("price", -1.0)),
            "outcome": outcome,
            "attempts": attempts,
            "failureType": failure_type,
            "processedAt": now_utc(),
        }
        try:
            self._producer.produce(
                topic=config.EVENTS_TOPIC,
                key=self._key_serializer(record["product"] or "unknown"),
                value=self._serializer(record, self._ctx),
            )
            self._producer.poll(0)
        except Exception:                  # noqa: BLE001 - telemetry only
            # Never let a dashboard feed break order processing.
            pass


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------

def main() -> int:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    rng = random.Random(config.RANDOM_SEED)

    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    order_deserializer = AvroDeserializer(schema_registry, config.ORDER_SCHEMA)
    dlq_serializer = AvroSerializer(schema_registry, config.FAILED_ORDER_SCHEMA)
    stats_serializer = AvroSerializer(schema_registry, config.ORDER_STATS_SCHEMA)
    key_serializer = StringSerializer("utf_8")

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP,
        "client.id": "order-consumer",
        "auto.offset.reset": "earliest",
        # Manual offset handling -- at-least-once. An offset is stored only
        # after the message reached a terminal state.
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        # Retry backoff can hold a message for seconds; give the group enough
        # slack that a legitimate retry is not mistaken for a dead consumer.
        "max.poll.interval.ms": 600_000,
        "session.timeout.ms": 45_000,
    })

    side_producer = Producer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "client.id": "order-consumer-side",
        "acks": "all",
        "enable.idempotence": True,
    })
    dlq = DeadLetterQueue(side_producer, dlq_serializer)

    event_serializer = AvroSerializer(schema_registry,
                                      config.PROCESSING_EVENT_SCHEMA)
    events = EventStream(side_producer, event_serializer)

    aggregator = Aggregator()
    stats_ctx = SerializationContext(config.STATS_TOPIC, MessageField.VALUE)
    order_ctx = SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)

    def emit_stats() -> None:
        """Publish the current aggregates to the compacted stats topic.

        Per product keys only. The global aggregate is deliberately not
        published, because it is not globally true.

        Each consumer in the group owns a subset of the partitions and keeps
        its own in-memory aggregate, so its "ALL" covers only the orders it
        personally saw. With two consumers running, both write their own
        partial total to the same compacted key and overwrite each other. The
        result is a global count smaller than the sum of the per product
        counts, which is how this was found.

        The per product keys do not have that problem. Orders are keyed by
        product, so every order for a product lands on one partition and is
        therefore owned by exactly one consumer. Each product aggregate is
        complete, and any reader can recover the true global by summing them.
        """
        ts = now_utc()
        for stats in aggregator.per_product.values():
            side_producer.produce(
                topic=config.STATS_TOPIC,
                key=key_serializer(stats.key),
                value=stats_serializer(stats.as_record(ts), stats_ctx),
            )
        side_producer.poll(0)

    consumer.subscribe([config.ORDERS_TOPIC])

    print("=" * 72)
    print("  ORDER CONSUMER (Avro) -- retry + DLQ + running average")
    print(f"  brokers          : {config.BOOTSTRAP_SERVERS}")
    print(f"  schema registry  : {config.SCHEMA_REGISTRY_URL}")
    print(f"  topic / group    : {config.ORDERS_TOPIC} / {config.CONSUMER_GROUP}")
    print(f"  DLQ topic        : {config.DLQ_TOPIC}")
    print(f"  stats topic      : {config.STATS_TOPIC}")
    print(f"  retry budget     : {config.MAX_RETRIES} retries, "
          f"base {config.RETRY_BASE_DELAY_SEC}s, cap {config.RETRY_MAX_DELAY_SEC}s")
    print(f"  transient rate   : {config.TRANSIENT_FAILURE_RATE:.0%} (simulated)")
    print("=" * 72)

    processed = 0

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            order = None
            try:
                order = order_deserializer(msg.value(), order_ctx)
                if order is None:
                    raise PermanentError("tombstone / empty value")
            except Exception as exc:  # noqa: BLE001 - undecodable payload
                # Bad bytes or an incompatible schema: no amount of retrying
                # will help, so this is dead-lettered immediately.
                dlq.send(msg, None, "DESERIALIZATION", str(exc), attempts=1)
                events.emit(None, "DEAD_LETTERED", 1, "DESERIALIZATION")
                consumer.store_offsets(msg)
                continue

            print(f"  recv orderId={order['orderId']} product={order['product']:<6} "
                  f"price={order['price']:>9.2f}  "
                  f"[{msg.topic()}[{msg.partition()}]@{msg.offset()}]")

            try:
                attempts = process_with_retry(order, rng)
            except PermanentError as exc:
                failure_type = ("TRANSIENT_EXHAUSTED"
                                if "retry budget exhausted" in str(exc)
                                else "VALIDATION")
                attempts = (config.MAX_RETRIES + 1
                            if failure_type == "TRANSIENT_EXHAUSTED" else 1)
                dlq.send(msg, order, failure_type, str(exc), attempts)
                events.emit(order, "DEAD_LETTERED", attempts, failure_type)
                consumer.store_offsets(msg)
                continue

            # ---- success: fold into the running average -------------------
            overall, product_stats = aggregator.update(order["product"],
                                                       float(order["price"]))
            events.emit(order, "PROCESSED", attempts)
            processed += 1
            print(f"     OK in {attempts} attempt(s) | "
                  f"running avg ALL = {overall.avg:.2f} (n={overall.count}) | "
                  f"{order['product']} avg = {product_stats.avg:.2f} "
                  f"(n={product_stats.count})")

            emit_stats()
            consumer.store_offsets(msg)

            if processed % STATS_EVERY == 0:
                print()
                for line in aggregator.summary_lines():
                    print("  " + line)
                print(f"  dead-lettered so far: {dlq.count}\n")

            # Periodic commit of everything stored so far.
            consumer.commit(asynchronous=True)

    except KafkaException as exc:
        print(f"FATAL Kafka error: {exc}")
        return 1
    finally:
        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            pass          # nothing to commit
        dlq.flush()
        side_producer.flush(10)
        consumer.close()

        print("\n" + "=" * 72)
        print("  FINAL AGGREGATION")
        for line in aggregator.summary_lines():
            print("  " + line)
        print(f"\n  processed OK    : {processed}")
        print(f"  dead-lettered   : {dlq.count}")
        print("=" * 72)

    return 0


if __name__ == "__main__":
    sys.exit(main())
