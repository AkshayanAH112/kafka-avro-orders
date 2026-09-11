"""Web UI for the order pipeline.

A dashboard showing the four things the assignment asks for, live:

    Avro serialisation    the order feed, decoded from Confluent framed Avro
                          using the schema fetched from the registry
    running average       rebuilt from the log compacted orders.stats topic
    retry logic           from orders.events, which carries the attempt count
                          for every order that reached a terminal state
    dead letter queue     from orders.DLQ, with the failure type breakdown

It holds no database. Every figure is rebuilt by consuming Kafka, which is the
point: orders.stats is log compacted precisely so that state can be recovered
by replaying it, and this service is the proof that works. Restarting it
reconstructs the whole dashboard from the topics.

Each topic is read by its own background thread with its own consumer group,
so the dashboard never disturbs the offsets of the real processing group.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from src import config
from web.dashboard import render_dashboard

app = FastAPI(
    title="Order Pipeline Dashboard",
    description="Live view of the Kafka and Avro order pipeline: running "
                "averages, retry behaviour and the dead letter queue.",
    version="1.0.0",
)

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
# Written by the reader threads, read by the request handlers. A single lock is
# enough: the critical sections are tiny and the contention is negligible at
# this message rate, so anything more elaborate would be harder to reason about
# for no measurable gain.

_lock = threading.Lock()

_stats: dict[str, dict] = {}                 # latest OrderStats per key
_orders: deque = deque(maxlen=60)            # recent orders off the topic
_events: deque = deque(maxlen=200)           # recent terminal outcomes
_dlq: list[dict] = []                        # every dead letter seen
_started_at = time.time()

_topic_status: dict[str, str] = {
    config.ORDERS_TOPIC: "connecting",
    config.STATS_TOPIC: "connecting",
    config.EVENTS_TOPIC: "connecting",
    config.DLQ_TOPIC: "connecting",
}


def _iso(value: Any) -> Any:
    """Avro timestamp-millis round trips as an aware datetime, not an int."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="seconds")
    return value


# ---------------------------------------------------------------------------
# Topic readers
# ---------------------------------------------------------------------------

def _reader(topic: str, schema: str, group: str, handler, offset: str) -> None:
    """Consume one topic forever, handing each decoded record to a handler.

    Errors are swallowed and retried rather than killing the thread. A
    dashboard that goes permanently blank because the broker restarted once
    would be worse than one that is briefly stale.
    """
    while True:
        try:
            registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
            deserializer = AvroDeserializer(registry, schema)
            ctx = SerializationContext(topic, MessageField.VALUE)

            consumer = Consumer({
                "bootstrap.servers": config.BOOTSTRAP_SERVERS,
                # A dedicated read only group per topic, so watching the
                # dashboard never moves the processing group's offsets.
                "group.id": group,
                "auto.offset.reset": offset,
                "enable.auto.commit": False,
            })
            consumer.subscribe([topic])

            with _lock:
                _topic_status[topic] = "reading"

            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(str(msg.error()))

                try:
                    record = deserializer(msg.value(), ctx)
                except Exception:          # noqa: BLE001 - undecodable payload
                    # The consumer under test dead letters these; the dashboard
                    # only needs to not fall over.
                    continue

                if record is not None:
                    with _lock:
                        handler(record, msg)

        except Exception as exc:           # noqa: BLE001 - keep the thread alive
            with _lock:
                _topic_status[topic] = f"reconnecting: {str(exc)[:60]}"
            time.sleep(5)


def _on_order(record: dict, msg) -> None:
    _orders.appendleft({
        "orderId": record["orderId"],
        "product": record["product"],
        "price": round(float(record["price"]), 2),
        "partition": msg.partition(),
        "offset": msg.offset(),
    })


def _on_stats(record: dict, _msg) -> None:
    # A stale "ALL" key may linger in the compacted topic from an older version
    # that published one. It is skipped, because a per consumer partial total
    # written to a shared key is not a global figure. The true global is derived
    # in /api/stats by summing the per product rows.
    if record["windowKey"] == "ALL":
        return

    _stats[record["windowKey"]] = {
        "key": record["windowKey"],
        "count": record["count"],
        "sum": round(record["sum"], 2),
        "avg": round(record["avgPrice"], 2),
        "min": round(record["minPrice"], 2),
        "max": round(record["maxPrice"], 2),
        "updatedAt": _iso(record["updatedAt"]),
    }


def _on_event(record: dict, _msg) -> None:
    _events.appendleft({
        "orderId": record["orderId"],
        "product": record["product"],
        "price": round(float(record["price"]), 2),
        "outcome": record["outcome"],
        "attempts": record["attempts"],
        "failureType": record.get("failureType"),
        "processedAt": _iso(record["processedAt"]),
    })


def _on_dlq(record: dict, _msg) -> None:
    _dlq.append({
        "orderId": record["orderId"],
        "product": record["product"],
        "price": round(float(record["price"]), 2),
        "failureType": record["failureType"],
        "errorMessage": record["errorMessage"],
        "attempts": record["attempts"],
        "source": f"{record['sourceTopic']}[{record['sourcePartition']}]"
                  f"@{record['sourceOffset']}",
        "failedAt": _iso(record["failedAt"]),
    })


@app.on_event("startup")
def start_readers() -> None:
    """One daemon thread per topic.

    Only the raw order feed starts at the latest offset. It is genuinely a
    live tail, and replaying yesterday's orders into it would misrepresent what
    is happening right now.

    Everything else starts from the beginning, because it is state the
    dashboard is meant to reconstruct rather than a tail: the compacted stats
    topic, the dead letter queue, and the processing events. Reading events
    from the beginning is what keeps the retry panel populated across a
    restart; its retention is one hour and the buffer is capped, so the replay
    is bounded either way.
    """
    readers = [
        (config.ORDERS_TOPIC, config.ORDER_SCHEMA, "ui-orders", _on_order, "latest"),
        (config.STATS_TOPIC, config.ORDER_STATS_SCHEMA, "ui-stats", _on_stats, "earliest"),
        (config.EVENTS_TOPIC, config.PROCESSING_EVENT_SCHEMA, "ui-events", _on_event, "earliest"),
        (config.DLQ_TOPIC, config.FAILED_ORDER_SCHEMA, "ui-dlq", _on_dlq, "earliest"),
    ]
    for topic, schema, group, handler, offset in readers:
        threading.Thread(
            target=_reader,
            args=(topic, schema, group, handler, offset),
            daemon=True,
            name=f"reader-{topic}",
        ).start()


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@app.get("/api/stats", tags=["aggregation"])
def stats():
    """Running averages, rebuilt from the log compacted stats topic.

    The global figure is summed here rather than read from the topic. Each
    consumer in the group owns a subset of the partitions and aggregates only
    what it sees, so a global published by one consumer covers only its own
    share. Per product aggregates do not have that problem, because orders are
    keyed by product and so each product belongs to exactly one consumer.
    Summing them gives a global that stays correct however many consumers run.
    """
    with _lock:
        rows = sorted(_stats.values(), key=lambda r: r["key"])

    overall = None
    if rows:
        count = sum(r["count"] for r in rows)
        total = sum(r["sum"] for r in rows)
        overall = {
            "key": "ALL",
            "count": count,
            "sum": round(total, 2),
            "avg": round(total / count, 2) if count else 0.0,
            "min": min(r["min"] for r in rows),
            "max": max(r["max"] for r in rows),
            "derived": "summed from the per product aggregates",
        }

    return {
        "source": f"{config.STATS_TOPIC} (log compacted)",
        "overall": overall,
        "per_product": rows,
    }


@app.get("/api/orders", tags=["feed"])
def orders(limit: int = 25):
    """The live order feed, decoded from Avro on the wire."""
    with _lock:
        return {"source": config.ORDERS_TOPIC, "orders": list(_orders)[:limit]}


@app.get("/api/retries", tags=["retry"])
def retries(limit: int = 25):
    """Retry behaviour, derived from the processing event stream.

    This is the only place a *successful* retry is visible. An order that
    recovered on its third attempt ends up in the running average looking
    identical to one that succeeded immediately, so without the attempt count
    the retry logic cannot be observed from outside the consumer.
    """
    with _lock:
        events = list(_events)

    processed = [e for e in events if e["outcome"] == "PROCESSED"]
    retried = [e for e in processed if e["attempts"] > 1]
    exhausted = [e for e in events if e.get("failureType") == "TRANSIENT_EXHAUSTED"]

    histogram: dict[int, int] = {}
    for e in events:
        histogram[e["attempts"]] = histogram.get(e["attempts"], 0) + 1

    return {
        "source": config.EVENTS_TOPIC,
        "window": len(events),
        "first_attempt_success": len(processed) - len(retried),
        "recovered_after_retry": len(retried),
        "exhausted_to_dlq": len(exhausted),
        "attempt_histogram": [
            {"attempts": k, "count": histogram[k]} for k in sorted(histogram)
        ],
        "recent": events[:limit],
    }


@app.get("/api/dlq", tags=["dlq"])
def dlq(limit: int = 25):
    """Dead letters with the breakdown by failure type."""
    with _lock:
        entries = list(reversed(_dlq))

    breakdown: dict[str, int] = {}
    for entry in entries:
        breakdown[entry["failureType"]] = breakdown.get(entry["failureType"], 0) + 1

    return {
        "source": config.DLQ_TOPIC,
        "total": len(entries),
        "by_type": [{"failureType": k, "count": v}
                    for k, v in sorted(breakdown.items(), key=lambda kv: -kv[1])],
        "recent": entries[:limit],
    }


@app.get("/api/health", tags=["observability"])
def health():
    """Whether each topic reader is actually consuming."""
    with _lock:
        status = dict(_topic_status)
        counts = {
            "stats_keys": len(_stats),
            "orders_buffered": len(_orders),
            "events_buffered": len(_events),
            "dead_letters": len(_dlq),
        }
    healthy = all(v == "reading" for v in status.values())
    return {
        "healthy": healthy,
        "uptime_seconds": round(time.time() - _started_at, 1),
        "topics": status,
        "buffered": counts,
    }


@app.get("/", response_class=HTMLResponse, tags=["dashboard"])
def dashboard():
    return render_dashboard()
