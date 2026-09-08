# Live Demonstration Script

Roughly 8 minutes. Have four terminals open in the repository root, plus a
browser tab on <http://localhost:8080>.

---

## 0. Before the demo (do this in advance)

```bash
docker compose up -d kafka schema-registry kafka-ui
docker compose build            # pre-build the app image so nothing compiles on stage
docker compose run --rm init-topics
```

Confirm the stack is healthy:

```bash
docker compose ps
curl http://localhost:8081/subjects        # -> []  (registry is up, nothing registered yet)
```

Reset to a clean slate right before starting (optional but makes the numbers
easier to narrate):

```bash
docker compose down -v && docker compose up -d kafka schema-registry kafka-ui
docker compose run --rm init-topics
```

---

## 1. Show the schema (30 s)

> "Every message on the wire is Avro. Here is the contract."

```bash
cat schemas/order.avsc
```

Point out the three fields the assignment specifies: `orderId` (string),
`product` (string), `price` (float).

---

## 2. Start the consumer (30 s)

**Terminal A:**

```bash
docker compose run --rm consumer
```

Read out the banner: broker, registry, topics, retry budget (3 retries),
simulated transient failure rate (20 %). The consumer now sits idle on `poll`.

---

## 3. Start the producer — Avro + running average (2 min)

**Terminal B:**

```bash
docker compose run --rm producer
```

Narrate, in this order:

1. **Producer side** — each line shows the order it sent and the broker ack
   with the partition and offset it landed on. Lines marked `!!` and `[POISON]`
   are the deliberately invalid ones.
2. **Consumer side** — every accepted order updates the **running average**
   immediately:
   ```
   OK in 1 attempt(s) | running avg ALL = 244.17 (n=12) | Item3 avg = 198.02 (n=4)
   ```
   Stress that this is O(1): the consumer keeps `(count, sum, min, max)`, never
   a list of past prices, so it works on an unbounded stream.
3. Every ten orders the full aggregate table prints — global plus per product.

**Prove the schema is registered:**

```bash
curl -s http://localhost:8081/subjects
curl -s http://localhost:8081/subjects/orders-value/versions/1 | python -m json.tool
```

---

## 4. Retry logic (1.5 min)

Wait for a retry sequence to scroll past in Terminal A — at a 20 % failure rate
one appears every few messages:

```
  recv orderId=1043 product=Item3  price=  187.40  [orders[1]@27]
     transient failure on attempt 1/4: downstream service unavailable (simulated 503) -> retrying in 0.43s
     transient failure on attempt 2/4: downstream service unavailable (simulated 503) -> retrying in 1.18s
     recovered on attempt 3/4
     OK in 3 attempt(s) | running avg ALL = 238.91 (n=44)
```

Points to make:

* Delays double — 0.5 s, 1 s, 2 s, 4 s — capped at 8 s, with ±30 % jitter so a
  whole consumer group does not retry in lockstep and re-stampede a recovering
  downstream.
* Only `TransientError` is retried. Bad *data* is never retried, because
  retrying it cannot change the outcome and it would block the partition.

**Force a guaranteed retry storm** — every message fails transiently, so
everything exhausts the budget and lands in the DLQ:

```bash
docker compose run --rm -e TRANSIENT_FAILURE_RATE=1.0 -e MAX_RETRIES=2 consumer
```

---

## 5. Dead Letter Queue (2 min)

**Terminal C:**

```bash
docker compose run --rm dlq-viewer
```

Walk the table and point at each of the three failure types:

| `failureType` | What happened | Retried? |
|---|---|---|
| `VALIDATION` | Negative price, or price above 10 000 | No — permanent by nature |
| `TRANSIENT_EXHAUSTED` | Flaky downstream survived all 4 attempts | Yes, 4× |
| `DESERIALIZATION` | Payload was not valid Avro for the schema | No |

Then show *why* the DLQ record is Avro rather than a text blob: it carries the
original order **plus** `failureType`, `errorMessage`, `attempts`, and the
source topic / partition / offset, so any dead letter is traceable back to the
exact bytes it came from. The original payload is also attached as the Kafka
header `dlq-original-value` for replay.

**Force a `DESERIALIZATION` dead letter on demand** — inject bytes that are not
Confluent-framed Avro:

```bash
docker compose exec kafka bash -c \
  'echo "this-is-not-avro" | kafka-console-producer --bootstrap-server kafka:29092 --topic orders'
```

Terminal A immediately shows:

```
  >> DLQ  orderId=<undecodable> type=DESERIALIZATION attempts=1 reason=...
```

---

## 6. Kafka UI — the whole picture (1 min)

Open <http://localhost:8080>:

* **Topics** → `orders`, `orders.DLQ`, `orders.stats` with their message counts.
* **Topics → orders → Messages** — Kafka UI decodes the Avro automatically
  using the Schema Registry, so the JSON view is proof the messages really are
  Avro-encoded and not JSON on the wire.
* **Topics → orders.stats** — log-compacted, so it holds the *latest* running
  average per key: one record for `ALL`, one per product.
* **Schema Registry** → `orders-value`, `orders.DLQ-value`, `orders.stats-value`.

---

## 7. Graceful shutdown (30 s)

`Ctrl+C` in Terminal A. The consumer commits outstanding offsets, flushes the
DLQ producer, and prints the final aggregation:

```
========================================================================
  FINAL AGGREGATION
  KEY          COUNT           SUM         AVG       MIN       MAX
  ------------------------------------------------------------------
  ALL             54      13284.71      245.98     11.62    496.03
  Item1          12       2946.22      245.52     32.40    468.77
  ...

  processed OK    : 54
  dead-lettered   : 9
========================================================================
```

**At-least-once, demonstrated:** restart the consumer and it resumes from the
last committed offset rather than replaying everything, because offsets are
stored only after a message reaches a terminal state (processed, or parked in
the DLQ).

---

## Anticipated questions

**Why not retry inside a retry *topic* instead of in-process?**
In-process retry is the right tool for short, sub-second blips: it keeps
ordering and needs no extra infrastructure. Its cost is head-of-line blocking —
the partition stalls while we back off. For long outages (minutes) the standard
answer is a tiered retry-topic ladder (`orders.retry.5s`, `orders.retry.1m`, …)
so the main partition keeps flowing. With a capped 8 s backoff, in-process is
the correct trade-off here.

**Why is `price` a `float` and the aggregate a `double`?**
`float` is what the assignment specifies for the message. Summing thousands of
32-bit floats accumulates rounding error, so the aggregate accumulator widens
to `double`. Real money would use `bytes` with `logicalType: decimal`.

**What happens if the Schema Registry is down?**
The producer cannot serialise and fails fast rather than writing unreadable
bytes. The consumer caches schema ids it has already seen, so it keeps
processing known schemas and only stalls on a genuinely new one.

**Why key by `product`?**
It puts all orders for one product on one partition, so the per-product running
average sees them in order. Keying by `orderId` would spread them evenly but
give up that ordering guarantee.

**Is the running average lost if the consumer restarts?**
The in-memory aggregate is, yes — but every update is published to the
log-compacted `orders.stats` topic, so the latest value per key survives and a
restarting consumer (or a dashboard) can rebuild state by reading that topic
from the beginning.
