# Live Demonstration Script

Roughly 11 minutes, or 10 if you skip the optional scaling section. Have four
terminals open in the repository root, plus two browser tabs: the dashboard on
<http://localhost:18186> and Kafka UI on <http://localhost:18185>.

> **Shell:** every command below is written for **bash**, and was verified in
> Git Bash on Windows. Windows PowerShell 5.1 will reject some of them: `&&` is
> not a valid separator, `curl` is an alias for `Invoke-WebRequest` and does not
> accept `-s`, and `head` and `tail` do not exist. Open Git Bash first:
>
> ```
> bash -l
> ```
>
> Or right click the project folder and choose "Git Bash Here".


---

## 0. Before the demo (do this in advance)

```bash
docker compose up -d kafka schema-registry kafka-ui
docker compose build            # pre-build the app image so nothing compiles on stage
docker compose run --rm init-topics
docker compose up -d web        # the dashboard
```

Confirm the stack is healthy:

```bash
docker compose ps
curl http://localhost:18181/subjects        # -> []  (registry is up, nothing registered yet)
curl -s http://localhost:18186/api/health   # -> all four readers "reading"
```

Open **<http://localhost:18186>** in a browser and leave it on screen for the
whole demo. It is the spine of the walkthrough; the terminals are the detail
view behind each panel.

**Run only one consumer.** More than one is a legitimate thing to do, and
section 7 covers what happens, but starting the demo with two makes the numbers
harder to narrate.

Reset to a clean slate right before starting (optional but makes the numbers
easier to narrate):

```bash
docker compose down -v
docker compose up -d kafka schema-registry kafka-ui
docker compose run --rm init-topics
docker compose up -d web
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
curl -s http://localhost:18181/subjects
curl -s http://localhost:18181/subjects/orders-value/versions/1 | python -m json.tool
```

---

## 3b. The dashboard, everything at once (1 min)

Switch to **<http://localhost:18186>**. This is the fastest way to show all
four requirements in one frame, and it is worth pausing on.

> "Six panels. Top left is the running average, rebuilt by replaying the
> compacted stats topic. Top middle is retry behaviour. Top right is the dead
> letter queue. Bottom left is the live order feed, decoded from Avro on the
> wire."

Two things to say explicitly, because they are the design points a marker
cares about:

> "There is no database anywhere in this. Every number on the page is rebuilt
> by consuming Kafka. That is what the compacted stats topic is for, and this
> page is the proof it works: I can restart this container and the whole
> dashboard comes back."

```bash
docker compose restart web
```

**It takes about 45 seconds**, most of it the four consumer groups joining, so
start it and keep talking. Measured, not estimated. Use the wait:

> "While that comes back, notice what it has to do. It has no state of its own
> to reload. It has to replay the compacted stats topic from offset zero and
> the dead letter queue from offset zero, and rebuild both from scratch.
>
> The per product averages you are about to see again were computed by a
> consumer process that is still running and never told this page anything.
> They came out of Kafka."

Then refresh. The running average and the dead letter queue are both back,
including dead letters from before the page was ever opened.

If you are short of time, skip the restart and make the same point from what is
already on screen: the DLQ panel shows failures that happened before you opened
the browser, which it could only know by replaying the topic.

> "And each panel reads with its own consumer group, so watching this dashboard
> never moves the processing consumer's offsets. Observing the system does not
> disturb it."

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

Now show the same thing on the dashboard, **Retry behaviour** panel. The three
figures are first try, recovered on retry, and budget exhausted, with the
attempt distribution underneath.

> "This panel exists because a successful retry is otherwise invisible. An
> order that recovered on its third attempt ends up in the running average
> looking exactly like one that succeeded immediately. So the consumer
> publishes one event per order carrying the attempt count, and that is the
> only place the retry logic can be observed from outside the process."

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

The dashboard's **Dead letter queue** panel picks it up within two seconds, and
the breakdown at the top gains a `deserialization` count beside `validation`
and `transient exhausted`.

---

## 5b. The aggregate outside the process (45 s)

**Terminal D:**

```bash
docker compose run --rm -e STATS_ONCE=1 stats-viewer
```

This replays the log-compacted `orders.stats` topic from offset 0 and rebuilds
the running averages — and prints the *same* table the consumer holds in
memory:

```
KEY          COUNT           SUM         AVG       MIN       MAX  UPDATED
----------------------------------------------------------------------------
ALL             36       8334.76      231.52      8.22    485.68  10:56:32
Item1            9       2120.63      235.63     20.73    446.63  10:56:32
...
```

The point: the aggregation is not trapped in the consumer. Compaction keeps
the *latest* record per key, so any dashboard — or a restarted consumer — can
recover the current state by reading the topic from the beginning.

---

## 6. Kafka UI — the whole picture (1 min)

Open <http://localhost:18185>:

* **Topics** → `orders`, `orders.DLQ`, `orders.stats` with their message counts.
* **Topics → orders → Messages** — Kafka UI decodes the Avro automatically
  using the Schema Registry, so the JSON view is proof the messages really are
  Avro-encoded and not JSON on the wire.
* **Topics → orders.stats** — log-compacted, so it holds the *latest* running
  average per key: one record for `ALL`, one per product.
* **Schema Registry** → `orders-value`, `orders.DLQ-value`, `orders.stats-value`.

---

## 7. Scaling the consumer, optional but strong (1 min)

Only if you have time. It shows you understand the design's limits, which is
usually worth more than showing it working.

Start a second consumer in a spare terminal:

```bash
docker compose run --rm consumer
```

```bash
docker compose exec kafka kafka-consumer-groups   --bootstrap-server kafka:29092 --group order-processor --describe
```

> "The group rebalances and the three partitions split across the two
> consumers. Throughput roughly doubles.
>
> This is also where I found a bug. Each consumer keeps its own in-memory
> aggregate. Originally both published a global 'ALL' figure to the same
> compacted key, so they overwrote each other and the dashboard showed a global
> count smaller than the sum of the per product counts.
>
> The per product keys were never affected, because orders are keyed by
> product, so every order for a product lands on one partition owned by exactly
> one consumer. Each product aggregate is complete. Only the global was wrong.
>
> So the consumer no longer publishes a global at all. Readers sum the per
> product rows instead, which is correct for any number of consumers rather
> than only for one."

Check the dashboard: the per product counts still sum exactly to the ALL count,
with two consumers running.

Stop the second consumer with Ctrl+C and the group rebalances back.

---

## 8. Graceful shutdown (30 s)

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

**What happens if you run more than one consumer?**
The group rebalances and the partitions split, so throughput scales. The
aggregation needed a fix to survive it, covered in section 7: each consumer
aggregates only its own partitions, so a global figure published by one
consumer covers only its share. Per product aggregates are safe because orders
are keyed by product, so a product is owned by exactly one consumer. The
consumer therefore publishes per product keys only and readers derive the
global by summing. Ordering is preserved per product either way, which is what
the aggregation actually depends on.

**Why is there a separate orders.events topic?**
Because a successful retry is otherwise invisible. An order that recovered on
its third attempt lands in the running average looking identical to one that
succeeded first time, so nothing outside the consumer process can tell whether
the retry logic is working or whether the downstream is healthy. The events
topic carries the attempt count for every order that reached a terminal state.
It is telemetry, not a record of truth: retention is one hour, publishing is
fire and forget so it can never block order processing, and the DLQ remains
the durable evidence.

**Does the dashboard affect the pipeline?**
No. Each panel reads with its own consumer group, so it never moves the
processing group's offsets, and it only ever reads. Stopping the dashboard
changes nothing about processing.

**What is that yellow GETPID warning when the consumer starts?**
It appears once, in the first second or two after a fresh `docker compose up`:

```
%4|...|GETPID|order-consumer-side#producer-2| [thrd:main]: Failed to acquire
idempotence PID from broker kafka:29092/1: Broker: Coordinator load in progress: retrying
```

`%4` is a warning, not an error. The DLQ producer runs with
`enable.idempotence=true`, so it needs a Producer ID before its first send. The
broker's healthcheck passes as soon as the metadata API answers, which is
slightly earlier than the transaction coordinator finishes loading, so the
consumer asks a moment too soon. librdkafka retries on its own and blocks the
first send until it has the PID, so nothing is dropped or duplicated. Restart
the consumer against an already warm broker and it does not appear at all.


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
