"""Create the three topics the demo needs, then exit.

Run automatically by docker compose (the ``init-topics`` service) before the
producer and consumer start, because the broker is configured with
``auto.create.topics.enable=false`` -- topics in this project are explicit.
"""

from __future__ import annotations

import sys

from confluent_kafka.admin import AdminClient, NewTopic

from src import config


def main() -> int:
    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS})

    wanted = {
        # The DLQ is given a long retention: dead letters must survive long
        # enough for a human to look at them.
        config.ORDERS_TOPIC: {},
        config.DLQ_TOPIC: {"retention.ms": str(30 * 24 * 60 * 60 * 1000)},
        config.STATS_TOPIC: {"cleanup.policy": "compact"},
        # Events are a short lived feed for the dashboard, not a record of
        # truth, so they expire quickly. The DLQ is the durable evidence.
        config.EVENTS_TOPIC: {"retention.ms": str(60 * 60 * 1000)},
    }

    existing = set(admin.list_topics(timeout=15).topics)
    missing = [name for name in wanted if name not in existing]

    for name in wanted:
        if name not in missing:
            print(f"  = {name:<14} already exists")

    if not missing:
        print("All topics present.")
        return 0

    new_topics = [
        NewTopic(
            name,
            num_partitions=config.TOPIC_PARTITIONS,
            replication_factor=config.TOPIC_REPLICATION,
            config=wanted[name],
        )
        for name in missing
    ]

    failed = False
    for name, future in admin.create_topics(new_topics).items():
        try:
            future.result()
            print(f"  + {name:<14} created "
                  f"({config.TOPIC_PARTITIONS} partitions, rf={config.TOPIC_REPLICATION})")
        except Exception as exc:  # noqa: BLE001 - report and keep going
            print(f"  ! {name:<14} FAILED: {exc}")
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
