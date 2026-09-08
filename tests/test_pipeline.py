"""Unit tests for the logic the assignment is graded on.

These need no broker: the aggregation, the transient/permanent classification
and the backoff curve are all pure functions.

    docker compose run --rm tests
    # or, locally:  pip install pytest && python -m pytest -q
"""

from __future__ import annotations

import random
from datetime import datetime, timezone

import pytest

from src import config
from src.aggregator import Aggregator, RunningStats
from src.consumer import backoff_delay, process_with_retry, validate
from src.errors import PermanentError, TransientError


# --------------------------------------------------------------------------
# Real-time aggregation
# --------------------------------------------------------------------------

def test_running_average_matches_a_plain_mean():
    prices = [10.0, 20.0, 30.0, 45.5, 100.25]
    stats = RunningStats("Item1")
    for p in prices:
        stats.add(p)

    assert stats.count == len(prices)
    assert stats.avg == pytest.approx(sum(prices) / len(prices))
    assert stats.min_price == pytest.approx(min(prices))
    assert stats.max_price == pytest.approx(max(prices))


def test_running_average_updates_after_every_message():
    """The average must be correct at *every* step, not just at the end."""
    stats = RunningStats("ALL")
    seen: list[float] = []
    for price in (5.0, 15.0, 100.0, 2.5):
        stats.add(price)
        seen.append(price)
        assert stats.avg == pytest.approx(sum(seen) / len(seen))


def test_empty_stats_do_not_divide_by_zero():
    assert RunningStats("ALL").avg == 0.0


def test_aggregator_keeps_global_and_per_product_state_separate():
    agg = Aggregator()
    agg.update("Item1", 100.0)
    agg.update("Item2", 200.0)
    agg.update("Item1", 50.0)

    assert agg.overall.count == 3
    assert agg.overall.avg == pytest.approx((100 + 200 + 50) / 3)
    assert agg.per_product["Item1"].count == 2
    assert agg.per_product["Item1"].avg == pytest.approx(75.0)
    assert agg.per_product["Item2"].avg == pytest.approx(200.0)


def test_as_record_matches_the_avro_stats_schema():
    stats = RunningStats("Item1")
    stats.add(10.0)
    stats.add(30.0)
    rec = stats.as_record(datetime.now(tz=timezone.utc))

    assert set(rec) == {"windowKey", "count", "sum", "avgPrice",
                        "minPrice", "maxPrice", "updatedAt"}
    assert rec["avgPrice"] == pytest.approx(20.0)
    # timestamp-millis round-trips as an aware datetime, not an int.
    assert isinstance(rec["updatedAt"], datetime)
    assert rec["updatedAt"].tzinfo is not None


# --------------------------------------------------------------------------
# Validation -- permanent failures
# --------------------------------------------------------------------------

@pytest.mark.parametrize("order", [
    {"orderId": "1001", "product": "Item1", "price": 0.0},
    {"orderId": "1002", "product": "Item2", "price": 199.99},
    {"orderId": "1003", "product": "Item3", "price": config.MAX_ALLOWED_PRICE},
])
def test_valid_orders_pass(order):
    validate(order)          # must not raise


@pytest.mark.parametrize("order", [
    {"orderId": "", "product": "Item1", "price": 10.0},          # no id
    {"orderId": "1001", "product": "", "price": 10.0},           # no product
    {"orderId": "1001", "product": "Item1", "price": None},      # no price
    {"orderId": "1001", "product": "Item1", "price": -0.01},     # negative
    {"orderId": "1001", "product": "Item1", "price": 1e9},       # absurd
])
def test_invalid_orders_raise_permanent(order):
    with pytest.raises(PermanentError):
        validate(order)


# --------------------------------------------------------------------------
# Retry logic
# --------------------------------------------------------------------------

def test_backoff_grows_exponentially_and_is_capped(monkeypatch):
    monkeypatch.setattr(config, "RETRY_JITTER", 0.0)      # isolate the curve
    monkeypatch.setattr(config, "RETRY_BASE_DELAY_SEC", 0.5)
    monkeypatch.setattr(config, "RETRY_MAX_DELAY_SEC", 2.0)

    rng = random.Random(0)
    assert backoff_delay(1, rng) == pytest.approx(0.5)
    assert backoff_delay(2, rng) == pytest.approx(1.0)
    assert backoff_delay(3, rng) == pytest.approx(2.0)
    assert backoff_delay(9, rng) == pytest.approx(2.0)    # capped, not 128s


def test_backoff_jitter_stays_within_bounds(monkeypatch):
    monkeypatch.setattr(config, "RETRY_JITTER", 0.3)
    monkeypatch.setattr(config, "RETRY_BASE_DELAY_SEC", 1.0)
    monkeypatch.setattr(config, "RETRY_MAX_DELAY_SEC", 8.0)

    rng = random.Random(1)
    for _ in range(200):
        assert 0.7 - 1e-9 <= backoff_delay(1, rng) <= 1.3 + 1e-9


def test_transient_failure_is_retried_then_succeeds(monkeypatch):
    monkeypatch.setattr(config, "MAX_RETRIES", 3)
    monkeypatch.setattr(config, "RETRY_BASE_DELAY_SEC", 0.0)

    calls = {"n": 0}

    def flaky_twice(_order, _rng):
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("simulated 503")

    monkeypatch.setattr("src.consumer.process", flaky_twice)

    order = {"orderId": "1001", "product": "Item1", "price": 10.0}
    assert process_with_retry(order, random.Random(0)) == 3
    assert calls["n"] == 3


def test_retry_budget_is_exhausted_then_dead_lettered(monkeypatch):
    monkeypatch.setattr(config, "MAX_RETRIES", 2)         # 3 attempts total
    monkeypatch.setattr(config, "RETRY_BASE_DELAY_SEC", 0.0)

    calls = {"n": 0}

    def always_fails(_order, _rng):
        calls["n"] += 1
        raise TransientError("simulated 503")

    monkeypatch.setattr("src.consumer.process", always_fails)

    order = {"orderId": "1001", "product": "Item1", "price": 10.0}
    with pytest.raises(PermanentError, match="retry budget exhausted"):
        process_with_retry(order, random.Random(0))

    assert calls["n"] == 3        # not retried forever


def test_permanent_failure_is_never_retried(monkeypatch):
    """Invalid data must not consume the retry budget at all."""
    calls = {"n": 0}

    def counting_process(_order, _rng):
        calls["n"] += 1

    monkeypatch.setattr("src.consumer.process", counting_process)

    bad = {"orderId": "1001", "product": "Item1", "price": -5.0}
    with pytest.raises(PermanentError):
        process_with_retry(bad, random.Random(0))

    assert calls["n"] == 0        # rejected before processing was attempted
