"""Real-time aggregation state.

Holds a running average of order prices, globally and per product. The update
is O(1) per message and keeps only (count, sum, min, max) -- no window of past
prices is retained, which is what makes it usable on an unbounded stream.

    avg_n = sum_n / n           with   sum_n = sum_(n-1) + price_n
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RunningStats:
    """Incremental aggregate for one key (a product name, or ``ALL``)."""

    key: str
    count: int = 0
    total: float = 0.0
    min_price: float = float("inf")
    max_price: float = float("-inf")

    def add(self, price: float) -> None:
        self.count += 1
        self.total += price
        self.min_price = min(self.min_price, price)
        self.max_price = max(self.max_price, price)

    @property
    def avg(self) -> float:
        return self.total / self.count if self.count else 0.0

    def as_record(self, updated_at_ms: int) -> dict:
        """Shape this aggregate as an ``OrderStats`` Avro record."""
        return {
            "windowKey": self.key,
            "count": self.count,
            "sum": round(self.total, 4),
            "avgPrice": round(self.avg, 4),
            "minPrice": 0.0 if self.count == 0 else self.min_price,
            "maxPrice": 0.0 if self.count == 0 else self.max_price,
            "updatedAt": updated_at_ms,
        }


@dataclass
class Aggregator:
    """Global running average plus one running average per product."""

    overall: RunningStats = field(default_factory=lambda: RunningStats("ALL"))
    per_product: dict[str, RunningStats] = field(default_factory=dict)

    def update(self, product: str, price: float) -> tuple[RunningStats, RunningStats]:
        """Fold one order in. Returns ``(global_stats, product_stats)``."""
        self.overall.add(price)
        stats = self.per_product.setdefault(product, RunningStats(product))
        stats.add(price)
        return self.overall, stats

    def summary_lines(self) -> list[str]:
        """Rows for the periodic console table."""
        header = f"{'KEY':<10}{'COUNT':>8}{'SUM':>14}{'AVG':>12}{'MIN':>10}{'MAX':>10}"
        rows = [header, "-" * len(header)]

        def row(s: RunningStats) -> str:
            return (f"{s.key:<10}{s.count:>8}{s.total:>14.2f}{s.avg:>12.2f}"
                    f"{s.min_price:>10.2f}{s.max_price:>10.2f}")

        rows.append(row(self.overall))
        for key in sorted(self.per_product):
            rows.append(row(self.per_product[key]))
        return rows
