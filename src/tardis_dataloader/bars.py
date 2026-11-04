"""One-minute bar tables built from the loaded tick tables.

Ticks are bucketed by ``local_timestamp``, the time the row was received, so a
bar only holds what was known by its close and every bar day maps to exactly
one source partition. A bar's ``timestamp`` is the minute's right edge: the
bar at 00:01 holds the rows received in [00:00, 00:01). Bar tables are
therefore partitioned by ``toDate(timestamp - INTERVAL 1 MINUTE)``, the
source day, and each day is rebuilt by replacing its partition.

Each source is aggregated per (symbol, minute) on its own; the results are
combined with UNION ALL and a second GROUP BY that merges each column with
``sum`` (counts and quantities, 0 where a source has no rows) or ``any`` (the
one non-NULL value). A minute gets a row when any source has rows in it, so a
futures minute with ticker updates but no trades has NULL prices.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

BAR_LOG_TABLE = "tardis_bar_log"
PARTITION_BY = "toDate(timestamp - INTERVAL 1 MINUTE)"
BAR_TS = "toDateTime(toStartOfMinute(local_timestamp), 'UTC') + INTERVAL 1 MINUTE"

FLOAT = "Float64"
NULLABLE_FLOAT = "Nullable(Float64)"
COUNT = "UInt32"
CODECS = {FLOAT: "ZSTD(1)", NULLABLE_FLOAT: "ZSTD(1)", COUNT: "T64, ZSTD(1)"}


@dataclass(frozen=True)
class BarColumn:
    name: str
    type: str
    comment: str
    # How per-source aggregates combine: "sum", or "any" for the one non-NULL value.
    merge: str = "any"
    # SQL over other bar columns, for a column no source provides.
    derived: str | None = None


@dataclass(frozen=True)
class Source:
    table: str
    # Inner projection of the tick table. It renames every raw column a bar
    # column shares a name with, since ClickHouse resolves names to aliases first.
    projection: str
    # Bar column -> aggregate over the projection, per (symbol, minute).
    aggregates: dict[str, str]


@dataclass(frozen=True)
class BarTable:
    name: str
    table: str
    exchange: str
    columns: tuple[BarColumn, ...]
    sources: tuple[Source, ...]
    # (aggregate over the staged bars, source table): the aggregate must equal
    # that table's loaded row count for the day.
    checks: tuple[tuple[str, str], ...] = field(default=())

    @property
    def column_names(self) -> list[str]:
        return ["exchange", "symbol", "timestamp", *(c.name for c in self.columns)]

    @property
    def source_tables(self) -> list[str]:
        return [s.table for s in self.sources]

    def create_table_sql(self, database: str) -> str:
        defs = [
            "`exchange` LowCardinality(String)",
            "`symbol` LowCardinality(String)",
            "`timestamp` DateTime('UTC') COMMENT 'Bar close: the bar holds rows with "
            "local_timestamp in [timestamp - 1 minute, timestamp)' CODEC(DoubleDelta, ZSTD(1))",
        ]
        for c in self.columns:
            comment = c.comment.replace("'", "\\'")
            defs.append(f"`{c.name}` {c.type} COMMENT '{comment}' CODEC({CODECS[c.type]})")
        cols = ",\n    ".join(defs)
        return (
            f"CREATE TABLE IF NOT EXISTS `{database}`.`{self.table}`\n(\n    {cols}\n)\n"
            f"ENGINE = MergeTree\n"
            f"PARTITION BY {PARTITION_BY}\n"
            f"ORDER BY (symbol, timestamp)"
        )

    def select_sql(self, database: str, day: dt.date) -> str:
        """The bars of ``day``: rows received on that UTC day, one per symbol and minute."""
        start = f"toDateTime('{day.isoformat()}', 'UTC')"
        merged = [c for c in self.columns if c.derived is None]
        branches = []
        for s in self.sources:
            values = []
            for c in merged:
                if c.name in s.aggregates:
                    value = s.aggregates[c.name]
                else:
                    value = "0" if c.merge == "sum" else "NULL"
                values.append(f"CAST({value}, '{c.type}') AS `{c.name}`")
            branches.append(
                f"SELECT symbol, {BAR_TS} AS bar_ts,\n        "
                + ",\n        ".join(values)
                + f"\n    FROM (SELECT {s.projection} FROM `{database}`.`{s.table}`"
                f" WHERE local_timestamp >= {start}"
                f" AND local_timestamp < {start} + INTERVAL 1 DAY)"
                f"\n    GROUP BY symbol, bar_ts"
            )
        outer = []
        for c in self.columns:
            expr = c.derived if c.derived is not None else f"{c.merge}(`{c.name}`)"
            outer.append(f"{expr} AS `{c.name}`")
        return (
            f"SELECT '{self.exchange}' AS exchange, symbol, bar_ts AS timestamp,\n    "
            + ",\n    ".join(outer)
            + "\nFROM\n(\n    "
            + "\n    UNION ALL\n    ".join(branches)
            + "\n)\nGROUP BY symbol, bar_ts"
        )


def _trade_columns(price_type: str) -> tuple[BarColumn, ...]:
    """Columns from trades. ``side`` is the taker's side."""
    return (
        BarColumn("open", price_type, "Price of the first trade, by exchange time and id"),
        BarColumn("high", price_type, "Highest trade price"),
        BarColumn("low", price_type, "Lowest trade price"),
        BarColumn("close", price_type, "Price of the last trade, by exchange time and id"),
        BarColumn("vwap", price_type, "Sum of price times amount over amount"),
        BarColumn("amount", FLOAT, "Traded quantity, base units", "sum"),
        BarColumn("buy_amount", FLOAT, "Quantity of trades whose taker bought", "sum"),
        BarColumn("sell_amount", FLOAT, "Quantity of trades whose taker sold", "sum"),
        BarColumn("buy_vwap", NULLABLE_FLOAT, "VWAP of taker buys; NULL without any"),
        BarColumn("sell_vwap", NULLABLE_FLOAT, "VWAP of taker sells; NULL without any"),
        BarColumn("trade_count", COUNT, "Number of trades", "sum"),
        BarColumn("buy_trade_count", COUNT, "Number of taker buys", "sum"),
        BarColumn("sell_trade_count", COUNT, "Number of taker sells", "sum"),
    )


def _trades_source(table: str) -> Source:
    return Source(
        table=table,
        projection="symbol, local_timestamp, timestamp AS ts, id, side, price AS px, amount AS qty",
        aggregates={
            "open": "argMin(px, (ts, id))",
            "high": "max(px)",
            "low": "min(px)",
            "close": "argMax(px, (ts, id))",
            "vwap": "sum(px * qty) / sum(qty)",
            "amount": "sum(qty)",
            "buy_amount": "sumIf(qty, side = 'buy')",
            "sell_amount": "sumIf(qty, side = 'sell')",
            "buy_vwap": "sumIf(px * qty, side = 'buy') / nullIf(sumIf(qty, side = 'buy'), 0)",
            "sell_vwap": "sumIf(px * qty, side = 'sell') / nullIf(sumIf(qty, side = 'sell'), 0)",
            "trade_count": "count()",
            "buy_trade_count": "countIf(side = 'buy')",
            "sell_trade_count": "countIf(side = 'sell')",
        },
    )


def _last(column: str) -> str:
    """The minute's last non-NULL value, by receive time then exchange time."""
    return f"argMaxIf({column}, (local_timestamp, ts), {column} IS NOT NULL)"


SPOT_BARS = BarTable(
    name="spot",
    table="tardis_binance_spot_bars_1m",
    exchange="binance",
    columns=_trade_columns(FLOAT),
    sources=(_trades_source("tardis_binance_spot_trades"),),
    checks=(("sum(trade_count)", "tardis_binance_spot_trades"),),
)

FUTURES_BARS = BarTable(
    name="futures",
    table="tardis_binance_futures_bars_1m",
    exchange="binance-futures",
    columns=(
        *_trade_columns(NULLABLE_FLOAT),
        BarColumn("open_interest", NULLABLE_FLOAT, "Last open interest in the minute, contracts"),
        BarColumn("funding_rate", NULLABLE_FLOAT, "Last funding rate in the minute"),
        BarColumn("mark_price", NULLABLE_FLOAT, "Last mark price in the minute"),
        BarColumn("index_price", NULLABLE_FLOAT, "Last index price in the minute"),
        BarColumn(
            "long_liquidation", FLOAT, "Quantity of sell-side liquidations, closing longs", "sum"
        ),
        BarColumn("long_liquidation_count", COUNT, "Number of sell-side liquidations", "sum"),
        BarColumn("long_liquidation_vwap", NULLABLE_FLOAT, "VWAP of sell-side liquidations"),
        BarColumn(
            "short_liquidation", FLOAT, "Quantity of buy-side liquidations, closing shorts", "sum"
        ),
        BarColumn("short_liquidation_count", COUNT, "Number of buy-side liquidations", "sum"),
        BarColumn("short_liquidation_vwap", NULLABLE_FLOAT, "VWAP of buy-side liquidations"),
        BarColumn(
            "net_liquidation",
            FLOAT,
            "long_liquidation - short_liquidation",
            derived="long_liquidation - short_liquidation",
        ),
    ),
    sources=(
        _trades_source("tardis_binance_futures_trades"),
        Source(
            table="tardis_binance_futures_derivative_ticker",
            projection=(
                "symbol, local_timestamp, timestamp AS ts, open_interest AS oi, "
                "funding_rate AS fr, mark_price AS mp, index_price AS ip"
            ),
            aggregates={
                "open_interest": _last("oi"),
                "funding_rate": _last("fr"),
                "mark_price": _last("mp"),
                "index_price": _last("ip"),
            },
        ),
        Source(
            table="tardis_binance_futures_liquidations",
            projection="symbol, local_timestamp, side, price AS px, amount AS qty",
            aggregates={
                "long_liquidation": "sumIf(qty, side = 'sell')",
                "long_liquidation_count": "countIf(side = 'sell')",
                "long_liquidation_vwap": (
                    "sumIf(px * qty, side = 'sell') / nullIf(sumIf(qty, side = 'sell'), 0)"
                ),
                "short_liquidation": "sumIf(qty, side = 'buy')",
                "short_liquidation_count": "countIf(side = 'buy')",
                "short_liquidation_vwap": (
                    "sumIf(px * qty, side = 'buy') / nullIf(sumIf(qty, side = 'buy'), 0)"
                ),
            },
        ),
    ),
    checks=(
        ("sum(trade_count)", "tardis_binance_futures_trades"),
        (
            "sum(long_liquidation_count + short_liquidation_count)",
            "tardis_binance_futures_liquidations",
        ),
    ),
)

BAR_TABLES: dict[str, BarTable] = {b.name: b for b in (SPOT_BARS, FUTURES_BARS)}


def create_bar_log_sql(database: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{BAR_LOG_TABLE}`\n(\n"
        "    `table` LowCardinality(String),\n"
        "    `day` Date,\n"
        "    `bars` UInt64,\n"
        "    `built_at` DateTime64(3, 'UTC') DEFAULT now64(3)\n"
        ")\n"
        "ENGINE = ReplacingMergeTree(built_at)\n"
        "ORDER BY (`table`, `day`)"
    )
