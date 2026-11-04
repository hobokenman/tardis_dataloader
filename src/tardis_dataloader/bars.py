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

Hourly tables are resampled from the minute tables with each column's
``resample`` rule, labelled with the hour's close the same way, and partitioned
by month, since an hour of bars is small.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

BAR_LOG_TABLE = "tardis_bar_log"
PARTITION_BY = "toDate(timestamp - INTERVAL 1 MINUTE)"
BAR_TS = "toDateTime(toStartOfMinute(local_timestamp), 'UTC') + INTERVAL 1 MINUTE"
HOURLY_PARTITION_BY = "toYYYYMM(timestamp - INTERVAL 1 MINUTE)"
# A minute bar closing at T belongs to the hour closing at the next full hour >= T.
HOURLY_BAR_TS = "toStartOfHour(m_timestamp - INTERVAL 1 MINUTE) + INTERVAL 1 HOUR"

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
    # How minute bars resample to longer bars: "sum", "max", "min", "first" or
    # "last" (the first or last non-NULL value), or "vwap:<quantity column>".
    resample: str = "sum"

    def resample_sql(self) -> str:
        """Aggregate over minute bars whose columns are prefixed ``m_``."""
        col = f"m_{self.name}"
        if self.resample in ("sum", "max", "min"):
            return f"{self.resample}({col})"
        if self.resample == "first":
            return f"argMinIf({col}, m_timestamp, {col} IS NOT NULL)"
        if self.resample == "last":
            return f"argMaxIf({col}, m_timestamp, {col} IS NOT NULL)"
        if self.resample.startswith("vwap:"):
            qty = f"m_{self.resample.removeprefix('vwap:')}"
            return f"sum({col} * {qty}) / nullIf(sum({qty}), 0)"
        raise ValueError(f"{self.name}: unknown resample rule {self.resample!r}")


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
        return _create_table_sql(database, self.table, self.columns, "minute", PARTITION_BY)

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
        BarColumn(
            "open",
            price_type,
            "Price of the first trade, by exchange time and id",
            resample="first",
        ),
        BarColumn("high", price_type, "Highest trade price", resample="max"),
        BarColumn("low", price_type, "Lowest trade price", resample="min"),
        BarColumn(
            "close", price_type, "Price of the last trade, by exchange time and id", resample="last"
        ),
        BarColumn(
            "vwap", price_type, "Sum of price times amount over amount", resample="vwap:amount"
        ),
        BarColumn("amount", FLOAT, "Traded quantity, base units", "sum"),
        BarColumn("buy_amount", FLOAT, "Quantity of trades whose taker bought", "sum"),
        BarColumn("sell_amount", FLOAT, "Quantity of trades whose taker sold", "sum"),
        BarColumn(
            "buy_vwap",
            NULLABLE_FLOAT,
            "VWAP of taker buys; NULL without any",
            resample="vwap:buy_amount",
        ),
        BarColumn(
            "sell_vwap",
            NULLABLE_FLOAT,
            "VWAP of taker sells; NULL without any",
            resample="vwap:sell_amount",
        ),
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
        BarColumn(
            "open_interest",
            NULLABLE_FLOAT,
            "Last open interest in the minute, contracts",
            resample="last",
        ),
        BarColumn(
            "funding_rate", NULLABLE_FLOAT, "Last funding rate in the minute", resample="last"
        ),
        BarColumn("mark_price", NULLABLE_FLOAT, "Last mark price in the minute", resample="last"),
        BarColumn("index_price", NULLABLE_FLOAT, "Last index price in the minute", resample="last"),
        BarColumn(
            "long_liquidation", FLOAT, "Quantity of sell-side liquidations, closing longs", "sum"
        ),
        BarColumn("long_liquidation_count", COUNT, "Number of sell-side liquidations", "sum"),
        BarColumn(
            "long_liquidation_vwap",
            NULLABLE_FLOAT,
            "VWAP of sell-side liquidations",
            resample="vwap:long_liquidation",
        ),
        BarColumn(
            "short_liquidation", FLOAT, "Quantity of buy-side liquidations, closing shorts", "sum"
        ),
        BarColumn("short_liquidation_count", COUNT, "Number of buy-side liquidations", "sum"),
        BarColumn(
            "short_liquidation_vwap",
            NULLABLE_FLOAT,
            "VWAP of buy-side liquidations",
            resample="vwap:short_liquidation",
        ),
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


@dataclass(frozen=True)
class HourlyBarTable:
    """Hourly bars resampled from a minute bar table, one partition per month."""

    name: str
    table: str
    minute: BarTable

    @property
    def columns(self) -> tuple[BarColumn, ...]:
        return tuple(
            BarColumn(c.name, c.type, c.comment.replace("minute", "hour"), c.merge, c.derived)
            for c in self.minute.columns
        )

    @property
    def column_names(self) -> list[str]:
        return self.minute.column_names

    @property
    def count_columns(self) -> list[str]:
        """Columns whose totals must survive resampling exactly."""
        return [c.name for c in self.minute.columns if c.type == COUNT]

    def create_table_sql(self, database: str) -> str:
        return _create_table_sql(database, self.table, self.columns, "hour", HOURLY_PARTITION_BY)

    def select_sql(self, database: str, month: dt.date) -> str:
        """The hourly bars of the minute bars received in ``month``."""
        values = []
        for c in self.minute.columns:
            expr = c.derived if c.derived is not None else c.resample_sql()
            values.append(f"CAST({expr}, '{c.type}') AS `{c.name}`")
        renamed = ", ".join(f"`{name}` AS m_{name}" for name in self.minute.column_names[2:])
        return (
            f"SELECT exchange, symbol, {HOURLY_BAR_TS} AS timestamp,\n    "
            + ",\n    ".join(values)
            + f"\nFROM (SELECT exchange, symbol, {renamed}"
            f" FROM `{database}`.`{self.minute.table}` WHERE {minute_range(month)})"
            f"\nGROUP BY exchange, symbol, timestamp"
        )


def minute_range(month: dt.date) -> str:
    """Minute bars of the rows received in ``month``: closes in (month start, next month start]."""
    start = month.replace(day=1)
    end = (start + dt.timedelta(days=32)).replace(day=1)
    return (
        f"timestamp > toDateTime('{start.isoformat()}', 'UTC') "
        f"AND timestamp <= toDateTime('{end.isoformat()}', 'UTC')"
    )


SPOT_BARS_1H = HourlyBarTable("spot-1h", "tardis_binance_spot_bars_1h", SPOT_BARS)
FUTURES_BARS_1H = HourlyBarTable("futures-1h", "tardis_binance_futures_bars_1h", FUTURES_BARS)
HOURLY_BAR_TABLES: dict[str, HourlyBarTable] = {b.name: b for b in (SPOT_BARS_1H, FUTURES_BARS_1H)}


def _create_table_sql(
    database: str, table: str, columns: tuple[BarColumn, ...], period: str, partition_by: str
) -> str:
    defs = [
        "`exchange` LowCardinality(String)",
        "`symbol` LowCardinality(String)",
        "`timestamp` DateTime('UTC') COMMENT 'Bar close: the bar holds rows with "
        f"local_timestamp in [timestamp - 1 {period}, timestamp)' CODEC(DoubleDelta, ZSTD(1))",
    ]
    for c in columns:
        comment = c.comment.replace("'", "\\'")
        defs.append(f"`{c.name}` {c.type} COMMENT '{comment}' CODEC({CODECS[c.type]})")
    cols = ",\n    ".join(defs)
    return (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}`\n(\n    {cols}\n)\n"
        f"ENGINE = MergeTree\n"
        f"PARTITION BY {partition_by}\n"
        f"ORDER BY (symbol, timestamp)"
    )


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
