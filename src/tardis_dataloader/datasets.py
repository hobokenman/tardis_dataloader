"""Tardis dataset definitions and their ClickHouse table schemas.

Column names match the tardis CSV header exactly, in order; the header is
checked before every load and columns are then inserted positionally.

Tardis timestamps are integer microseconds since the epoch, which ClickHouse
cannot parse from CSV into DateTime64. Each ``MICROS`` column is therefore
inserted into an ``<name>_us Int64 EPHEMERAL`` column and stored as
``DateTime64(6, 'UTC')`` via ``fromUnixTimestamp64Micro``. ``NULLABLE_MICROS``
does the same for timestamps that may be empty, which are stored as NULL.

Tables are partitioned by the UTC day of ``local_timestamp``. Tardis splits
daily files on local_timestamp, so one file maps to exactly one partition,
which lets a file be reloaded idempotently by replacing its partition.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

MICROS = "MICROS"
NULLABLE_MICROS = "NULLABLE_MICROS"
# Timestamp pseudo-type -> (ephemeral integer type, stored type).
TIMESTAMP_TYPES = {
    MICROS: ("Int64", "DateTime64(6, 'UTC')"),
    NULLABLE_MICROS: ("Nullable(Int64)", "Nullable(DateTime64(6, 'UTC'))"),
}
PARTITION_BY = "toDate(local_timestamp)"
LOAD_LOG_TABLE = "tardis_load_log"


@dataclass(frozen=True)
class Dataset:
    name: str
    source_dir: str
    file_kind: str
    table: str
    columns: tuple[tuple[str, str], ...]
    order_by: tuple[str, ...]
    # SQL predicate for source rows that must not be stored; they are deleted in
    # staging before the swap and counted in the load log's ``dropped`` column.
    invalid_rows: str = "0"
    # Files after this day are never loaded, even without --end.
    last_day: dt.date | None = None

    @property
    def column_names(self) -> list[str]:
        """The tardis CSV header."""
        return [name for name, _ in self.columns]

    @property
    def insert_columns(self) -> list[str]:
        """Table columns the CSV fields are inserted into, positionally."""
        return [f"{name}_us" if type_ in TIMESTAMP_TYPES else name for name, type_ in self.columns]

    def create_table_sql(self, database: str) -> str:
        defs = []
        for name, type_ in self.columns:
            if type_ in TIMESTAMP_TYPES:
                raw_type, stored_type = TIMESTAMP_TYPES[type_]
                defs.append(f"`{name}_us` {raw_type} EPHEMERAL")
                defs.append(
                    f"`{name}` {stored_type} "
                    f"DEFAULT fromUnixTimestamp64Micro(`{name}_us`, 'UTC') CODEC(Delta, ZSTD(1))"
                )
            else:
                defs.append(f"`{name}` {type_}")
        cols = ",\n    ".join(defs)
        return (
            f"CREATE TABLE IF NOT EXISTS `{database}`.`{self.table}`\n(\n    {cols}\n)\n"
            f"ENGINE = MergeTree\n"
            f"PARTITION BY {PARTITION_BY}\n"
            f"ORDER BY ({', '.join(self.order_by)})"
        )


LIQUIDATIONS = Dataset(
    name="liquidations",
    source_dir="binance-futures/liquidations",
    file_kind="PERPETUALS",
    table="tardis_binance_futures_liquidations",
    columns=(
        ("exchange", "LowCardinality(String)"),
        ("symbol", "LowCardinality(String)"),
        ("timestamp", MICROS),
        ("local_timestamp", MICROS),
        ("id", "String"),
        ("side", "Enum8('unknown' = 0, 'buy' = 1, 'sell' = 2)"),
        ("price", "Float64"),
        ("amount", "Float64"),
    ),
    order_by=("symbol", "timestamp"),
    # Source files contain some liquidations with price 0. NOT (x > 0) also catches NaN.
    invalid_rows="NOT (price > 0 AND amount > 0)",
)

TRADE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("exchange", "LowCardinality(String)"),
    ("symbol", "LowCardinality(String)"),
    ("timestamp", MICROS),
    ("local_timestamp", MICROS),
    # Binance trade ids are numeric and increase per symbol, so Delta packs them well.
    ("id", "UInt64 CODEC(Delta, ZSTD(1))"),
    ("side", "Enum8('unknown' = 0, 'buy' = 1, 'sell' = 2)"),
    ("price", "Float64 CODEC(ZSTD(1))"),
    ("amount", "Float64 CODEC(ZSTD(1))"),
)

SPOT_TRADES = Dataset(
    name="spot-trades",
    source_dir="binance/trades",
    file_kind="SPOT",
    table="tardis_binance_spot_trades",
    columns=TRADE_COLUMNS,
    order_by=("symbol", "timestamp"),
    invalid_rows="NOT (price > 0 AND amount > 0)",
    last_day=dt.date(2025, 9, 10),
)

FUTURES_TRADES = Dataset(
    name="futures-trades",
    source_dir="binance-futures/trades",
    file_kind="PERPETUALS",
    table="tardis_binance_futures_trades",
    columns=TRADE_COLUMNS,
    order_by=("symbol", "timestamp"),
    invalid_rows="NOT (price > 0 AND amount > 0)",
    last_day=dt.date(2025, 9, 10),
)

NULLABLE_FLOAT = "Nullable(Float64) CODEC(ZSTD(1))"

DERIVATIVE_TICKER = Dataset(
    name="derivative-ticker",
    source_dir="binance-futures/derivative_ticker",
    file_kind="PERPETUALS",
    table="tardis_binance_futures_derivative_ticker",
    # Every field after local_timestamp can be empty (e.g. predicted_funding_rate
    # always is on Binance); empty fields are stored as NULL.
    columns=(
        ("exchange", "LowCardinality(String)"),
        ("symbol", "LowCardinality(String)"),
        ("timestamp", MICROS),
        ("local_timestamp", MICROS),
        ("funding_timestamp", NULLABLE_MICROS),
        ("funding_rate", NULLABLE_FLOAT),
        ("predicted_funding_rate", NULLABLE_FLOAT),
        ("open_interest", NULLABLE_FLOAT),
        ("last_price", NULLABLE_FLOAT),
        ("index_price", NULLABLE_FLOAT),
        ("mark_price", NULLABLE_FLOAT),
    ),
    order_by=("symbol", "timestamp"),
    # A present value must be sane; NULLs are fine. NOT (x > 0) also catches NaN.
    invalid_rows=(
        "NOT (ifNull(last_price, 1) > 0 AND ifNull(index_price, 1) > 0"
        " AND ifNull(mark_price, 1) > 0 AND ifNull(open_interest, 0) >= 0"
        " AND isFinite(ifNull(funding_rate, 0)) AND isFinite(ifNull(predicted_funding_rate, 0)))"
    ),
    last_day=dt.date(2025, 9, 10),
)

DATASETS: dict[str, Dataset] = {
    d.name: d for d in (LIQUIDATIONS, SPOT_TRADES, FUTURES_TRADES, DERIVATIVE_TICKER)
}


def create_load_log_sql(database: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{LOAD_LOG_TABLE}`\n(\n"
        "    `table` LowCardinality(String),\n"
        "    `file` String,\n"
        "    `day` Date,\n"
        "    `file_size` UInt64,\n"
        "    `file_mtime` DateTime('UTC'),\n"
        "    `rows` UInt64,\n"
        "    `dropped` UInt64 DEFAULT 0,\n"
        "    `loaded_at` DateTime64(3, 'UTC') DEFAULT now64(3)\n"
        ")\n"
        "ENGINE = ReplacingMergeTree(loaded_at)\n"
        "ORDER BY (`table`, `file`)"
    )


def add_load_log_dropped_sql(database: str) -> str:
    """Upgrade a load log created before the ``dropped`` column existed."""
    return (
        f"ALTER TABLE `{database}`.`{LOAD_LOG_TABLE}` "
        "ADD COLUMN IF NOT EXISTS `dropped` UInt64 DEFAULT 0 AFTER `rows`"
    )
