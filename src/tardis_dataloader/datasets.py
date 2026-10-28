"""Tardis dataset definitions and their ClickHouse table schemas.

Column names match the tardis CSV header exactly, in order; the header is
checked before every load and columns are then inserted positionally.

Tardis timestamps are integer microseconds since the epoch, which ClickHouse
cannot parse from CSV into DateTime64. Each ``MICROS`` column is therefore
inserted into an ``<name>_us Int64 EPHEMERAL`` column and stored as
``DateTime64(6, 'UTC')`` via ``fromUnixTimestamp64Micro``.

Tables are partitioned by the UTC day of ``local_timestamp``. Tardis splits
daily files on local_timestamp, so one file maps to exactly one partition,
which lets a file be reloaded idempotently by replacing its partition.
"""

from __future__ import annotations

from dataclasses import dataclass

MICROS = "MICROS"
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

    @property
    def column_names(self) -> list[str]:
        """The tardis CSV header."""
        return [name for name, _ in self.columns]

    @property
    def insert_columns(self) -> list[str]:
        """Table columns the CSV fields are inserted into, positionally."""
        return [f"{name}_us" if type_ == MICROS else name for name, type_ in self.columns]

    def create_table_sql(self, database: str) -> str:
        defs = []
        for name, type_ in self.columns:
            if type_ == MICROS:
                defs.append(f"`{name}_us` Int64 EPHEMERAL")
                defs.append(
                    f"`{name}` DateTime64(6, 'UTC') "
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
    invalid_rows="NOT (price > 0)",
)

DATASETS: dict[str, Dataset] = {d.name: d for d in (LIQUIDATIONS,)}


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
