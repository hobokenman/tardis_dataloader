"""Build one-minute bars for a day, one partition per day.

Like the loader, each day is built into an empty staging copy of the target,
checked there (exactly the day's partition, and per-source totals equal to the
source rows the load log recorded for that day), and only then swapped in with
REPLACE PARTITION. Run one builder per bar table at a time.
"""

from __future__ import annotations

import datetime as dt
import logging
import time

from clickhouse_connect.driver.client import Client

from tardis_dataloader.bars import BAR_LOG_TABLE, BarTable, create_bar_log_sql
from tardis_dataloader.datasets import LOAD_LOG_TABLE

log = logging.getLogger(__name__)


class BuildError(RuntimeError):
    pass


class BarBuilder:
    def __init__(self, client: Client, database: str, bars: BarTable) -> None:
        self.client = client
        self.database = database
        self.bars = bars
        self.table = f"`{database}`.`{bars.table}`"
        self.staging = f"`{database}`.`{bars.table}_staging`"

    def ensure_tables(self) -> None:
        self.client.command(create_bar_log_sql(self.database))
        self.client.command(self.bars.create_table_sql(self.database))
        # Recreated each run so it always matches the target, which REPLACE PARTITION requires.
        self.client.command(f"DROP TABLE IF EXISTS {self.staging}")
        self.client.command(f"CREATE TABLE {self.staging} AS {self.table}")

    def source_days(self) -> dict[dt.date, dict[str, tuple[int, dt.datetime]]]:
        """day -> source table -> (rows, loaded_at), for days every source has loaded."""
        result = self.client.query(
            f"SELECT day, `table`, sum(rows), max(loaded_at) "
            f"FROM `{self.database}`.`{LOAD_LOG_TABLE}` FINAL "
            f"WHERE `table` IN {{tables:Array(String)}} GROUP BY day, `table`",
            parameters={"tables": self.bars.source_tables},
        )
        days: dict[dt.date, dict[str, tuple[int, dt.datetime]]] = {}
        for day, table, rows, loaded_at in result.result_rows:
            days.setdefault(day, {})[table] = (int(rows), loaded_at)
        wanted = set(self.bars.source_tables)
        return {d: s for d, s in sorted(days.items()) if set(s) == wanted}

    def built_days(self) -> dict[dt.date, dt.datetime]:
        """day -> built_at for days already built."""
        if not self.client.command(f"EXISTS TABLE `{self.database}`.`{BAR_LOG_TABLE}`"):
            return {}
        result = self.client.query(
            f"SELECT day, built_at FROM `{self.database}`.`{BAR_LOG_TABLE}` FINAL "
            f"WHERE `table` = {{t:String}}",
            parameters={"t": self.bars.table},
        )
        return {day: built_at for day, built_at in result.result_rows}

    def build_day(self, day: dt.date, sources: dict[str, tuple[int, dt.datetime]]) -> int:
        """Replace the day's partition with bars built from its sources; return the bar count."""
        partition_id = day.strftime("%Y%m%d")
        started = time.monotonic()
        self.client.command(f"TRUNCATE TABLE {self.staging}")
        columns = ", ".join(f"`{c}`" for c in self.bars.column_names)
        self.client.command(
            f"INSERT INTO {self.staging} ({columns})\n{self.bars.select_sql(self.database, day)}"
        )

        checks = "".join(f", {expr}" for expr, _ in self.bars.checks)
        rows = self.client.query(
            f"SELECT _partition_id, count(){checks} FROM {self.staging} GROUP BY _partition_id"
        ).result_rows
        staged = {str(row[0]): int(row[1]) for row in rows}
        bars = sum(staged.values())
        if bars and list(staged) != [partition_id]:
            raise BuildError(
                f"{self.bars.table} {day}: staging holds partitions {staged} (id -> bars), "
                f"expected only {partition_id}. Target table left unchanged."
            )
        for i, (expr, table) in enumerate(self.bars.checks):
            got = int(rows[0][2 + i]) if rows else 0
            expected = sources[table][0]
            if got != expected:
                raise BuildError(
                    f"{self.bars.table} {day}: {expr} = {got} but {table} has {expected} "
                    f"rows that day. Target table left unchanged."
                )

        if bars:
            self.client.command(
                f"ALTER TABLE {self.table} "
                f"REPLACE PARTITION ID '{partition_id}' FROM {self.staging}"
            )
        else:
            self.client.command(f"ALTER TABLE {self.table} DROP PARTITION ID '{partition_id}'")
        self.client.command(f"TRUNCATE TABLE {self.staging}")

        self.client.insert(
            f"{self.database}.{BAR_LOG_TABLE}",
            [[self.bars.table, day, bars]],
            column_names=["table", "day", "bars"],
        )
        log.info("%s %s: %d bars, %.1fs", self.bars.table, day, bars, time.monotonic() - started)
        return bars
