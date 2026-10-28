"""Load daily tardis CSV files into ClickHouse, one partition per file.

Each file is inserted into an empty staging table with the target's exact
structure, checked there (exactly one partition, the file's day, holding every
inserted row), and only then swapped into the target with REPLACE PARTITION.
A bad file therefore never touches the target, and readers never see a day
half-loaded. The staging table is shared per dataset, so run one loader per
dataset at a time.
"""

from __future__ import annotations

import logging
import time

from clickhouse_connect.driver.client import Client

from tardis_dataloader.datasets import LOAD_LOG_TABLE, Dataset, create_load_log_sql
from tardis_dataloader.files import SourceFile, detect_compression, read_header

log = logging.getLogger(__name__)

INSERT_SETTINGS = {
    # Skip the header row and insert positionally into Dataset.insert_columns;
    # the header itself is validated in Python before the insert.
    "input_format_with_names_use_header": 0,
}


class LoadError(RuntimeError):
    pass


class Loader:
    def __init__(self, client: Client, database: str, dataset: Dataset) -> None:
        self.client = client
        self.database = database
        self.dataset = dataset
        self.table = f"`{database}`.`{dataset.table}`"
        self.staging = f"`{database}`.`{dataset.table}_staging`"

    def ensure_tables(self) -> None:
        self.client.command(create_load_log_sql(self.database))
        self.client.command(self.dataset.create_table_sql(self.database))
        # Recreated each run so it always matches the target, which REPLACE PARTITION requires.
        self.client.command(f"DROP TABLE IF EXISTS {self.staging}")
        self.client.command(f"CREATE TABLE {self.staging} AS {self.table}")

    def loaded_files(self) -> dict[str, tuple[int, int]]:
        """file name -> (size, mtime epoch seconds) for files already loaded."""
        if not self.client.command(f"EXISTS TABLE `{self.database}`.`{LOAD_LOG_TABLE}`"):
            return {}
        result = self.client.query(
            f"SELECT file, file_size, toUnixTimestamp(file_mtime) "
            f"FROM `{self.database}`.`{LOAD_LOG_TABLE}` FINAL WHERE `table` = {{t:String}}",
            parameters={"t": self.dataset.table},
        )
        return {row[0]: (int(row[1]), int(row[2])) for row in result.result_rows}

    def is_loaded(self, source: SourceFile, loaded: dict[str, tuple[int, int]]) -> bool:
        return loaded.get(source.name) == (source.size, int(source.mtime.timestamp()))

    def load_file(self, source: SourceFile) -> int:
        """Replace the file's day partition with the file's rows; return the row count."""
        compression = detect_compression(source.path)
        header = read_header(source.path, compression)
        if header != self.dataset.column_names:
            raise LoadError(
                f"{source.path}: header {header} != expected {self.dataset.column_names}"
            )

        partition_id = source.day.strftime("%Y%m%d")
        started = time.monotonic()
        self.client.command(f"TRUNCATE TABLE {self.staging}")
        with source.path.open("rb") as body:
            summary = self.client.raw_insert(
                self.staging,
                column_names=self.dataset.insert_columns,
                insert_block=body,
                fmt="CSVWithNames",
                compression=compression,
                settings=INSERT_SETTINGS,
            )
        written = int(summary.written_rows)

        staged = {
            str(p): int(n)
            for p, n in self.client.query(
                f"SELECT _partition_id, count() FROM {self.staging} GROUP BY _partition_id"
            ).result_rows
        }
        expected = {partition_id: written} if written else {}
        if staged != expected:
            raise LoadError(
                f"{source.path}: inserted {written} rows but staging holds {staged} "
                f"(partition id -> rows); expected only {partition_id}, the file's "
                f"local_timestamp day. Target table left unchanged."
            )

        if written:
            self.client.command(
                f"ALTER TABLE {self.table} "
                f"REPLACE PARTITION ID '{partition_id}' FROM {self.staging}"
            )
        else:
            self.client.command(f"ALTER TABLE {self.table} DROP PARTITION ID '{partition_id}'")
        self.client.command(f"TRUNCATE TABLE {self.staging}")

        self.client.insert(
            f"{self.database}.{LOAD_LOG_TABLE}",
            [[self.dataset.table, source.name, source.day, source.size, source.mtime, written]],
            column_names=["table", "file", "day", "file_size", "file_mtime", "rows"],
        )
        log.info(
            "%s: %d rows, %.1f MB %s, %.1fs",
            source.name,
            written,
            source.size / 1e6,
            compression,
            time.monotonic() - started,
        )
        return written
