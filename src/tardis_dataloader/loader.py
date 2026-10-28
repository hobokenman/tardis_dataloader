"""Load daily tardis CSV files into ClickHouse, one partition per file."""

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

    def ensure_tables(self) -> None:
        self.client.command(create_load_log_sql(self.database))
        self.client.command(self.dataset.create_table_sql(self.database))

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
        self.client.command(f"ALTER TABLE {self.table} DROP PARTITION ID '{partition_id}'")
        with source.path.open("rb") as body:
            summary = self.client.raw_insert(
                self.table,
                column_names=self.dataset.insert_columns,
                insert_block=body,
                fmt="CSVWithNames",
                compression=compression,
                settings=INSERT_SETTINGS,
            )
        written = int(summary.written_rows)

        in_partition = int(
            self.client.query(
                f"SELECT count() FROM {self.table} WHERE _partition_id = {{p:String}}",
                parameters={"p": partition_id},
            ).result_rows[0][0]
        )
        if in_partition != written:
            raise LoadError(
                f"{source.path}: inserted {written} rows but partition {partition_id} has "
                f"{in_partition}; rows may fall outside {source.day} (local_timestamp)"
            )

        self.client.insert(
            f"{self.database}.{LOAD_LOG_TABLE}",
            [
                [
                    self.dataset.table,
                    source.name,
                    source.day,
                    source.size,
                    source.mtime,
                    written,
                ]
            ],
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
