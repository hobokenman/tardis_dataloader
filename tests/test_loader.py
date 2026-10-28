from __future__ import annotations

import datetime as dt
import gzip
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from clickhouse_connect.driver.client import Client

from tardis_dataloader.datasets import LIQUIDATIONS
from tardis_dataloader.files import SourceFile
from tardis_dataloader.loader import Loader, LoadError

HEADER = ",".join(LIQUIDATIONS.column_names)
ROW = "binance-futures,YFIUSDT,1640995238529000,1640995238551104,,buy,33591.8,0.002"


class FakeClient:
    def __init__(self, written: int, in_partition: int) -> None:
        self.written = written
        self.in_partition = in_partition
        self.calls: list[tuple[str, Any]] = []

    def command(self, cmd: str, parameters: dict[str, Any] | None = None) -> None:
        self.calls.append(("command", cmd))

    def query(self, query: str, parameters: dict[str, Any] | None = None) -> Any:
        self.calls.append(("query", query))
        return SimpleNamespace(result_rows=[(self.in_partition,)])

    def raw_insert(self, table: str, **kwargs: Any) -> Any:
        self.calls.append(("raw_insert", (kwargs["compression"], kwargs["column_names"])))
        return SimpleNamespace(written_rows=self.written)

    def insert(self, table: str, data: list[list[Any]], **kwargs: Any) -> None:
        self.calls.append(("insert", data))


def _source(tmp_path: Path, header: str = HEADER) -> SourceFile:
    path = tmp_path / "2022-01-01_PERPETUALS.csv.gz"
    path.write_bytes(gzip.compress(f"{header}\n{ROW}\n".encode()))
    return SourceFile(
        path=path,
        day=dt.date(2022, 1, 1),
        size=path.stat().st_size,
        mtime=dt.datetime(2025, 9, 11, tzinfo=dt.UTC),
    )


def _loader(client: FakeClient) -> Loader:
    return Loader(cast(Client, client), "market", LIQUIDATIONS)


def test_load_file_replaces_partition_then_logs(tmp_path: Path) -> None:
    client = FakeClient(written=1, in_partition=1)
    assert _loader(client).load_file(_source(tmp_path)) == 1
    kinds = [kind for kind, _ in client.calls]
    assert kinds == ["command", "raw_insert", "query", "insert"]
    assert "DROP PARTITION ID '20220101'" in client.calls[0][1]
    compression, columns = client.calls[1][1]
    assert compression == "gzip"
    assert columns[2:4] == ["timestamp_us", "local_timestamp_us"]
    logged = client.calls[3][1][0]
    assert logged[:2] == [LIQUIDATIONS.table, "2022-01-01_PERPETUALS.csv.gz"]
    assert logged[-1] == 1


def test_load_file_rejects_unexpected_header_before_touching_db(tmp_path: Path) -> None:
    client = FakeClient(written=1, in_partition=1)
    with pytest.raises(LoadError, match="header"):
        _loader(client).load_file(_source(tmp_path, header="exchange,symbol"))
    assert client.calls == []


def test_load_file_fails_when_rows_land_outside_partition(tmp_path: Path) -> None:
    client = FakeClient(written=10, in_partition=7)
    with pytest.raises(LoadError, match="inserted 10 rows"):
        _loader(client).load_file(_source(tmp_path))
    assert "insert" not in [kind for kind, _ in client.calls]


def test_is_loaded_requires_matching_size_and_mtime(tmp_path: Path) -> None:
    source = _source(tmp_path)
    loader = _loader(FakeClient(0, 0))
    stamp = int(source.mtime.timestamp())
    assert loader.is_loaded(source, {source.name: (source.size, stamp)})
    assert not loader.is_loaded(source, {source.name: (source.size + 1, stamp)})
    assert not loader.is_loaded(source, {})


def test_table_columns_match_tardis_header_order() -> None:
    sql = LIQUIDATIONS.create_table_sql("market")
    positions = [sql.index(f"`{name}`") for name in LIQUIDATIONS.insert_columns]
    assert positions == sorted(positions)
    assert "`timestamp_us` Int64 EPHEMERAL" in sql
    assert "fromUnixTimestamp64Micro(`local_timestamp_us`, 'UTC')" in sql
    assert "PARTITION BY toDate(local_timestamp)" in sql
