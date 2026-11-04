from __future__ import annotations

import datetime as dt
import gzip
from pathlib import Path
from typing import cast

import pytest
from clickhouse_connect.driver.client import Client
from fakes import FakeClient

from tardis_dataloader.datasets import DERIVATIVE_TICKER, FUTURES_TRADES, LIQUIDATIONS
from tardis_dataloader.files import SourceFile
from tardis_dataloader.loader import Loader, LoadError

HEADER = ",".join(LIQUIDATIONS.column_names)
ROW = "binance-futures,YFIUSDT,1640995238529000,1640995238551104,,buy,33591.8,0.002"
TARGET = f"`market`.`{LIQUIDATIONS.table}`"
STAGING = f"`market`.`{LIQUIDATIONS.table}_staging`"


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


def _target_commands(client: FakeClient) -> list[str]:
    return [c for c in client.commands() if c.startswith(f"ALTER TABLE {TARGET}")]


def test_load_file_stages_then_swaps_partition_then_logs(tmp_path: Path) -> None:
    client = FakeClient(written=1)
    assert _loader(client).load_file(_source(tmp_path)) == 1
    assert client.kinds() == ["command", "raw_insert", "query", "command", "command", "insert"]
    assert client.commands()[0] == f"TRUNCATE TABLE {STAGING}"
    insert = client.calls[1][1]
    assert insert["table"] == STAGING
    assert insert["compression"] == "gzip"
    assert insert["column_names"][2:4] == ["timestamp_us", "local_timestamp_us"]
    assert _target_commands(client) == [
        f"ALTER TABLE {TARGET} REPLACE PARTITION ID '20220101' FROM {STAGING}"
    ]
    assert not any("DELETE" in c for c in client.commands())
    logged = client.calls[-1][1][0]
    assert logged[:2] == [LIQUIDATIONS.table, "2022-01-01_PERPETUALS.csv.gz"]
    assert logged[-2:] == [1, 0]


def test_load_file_rejects_unexpected_header_before_touching_db(tmp_path: Path) -> None:
    client = FakeClient()
    with pytest.raises(LoadError, match="header"):
        _loader(client).load_file(_source(tmp_path, header="exchange,symbol"))
    assert client.calls == []


@pytest.mark.parametrize(
    "staged",
    [
        pytest.param({"20220101": 9, "20211231": 1}, id="rows-on-another-day"),
        pytest.param({"20220101": 7}, id="row-count-mismatch"),
        pytest.param({"20220102": 10}, id="wrong-day"),
    ],
)
def test_bad_staging_leaves_target_untouched(tmp_path: Path, staged: dict[str, int]) -> None:
    client = FakeClient(written=10, staged=staged)
    with pytest.raises(LoadError, match="Target table left unchanged"):
        _loader(client).load_file(_source(tmp_path))
    assert _target_commands(client) == []
    assert "insert" not in client.kinds()


def test_invalid_rows_are_deleted_in_staging_before_swap(tmp_path: Path) -> None:
    client = FakeClient(written=10, invalid=3)
    assert _loader(client).load_file(_source(tmp_path)) == 7
    commands = client.commands()
    delete = f"ALTER TABLE {STAGING} DELETE WHERE {LIQUIDATIONS.invalid_rows}"
    replace = f"ALTER TABLE {TARGET} REPLACE PARTITION ID '20220101' FROM {STAGING}"
    assert commands.index(delete) < commands.index(replace)
    logged = client.calls[-1][1][0]
    assert logged[-2:] == [7, 3]


def test_failed_invalid_row_delete_leaves_target_untouched(tmp_path: Path) -> None:
    client = FakeClient(written=10, invalid=3, remaining=10)
    with pytest.raises(LoadError, match="left 10 in staging, expected 7"):
        _loader(client).load_file(_source(tmp_path))
    assert _target_commands(client) == []
    assert "insert" not in client.kinds()


def test_day_with_only_invalid_rows_is_dropped(tmp_path: Path) -> None:
    client = FakeClient(written=2, invalid=2)
    assert _loader(client).load_file(_source(tmp_path)) == 0
    assert _target_commands(client) == [f"ALTER TABLE {TARGET} DROP PARTITION ID '20220101'"]


def test_empty_file_drops_the_day(tmp_path: Path) -> None:
    client = FakeClient(written=0)
    assert _loader(client).load_file(_source(tmp_path)) == 0
    assert _target_commands(client) == [f"ALTER TABLE {TARGET} DROP PARTITION ID '20220101'"]
    assert client.calls[-1][1][0][-2:] == [0, 0]


def test_ensure_tables_recreates_staging_from_target() -> None:
    client = FakeClient()
    _loader(client).ensure_tables()
    assert client.commands()[-2:] == [
        f"DROP TABLE IF EXISTS {STAGING}",
        f"CREATE TABLE {STAGING} AS {TARGET}",
    ]


def test_loaded_files_without_log_table_is_empty() -> None:
    client = FakeClient(log_exists=False)
    assert _loader(client).loaded_files() == {}
    assert "query" not in client.kinds()


def test_is_loaded_requires_matching_size_and_mtime(tmp_path: Path) -> None:
    source = _source(tmp_path)
    loader = _loader(FakeClient())
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


def test_trade_tables_store_numeric_ids() -> None:
    sql = FUTURES_TRADES.create_table_sql("market")
    assert "`id` UInt64 CODEC(Delta, ZSTD(1))" in sql
    assert FUTURES_TRADES.column_names == LIQUIDATIONS.column_names


def test_nullable_timestamps_stay_nullable() -> None:
    sql = DERIVATIVE_TICKER.create_table_sql("market")
    assert "`funding_timestamp_us` Nullable(Int64) EPHEMERAL" in sql
    assert "`funding_timestamp` Nullable(DateTime64(6, 'UTC'))" in sql
    assert "`timestamp_us` Int64 EPHEMERAL" in sql
    assert DERIVATIVE_TICKER.insert_columns[2:5] == [
        "timestamp_us",
        "local_timestamp_us",
        "funding_timestamp_us",
    ]
