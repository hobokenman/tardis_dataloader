from __future__ import annotations

import datetime as dt
import re
from types import SimpleNamespace
from typing import Any, cast

import pytest
from clickhouse_connect.driver.client import Client

from tardis_dataloader import cli
from tardis_dataloader.bar_builder import BarBuilder, BuildError
from tardis_dataloader.bars import BAR_TABLES, FUTURES_BARS, SPOT_BARS, BarTable

DAY = dt.date(2025, 9, 10)
LOADED = dt.datetime(2025, 9, 11)
TARGET = f"`market`.`{FUTURES_BARS.table}`"
STAGING = f"`market`.`{FUTURES_BARS.table}_staging`"
SOURCES = {
    "tardis_binance_futures_trades": (100, LOADED),
    "tardis_binance_futures_derivative_ticker": (50, LOADED),
    "tardis_binance_futures_liquidations": (3, LOADED),
}


class FakeClient:
    """Records calls; the staged bars are ``staged`` rows of (partition id, bars, *checks)."""

    def __init__(
        self,
        staged: list[tuple[Any, ...]] | None = None,
        log_rows: list[tuple[Any, ...]] | None = None,
        built_rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self.staged = [("20250910", 10, 100, 3)] if staged is None else staged
        self.log_rows = log_rows or []
        self.built_rows = built_rows or []
        self.calls: list[tuple[str, Any]] = []

    def command(self, cmd: str, parameters: Any = None, settings: Any = None) -> Any:
        self.calls.append(("command", cmd))
        return 1 if cmd.startswith("EXISTS TABLE") else None

    def query(self, query: str, parameters: dict[str, Any] | None = None) -> Any:
        self.calls.append(("query", query))
        if "GROUP BY _partition_id" in query:
            return SimpleNamespace(result_rows=self.staged)
        if "tardis_bar_log" in query:
            return SimpleNamespace(result_rows=self.built_rows)
        return SimpleNamespace(result_rows=self.log_rows)

    def insert(self, table: str, data: list[list[Any]], **kwargs: Any) -> None:
        self.calls.append(("insert", data))

    def commands(self) -> list[str]:
        return [arg for kind, arg in self.calls if kind == "command"]

    def target_commands(self) -> list[str]:
        return [c for c in self.commands() if c.startswith(f"ALTER TABLE {TARGET}")]


def _builder(client: FakeClient) -> BarBuilder:
    return BarBuilder(cast(Client, client), "market", FUTURES_BARS)


def test_build_day_stages_checks_then_swaps_partition() -> None:
    client = FakeClient()
    assert _builder(client).build_day(DAY, SOURCES) == 10
    commands = client.commands()
    assert commands[0] == f"TRUNCATE TABLE {STAGING}"
    assert commands[1].startswith(f"INSERT INTO {STAGING} (`exchange`, `symbol`, `timestamp`")
    assert client.target_commands() == [
        f"ALTER TABLE {TARGET} REPLACE PARTITION ID '20250910' FROM {STAGING}"
    ]
    assert client.calls[-1] == ("insert", [[FUTURES_BARS.table, DAY, 10]])


@pytest.mark.parametrize(
    "staged",
    [
        pytest.param([("20250910", 10, 99, 3)], id="trade-count-mismatch"),
        pytest.param([("20250910", 10, 100, 2)], id="liquidation-count-mismatch"),
        pytest.param([("20250910", 9, 90, 3), ("20250911", 1, 10, 0)], id="two-partitions"),
        pytest.param([("20250911", 10, 100, 3)], id="wrong-day"),
    ],
)
def test_bad_staging_leaves_target_untouched(staged: list[tuple[Any, ...]]) -> None:
    client = FakeClient(staged=staged)
    with pytest.raises(BuildError, match="Target table left unchanged"):
        _builder(client).build_day(DAY, SOURCES)
    assert client.target_commands() == []
    assert "insert" not in [kind for kind, _ in client.calls]


def test_day_without_ticks_drops_partition() -> None:
    client = FakeClient(staged=[])
    empty = {table: (0, LOADED) for table in SOURCES}
    assert _builder(client).build_day(DAY, empty) == 0
    assert client.target_commands() == [f"ALTER TABLE {TARGET} DROP PARTITION ID '20250910'"]


def test_source_days_require_every_source() -> None:
    rows = [(DAY, table, rows, loaded) for table, (rows, loaded) in SOURCES.items()]
    rows.append((DAY + dt.timedelta(days=1), "tardis_binance_futures_trades", 5, LOADED))
    days = _builder(FakeClient(log_rows=rows)).source_days()
    assert days == {DAY: SOURCES}


@pytest.mark.parametrize("bars", BAR_TABLES.values(), ids=BAR_TABLES)
def test_union_branches_align_and_projections_avoid_alias_clashes(bars: BarTable) -> None:
    sql = bars.select_sql("market", DAY)
    merged = [c.name for c in bars.columns if c.derived is None]
    branches = sql.split("\nFROM\n(\n", 1)[1].split("UNION ALL")
    assert len(branches) == len(bars.sources)
    for branch in branches:
        assert re.findall(r"AS `(\w+)`\n?,?", branch.split("FROM (SELECT")[0]) == merged
    for source in bars.sources:
        outputs = {part.split()[-1] for part in source.projection.split(",")}
        # symbol is the grouping key; every other bar column must not shadow a raw column.
        assert outputs & set(bars.column_names) == {"symbol"}, source.table
        assert set(source.aggregates) <= set(merged)


def test_bars_bucket_by_receive_time_and_label_minute_close() -> None:
    sql = FUTURES_BARS.select_sql("market", DAY)
    assert sql.count("WHERE local_timestamp >= toDateTime('2025-09-10', 'UTC')") == 3
    assert "toStartOfMinute(local_timestamp), 'UTC') + INTERVAL 1 MINUTE AS bar_ts" in sql
    ddl = FUTURES_BARS.create_table_sql("market")
    assert "PARTITION BY toDate(timestamp - INTERVAL 1 MINUTE)" in ddl
    assert "ORDER BY (symbol, timestamp)" in ddl


def test_spot_prices_are_not_nullable_but_futures_prices_are() -> None:
    spot = {c.name: c.type for c in SPOT_BARS.columns}
    futures = {c.name: c.type for c in FUTURES_BARS.columns}
    assert spot["close"] == "Float64"
    assert futures["close"] == "Nullable(Float64)"
    assert futures["open_interest"] == "Nullable(Float64)"


def test_bars_main_skips_built_days_unless_a_source_was_reloaded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    next_day = DAY + dt.timedelta(days=1)
    rows = [(d, table, n, LOADED) for d in (DAY, next_day) for table, (n, _) in SOURCES.items()]
    # DAY was built after its sources loaded; next_day before a reload.
    built = [(DAY, LOADED + dt.timedelta(hours=1)), (next_day, LOADED - dt.timedelta(hours=1))]
    client = FakeClient(log_rows=rows, built_rows=built)
    monkeypatch.setattr(cli, "connect", lambda settings: client)
    monkeypatch.setattr(cli, "load_settings", lambda: SimpleNamespace(database="market"))
    built_days: list[dt.date] = []

    def build_day(self: BarBuilder, day: dt.date, sources: object) -> int:
        built_days.append(day)
        return 1

    monkeypatch.setattr(BarBuilder, "build_day", build_day)
    assert cli.bars_main(["futures"]) == 0
    assert built_days == [next_day]
