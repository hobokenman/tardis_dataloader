from __future__ import annotations

import datetime as dt
import re
from types import SimpleNamespace
from typing import Any, cast

import pytest
from clickhouse_connect.driver.client import Client

from tardis_dataloader import cli
from tardis_dataloader.bar_builder import BarBuilder, BuildError, HourlyBarBuilder
from tardis_dataloader.bars import (
    BAR_TABLES,
    FUTURES_BARS,
    FUTURES_BARS_1H,
    SPOT_BARS,
    BarTable,
)

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
        minute_totals: tuple[int, ...] = (),
        bar_log: dict[str, list[tuple[Any, ...]]] | None = None,
    ) -> None:
        self.staged = [("20250910", 10, 100, 3)] if staged is None else staged
        self.log_rows = log_rows or []
        self.built_rows = built_rows or []
        self.minute_totals = minute_totals
        # Bar log rows by bar table, for builders that read several tables' entries.
        self.bar_log = bar_log
        self.calls: list[tuple[str, Any]] = []

    def command(self, cmd: str, parameters: Any = None, settings: Any = None) -> Any:
        self.calls.append(("command", cmd))
        return 1 if cmd.startswith("EXISTS TABLE") else None

    def query(self, query: str, parameters: dict[str, Any] | None = None) -> Any:
        self.calls.append(("query", query))
        if "GROUP BY _partition_id" in query:
            return SimpleNamespace(result_rows=self.staged)
        if query.startswith("SELECT 0"):
            return SimpleNamespace(result_rows=[(0, *self.minute_totals)])
        if "tardis_bar_log" in query:
            if self.bar_log is not None and parameters is not None:
                return SimpleNamespace(result_rows=self.bar_log.get(parameters["t"], []))
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


MONTH = dt.date(2025, 8, 1)
HOURLY_TARGET = f"`market`.`{FUTURES_BARS_1H.table}`"
HOURLY_STAGING = f"`market`.`{FUTURES_BARS_1H.table}_staging`"
# trade_count, buy_trade_count, sell_trade_count, long and short liquidation counts.
TOTALS = (100, 60, 40, 2, 1)


def _hourly(client: FakeClient) -> HourlyBarBuilder:
    return HourlyBarBuilder(cast(Client, client), "market", FUTURES_BARS_1H)


def test_build_month_checks_counts_against_minute_bars_then_swaps() -> None:
    client = FakeClient(staged=[("202508", 24, *TOTALS)], minute_totals=TOTALS)
    assert _hourly(client).build_month(MONTH) == 24
    minute_query = next(q for kind, q in client.calls if kind == "query" and "SELECT 0" in q)
    assert "timestamp > toDateTime('2025-08-01', 'UTC')" in minute_query
    assert "timestamp <= toDateTime('2025-09-01', 'UTC')" in minute_query
    assert [c for c in client.commands() if c.startswith(f"ALTER TABLE {HOURLY_TARGET}")] == [
        f"ALTER TABLE {HOURLY_TARGET} REPLACE PARTITION ID '202508' FROM {HOURLY_STAGING}"
    ]
    assert client.calls[-1] == ("insert", [[FUTURES_BARS_1H.table, MONTH, 24]])


@pytest.mark.parametrize(
    "staged",
    [
        pytest.param([("202508", 24, 99, 60, 40, 2, 1)], id="count-mismatch"),
        pytest.param([("202508", 20, *TOTALS), ("202509", 4, 0, 0, 0, 0, 0)], id="two-months"),
    ],
)
def test_bad_hourly_staging_leaves_target_untouched(staged: list[tuple[Any, ...]]) -> None:
    client = FakeClient(staged=staged, minute_totals=TOTALS)
    with pytest.raises(BuildError, match="Target table left unchanged"):
        _hourly(client).build_month(MONTH)
    assert not [c for c in client.commands() if c.startswith(f"ALTER TABLE {HOURLY_TARGET}")]


def test_hourly_bars_resample_minute_bars_and_label_hour_close() -> None:
    sql = FUTURES_BARS_1H.select_sql("market", MONTH)
    assert "toStartOfHour(m_timestamp - INTERVAL 1 MINUTE) + INTERVAL 1 HOUR AS timestamp" in sql
    assert (
        "argMinIf(m_open, m_timestamp, m_open IS NOT NULL), 'Nullable(Float64)') AS `open`" in sql
    )
    assert "argMaxIf(m_open_interest, m_timestamp, m_open_interest IS NOT NULL)" in sql
    assert "sum(m_vwap * m_amount) / nullIf(sum(m_amount), 0)" in sql
    assert "sum(m_long_liquidation_vwap * m_long_liquidation)" in sql
    assert "CAST(long_liquidation - short_liquidation, 'Float64') AS `net_liquidation`" in sql
    ddl = FUTURES_BARS_1H.create_table_sql("market")
    assert "PARTITION BY toYYYYMM(timestamp - INTERVAL 1 MINUTE)" in ddl
    assert "[timestamp - 1 hour, timestamp)" in ddl
    assert "Last open interest in the hour" in ddl


@pytest.mark.parametrize("bars", BAR_TABLES.values(), ids=BAR_TABLES)
def test_every_column_has_a_valid_resample_rule(bars: BarTable) -> None:
    for c in bars.columns:
        if c.derived is None:
            c.resample_sql()
        if c.resample.startswith("vwap:"):
            assert c.resample.removeprefix("vwap:") in bars.column_names


def test_hourly_main_rebuilds_months_whose_minute_bars_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jul, aug = dt.date(2025, 7, 1), MONTH
    client = FakeClient(
        bar_log={
            FUTURES_BARS.table: [(jul, LOADED), (aug, LOADED)],
            # July was built after its minute bars; August before they were rebuilt.
            FUTURES_BARS_1H.table: [
                (jul, LOADED + dt.timedelta(hours=1)),
                (aug, LOADED - dt.timedelta(hours=1)),
            ],
        }
    )
    monkeypatch.setattr(cli, "connect", lambda settings: client)
    monkeypatch.setattr(cli, "load_settings", lambda: SimpleNamespace(database="market"))
    built: list[dt.date] = []

    def build_month(self: HourlyBarBuilder, month: dt.date) -> int:
        built.append(month)
        return 1

    monkeypatch.setattr(HourlyBarBuilder, "build_month", build_month)
    assert cli.bars_main(["futures-1h"]) == 0
    assert built == [aug]
