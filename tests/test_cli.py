from __future__ import annotations

import gzip
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import clickhouse_connect
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from fakes import FakeClient

from tardis_dataloader import cli
from tardis_dataloader.config import Settings
from tardis_dataloader.datasets import FUTURES_TRADES, LIQUIDATIONS
from tardis_dataloader.files import SourceFile
from tardis_dataloader.loader import Loader

DAYS = ["2022-01-01", "2022-01-02", "2022-01-03"]


@pytest.fixture
def data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    src = tmp_path / LIQUIDATIONS.source_dir
    src.mkdir(parents=True)
    for day in DAYS:
        body = ",".join(LIQUIDATIONS.column_names) + "\n"
        (src / f"{day}_PERPETUALS.csv.gz").write_bytes(gzip.compress(body.encode()))
    settings = Settings("localhost", 8123, "default", "", "market", tmp_path)
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    return src


def _run(
    monkeypatch: pytest.MonkeyPatch, client: FakeClient, *args: str, dataset: str = "liquidations"
) -> int:
    def get_client(**kwargs: Any) -> FakeClient:
        return client

    monkeypatch.setattr(clickhouse_connect, "get_client", get_client)
    return cli.main([dataset, *args])


def _inserted_days(client: FakeClient) -> list[str]:
    return [
        Path(arg["insert_block"].name).name[:10]
        for kind, arg in client.calls
        if kind == "raw_insert"
    ]


def test_dry_run_makes_no_changes(
    data_root: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    client = FakeClient(log_exists=False)
    assert _run(monkeypatch, client, "--dry-run") == 0
    assert client.commands() == ["EXISTS TABLE `market`.`tardis_load_log`"]
    assert client.kinds() == ["command"]
    assert capsys.readouterr().out.splitlines() == [
        str(data_root / f"{d}_PERPETUALS.csv.gz") for d in DAYS
    ]


def test_skips_loaded_files_unless_forced(data_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    first = data_root / f"{DAYS[0]}_PERPETUALS.csv.gz"
    stat = first.stat()
    log_rows = [(first.name, stat.st_size, int(stat.st_mtime))]

    client = FakeClient(written=0, log_rows=log_rows)
    assert _run(monkeypatch, client) == 0
    assert _inserted_days(client) == DAYS[1:]

    forced = FakeClient(written=0, log_rows=log_rows)
    assert _run(monkeypatch, forced, "--force") == 0
    assert _inserted_days(forced) == DAYS


def test_limit_caps_files_loaded(data_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient(written=0)
    assert _run(monkeypatch, client, "--limit", "1") == 0
    assert _inserted_days(client) == DAYS[:1]


def test_stops_at_first_failed_file(data_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient(written=5, staged={"20211231": 5})
    assert _run(monkeypatch, client) == 1
    assert _inserted_days(client) == DAYS[:1]


@pytest.mark.parametrize(
    "args",
    [
        ["--limit", "0"],
        ["--limit", "-1"],
        ["--start", "2022-01-03", "--end", "2022-01-01"],
    ],
)
def test_rejects_bad_arguments(args: list[str]) -> None:
    with pytest.raises(SystemExit):
        cli.parse_args(["liquidations", *args])


def test_last_day_caps_files_even_with_later_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = tmp_path / FUTURES_TRADES.source_dir
    src.mkdir(parents=True)
    body = (",".join(FUTURES_TRADES.column_names) + "\n").encode()
    for day in ["2025-09-10", "2025-09-11", "2025-10-01"]:
        (src / f"{day}_PERPETUALS.csv.gz").write_bytes(gzip.compress(body))
    settings = Settings("localhost", 8123, "default", "", "market", tmp_path)
    monkeypatch.setattr(cli, "load_settings", lambda: settings)

    for args in ([], ["--end", "2025-12-31"]):
        client = FakeClient(written=0)
        assert _run(monkeypatch, client, *args, dataset="futures-trades") == 0
        assert _inserted_days(client) == ["2025-09-10"]


class FlakyLoader:
    def __init__(self, errors: list[Exception]) -> None:
        self.errors = errors
        self.attempts = 0

    def load_file(self, source: object) -> int:
        self.attempts += 1
        if self.errors:
            raise self.errors.pop(0)
        return 7


def _oom() -> DatabaseError:
    return DatabaseError("memory limit exceeded", code=cli.MEMORY_LIMIT_EXCEEDED)


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    calls: list[float] = []
    monkeypatch.setattr("tardis_dataloader.cli.time.sleep", calls.append)
    return calls


def _retry(loader: FlakyLoader) -> int:
    return cli.load_with_retry(cast(Loader, loader), cast(SourceFile, SimpleNamespace(name="f")))


def test_retries_after_out_of_memory(sleeps: list[float]) -> None:
    loader = FlakyLoader([_oom(), _oom()])
    assert _retry(loader) == 7
    assert loader.attempts == 3
    assert sleeps == list(cli.RETRY_DELAYS_S[:2])


def test_retries_after_lost_connection(sleeps: list[float]) -> None:
    loader = FlakyLoader([OperationalError("Connection aborted.")])
    assert _retry(loader) == 7
    assert loader.attempts == 2
    assert sleeps == list(cli.RETRY_DELAYS_S[:1])


def test_gives_up_after_last_retry(sleeps: list[float]) -> None:
    loader = FlakyLoader([_oom() for _ in range(len(cli.RETRY_DELAYS_S) + 1)])
    with pytest.raises(DatabaseError):
        _retry(loader)
    assert loader.attempts == len(cli.RETRY_DELAYS_S) + 1


def test_other_errors_are_not_retried(sleeps: list[float]) -> None:
    loader = FlakyLoader([DatabaseError("syntax error", code=62)])
    with pytest.raises(DatabaseError):
        _retry(loader)
    assert loader.attempts == 1
    assert sleeps == []
