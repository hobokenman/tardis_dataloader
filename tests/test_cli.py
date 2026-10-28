from __future__ import annotations

import gzip
from pathlib import Path
from typing import Any

import clickhouse_connect
import pytest
from fakes import FakeClient

from tardis_dataloader import cli
from tardis_dataloader.config import Settings
from tardis_dataloader.datasets import LIQUIDATIONS

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


def _run(monkeypatch: pytest.MonkeyPatch, client: FakeClient, *args: str) -> int:
    def get_client(**kwargs: Any) -> FakeClient:
        return client

    monkeypatch.setattr(clickhouse_connect, "get_client", get_client)
    return cli.main(["liquidations", *args])


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
