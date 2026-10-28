# tardis_dataloader

Imports tick data files in [tardis.dev](https://tardis.dev) CSV format from local disk into ClickHouse.

## Setup

```sh
uv sync
cp .env.example .env   # fill in ClickHouse credentials
```

## Usage

```sh
uv run tardis-load liquidations --dry-run          # list files that would load
uv run tardis-load liquidations --start 2022-01-01 --end 2022-01-31
uv run tardis-load liquidations                    # everything not yet loaded
```

Each daily file replaces its own day partition (`toDate(local_timestamp)`), and loaded
files are recorded in `market.tardis_load_log`, so reruns skip files that are already
loaded. Use `--force` to reload them.

Rows matching a dataset's invalid-row predicate (for liquidations, `price` not > 0) are
dropped before they reach the table; `tardis_load_log.dropped` records how many per file.

| Dataset | Source | Table |
|---|---|---|
| `liquidations` | `binance-futures/liquidations` | `market.tardis_binance_futures_liquidations` |
