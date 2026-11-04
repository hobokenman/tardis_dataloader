# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Imports tick data files in tardis.dev format from local disk into ClickHouse. Python, managed with uv. Downloading from tardis.dev is out of scope for now — don't add tardis API clients or download code.

## Commands

Always go through `uv` — never call `pip`, `python`, or tools from a global install.

- Add deps: `uv add <pkg>` (dev tools: `uv add --dev <pkg>`)
- Tests: `uv run pytest` (single test: `uv run pytest path/to/test.py::test_name`)
- Lint: `uv run ruff check --fix .`
- Format: `uv run ruff format .`
- Types: `uv run mypy` (paths come from pyproject; `mypy .` would scan `.venv`)
- Load: `uv run tardis-load <dataset> [--start/--end YYYY-MM-DD] [--limit N] [--force] [--dry-run]`
- Build 1m bars from loaded ticks: `uv run tardis-bars {futures,spot} [same options]` (skips built days unless a source day was reloaded since)
- Build 1h bars from the 1m bars: `uv run tardis-bars {futures-1h,spot-1h}` (one month per run step; rebuilds months whose 1m bars were rebuilt since)

Run `/check` (all of the above) before declaring work done or opening a PR.

## Configuration

- ClickHouse connection settings come from environment variables, loaded from a gitignored `.env` (see `.env.example`). Never hardcode or commit credentials.
- ClickHouse runs locally in Docker (container `clickhouse`). Tables go in the existing `market` database with a `tardis_` prefix. Other `market` tables belong to separate research work — never modify or drop them.
- To test against a real server without touching `market`, start a throwaway container from the same image on another port (e.g. `-p 127.0.0.1:18123:8123 -e CLICKHOUSE_SKIP_USER_SETUP=1`) and point `CLICKHOUSE_PORT` at it.

## Raw data (`/Volumes/DeskSSD/datasets`, ~4.3 TB, read-only)

- One file per UTC day: `<exchange>/<data_type>/YYYY-MM-DD_<KIND>.csv.gz`. Directories contain macOS `._*` metadata files — skip them.
- Detect compression from magic bytes, never the extension: the `binance-futures/trades/*.csv.xz` files are actually zstd.
- Timestamps are integer microseconds. ClickHouse can't parse them from CSV into `DateTime64` (neither `basic` nor `best_effort`), so they're inserted into `Int64 EPHEMERAL` columns and converted with `fromUnixTimestamp64Micro`.
- Daily files are split on `local_timestamp` (exchange `timestamp` can fall on the previous day), so tables partition by `toDate(local_timestamp)`. Each file is inserted into a `<table>_staging` copy, checked to hold exactly its own day, then swapped in with `REPLACE PARTITION` — never insert straight into a target table.
- Source files contain invalid rows (e.g. ~3k liquidations with `price = 0`). Each `Dataset.invalid_rows` predicate is deleted in staging before the swap and counted in `tardis_load_log.dropped` — don't store invalid rows in target tables.
- `binance-futures/book_snapshot_5` is parquet, not tardis CSV. Files before 2025-08-10 have all-null timestamps (and the column is misspelled `timstamp`) — out of scope for now.

## Bars (`tardis_binance_{futures,spot}_bars_{1m,1h}`)

- Columns follow the minute-factor input in `~/workspace/quantlib/quant_lib/factors_min.py` (`amount`, `buy_amount`, `trade_count`, `long_liquidation_vwap`, ...), so the bars drop into those factors unrenamed. `amount` is base quantity; `long_*` liquidations are sell-side.
- Ticks are bucketed by `local_timestamp` (receive time, the source partition key); `timestamp` is the minute's right edge (close). Tables partition by `toDate(timestamp - INTERVAL 1 MINUTE)`, the source day.
- Built in SQL per day into `<table>_staging`, checked against `tardis_load_log` (`sum(trade_count)` and liquidation counts must equal the day's source rows), then swapped in with `REPLACE PARTITION`; logged in `tardis_bar_log`.
- 1h tables resample the 1m tables per `BarColumn.resample` (first/last non-NULL, max/min, sums, quantity-weighted VWAPs), use the same right-edge label, and partition by month (`toYYYYMM(timestamp - INTERVAL 1 MINUTE)`); each month's count totals are checked against the 1m bars.
- Not available yet: the factors' `bid_price`/`ask_price`/`bid_amount`/`ask_amount` need book data (`book_snapshot_5`, out of scope).

## Git

- Never commit to `main`. Work on a feature branch and open a PR with `gh pr create`.
