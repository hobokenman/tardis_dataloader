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

Run `/check` (all of the above) before declaring work done or opening a PR.

## Configuration

- ClickHouse connection settings come from environment variables, loaded from a gitignored `.env` (see `.env.example`). Never hardcode or commit credentials.
- ClickHouse runs locally in Docker (container `clickhouse`). Tables go in the existing `market` database with a `tardis_` prefix. Other `market` tables belong to separate research work — never modify or drop them.
- To test against a real server without touching `market`, start a throwaway container from the same image on another port (e.g. `-p 127.0.0.1:18123:8123 -e CLICKHOUSE_SKIP_USER_SETUP=1`) and point `CLICKHOUSE_PORT` at it.

## Raw data (`/Volumes/DeskSSD/datasets`, ~4.3 TB, read-only)

- One file per UTC day: `<exchange>/<data_type>/YYYY-MM-DD_<KIND>.csv.gz`. Directories contain macOS `._*` metadata files — skip them.
- Detect compression from magic bytes, never the extension: the `binance-futures/trades/*.csv.xz` files are actually zstd.
- Timestamps are integer microseconds. ClickHouse can't parse them from CSV into `DateTime64` (neither `basic` nor `best_effort`), so they're inserted into `Int64 EPHEMERAL` columns and converted with `fromUnixTimestamp64Micro`.
- Daily files are split on `local_timestamp` (exchange `timestamp` can fall on the previous day), so tables partition by `toDate(local_timestamp)` and a file reload is DROP PARTITION + insert.
- `binance-futures/book_snapshot_5` is parquet, not tardis CSV. Files before 2025-08-10 have all-null timestamps (and the column is misspelled `timstamp`) — out of scope for now.

## Git

- Never commit to `main`. Work on a feature branch and open a PR with `gh pr create`.
