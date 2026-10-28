# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Loads historical crypto market data from tardis.dev into ClickHouse. Python, managed with uv.

## Commands

Always go through `uv` — never call `pip`, `python`, or tools from a global install.

- Add deps: `uv add <pkg>` (dev tools: `uv add --dev <pkg>`)
- Tests: `uv run pytest` (single test: `uv run pytest path/to/test.py::test_name`)
- Lint: `uv run ruff check --fix .`
- Format: `uv run ruff format .`
- Types: `uv run mypy .`

Run `/check` (all of the above) before declaring work done or opening a PR.

## Configuration

- `TARDIS_API_KEY` and ClickHouse connection settings come from environment variables, loaded from a gitignored `.env`. Never hardcode or commit credentials.

## Git

- Never commit to `main`. Work on a feature branch and open a PR with `gh pr create`.
