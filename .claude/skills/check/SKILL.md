---
name: check
description: Run the full quality gate (ruff format check, ruff lint, mypy, pytest) via uv. Use before declaring a change done, before committing, or before opening a PR.
---

Run each step from the repo root, in order, and keep going even if one fails so the user sees every problem at once:

1. `uv run ruff format --check .`
2. `uv run ruff check .`
3. `uv run mypy .`
4. `uv run pytest -q`

If `pyproject.toml` doesn't exist yet, say so and stop. Don't scaffold the project unless asked.

Report a short pass/fail line per step. For failures, show the relevant error lines (not the full output) and propose or apply fixes. Formatting and auto-fixable lint (`ruff format .`, `ruff check --fix .`) may be applied directly; for mypy and test failures, fix the code, then re-run only the failing steps to confirm.

Never "fix" a failure by deleting tests, loosening mypy config, or adding blanket `# type: ignore` / `# noqa` without telling the user.
