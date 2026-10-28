from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any


class FakeClient:
    """Records every call; returns scripted results for the reads the loader makes.

    By default the staging table holds exactly the inserted rows, in the
    partition of the inserted file's day; pass ``staged`` to override. ``invalid``
    of those rows match the dataset's invalid predicate, and deleting them leaves
    ``remaining`` rows (default: the correct count).
    """

    def __init__(
        self,
        written: int = 1,
        staged: dict[str, int] | None = None,
        log_exists: bool = True,
        log_rows: list[tuple[str, int, int]] | None = None,
        invalid: int = 0,
        remaining: int | None = None,
    ) -> None:
        self.written = written
        self.staged = staged
        self.invalid = invalid
        self.remaining = remaining
        self.log_exists = log_exists
        self.log_rows = log_rows or []
        self.calls: list[tuple[str, Any]] = []
        self._last_partition = ""

    def command(self, cmd: str, parameters: Any = None, settings: Any = None) -> Any:
        self.calls.append(("command", cmd))
        if cmd.startswith("EXISTS TABLE"):
            return int(self.log_exists)
        return None

    def query(self, query: str, parameters: dict[str, Any] | None = None) -> Any:
        self.calls.append(("query", query))
        if "GROUP BY _partition_id" in query:
            staged = self.staged
            if staged is None:
                staged = {self._last_partition: self.written} if self.written else {}
            rows = [
                (p, n, self.invalid if i == 0 else 0) for i, (p, n) in enumerate(staged.items())
            ]
            return SimpleNamespace(result_rows=rows)
        if query.startswith("SELECT count() FROM"):
            remaining = self.written - self.invalid if self.remaining is None else self.remaining
            return SimpleNamespace(result_rows=[(remaining,)])
        return SimpleNamespace(result_rows=self.log_rows)

    def raw_insert(self, table: str, **kwargs: Any) -> Any:
        self.calls.append(("raw_insert", {"table": table, **kwargs}))
        self._last_partition = Path(kwargs["insert_block"].name).name[:10].replace("-", "")
        return SimpleNamespace(written_rows=self.written)

    def insert(self, table: str, data: list[list[Any]], **kwargs: Any) -> None:
        self.calls.append(("insert", data))

    def commands(self) -> list[str]:
        return [arg for kind, arg in self.calls if kind == "command"]

    def kinds(self) -> list[str]:
        return [kind for kind, _ in self.calls]
