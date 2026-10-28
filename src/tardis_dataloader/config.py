"""Connection settings, read from the environment (and a local, gitignored .env)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

DEFAULT_DATA_ROOT = Path("/Volumes/DeskSSD/datasets")


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    user: str
    password: str
    database: str
    data_root: Path


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "market"),
        data_root=Path(os.getenv("TARDIS_DATA_ROOT", str(DEFAULT_DATA_ROOT))),
    )
