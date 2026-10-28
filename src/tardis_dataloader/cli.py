"""Command line entry point: ``uv run tardis-load <dataset> [options]``."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
import time

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

from tardis_dataloader.config import load_settings
from tardis_dataloader.datasets import DATASETS
from tardis_dataloader.files import SourceFile, discover
from tardis_dataloader.loader import Loader, LoadError

log = logging.getLogger("tardis_dataloader")

MEMORY_LIMIT_EXCEEDED = 241
RETRY_DELAYS_S = (60, 300, 900)


def positive_int(value: str) -> int:
    n = int(value)
    if n < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {n}")
    return n


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load tardis CSV files into ClickHouse.")
    p.add_argument("dataset", choices=sorted(DATASETS))
    p.add_argument("--start", type=dt.date.fromisoformat, help="first day (YYYY-MM-DD)")
    p.add_argument("--end", type=dt.date.fromisoformat, help="last day (YYYY-MM-DD)")
    p.add_argument("--limit", type=positive_int, help="load at most N files")
    p.add_argument("--force", action="store_true", help="reload files already loaded")
    p.add_argument("--dry-run", action="store_true", help="list files that would be loaded")
    args = p.parse_args(argv)
    if args.start and args.end and args.start > args.end:
        p.error(f"--start {args.start} is after --end {args.end}")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()
    dataset = DATASETS[args.dataset]

    end = min((d for d in (args.end, dataset.last_day) if d), default=None)
    files = discover(settings.data_root / dataset.source_dir, dataset.file_kind, args.start, end)
    client = clickhouse_connect.get_client(
        host=settings.host,
        port=settings.port,
        username=settings.user,
        password=settings.password,
        database=settings.database,
        # Large trade files can stream for a long time before the server replies.
        send_receive_timeout=6 * 3600,
    )
    loader = Loader(client, settings.database, dataset)
    if not args.dry_run:
        loader.ensure_tables()
    if not args.force:
        loaded = loader.loaded_files()
        files = [f for f in files if not loader.is_loaded(f, loaded)]
    if args.limit is not None:
        files = files[: args.limit]

    log.info("%s: %d files to load into %s", dataset.name, len(files), dataset.table)
    if args.dry_run:
        for f in files:
            print(f.path)
        return 0

    total = 0
    for i, f in enumerate(files, 1):
        try:
            total += load_with_retry(loader, f)
        except LoadError as e:
            log.error("[%d/%d] %s", i, len(files), e)
            return 1
    log.info("done: %d files, %d rows", len(files), total)
    return 0


def load_with_retry(loader: Loader, source: SourceFile) -> int:
    """Load a file, retrying after server out-of-memory errors.

    Retrying is safe because every attempt starts from an empty staging table
    and the target is only touched once the staged day has been verified.
    """
    for delay in (*RETRY_DELAYS_S, None):
        try:
            return loader.load_file(source)
        except ClickHouseError as e:
            if getattr(e, "code", None) != MEMORY_LIMIT_EXCEEDED or delay is None:
                raise
            log.warning("%s: server out of memory, retrying in %ds", source.name, delay)
            time.sleep(delay)
    raise AssertionError("unreachable")


if __name__ == "__main__":
    sys.exit(main())
