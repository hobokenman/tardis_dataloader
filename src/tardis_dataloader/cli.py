"""Command line entry points: ``uv run tardis-load <dataset>`` and ``uv run tardis-bars <bars>``."""

from __future__ import annotations

import argparse
import datetime as dt
import functools
import logging
import sys
import time
from collections.abc import Callable
from typing import TypeVar

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError, OperationalError

from tardis_dataloader.bar_builder import BarBuilder, BuildError, HourlyBarBuilder
from tardis_dataloader.bars import BAR_TABLES, HOURLY_BAR_TABLES
from tardis_dataloader.config import Settings, load_settings
from tardis_dataloader.datasets import DATASETS
from tardis_dataloader.files import SourceFile, discover
from tardis_dataloader.loader import Loader, LoadError

log = logging.getLogger("tardis_dataloader")

MEMORY_LIMIT_EXCEEDED = 241
RETRY_DELAYS_S = (60, 300, 900)

T = TypeVar("T")


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
    loader = Loader(connect(settings), settings.database, dataset)
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


def connect(settings: Settings) -> Client:
    return clickhouse_connect.get_client(
        host=settings.host,
        port=settings.port,
        username=settings.user,
        password=settings.password,
        database=settings.database,
        # Large trade files can stream for a long time before the server replies.
        send_receive_timeout=6 * 3600,
        # urllib3 applies the connect timeout, not the read timeout, while a request body
        # is being sent, so a short one aborts an upload whenever the server pauses reading.
        connect_timeout=600,
    )


def parse_bars_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build one-minute bars from the loaded tick tables, or hourly bars "
        "(*-1h, one month at a time) from the one-minute bars."
    )
    p.add_argument("bars", choices=sorted([*BAR_TABLES, *HOURLY_BAR_TABLES]))
    p.add_argument("--start", type=dt.date.fromisoformat, help="first day (YYYY-MM-DD)")
    p.add_argument("--end", type=dt.date.fromisoformat, help="last day (YYYY-MM-DD)")
    p.add_argument("--limit", type=positive_int, help="build at most N days or months")
    p.add_argument("--force", action="store_true", help="rebuild days or months already built")
    p.add_argument("--dry-run", action="store_true", help="list what would be built")
    args = p.parse_args(argv)
    if args.start and args.end and args.start > args.end:
        p.error(f"--start {args.start} is after --end {args.end}")
    return args


def bars_main(argv: list[str] | None = None) -> int:
    args = parse_bars_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()
    if args.bars in HOURLY_BAR_TABLES:
        return hourly_bars_main(args, settings)
    bars = BAR_TABLES[args.bars]
    builder = BarBuilder(connect(settings), settings.database, bars)
    if not args.dry_run:
        builder.ensure_tables()

    days = builder.source_days()
    days = {
        d: s
        for d, s in days.items()
        if (args.start is None or d >= args.start) and (args.end is None or d <= args.end)
    }
    if not args.force:
        built = builder.built_days()
        # A day is rebuilt when any of its sources was reloaded after it was built.
        days = {
            d: s
            for d, s in days.items()
            if d not in built or built[d] < max(loaded for _, loaded in s.values())
        }
    todo = list(days.items())
    if args.limit is not None:
        todo = todo[: args.limit]

    log.info("%s: %d days to build into %s", bars.name, len(todo), bars.table)
    if args.dry_run:
        for day, _ in todo:
            print(day)
        return 0

    total = 0
    for i, (day, sources) in enumerate(todo, 1):
        try:
            total += with_retry(str(day), functools.partial(builder.build_day, day, sources))
        except BuildError as e:
            log.error("[%d/%d] %s", i, len(todo), e)
            return 1
    log.info("done: %d days, %d bars", len(todo), total)
    return 0


def hourly_bars_main(args: argparse.Namespace, settings: Settings) -> int:
    bars = HOURLY_BAR_TABLES[args.bars]
    builder = HourlyBarBuilder(connect(settings), settings.database, bars)
    if not args.dry_run:
        builder.ensure_tables()

    months = {
        m: built
        for m, built in builder.minute_months().items()
        if (args.start is None or m >= args.start.replace(day=1))
        and (args.end is None or m <= args.end)
    }
    if not args.force:
        done = builder.built_months()
        # A month is rebuilt when any of its minute bars was rebuilt after it.
        months = {m: built for m, built in months.items() if m not in done or done[m] < built}
    todo = list(months)
    if args.limit is not None:
        todo = todo[: args.limit]

    log.info("%s: %d months to build into %s", bars.name, len(todo), bars.table)
    if args.dry_run:
        for month in todo:
            print(f"{month:%Y-%m}")
        return 0

    total = 0
    for i, month in enumerate(todo, 1):
        try:
            total += with_retry(f"{month:%Y-%m}", functools.partial(builder.build_month, month))
        except BuildError as e:
            log.error("[%d/%d] %s", i, len(todo), e)
            return 1
    log.info("done: %d months, %d bars", len(todo), total)
    return 0


def load_with_retry(loader: Loader, source: SourceFile) -> int:
    return with_retry(source.name, lambda: loader.load_file(source))


def with_retry(label: str, attempt: Callable[[], T]) -> T:
    """Run ``attempt``, retrying after server out-of-memory errors and lost connections.

    Retrying is safe because every attempt starts from an empty staging table
    and the target is only touched once the staged day has been verified.
    """
    for delay in (*RETRY_DELAYS_S, None):
        try:
            return attempt()
        except ClickHouseError as e:
            if isinstance(e, OperationalError):
                reason = "connection to server lost"
            elif getattr(e, "code", None) == MEMORY_LIMIT_EXCEEDED:
                reason = "server out of memory"
            else:
                raise
            if delay is None:
                raise
            log.warning("%s: %s, retrying in %ds", label, reason, delay)
            time.sleep(delay)
    raise AssertionError("unreachable")


if __name__ == "__main__":
    sys.exit(main())
