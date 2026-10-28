"""Discovery and inspection of raw tardis files on disk."""

from __future__ import annotations

import datetime as dt
import gzip
import io
import lzma
import re
from dataclasses import dataclass
from pathlib import Path

import zstandard

# Some files on disk carry a misleading extension (e.g. ``.csv.xz`` files that
# are really zstd), so compression is always detected from magic bytes.
MAGIC: dict[bytes, str] = {
    b"\x1f\x8b": "gzip",
    b"\x28\xb5\x2f\xfd": "zstd",
    b"\xfd7zXZ\x00": "xz",
}

FILENAME_RE = re.compile(r"^(?P<day>\d{4}-\d{2}-\d{2})_(?P<kind>[A-Za-z]+)\.csv\.[a-z]+$")


@dataclass(frozen=True)
class SourceFile:
    path: Path
    day: dt.date
    size: int
    mtime: dt.datetime

    @property
    def name(self) -> str:
        return self.path.name


def detect_compression(path: Path) -> str:
    with path.open("rb") as f:
        head = f.read(6)
    for magic, method in MAGIC.items():
        if head.startswith(magic):
            return method
    raise ValueError(f"{path}: unrecognised compression (first bytes {head.hex()})")


def read_header(path: Path, compression: str) -> list[str]:
    with path.open("rb") as raw:
        stream: io.BufferedIOBase
        if compression == "gzip":
            stream = gzip.GzipFile(fileobj=raw)
        elif compression == "zstd":
            stream = io.BufferedReader(zstandard.ZstdDecompressor().stream_reader(raw))
        elif compression == "xz":
            stream = lzma.LZMAFile(raw)
        else:
            raise ValueError(f"unsupported compression {compression!r}")
        with stream:
            line = stream.readline()
    return line.decode().strip().split(",")


def discover(
    directory: Path,
    kind: str,
    start: dt.date | None = None,
    end: dt.date | None = None,
) -> list[SourceFile]:
    """Daily files of ``kind`` in ``directory``, sorted by day, within [start, end]."""
    found = []
    for path in directory.iterdir():
        # Skip macOS AppleDouble metadata files (``._<name>``).
        if path.name.startswith("._") or not path.is_file():
            continue
        m = FILENAME_RE.match(path.name)
        if m is None or m["kind"] != kind:
            continue
        day = dt.date.fromisoformat(m["day"])
        if (start and day < start) or (end and day > end):
            continue
        stat = path.stat()
        mtime = dt.datetime.fromtimestamp(int(stat.st_mtime), tz=dt.UTC)
        found.append(SourceFile(path=path, day=day, size=stat.st_size, mtime=mtime))
    days = [f.day for f in found]
    dupes = sorted({d for d in days if days.count(d) > 1})
    if dupes:
        raise ValueError(f"{directory}: multiple {kind} files for {dupes}")
    return sorted(found, key=lambda f: f.day)
