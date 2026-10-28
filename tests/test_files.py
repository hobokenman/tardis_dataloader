from __future__ import annotations

import datetime as dt
import gzip
import lzma
from pathlib import Path

import pytest
import zstandard

from tardis_dataloader.files import detect_compression, discover, read_header

CSV = b"exchange,symbol,timestamp\nbinance,BTCUSDT,1640995199993000\n"

COMPRESSORS = {
    "gzip": gzip.compress,
    "zstd": zstandard.ZstdCompressor().compress,
    "xz": lzma.compress,
}


@pytest.mark.parametrize("method", sorted(COMPRESSORS))
def test_detect_and_read_header(tmp_path: Path, method: str) -> None:
    path = tmp_path / "f.csv.bin"
    path.write_bytes(COMPRESSORS[method](CSV))
    assert detect_compression(path) == method
    assert read_header(path, method) == ["exchange", "symbol", "timestamp"]


def test_detect_ignores_misleading_extension(tmp_path: Path) -> None:
    path = tmp_path / "2025-10-01_PERPETUALS.csv.xz"
    path.write_bytes(COMPRESSORS["zstd"](CSV))
    assert detect_compression(path) == "zstd"


def test_detect_rejects_plain_text(tmp_path: Path) -> None:
    path = tmp_path / "f.csv.gz"
    path.write_bytes(CSV)
    with pytest.raises(ValueError, match="unrecognised compression"):
        detect_compression(path)


def _touch(directory: Path, *names: str) -> None:
    for name in names:
        (directory / name).write_bytes(b"x")


def test_discover_filters_and_sorts(tmp_path: Path) -> None:
    _touch(
        tmp_path,
        "2022-01-03_PERPETUALS.csv.gz",
        "2022-01-01_PERPETUALS.csv.gz",
        "2022-01-02_PERPETUALS.csv.xz",
        "._2022-01-02_PERPETUALS.csv.gz",
        "2022-01-02_SPOT.csv.gz",
        "notes.txt",
    )
    files = discover(tmp_path, "PERPETUALS")
    assert [f.name for f in files] == [
        "2022-01-01_PERPETUALS.csv.gz",
        "2022-01-02_PERPETUALS.csv.xz",
        "2022-01-03_PERPETUALS.csv.gz",
    ]
    ranged = discover(tmp_path, "PERPETUALS", dt.date(2022, 1, 2), dt.date(2022, 1, 2))
    assert [f.day for f in ranged] == [dt.date(2022, 1, 2)]


def test_discover_rejects_two_files_for_one_day(tmp_path: Path) -> None:
    _touch(tmp_path, "2022-01-01_PERPETUALS.csv.gz", "2022-01-01_PERPETUALS.csv.xz")
    with pytest.raises(ValueError, match="multiple PERPETUALS files"):
        discover(tmp_path, "PERPETUALS")
