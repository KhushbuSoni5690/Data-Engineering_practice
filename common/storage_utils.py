from pathlib import Path


def dir_size_mb(path: str | Path) -> float:
    total_bytes = 0

    for file in Path(path).rglob("*"):
        if file.is_file():
            total_bytes += file.stat().st_size

    return total_bytes / 1_000_000


def count_files(path: str | Path, suffix: str) -> int:
    count = 0

    for file in Path(path).rglob("*"):
        if file.is_file() and file.suffix == suffix:
            count += 1

    return count