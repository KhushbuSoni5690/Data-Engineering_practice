from pathlib import Path

from common.storage_utils import dir_size_mb, count_files


def test_dir_size_mb_includes_subfolders(tmp_path: Path) -> None:
    (tmp_path / "first.txt").write_bytes(b"a" * 500_000)

    subfolder = tmp_path / "nested"
    subfolder.mkdir()
    (subfolder / "second.txt").write_bytes(b"b" * 500_000)

    assert dir_size_mb(tmp_path) == 1.0


def test_count_files_matches_suffix_in_subfolders(tmp_path: Path) -> None:
    (tmp_path / "first.parquet").touch()
    (tmp_path / "notes.txt").touch()
    (tmp_path / "_SUCCESS").touch()

    subfolder = tmp_path / "nested"
    subfolder.mkdir()
    (subfolder / "second.parquet").touch()

    assert count_files(tmp_path, ".parquet") == 2