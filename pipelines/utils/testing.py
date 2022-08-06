from pathlib import Path


def assert_file(file_path: Path):
    assert file_path.exists()
    assert file_path.is_file()
