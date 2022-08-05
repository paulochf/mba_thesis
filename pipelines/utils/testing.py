from pathlib import Path


def assert_file(path_prefix: Path, file_name: str):
    file_path = path_prefix / file_name
    assert file_path.exists()
    assert file_path.is_file()
