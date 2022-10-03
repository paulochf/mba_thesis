from pathlib import Path


def path_as_parquet(folder_path: Path, file_name: str) -> Path:
    return (folder_path / file_name).with_suffix(".parquet")
