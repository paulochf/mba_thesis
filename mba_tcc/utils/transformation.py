from pathlib import Path


def path_as_parquet(folder_path: Path, file_name: str) -> Path:
    return (folder_path / file_name).with_suffix(".parquet")


def get_dataset_folder(parent_path: Path, file_number: int, mnemonic: str) -> Path:
    return parent_path / f"{file_number:03d}_{mnemonic}"
