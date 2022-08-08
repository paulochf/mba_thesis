from json import load
from pathlib import Path

from pytest import fixture


@fixture(autouse=True)
def file_index_row():
    file_index_row_path = Path("tests/resources/file_index_row.json")
    with open(file_index_row_path) as f:
        row = load(f)
    return row
