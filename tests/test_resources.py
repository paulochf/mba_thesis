import sys

import pandas as pd

from pathlib import Path

from pipeline.tasks.etl.index import make_files_index
from utils.config import get_env_var_as_path


if __name__ == "__main__":
    index_path: Path = get_env_var_as_path("PATH_DATA_FINAL")
    index_file_path = index_path / "files_index.parquet"

    if not index_file_path.exists():
        make_files_index(index_path)

    test_resource_path = get_env_var_as_path("PATH_CODE_SRC_TEST_RESOURCES")
    index_df: pd.DataFrame = pd.read_parquet(index_path / "files_index.parquet")
    index_df[index_df.file_number == 1].to_json(test_resource_path, orient="records", lines=True)

    sys.exit(0)
