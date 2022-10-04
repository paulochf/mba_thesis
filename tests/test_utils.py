import pandas as pd

from pathlib import Path

from mba_tcc.pipeline.tasks.etl.index import make_files_index
from mba_tcc.utils.config import get_env_var_as_path


def make_test_resource():
    index_path: Path = get_env_var_as_path("PATH_DATA_FINAL")
    index_file_path = index_path / "files_index_tagged.parquet"

    if not index_file_path.exists():
        make_files_index(index_path)

    index_df: pd.DataFrame = pd.read_parquet(index_file_path)

    test_resource_path = get_env_var_as_path("PATH_CODE_TEST_RESOURCES")
    test_resource_file_path = test_resource_path / "file_index_row.json"
    index_df[index_df.file_number == 1].to_json(
        test_resource_file_path,
        orient="records",
        lines=True,
    )
