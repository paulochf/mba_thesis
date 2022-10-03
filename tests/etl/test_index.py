from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from pipeline.tasks.etl.index import make_files_index
from utils.config import get_env_var_as_path
from utils.testing import assert_file


def test_make_files_index():
    dataset_path = get_env_var_as_path("PATH_DATA_RAW_UCR")
    final_path = get_env_var_as_path("PATH_DATA_FINAL")
    file_name = "files_index.parquet"

    with TemporaryDirectory(dir=final_path) as tmp_dir:
        path_tmp_dir = Path(tmp_dir)

        # Function runs without errors
        result = make_files_index.fn(path_tmp_dir)
        assert result is True

        # Check if function created parquet file with correct name
        assert_file(path_tmp_dir / file_name)

        # Index parquet file contains information regarding all raw files
        index_df = pd.read_parquet(path_tmp_dir / file_name)
        all_raw_files = list(dataset_path.glob("./*.txt"))
        assert len(index_df) == len(all_raw_files)

        # Index parquet file contains all expected columns, in order
        df_columns = ("file_path", "file_name", "file_number", "mnemonic", "training_index_end", "anomaly_index_start", "anomaly_index_end")
        assert tuple(index_df.columns) == df_columns
