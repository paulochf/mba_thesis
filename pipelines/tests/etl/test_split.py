from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from mba_tcc.tasks.etl.split import split_file
from utils.config import get_env_var_as_path
from utils.testing import assert_file
from utils.transformation import as_parquet


def test_split_file():
    index_path = get_env_var_as_path("PATH_DATA_FINAL")
    index_df = pd.read_parquet(index_path / "files_index.parquet")

    interim_path = get_env_var_as_path("PATH_DATA_INTERIM_RAW2PARQUET")
    final_train_path = get_env_var_as_path("PATH_DATA_FINAL_TRAIN")
    final_test_path = get_env_var_as_path("PATH_DATA_FINAL_TEST")

    raw_data_file = index_df.iloc[0].to_dict()
    file_name = raw_data_file["file_name"]

    with (TemporaryDirectory(dir=final_train_path) as tmp_train_dir,
          TemporaryDirectory(dir=final_test_path) as tmp_test_dir):
        path_train_dir = Path(tmp_train_dir)
        path_test_dir = Path(tmp_test_dir)

        # Function runs without errors
        result = split_file.fn(**raw_data_file, output_train_path=path_train_dir, output_test_path=path_test_dir)
        assert result is True

        # Prepare train and test parquet names
        train_file = as_parquet(path_train_dir, file_name)
        test_file = as_parquet(path_test_dir, file_name)

        # Check if function created parquet files with correct names
        assert_file(train_file)
        assert_file(test_file)

        # Open a file and verify files row counts
        data_file = pd.read_parquet(as_parquet(interim_path, file_name))

        assert len(pd.read_parquet(train_file)) == raw_data_file["training_index_end"]
        assert len(pd.read_parquet(test_file)) == len(data_file) - raw_data_file["training_index_end"]
