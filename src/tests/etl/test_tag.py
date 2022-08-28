from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from pipeline.tasks.etl.tag import tag_range
from utils.config import get_env_var_as_path
from utils.testing import assert_file
from utils.transformation import path_as_parquet


def test_split_file(file_index_row):
    final_train_path = get_env_var_as_path("PATH_DATA_FINAL_INPUT")

    file_name = file_index_row["file_name"]

    with TemporaryDirectory(dir=final_train_path) as save_dir:
        path_save_dir = Path(save_dir)

        # Function runs without errors
        result = tag_range.fn(**file_index_row, save_path=path_save_dir)
        assert result is True

        # Get resulted data parquet file name
        data_file_path = path_as_parquet(path_save_dir, file_name)

        # Check if function created parquet file with correct name
        assert_file(data_file_path)

        data_file = pd.read_parquet(data_file_path)

        assert len(data_file) == file_index_row["row_count"]
        assert data_file[data_file.train_set].sum(axis=1) == file_index_row["training_index_end"]
        assert data_file[data_file.test_set].sum(axis=1) == file_index_row["row_count"] - file_index_row["training_index_end"]
        assert data_file[data_file.anomaly_set].sum(axis=1) == file_index_row["anomaly_index_end"] - file_index_row["anomaly_index_start"]
