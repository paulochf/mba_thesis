from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from mba_tcc.pipeline.tasks.etl.tag import tag_range
from mba_tcc.utils.config import get_env_var_as_path
from mba_tcc.utils.testing import assert_file
from mba_tcc.utils.transformation import path_as_parquet


def test_split_file(file_index_row):
    final_input_path = get_env_var_as_path("PATH_DATA_FINAL_INPUT")

    file_name = file_index_row["file_name"]

    with TemporaryDirectory(dir=final_input_path) as save_dir:
        path_save_dir = Path(save_dir)

        # Function runs without errors
        result = tag_range.fn(**file_index_row, save_path=path_save_dir)
        assert result == 79795

        # Get resulted data parquet file name
        data_file_path = path_as_parquet(path_save_dir, file_name)

        # Check if function created parquet file with correct name
        assert_file(data_file_path)

        data_file = pd.read_parquet(data_file_path)
        assert len(data_file) == file_index_row["row_count"]

        tag_counts = data_file.sum()
        assert tag_counts['train_set'] == file_index_row["training_index_end"]
        assert tag_counts['test_set'] == file_index_row["row_count"] - file_index_row["training_index_end"]
        assert tag_counts['anomaly_set'] == file_index_row["anomaly_index_end"] - file_index_row["anomaly_index_start"] + 1
