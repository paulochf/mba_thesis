import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from mba_tcc.etl import fix_file, make_files_index
from pandas.testing import assert_series_equal
from utils.config import get_env_var_as_path

LOGGER = logging.getLogger(__name__)


def test_make_files_index():
    dataset_path = get_env_var_as_path('PATH_DATA_RAW_UCR')
    dataset_files = dataset_path.glob('./*.txt')
    final_path = get_env_var_as_path('PATH_DATA_FINAL')

    with TemporaryDirectory(dir=final_path, suffix='_test') as tmp_dir:
        path_tmp_dir = Path(tmp_dir)
        result = make_files_index.fn(dataset_files, path_tmp_dir)
        assert result is True

        index_file_path = path_tmp_dir / 'files_index.parquet'
        assert index_file_path.exists()
        assert index_file_path.is_file()

        index_df = pd.read_parquet(index_file_path)
        df_columns = ('file_path', 'file_name', 'file_number', 'mnemonic', 'training_index_end', 'anomaly_index_start', 'anomaly_index_end')
        assert len(index_df) == 250
        assert tuple(index_df.columns) == df_columns


def test_fix_file():
    final_path = get_env_var_as_path('PATH_DATA_FINAL')

    index_df = pd.read_parquet(final_path / 'files_index.parquet')

    with TemporaryDirectory(dir=final_path, suffix='_test') as tmp_dir:
        path_tmp_dir = Path(tmp_dir)
        file_1_infos = index_df[index_df.file_number == 1].to_dict(orient='records')[0]
        file_1_txt_path = Path(file_1_infos['file_path'])

        # Function runs without errors
        result = fix_file.fn(1, file_1_txt_path, file_1_infos['file_name'], path_tmp_dir)
        assert result is True

        # Function creates parquet file with same name as input
        file_1_parquet_path = path_tmp_dir / file_1_txt_path.with_suffix('.parquet').name
        assert file_1_parquet_path.exists()
        assert file_1_parquet_path.is_file()

        # Parquet file has the same values as input
        file_1_txt_df = pd.read_csv(file_1_txt_path, header=None, names=['vals'])
        file_1_parquet_df = pd.read_parquet(file_1_parquet_path)
        assert_series_equal(file_1_txt_df.vals, file_1_parquet_df.vals)
