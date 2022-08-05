import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from pandas.testing import assert_series_equal
from utils.config import get_env_var_as_path
from utils.testing import assert_file

from tasks.etl.fix import fix_file

LOGGER = logging.getLogger(__name__)


def test_fix_file():
    final_path = get_env_var_as_path("PATH_DATA_FINAL")

    index_df = pd.read_parquet(final_path / "files_index.parquet")

    with TemporaryDirectory(dir=final_path, suffix="_test") as tmp_dir:
        path_tmp_dir = Path(tmp_dir)
        file_1_infos = index_df[index_df.file_number == 1].to_dict(orient="records")[0]
        file_1_txt_path = Path(file_1_infos["file_path"])

        # Function runs without errors
        result = fix_file.fn(1, file_1_txt_path, file_1_infos["file_name"], path_tmp_dir)
        assert result is True

        # Function creates parquet file with same name as input
        parquet_file_name = file_1_txt_path.with_suffix(".parquet").name
        assert_file(path_tmp_dir, parquet_file_name)

        # Parquet file has the same values as input
        file_1_txt_df = pd.read_csv(file_1_txt_path, header=None, names=["vals"])
        file_1_parquet_df = pd.read_parquet(path_tmp_dir / parquet_file_name)
        assert_series_equal(file_1_txt_df.vals, file_1_parquet_df.vals)
