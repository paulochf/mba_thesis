import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from mba_tcc.etl import make_files_index
from utils.config import get_env_var_as_path

LOGGER = logging.getLogger(__name__)


def test_make_files_index():
    dataset_path = get_env_var_as_path("PATH_DATA_RAW_UCR")
    dataset_files = dataset_path.glob("./*.txt")
    final_path = get_env_var_as_path("PATH_DATA_FINAL")

    with TemporaryDirectory(dir=final_path, suffix="_test") as tmp_dir:
        path_tmp_dir = Path(tmp_dir)
        result = make_files_index.fn(dataset_files, path_tmp_dir)
        assert result is True

        index_file_path = path_tmp_dir / "files_index.parquet"
        assert index_file_path.exists()
        assert index_file_path.is_file()

        df = pd.read_parquet(index_file_path)
        assert len(df) == 250
