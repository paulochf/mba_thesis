from pathlib import Path

import pandas as pd

from mba_tcc.utils.config import get_env_var_as_path


def load_file_index():
    index_path: Path = get_env_var_as_path("PATH_DATA_FINAL")
    index_df: pd.DataFrame = pd.read_parquet(index_path / "files_index_tagged.parquet")
    return index_df
