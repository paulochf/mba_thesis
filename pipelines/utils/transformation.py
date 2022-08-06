from pathlib import Path
from typing import Tuple

import pandas as pd


def df_slice(df: pd.DataFrame, index_start: int = 0, index_end: int = None) -> pd.DataFrame:
    last_df_index = len(df)
    if not index_end:
        index_end = last_df_index

    return df.loc[index_start:index_end]


def df_split(df: pd.DataFrame, split_index: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    first_df = df_slice(df, index_end=split_index - 1)
    last_df = df_slice(df, index_start=split_index)

    return first_df, last_df


def as_parquet(folder_path: Path, file_name: str) -> Path:
    return (folder_path / file_name).with_suffix(".parquet")
