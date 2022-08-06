from collections import namedtuple

import pandas as pd
from pathlib import Path

from prefect import flow, task
from prefect_dask import DaskTaskRunner
from utils.config import get_env_var_as_path
from utils.transformation import df_split
from utils.transformation import as_parquet


@task(
    description="Splits a single file into 2 parts (training and test) according to the file specs.",
    tags=["index", "final"],
    version="1",
)
def split_file(
    file_name: str, training_index_end: int, output_train_path: Path, output_test_path: Path,
    *args, **kwargs
) -> bool:
    input_path = get_env_var_as_path("PATH_DATA_INTERIM_RAW2PARQUET")

    data_file = pd.read_parquet(as_parquet(input_path, file_name))

    train_df, test_df = df_split(data_file, training_index_end)

    train_df.to_parquet(as_parquet(output_train_path, file_name))
    test_df.to_parquet(as_parquet(output_test_path, file_name))

    return True


@flow(task_runner=DaskTaskRunner())
def split_files():
    final_train_path = get_env_var_as_path("PATH_DATA_FINAL_TRAIN")
    final_test_path = get_env_var_as_path("PATH_DATA_FINAL_TEST")
    final_train_path.mkdir(parents=True, exist_ok=True)
    final_test_path.mkdir(parents=True, exist_ok=True)

    index_path = get_env_var_as_path("PATH_DATA_FINAL")
    index_df = pd.read_parquet(index_path / "files_index.parquet")

    row: namedtuple
    for row in index_df.reset_index().itertuples():
        split_file(**row._asdict(), output_train_path=final_train_path, output_test_path=final_test_path)
