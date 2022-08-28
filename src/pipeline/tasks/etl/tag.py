from collections import namedtuple
from pathlib import Path
from typing import List

import pandas as pd
from prefect import flow, task
from prefect_dask import DaskTaskRunner

from utils.config import get_env_var_as_path
from utils.transformation import path_as_parquet


@task(
    description="Splits a single file into 2 parts (training and test) according to the file specs.",
    tags=["index", "final"],
    version="1",
)
def tag_range(file_name: str, training_index_end: int, anomaly_index_start: int, anomaly_index_end: int, save_path: Path, *args, **kwargs) -> int:
    input_path = get_env_var_as_path("PATH_DATA_INTERIM_RAW2PARQUET")
    data_file = pd.read_parquet(path_as_parquet(input_path, file_name))

    data_file.loc[:, ["train_set", "test_set", "anomaly_set"]] = 0
    data_file.loc[0:training_index_end-1, ["train_set"]] = 1
    data_file.loc[training_index_end:, ["test_set"]] = 1
    data_file.loc[anomaly_index_start-1:anomaly_index_end-1, ["anomaly_set"]] = 1

    data_file.to_parquet(path_as_parquet(save_path, file_name))

    return len(data_file)


@flow(task_runner=DaskTaskRunner())
def tag_ranges_and_counts():
    final_input_path: Path = get_env_var_as_path("PATH_DATA_FINAL_INPUT")
    final_input_path.mkdir(parents=True, exist_ok=True)

    index_path: Path = get_env_var_as_path("PATH_DATA_FINAL")
    index_df: pd.DataFrame = pd.read_parquet(index_path / "files_index.parquet")

    row_counts: List[int] = list()
    row: namedtuple
    for row in index_df.reset_index().itertuples():
        result = tag_range(**row._asdict(), save_path=final_input_path)
        row_counts.append(result)

    index_df["row_count"] = row_counts

    index_df.to_parquet(index_path / "files_index.parquet", index=False)
