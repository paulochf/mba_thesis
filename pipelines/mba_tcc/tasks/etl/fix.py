from collections import namedtuple
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_dask import DaskTaskRunner

from utils.transformation import as_parquet
from utils.config import get_env_var_as_path

INDEXES_TO_FIX = {204, 205, 206, 207, 208, 225, 226, 242, 243}


@task(
    description="Standardize the schema and fix the values in the files that have data them all in the first line.",
    tags=["files", "interim"],
    version="1",
)
def fix_file(file_number: int, file_path: Path, file_name: str, output_path: Path, *args, **kwargs) -> bool:
    parquet_file = as_parquet(output_path, file_name)
    if parquet_file.exists():
        return True
    
    file_to_fix = file_number in INDEXES_TO_FIX

    df = pd.read_csv(file_path, header=None, delim_whitespace=file_to_fix)

    if file_to_fix:
        df = df.T

    df = df.rename(columns={0: "vals"})
    df.to_parquet(parquet_file)

    return True


@flow(task_runner=DaskTaskRunner())
def fix_files():
    final_path = get_env_var_as_path("PATH_DATA_FINAL")
    interim_raw2parquet_path = get_env_var_as_path("PATH_DATA_INTERIM_RAW2PARQUET")
    interim_raw2parquet_path.mkdir(parents=True, exist_ok=True)

    index_path = final_path / "files_index.parquet"
    index_df = pd.read_parquet(index_path)

    row: namedtuple
    for row in index_df.reset_index().itertuples():
        fix_file(**row._asdict(), output_path=interim_raw2parquet_path)
