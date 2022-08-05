from pathlib import Path

from prefect import task, flow
from prefect_dask import DaskTaskRunner

from utils.config import get_env_var_as_path


@task(
    description="Splits a single file into 2 parts: training and anomaly.",
    tags=["index", "final"],
    version="1",
)
def split_file(file_path: Path, output_train_folder: Path, output_anomaly_path: Path) -> bool:
    pass


@flow(task_runner=DaskTaskRunner())
def split_files():
    interim_path = get_env_var_as_path("PATH_DATA_INTERIM_RAW2PARQUET")
    final_train_path = get_env_var_as_path("PATH_DATA_FINAL_TRAIN")
    final_anomaly_path = get_env_var_as_path("PATH_DATA_FINAL_ANOMALY")

    whole_files = interim_path.glob("./*.parquet")
    for file_path in whole_files:
        split_file(file_path, final_train_path, final_anomaly_path)
