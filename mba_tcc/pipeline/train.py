from typing import List

import pandas as pd

from prefect import flow
from prefect_dask import DaskTaskRunner

from mba_tcc.pipeline.tasks.train.sigma import calculate_series
from mba_tcc.utils.config import get_env_var_as_path


@flow(task_runner=DaskTaskRunner())
def train_flow():
    index_path = get_env_var_as_path("PATH_DATA_FINAL")
    index_df = pd.read_parquet(index_path / "files_index_tagged.parquet")

    final_prediction_path = get_env_var_as_path("PATH_DATA_FINAL_PREDICTION")
    final_prediction_path.mkdir(parents=True, exist_ok=True)

    index_records: List[dict] = index_df.to_dict(orient="records")

    params: dict
    for params in index_records:
        calculate_series(params, final_prediction_path)
