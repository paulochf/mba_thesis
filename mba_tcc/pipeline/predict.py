from pathlib import Path
from typing import List, Tuple

import pandas as pd
from prefect import flow
from prefect_dask import DaskTaskRunner

from mba_tcc.pipeline.tasks.predict.oneliner import oneliner_predict
from mba_tcc.pipeline.tasks.predict.sigma import sigma_predict
from mba_tcc.pipeline.tasks.train.sigma import sigma_method
from mba_tcc.utils.config import get_env_var_as_path
from mba_tcc.utils.datasets import load_file_index
from mba_tcc.utils.transformation import get_dataset_folder


@flow(task_runner=DaskTaskRunner())
def predict_flow():
    index_df = load_file_index()

    input_path: Path = get_env_var_as_path("PATH_DATA_FINAL_INPUT")
    trained_assets_path = get_env_var_as_path("PATH_DATA_FINAL_TRAINING")

    index_records: List[dict] = index_df.to_dict(orient="records")

    params: dict
    for params in index_records:
        anomaly_index_start: int = params["anomaly_index_start"]
        anomaly_index_end: int = params["anomaly_index_end"]

        file_folder_path = get_dataset_folder(trained_assets_path, **params)

        # Load data file
        dataset_path: Path = get_dataset_folder(input_path, **params)
        train_file: pd.DataFrame = pd.read_parquet(dataset_path / "data.parquet")

        # # Determine the anomaly range for plotting
        plot_range: Tuple[int, int] = (
            int(anomaly_index_start * 0.99),
            int(anomaly_index_end * 1.01)
        )

        sigma_predict(file_folder_path, all, plot_range)
        sigma_predict(file_folder_path, any, plot_range)
        oneliner_predict(train_file, file_folder_path, plot_range)
