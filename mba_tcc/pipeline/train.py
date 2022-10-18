from itertools import product
from pathlib import Path
from typing import List

import pandas as pd
from prefect import flow
from prefect_dask import DaskTaskRunner

from mba_tcc.pipeline.tasks import NP_LOGSPACE_2, Z_VALUES
from mba_tcc.pipeline.tasks.train.oneliner import oneliner_method
from mba_tcc.pipeline.tasks.train.sigma import sigma_method
from mba_tcc.pipeline.tasks.train.zscore import zscore_method
from mba_tcc.utils.config import get_env_var_as_path
from mba_tcc.utils.datasets import load_file_index
from mba_tcc.utils.transformation import get_dataset_folder


@flow(task_runner=DaskTaskRunner())
def train_flow():
    index_df = load_file_index()

    input_path: Path = get_env_var_as_path("PATH_DATA_FINAL_INPUT")

    trained_assets_path = get_env_var_as_path("PATH_DATA_FINAL_TRAINING")
    trained_assets_path.mkdir(parents=True, exist_ok=True)

    index_records: List[dict] = index_df.to_dict(orient="records")

    params: dict
    for params in index_records:
        file_folder_path = get_dataset_folder(trained_assets_path, **params)
        file_folder_path.mkdir(parents=True, exist_ok=True)

        # Load data file
        dataset_path: Path = get_dataset_folder(input_path, **params)
        train_file: pd.DataFrame = pd.read_parquet(dataset_path / "data.parquet")

        # for window_size, sigma in product(NP_LOGSPACE_2, Z_VALUES):
        #     sigma_method(train_file, file_folder_path, window_size=int(window_size), sigma=sigma, **params)

        # for z_limit in Z_VALUES:
        #     zscore_method(train_file, file_folder_path, z_limit=int(z_limit), **params)

        oneliner_method(train_file, file_folder_path, **params)
