from itertools import product
from typing import List

from prefect import flow
from prefect_dask import DaskTaskRunner

from mba_tcc.pipeline.tasks import NP_LOGSPACE_2, Z_VALUES
from mba_tcc.pipeline.tasks.analyze.oneliner import oneliner_analyze
from mba_tcc.pipeline.tasks.analyze.sigma import sigma_analyze
from mba_tcc.pipeline.tasks.analyze.zscore import zscore_analyze
from mba_tcc.utils.config import get_env_var_as_path
from mba_tcc.utils.datasets import load_file_index
from mba_tcc.utils.transformation import get_dataset_folder


@flow(task_runner=DaskTaskRunner())
def analyze_flow():
    index_df = load_file_index()

    trained_assets_path = get_env_var_as_path("PATH_DATA_FINAL_TRAINING")
    results_path = get_env_var_as_path("PATH_DATA_FINAL_RESULTS")

    index_records: List[dict] = index_df.to_dict(orient="records")

    params: dict
    for idx, params in enumerate(index_records):
        dataset_path = get_dataset_folder(trained_assets_path, **params)
        output_path = get_dataset_folder(results_path, **params)
        output_path.mkdir(parents=True, exist_ok=True)

        for window_size, sigma in product(NP_LOGSPACE_2, Z_VALUES):
            sigma_analyze(dataset_path, output_path, window_size=int(window_size), sigma=sigma, **params)

        for z_limit in range(3, 7):
            zscore_analyze(dataset_path, output_path, z_limit=int(z_limit), **params)

        oneliner_analyze(dataset_path, output_path, **params)
