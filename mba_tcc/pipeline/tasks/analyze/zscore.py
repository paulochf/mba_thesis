from itertools import product
from json import load
from pathlib import Path

import pandas as pd

from prefect import get_run_logger, task

from mba_tcc.pipeline.tasks import NP_LOGSPACE_2
from mba_tcc.pipeline.tasks.analyze import performance_metrics, DATASETS
from mba_tcc.utils.plotting import make_plot_lines_results
from mba_tcc.utils.transformation import save_as_json


def analyze(zscore_dataset_path: Path, zscore_output_path: Path, **params) -> None:
    logger = get_run_logger()
    logger.info(f"Analyze [zscore] :: {zscore_dataset_path}")
    z_limit: int = params["z_limit"]
    result_df: pd.DataFrame = pd.read_parquet(zscore_dataset_path / f"zscore_predictions_{z_limit}.parquet")

    alpha_params = [0.5, 1.0]
    params_space = product(alpha_params, DATASETS)

    metrics_list: list = list()
    alpha: float
    dataset: str
    for alpha, dataset in params_space:
        subset_df = result_df
        if dataset != "all":
            subset_df = subset_df[subset_df[dataset] == 1]

        results = load((zscore_dataset_path / f"zscore_params_{z_limit}.json").open())

        metrics: dict = performance_metrics(subset_df, alpha=alpha, logger=logger)
        metrics["dataset"] = dataset
        metrics["method"] = "zscore"
        metrics_list.append(results | metrics)

        make_plot_lines_results(result_df[result_df.train_set == 1], zscore_output_path / f"zscore_train_set_alpha_{alpha}_dataset_{dataset}.png", **params)
        make_plot_lines_results(result_df[result_df.test_set == 1], zscore_output_path / f"zscore_test_set_alpha_{alpha}_dataset_{dataset}.png", **params)
        make_plot_lines_results(result_df[result_df.anomaly_set == 1], zscore_output_path / f"zscore_anomaly_set_alpha_{alpha}_dataset_{dataset}.png", anomaly=True, **params)
        make_plot_lines_results(result_df, zscore_output_path / f"zscore_full_set_alpha_{alpha}_dataset_{dataset}.png", **params)

    params["metrics"] = metrics_list

    save_as_json(params, zscore_output_path / f"zscore_metrics_{z_limit}.json")


@task(
    description="Measure the outlier values for the zscore predicted values.",
    tags=["index", "final"],
    version="1",
)
def zscore_analyze(dataset_path: Path, output_path: Path, **params):
    zscore_dataset_path: Path = dataset_path / "zscore"
    zscore_output_path: Path = output_path / "zscore"
    zscore_output_path.mkdir(parents=True, exist_ok=True)

    analyze(zscore_dataset_path, zscore_output_path, **params)
