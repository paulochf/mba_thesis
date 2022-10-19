from itertools import product
from json import load
from pathlib import Path

import pandas as pd

from prefect import task

from mba_tcc.pipeline.tasks.analyze import performance_metrics, DATASETS
from mba_tcc.utils.plotting import make_plot_lines_results
from mba_tcc.utils.transformation import save_as_json


def analyze(sigma_dataset_path: Path, sigma_output_path: Path, window_size: int, sigma: int, **params) -> None:
    params_file = sigma_output_path / f"sigma_metrics_{window_size}w_{sigma}s.json"
    # if params_file.exists():
    #     return

    result_df: pd.DataFrame = pd.read_parquet(sigma_dataset_path / f"sigma_predictions_{window_size}w_{sigma}s.parquet")
    results = load((sigma_dataset_path / f"sigma_params_{window_size}w_{sigma}s.json").open())

    alpha_params = [0.5, 1.0]
    params_space = product(alpha_params, DATASETS)

    metrics_list: list = list()
    alpha: float
    dataset: str
    for alpha, dataset in params_space:
        subset_df = result_df
        if dataset != "all":
            subset_df = subset_df[subset_df[dataset] == 1]

        metrics: dict = performance_metrics(subset_df, alpha=alpha)
        metrics["dataset"] = dataset
        metrics["method"] = "sigma"
        metrics_list.append(results | metrics)

        # make_plot_lines_results(result_df[result_df.train_set == 1], sigma_output_path / "sigma_train_set.png", **params)
        # make_plot_lines_results(result_df[result_df.test_set == 1], sigma_output_path / "sigma_test_set.png", **params)
        # make_plot_lines_results(result_df[result_df.anomaly_set == 1], sigma_output_path / "sigma_anomaly_set.png", anomaly=True, **params)
        # make_plot_lines_results(result_df, sigma_output_path / "sigma_full_set.png", **params)

    params["metrics"] = metrics_list

    save_as_json(params, params_file)


@task(
    description="Measure the outlier values for the sigma predicted values.",
    tags=["index", "final"],
    version="1",
)
def sigma_analyze(dataset_path: Path, output_path: Path, **params):
    sigma_dataset_path: Path = dataset_path / "sigma"
    sigma_output_path: Path = output_path / "sigma"
    sigma_output_path.mkdir(parents=True, exist_ok=True)

    analyze(sigma_dataset_path, sigma_output_path, **params)
