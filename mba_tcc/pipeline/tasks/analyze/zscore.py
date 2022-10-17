from itertools import product
from pathlib import Path
from typing import Callable

import pandas as pd

from prefect import task

from mba_tcc.pipeline.tasks.analyze import performance_metrics
from mba_tcc.utils.transformation import save_as_json


@task(
    description="Measure the outlier values for the sigma predicted values.",
    tags=["index", "final"],
    version="1",
)
def sigma_analyze(dataset_path: Path, output_path: Path, condition: Callable, **kwargs) -> bool:
    print(">>> sigma_analyze")
    suffix: str = condition.__name__

    result_df: pd.DataFrame = pd.read_parquet(dataset_path / f"zscore_predictions_{suffix}.parquet")

    alpha_params = [0.5, 1.0]
    dataset_params = ["all", "test_set"]
    params_space = product(alpha_params, dataset_params)

    metrics_list: list = list()
    alpha: float
    dataset: str
    for alpha, dataset in params_space:
        subset_df = result_df
        if dataset != "all":
            subset_df = subset_df[subset_df[dataset] == 1]

        metrics: dict = performance_metrics(subset_df, alpha=alpha)
        metrics["condition"] = suffix
        metrics["dataset"] = dataset
        metrics["method"] = "sigma"
        metrics_list.append(metrics)

    final_metrics = kwargs
    final_metrics["metrics"] = metrics_list

    save_as_json(final_metrics, output_path / f"sigma_metrics_{suffix}.json")

    return True
