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

    metrics_list: list = list()
    alpha: float
    for alpha in alpha_params:
        print("alpha:", alpha)
        metrics: dict = performance_metrics(result_df, alpha=alpha)
        metrics["condition"] = suffix
        metrics_list.append(metrics)

    final_metrics = kwargs
    final_metrics["metrics"] = metrics_list

    save_as_json(final_metrics, output_path / f"sigma_metrics_{suffix}.json")

    return True
