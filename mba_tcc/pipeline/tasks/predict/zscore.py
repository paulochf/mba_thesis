from pathlib import Path
from typing import Callable, Tuple

import pandas as pd

from prefect import task

from mba_tcc.utils.config import DEFAULT_PREDICTED_VALUE, DEFAULT_PREDICTED_ANOMALY
from mba_tcc.utils.plotting import make_plot_lines_results


@task(
    description="Determine the outlier values using the sigma predicted values.",
    tags=["index", "final"],
    version="1",
)
def sigma_predict(output_path: Path, condition: Callable, plot_range: Tuple[int, int]) -> bool:
    suffix: str = condition.__name__

    result_df: pd.DataFrame = pd.read_parquet(output_path / f"zscore_results_{suffix}.parquet")
    zscores = result_df[DEFAULT_PREDICTED_VALUE]

    result_df[DEFAULT_PREDICTED_ANOMALY] = ((zscores < -3) | (zscores > 3)).astype("int")

    # Save results
    result_df.to_parquet(output_path / f"zscore_predictions_{suffix}.parquet")

    make_plot_lines_results(result_df[result_df.train_set == 1], output_path / f"zscore_train_set_{suffix}.png")
    make_plot_lines_results(result_df[result_df.test_set == 1], output_path / f"zscore_test_set_{suffix}.png")
    make_plot_lines_results(result_df[result_df.anomaly_set == 1], output_path / f"zscore_anomaly_set_{suffix}.png", plot_range)
    make_plot_lines_results(result_df, output_path / f"zscore_full_set_{suffix}.png")

    return True
