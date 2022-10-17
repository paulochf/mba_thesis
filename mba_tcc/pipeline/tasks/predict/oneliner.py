from json import load
from pathlib import Path
from typing import Tuple

import pandas as pd

from prefect import task

from mba_tcc.pipeline.tasks.train.oneliner import oneliner_train
from mba_tcc.utils.plotting import make_plot_lines_results


@task(
    description="Determine the outlier values using the oneliner predicted values.",
    tags=["index", "final"],
    version="1",
)
def oneliner_predict(train_file: pd.DataFrame, output_path: Path, plot_range: Tuple[int, int]) -> bool:
    result_df_path: Path = output_path / "oneliner_predictions.parquet"
    if result_df_path.exists(): return True

    result_best_path: Path = output_path / "oneliner_params.json"
    result_best: dict = load(result_best_path.open())

    result_df: pd.DataFrame = oneliner_train(train_file, return_data=True, **result_best)

    # Save results
    result_df.to_parquet(result_df_path)

    make_plot_lines_results(result_df[result_df.train_set == 1], output_path / "oneliner_train_set.png")
    make_plot_lines_results(result_df[result_df.test_set == 1], output_path / "oneliner_test_set.png")
    make_plot_lines_results(result_df[result_df.anomaly_set == 1], output_path / "oneliner_anomaly_set.png", plot_range)
    make_plot_lines_results(result_df, output_path / "oneliner_full_set.png")

    return True
