from datetime import datetime
from functools import partial
from json import dumps
from pathlib import Path
from typing import Tuple

import pandas as pd

from pandas.core.window import Rolling
from prefect import task

from mba_tcc.utils.config import get_env_var_as_path, DEFAULT_VAL_COLUMN
from mba_tcc.utils.plotting import make_plot_lines_results, make_plot_lines_raw
from mba_tcc.utils.transformation import get_dataset_folder, save_as_json


def make_column(w: int = None, stat_name: str = None, col_name: str = DEFAULT_VAL_COLUMN):
    return f"{col_name}_{stat_name}_{w}_w"


make_column_mean = partial(make_column, stat_name="rolling_mean")
make_column_std = partial(make_column, stat_name="rolling_std")
make_column_residual = partial(make_column, stat_name="rolling_residual")
make_column_zscore = partial(make_column, stat_name="rolling_zscore")


def zscore_metric(data: pd.DataFrame) -> int:
    outlier_counts = ((data < -3) | (data > 3)).sum()
    return int(outlier_counts)


def sigma_series(df: pd.DataFrame, w: int = 3, col_name: str = DEFAULT_VAL_COLUMN) -> pd.DataFrame:
    df_rolling: Rolling = df[col_name].rolling(window=w)
    df_rolling_mean: pd.Series = df_rolling.mean()
    df_rolling_std: pd.Series = df_rolling.std()

    new_cols: dict = {
        make_column_mean(col_name=col_name, w=w): df_rolling_mean,
        make_column_std(col_name=col_name, w=w): df_rolling_std,
        make_column_residual(col_name=col_name, w=w): lambda ddf: (ddf.vals - df_rolling_mean),
        make_column_zscore(col_name=col_name, w=w): lambda ddf: ddf[make_column_residual(col_name=col_name, w=w)] / df_rolling_std,
    }
    return df.assign(**new_cols)


def calculate_zscores(params, step, train_file):
    # Calculate mean +- 3 * stddev using z-scores
    best_window: int = None
    input_file_len: int = len(train_file)
    result: pd.DataFrame = None
    window_size: int = 0
    zscores_column: str = None
    zscores_min: float = -3
    zscores_max: float = 3

    while window_size <= input_file_len:
        window_size = window_size + step

        result = sigma_series(train_file, w=window_size)

        zscores_column = make_column_residual(w=window_size)
        zscores: pd.Series = result.loc[result.anomaly_set == 1, zscores_column]
        if any([
            zscores.min() < zscores_min,
            zscores.max() > zscores_max,
        ]):
            best_window = window_size
            zscores_min = zscores.min()
            zscores_max = zscores.max()
            break

    notanomaly_set: pd.Series = result.loc[result.anomaly_set == 0, zscores_column]

    params.update({
        "anomaly_set_best_window_size": best_window,
        "anomaly_set_zscore_min": float(zscores_min),
        "anomaly_set_zscore_max": float(zscores_max),
        "notanomaly_set_zscore_max": float(notanomaly_set.max()),
        "notanomaly_set_zscore_min": float(notanomaly_set.min()),
    })

    return result, zscores_column


@task(
    description="Calculates the mean and the 3 sigma band for the series.",
    tags=["index", "final"],
    version="1",
)
def sigma_method(train_file: pd.DataFrame, output_path: Path, plot_range: Tuple[int, int], params: dict, step: int = 3) -> bool:
    t_start: datetime = datetime.now()
    result, zscores_column = calculate_zscores(params, step, train_file)
    t_end: datetime = datetime.now()

    params["running_time_in_seconds"] = (t_end - t_start).total_seconds()

    # Save results
    save_as_json(params, output_path / "sigma_params.json")

    results: pd.DataFrame = pd.concat([
        train_file,
        result[[zscores_column]]
    ], axis=1)

    results.to_parquet(output_path / "results_zscore.parquet")

    make_plot_lines_results(results[results.train_set == 1], output_path / "zscore_train_set.png")
    make_plot_lines_results(results[results.test_set == 1], output_path / "zscore_test_set.png")
    make_plot_lines_results(results[results.anomaly_set == 1], output_path / "zscore_anomaly_set.png", plot_range)
    make_plot_lines_results(results, output_path / "zscore_full_set.png")

    return True
