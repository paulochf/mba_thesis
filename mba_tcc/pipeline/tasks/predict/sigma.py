from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Tuple, Callable

import pandas as pd

from pandas.core.window import Rolling
from prefect import task

from mba_tcc.utils.config import DEFAULT_VAL_COLUMN, DEFAULT_PREDICTED_COLUMN
from mba_tcc.utils.plotting import make_plot_lines_results
from mba_tcc.utils.transformation import save_as_json


def make_column(col_name: str = DEFAULT_VAL_COLUMN, stat_name: str = None, k: int = None) -> str:
    return f"{col_name}_{stat_name}_{k}"


make_column_mean = partial(make_column, stat_name="rolling_mean")
make_column_std = partial(make_column, stat_name="rolling_std")
make_column_residual = partial(make_column, stat_name="rolling_residual")
make_column_zscore = partial(make_column, stat_name="rolling_zscore")


def zscore_metric(data: pd.DataFrame) -> int:
    outlier_counts = ((data < -3) | (data > 3)).sum()
    return int(outlier_counts)


def sigma_series(df: pd.DataFrame, window_size: int = 3, col_name: str = DEFAULT_VAL_COLUMN) -> pd.DataFrame:
    df_rolling: Rolling = df[col_name].rolling(window=window_size)
    df_rolling_mean: pd.Series = df_rolling.mean()
    df_rolling_std: pd.Series = df_rolling.std()

    new_cols: dict = {
        make_column_mean(col_name=col_name, k=window_size): df_rolling_mean,
        make_column_std(col_name=col_name, k=window_size): df_rolling_std,
        make_column_residual(col_name=col_name, k=window_size): lambda ddf: (ddf.vals - df_rolling_mean),
        make_column_zscore(col_name=col_name, k=window_size): lambda ddf: ddf[make_column_residual(col_name=col_name, k=window_size)] / df_rolling_std,
    }
    return df.assign(**new_cols)


def calculate_zscores(train_file: pd.DataFrame, step: int, condition: Callable) -> Tuple[pd.DataFrame, int]:
    # Calculate mean +- 3 * stddev using z-scores
    best_window: int = None
    input_file_len: int = len(train_file)
    result: pd.DataFrame = None
    window_size: int = 0
    zscores_column: str = None
    zscores_min: float = -3.
    zscores_max: float = 3.

    if condition not in {all, any}:
        raise ValueError("Use only 'all' or 'any' functions.")

    while window_size <= input_file_len:
        window_size = window_size + step

        result = sigma_series(train_file, window_size=window_size)

        zscores_column = make_column_zscore(k=window_size)
        zscores: pd.Series = result.loc[result.anomaly_set == 1, zscores_column]
        if condition([
            zscores.min() < zscores_min,
            zscores.max() > zscores_max,
        ]):
            best_window = window_size
            zscores_min = zscores.min()
            zscores_max = zscores.max()
            break

    result = result.rename(columns={zscores_column: DEFAULT_PREDICTED_COLUMN})

    return result, best_window


@task(
    description="Calculates the mean and the 3 sigma band for the series.",
    tags=["index", "final"],
    version="1",
)
def sigma_method(train_file: pd.DataFrame, output_path: Path, plot_range: Tuple[int, int], params: dict, condition: Callable, step: int = 3) -> bool:
    suffix = condition.__name__

    t_start: datetime = datetime.now()
    result_df, best_window = calculate_zscores(train_file, step, condition)
    t_end: datetime = datetime.now()

    params["k"] = best_window
    params["running_time_in_seconds"] = (t_end - t_start).total_seconds()
    params["condition"] = suffix

    # Save results
    save_as_json(params, output_path / f"zscore_params_{suffix}.json")
    result_df.to_parquet(output_path / f"zscore_results_{suffix}.parquet")

    make_plot_lines_results(result_df[result_df.train_set == 1], output_path / f"zscore_train_set_{suffix}.png")
    make_plot_lines_results(result_df[result_df.test_set == 1], output_path / f"zscore_test_set_{suffix}.png")
    make_plot_lines_results(result_df[result_df.anomaly_set == 1], output_path / f"zscore_anomaly_set_{suffix}.png", plot_range)
    make_plot_lines_results(result_df, output_path / f"zscore_full_set_{suffix}.png")

    return True
