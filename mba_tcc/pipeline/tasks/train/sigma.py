from functools import partial
from json import dumps
from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from pandas.core.window import Rolling
from prefect import task

from mba_tcc.pipeline.train import load_dataset
from mba_tcc.utils.config import get_env_var_as_path
from mba_tcc.utils.transformation import path_as_parquet, get_dataset_folder


def make_column(w: int = None, stat_name: str = None, col_name: str = "vals"):
    return f"{col_name}_{stat_name}_{w}_w"


make_column_mean = partial(make_column, stat_name="rolling_mean")
make_column_std = partial(make_column, stat_name="rolling_std")
make_column_residual = partial(make_column, stat_name="rolling_residual")
make_column_zscore = partial(make_column, stat_name="rolling_zscore")


def sigma_series(df: pd.DataFrame, w: int = 3, col_name: str = "vals") -> pd.DataFrame:
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


@task(
    description="Calculates the mean and the 3 sigma band for the series.",
    tags=["index", "final"],
    version="1",
)
def calculate_series(params: dict, output_path: Path, step: int = 3) -> bool:
    anomaly_index_end: int = params["anomaly_index_end"]
    anomaly_index_start: int = params["anomaly_index_start"]
    file_name: str = params["file_name"]
    file_number: int = params["file_number"]
    mnemonic: str = params["mnemonic"]

    ###
    file_folder_path = get_dataset_folder(output_path, file_number, mnemonic)
    file_folder_path.mkdir(parents=True, exist_ok=True)

    ###
    input_path: Path = get_env_var_as_path("PATH_DATA_FINAL_INPUT")
    dataset_path: Path = get_dataset_folder(input_path, file_number, mnemonic)
    input_file: Path = path_as_parquet(dataset_path, file_name)
    train_file: pd.DataFrame = pd.read_parquet(input_file)

    ###
    plot_range: Tuple[int, int] = (
        int(anomaly_index_start * 0.99),
        int(anomaly_index_end * 1.01)
    )
    make_plot_lines_raw(train_file, file_folder_path, plot_range)

    ###
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

    params["window_size"] = best_window
    params["zscore_min"] = zscores_min
    params["zscore_max"] = zscores_max

    (file_folder_path / "params.json").write_text(
        dumps(params, indent=4, sort_keys=True)
    )

    results: pd.DataFrame = pd.concat([
        train_file,
        result[[zscores_column]]
    ], axis=1)
    results.to_parquet(file_folder_path / "results.parquet")
    make_plot_lines_results(results, file_folder_path, plot_range, zscores_column)

    return True


def make_plot_lines_results(results, export_path, plot_range, zscore_col):
    tmp_df = results.copy()
    tmp_df.loc[tmp_df.anomaly_set == 0, [zscore_col]] = np.nan
    tmp_df = tmp_df.loc[slice(*plot_range), :]
    secondary_y: List[str] = list(tmp_df.drop(columns=["vals"]).columns)
    ax = tmp_df.plot.line(
        figsize=(14, 6),
        secondary_y=secondary_y,
    )
    plt.axhline(3, color="red", linestyle="dotted")
    plt.axhline(-3, color="red", linestyle="dotted")
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax.right_ax.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc='lower left')
    plt.savefig(str(export_path / "results.png"))
    plt.close()


def make_plot_lines_raw(train_file, export_path, plot_range):
    sub_anomaly = train_file.loc[slice(*plot_range), ["vals", "anomaly_set", "test_set"]]
    ax = sub_anomaly.plot.line(figsize=(14, 6), secondary_y=["anomaly_set", "test_set"])
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax.right_ax.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc='lower left')
    plt.savefig(str(export_path / "input.png"))
    plt.close()


def make_plot_histogram(train_file, export_path):
    train_file.vals.plot.hist()
    plt.savefig(str(export_path / "histogram.png"))
    plt.close()
