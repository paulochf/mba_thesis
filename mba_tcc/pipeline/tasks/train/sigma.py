from datetime import datetime
from pathlib import Path

import pandas as pd

from pandas.core.window import Rolling
from prefect import task

from mba_tcc.pipeline.tasks.train import make_column_mean, make_column_std, make_column_sigma3_upper, make_column_sigma3_lower
from mba_tcc.utils.config import DEFAULT_VAL_COLUMN, DEFAULT_PREDICTED_ANOMALY
from mba_tcc.utils.transformation import save_as_json


def sigma_series(df: pd.DataFrame, window_size: int = 3, sigma: int = 3, col_name: str = DEFAULT_VAL_COLUMN) -> pd.DataFrame:
    df_rolling: Rolling = df[col_name].rolling(window=window_size)
    df_rolling_mean: pd.Series = df_rolling.mean()
    df_rolling_std: pd.Series = df_rolling.std()

    upper_bound = make_column_sigma3_upper(col_name=col_name, k=window_size)
    lower_bound = make_column_sigma3_lower(col_name=col_name, k=window_size)
    upper_bound_lagged = f"{upper_bound}_lagged"
    lower_bound_lagged = f"{lower_bound}_lagged"

    new_cols: dict = {
        make_column_mean(col_name=col_name, k=window_size): df_rolling_mean,
        make_column_std(col_name=col_name, k=window_size): df_rolling_std,
        upper_bound: df_rolling_mean + sigma * df_rolling_std,
        lower_bound: df_rolling_mean - sigma * df_rolling_std,
        upper_bound_lagged: lambda ddf: ddf[upper_bound].shift(),
        lower_bound_lagged: lambda ddf: ddf[lower_bound].shift(),
        DEFAULT_PREDICTED_ANOMALY: lambda ddf: ((ddf[DEFAULT_VAL_COLUMN] < ddf[lower_bound_lagged]) | (ddf[DEFAULT_VAL_COLUMN] > ddf[upper_bound_lagged])).astype("int")
    }
    return df.assign(**new_cols)


def calculate_sigmas(train_df: pd.DataFrame, output_path: Path, **params) -> None:
    window_size = params["window_size"]
    sigma = params["sigma"]

    t_start: datetime = datetime.now()
    sigma_df = sigma_series(train_df, window_size=window_size, sigma=sigma)
    t_end: datetime = datetime.now()

    params["runtimes_per_window_size"] = int((t_end - t_start).total_seconds())

    # Save results
    save_as_json(params, output_path / f"sigma_params_{window_size}w_{sigma}s.json")
    sigma_df.to_parquet(output_path / f"sigma_predictions_{window_size}w_{sigma}s.parquet")


@task(
    description="Calculates the sigma limit for mean ± 3 stdev for the series.",
    tags=["index", "final"],
    version="1",
)
def sigma_method(train_file: pd.DataFrame, output_path: Path, **params) -> bool:
    sigma_output_path = output_path / "sigma"
    sigma_output_path.mkdir(parents=True, exist_ok=True)

    calculate_sigmas(train_file, sigma_output_path, **params)

    return True
