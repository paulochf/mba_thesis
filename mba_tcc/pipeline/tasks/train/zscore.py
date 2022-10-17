from datetime import datetime
from pathlib import Path

import pandas as pd

from pandas.core.window import Rolling
from prefect import task

from mba_tcc.pipeline.tasks.train import make_column_mean, make_column_std, make_column_residual, make_column_zscore
from mba_tcc.utils.config import DEFAULT_VAL_COLUMN, DEFAULT_PREDICTED_ANOMALY
from mba_tcc.utils.transformation import save_as_json


def zscore_series(df: pd.DataFrame, z_limit: int = 3, col_name: str = DEFAULT_VAL_COLUMN) -> pd.DataFrame:
    df_mean: pd.Series = df[col_name].mean()
    df_std: pd.Series = df[col_name].std()

    residual_column = make_column_residual(col_name=col_name, k=z_limit)
    zscore_column = make_column_zscore(col_name=col_name, k=z_limit)

    new_cols: dict = {
        make_column_mean(col_name=col_name, k=z_limit): df_mean,
        make_column_std(col_name=col_name, k=z_limit): df_std,
        residual_column: lambda ddf: (ddf.vals - df_mean),
        zscore_column: lambda ddf: ddf[residual_column] / df_std,
        DEFAULT_PREDICTED_ANOMALY: lambda ddf: ((ddf[zscore_column] < -z_limit) | (ddf[zscore_column] > z_limit)).astype("int")
    }
    return df.assign(**new_cols)


def calculate_zscores(train_df: pd.DataFrame, output_path: Path, **params) -> None:
    z_limit = params["z_limit"]

    t_start: datetime = datetime.now()
    zscore_df = zscore_series(train_df, z_limit=z_limit)
    t_end: datetime = datetime.now()

    params["runtimes_per_window_size"] = int((t_end - t_start).total_seconds())

    # Save results
    save_as_json(params, output_path / f"zscore_params_{z_limit}.json")
    zscore_df.to_parquet(output_path / f"zscore_predictions_{z_limit}.parquet")


@task(
    description="Calculates the zscore limit for 3 stdev for the series.",
    tags=["index", "final"],
    version="1",
)
def zscore_method(train_file: pd.DataFrame, output_path: Path, **params) -> bool:
    zscore_output_path = output_path / "zscore"
    zscore_output_path.mkdir(parents=True, exist_ok=True)

    calculate_zscores(train_file, zscore_output_path, **params)

    return True
