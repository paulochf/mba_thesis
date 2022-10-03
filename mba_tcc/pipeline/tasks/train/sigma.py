from pathlib import Path

import pandas as pd
from prefect import flow
from prefect_dask import DaskTaskRunner

from utils.config import get_env_var_as_path




#
# def sigma_series(df: pd.DataFrame, col_name: str="vals", w: int = 3, sigma: int = 3) -> pd.DataFrame:
#     df_mean: float = df[col_name].mean()
#     df_rolling: Rolling = df[col_name].rolling(window=w)
#     df_rolling_mean: pd.Series = df_rolling.mean()
#     df_rolling_std: pd.Series = df_rolling.std()
#
#     new_cols: dict = {
#         (col_name, "mean", str(w), "w"): df_rolling_mean,
#         (col_name, "std", str(w), "w"): df_rolling_std,
#         (col_name, "residual", str(w), "w"): lambda df: (df.vals - df_rolling_mean),
#         (col_name, "zscore", str(w), "w"): lambda df: df["_".join((col_name, "residual", str(w), "w"))] / df_rolling_std,
#         (col_name, "lower", str(w), "w", str(sigma), "s"): df_rolling_mean - (sigma * df_rolling_std),
#         (col_name, "upper", str(w), "w", str(sigma), "s"): df_rolling_mean + (sigma * df_rolling_std),
#     }
#     return df.assign(**{"_".join(k): v for k, v in new_cols.items()})#.drop(columns=["train_set", "test_set", "anomaly_set", "vals"])
#
#
#
# def run(idx: int):
#     datafile = input_files[idx - 1]
#     print(datafile)
#     train_file: pd.DataFrame = pd.read_parquet(datafile)
#     train_file.info()
#
#     file_infos: dict = file_index[file_index.file_number == idx].to_dict(orient="records")[0]
#     pprint(file_infos)
#
#     plot_range = (
#         int(file_infos["anomaly_index_start"] * 0.99),
#         int(file_infos["anomaly_index_end"] * 1.01)
#     )
#     print("plot_range:", plot_range)
#
#     if "pandas_bokeh" in sys.modules:
#         train_file.plot_bokeh.hist()
#     else:
#         train_file.vals.plot.hist()
#     plt.show()
#
#     sub_anomaly = train_file.loc[slice(*plot_range), ["vals", "anomaly_set", "test_set"]]
#     ax = sub_anomaly.plot.line(figsize=(14, 6), secondary_y=["anomaly_set", "test_set"])
#     h1, l1 = ax.get_legend_handles_labels()
#     h2, l2 = ax.right_ax.get_legend_handles_labels()
#     ax.legend(h1 + h2, l1 + l2, loc='lower left')
#     plt.show()
#
#     w = 0
#     while True:
#         w = w + 3
#
#         result = sigma_series(train_file[train_file.test_set == 1], w=w)
#
#         zscore_col = f"vals_zscore_{w}_w"
#         zscores = result.loc[result.anomaly_set == 1, zscore_col]
#         if zscores.min() < -3 or zscores.max() > 3:
#             break
#
#     result.loc[result.anomaly_set == 0, [zscore_col]] = np.nan
#     results: pd.DataFrame = pd.concat([
#         train_file,
#         result[[zscore_col]]
#     ], axis=1).loc[slice(*plot_range), :]
#
#     # result.info(verbose=True)
#     secondary_y: List[str] = list(results.drop(columns=["vals"]).columns)
#
#     ax = results.plot.line(
#         figsize=(14, 6),
#         secondary_y=secondary_y,
#     )
#     plt.axhline(3, color="red", linestyle="dotted")
#     plt.axhline(-3, color="red", linestyle="dotted")
#     h1, l1 = ax.get_legend_handles_labels()
#     h2, l2 = ax.right_ax.get_legend_handles_labels()
#     ax.legend(h1 + h2, l1 + l2, loc='lower left')
#     plt.show()
