from pathlib import Path
from typing import List, Tuple

import pandas as pd
from matplotlib import pyplot as plt

from mba_tcc.utils.config import DEFAULT_VAL_COLUMN


def make_plot_histogram(train_file: pd.DataFrame, export_path: Path, plot_suffix: str = "full"):
    train_file.vals.plot.hist()
    plt.savefig(str(export_path / f"histogram_{plot_suffix}.png"))
    plt.close()


def make_plot_lines_raw(train_file: pd.DataFrame, export_path: Path, plot_range: Tuple[int, int]):
    sub_anomaly = train_file.loc[slice(*plot_range), ["vals", "anomaly_set", "test_set"]]
    ax = sub_anomaly.plot.line(figsize=(14, 6), secondary_y=["anomaly_set", "test_set"])
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax.right_ax.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc='lower left')
    plt.savefig(str(export_path / "input.png"))
    plt.close()


def make_plot_lines_results(df: pd.DataFrame, plot_name: Path, plot_range: Tuple[int, int] = None):
    if plot_range is not None:
        df = df.loc[slice(*plot_range), :]

    secondary_y: List[str]
    if "zscore" in str(plot_name):
        secondary_y = list(df.drop(columns=[DEFAULT_VAL_COLUMN]).columns)
    else:
        secondary_y = list(df.filter(like="_set").columns)

    ax = df.plot.line(
        figsize=(14, 6),
        secondary_y=secondary_y,
        alpha=.5,
    )

    if "zscore" in str(plot_name):
        plt.axhline(3, color="orange", linestyle="dotted")
        plt.axhline(-3, color="orange", linestyle="dotted")

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax.right_ax.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc='lower left')

    plt.savefig(str(plot_name))
    plt.close()
