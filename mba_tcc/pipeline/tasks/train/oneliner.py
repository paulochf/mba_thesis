from datetime import datetime
from pathlib import Path
from typing import Tuple, Callable, Union, Dict

import pandas as pd

from pandas.core.window import Rolling
from prefect import task

from mba_tcc.utils.plotting import make_plot_lines_results
from mba_tcc.utils.transformation import save_as_json

from functools import partial

from hyperopt import fmin, tpe, hp, STATUS_OK, Trials, pyll

from mba_tcc.utils.config import DEFAULT_VAL_COLUMN, DEFAULT_PREDICTED_VALUE, DEFAULT_PREDICTED_ANOMALY


def oneliner_metric(data: pd.DataFrame = None) -> int:
    counts = (data["anomaly_set"] - data["predicted_anomaly"] == 0).abs().sum()
    return int(counts)


def oneliner_train(data: pd.DataFrame = None, return_data: bool = False, **params) -> Union[pd.DataFrame, float]:
    b: float = params["B"]
    c: float = params["C"]
    k: int = int(params["K"])
    f_abs: int = params["f_abs"]
    f_diff: int = params["f_diff"]

    data["tmp"] = data[DEFAULT_VAL_COLUMN]

    if f_diff:
        data["tmp"] = data["tmp"].diff()

    if f_abs:
        data["tmp"] = data["tmp"].abs()

    df_rolling: Rolling = data["tmp"].rolling(window=k)
    df_rolling_mean: pd.Series = df_rolling.mean()
    df_rolling_std: pd.Series = df_rolling.std()

    data[DEFAULT_PREDICTED_VALUE] = (k != 1) * df_rolling_mean + c * df_rolling_std + b
    data[DEFAULT_PREDICTED_ANOMALY] = (data[DEFAULT_VAL_COLUMN] > data[DEFAULT_PREDICTED_VALUE]).astype("int")
    data = data.drop(columns=["tmp"])

    if return_data:
        return data

    return oneliner_metric(data)


def df_params(data: pd.DataFrame = None) -> Dict[str, pyll.base.Apply]:
    param_space = {
        'f_abs': hp.choice('f_abs', [0, 1]),
        'f_diff': hp.choice('f_diff', [0, 1]),
        'K': hp.quniform('K', 1, len(data), 1),
        'C': hp.uniform('C', 0, 3),
    }

    df_min = data[DEFAULT_VAL_COLUMN].min()
    df_max = data[DEFAULT_VAL_COLUMN].max()

    if df_max <= 5:
        b_range = hp.uniform('B', df_min, df_max)
    else:
        b_range = hp.quniform('B', df_min, df_max, 5)

    param_space['B'] = b_range

    return param_space


def f(variable_params: dict, data: pd.DataFrame = None) -> Dict[str, Union[str, float]]:
    errors = oneliner_train(data=data, **variable_params)
    return {'loss': errors, 'status': STATUS_OK}


def prepare_trial(df: pd.DataFrame) -> Tuple[Callable, dict]:
    func: Callable = partial(f, data=df)
    param_space: dict = df_params(df)
    return func, param_space


def calculate_oneliner(train_file: pd.DataFrame, **kwargs) -> Tuple[dict, Trials]:
    f: Callable
    params: dict

    f, params = prepare_trial(train_file)
    trials_results: Trials = Trials()
    best: dict = fmin(f, params, algo=tpe.suggest, max_evals=2500, trials=trials_results, **kwargs)

    return best, trials_results


@task(
    description="Calculates the oneliner values using the best params found.",
    tags=["index", "final"],
    version="1",
)
def oneliner_method(train_file: pd.DataFrame, output_path: Path, **params) -> bool:
    oneliner_output_path = output_path / "oneliner"
    oneliner_output_path.mkdir(parents=True, exist_ok=True)

    t_start: datetime = datetime.now()
    result_best, result_trials = calculate_oneliner(train_file)
    t_end: datetime = datetime.now()

    result_best["running_time_in_seconds"] = (t_end - t_start).total_seconds()

    # Save results
    save_as_json(result_best, output_path / "oneliner_params.json")
    save_as_json(result_trials.trials, output_path / "oneliner_trials.json")

    return True
