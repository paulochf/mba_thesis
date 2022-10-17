from functools import partial
from typing import Union, Dict

import ipdb
import pandas as pd

from prts import ts_precision, ts_recall

from mba_tcc.utils.config import DEFAULT_PREDICTED_ANOMALY, DEFAULT_PREDICTED_VALUE


precision_reciprocal = partial(ts_precision, cardinality="reciprocal")
recall_reciprocal = partial(ts_recall, cardinality="reciprocal")

precision_flat = partial(precision_reciprocal, bias="flat")
precision_front = partial(precision_reciprocal, bias="front")
precision_mid = partial(precision_reciprocal, bias="middle")
precision_back = partial(precision_reciprocal, bias="back")

recall_flat = partial(recall_reciprocal, bias="flat")
recall_front = partial(recall_reciprocal, bias="front")
recall_mid = partial(recall_reciprocal, bias="middle")
recall_back = partial(recall_reciprocal, bias="back")


def performance_metrics(df: pd.DataFrame, alpha: float = 1.) -> Dict[str, Union[int, float]]:
    true_anomalies: pd.Series = df["anomaly_set"].values
    predicted_anomalies: pd.Series = df[DEFAULT_PREDICTED_ANOMALY].values

    metrics_dict = {
        "alpha": alpha,
        "total_count": len(df),
    }

    if df[DEFAULT_PREDICTED_VALUE].isnull().sum() == len(df):
        return metrics_dict

    return metrics_dict | {
        "total_anomalies": int(predicted_anomalies.sum()),
        "true_positives": int(((true_anomalies == 1) & (predicted_anomalies == 1)).sum()),
        "true_negatives": int(((true_anomalies == 0) & (predicted_anomalies == 0)).sum()),
        "false_positives": int(((true_anomalies == 0) & (predicted_anomalies == 1)).sum()),
        "false_negatives": int(((true_anomalies == 1) & (predicted_anomalies == 0)).sum()),

        "precision_flat": float(precision_flat(true_anomalies, predicted_anomalies, alpha=alpha)),
        "precision_front": float(precision_front(true_anomalies, predicted_anomalies, alpha=alpha)),
        "precision_mid": float(precision_mid(true_anomalies, predicted_anomalies, alpha=alpha)),
        "precision_back": float(precision_back(true_anomalies, predicted_anomalies, alpha=alpha)),

        "recall_flat": float(precision_flat(true_anomalies, predicted_anomalies, alpha=alpha)),
        "recall_front": float(precision_front(true_anomalies, predicted_anomalies, alpha=alpha)),
        "recall_mid": float(precision_mid(true_anomalies, predicted_anomalies, alpha=alpha)),
        "recall_back": float(precision_back(true_anomalies, predicted_anomalies, alpha=alpha)),
    }
