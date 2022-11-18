from functools import partial
from typing import Union, Dict

import pandas as pd
from prefect import get_run_logger

from prts import ts_precision, ts_recall, ts_fscore

from mba_tcc.utils.config import DEFAULT_PREDICTED_ANOMALY


DATASETS = [
    "all",
    "train_set",
    "test_set",
    "anomaly_set",
]


precision_reciprocal = partial(ts_precision, cardinality="reciprocal")
recall_reciprocal = partial(ts_recall, cardinality="reciprocal")
fscore_reciprocal = partial(ts_fscore, cardinality="reciprocal")

precision_flat = partial(precision_reciprocal, bias="flat")
precision_front = partial(precision_reciprocal, bias="front")
precision_mid = partial(precision_reciprocal, bias="middle")
precision_back = partial(precision_reciprocal, bias="back")

recall_flat = partial(recall_reciprocal, bias="flat")
recall_front = partial(recall_reciprocal, bias="front")
recall_mid = partial(recall_reciprocal, bias="middle")
recall_back = partial(recall_reciprocal, bias="back")

fscore_flat = partial(fscore_reciprocal, bias="flat")
fscore_front = partial(fscore_reciprocal, bias="front")
fscore_mid = partial(fscore_reciprocal, bias="middle")
fscore_back = partial(fscore_reciprocal, bias="back")


def performance_metrics(df: pd.DataFrame, alpha: float = 1.) -> Dict[str, Union[int, float]]:
    true_anomalies: pd.Series = df["anomaly_set"].values
    predicted_anomalies: pd.Series = df[DEFAULT_PREDICTED_ANOMALY].values

    metrics_dict = {
        "alpha": alpha,

        "total_anomalies": int(predicted_anomalies.sum()),
        "true_positives": int(((true_anomalies == 1) & (predicted_anomalies == 1)).sum()),
        "true_negatives": int(((true_anomalies == 0) & (predicted_anomalies == 0)).sum()),
        "false_positives": int(((true_anomalies == 0) & (predicted_anomalies == 1)).sum()),
        "false_negatives": int(((true_anomalies == 1) & (predicted_anomalies == 0)).sum()),
    }

    try:
        prts_dict = {
            "precision_flat": float(precision_flat(true_anomalies, predicted_anomalies, alpha=alpha)),
            "precision_front": float(precision_front(true_anomalies, predicted_anomalies, alpha=alpha)),
            "precision_mid": float(precision_mid(true_anomalies, predicted_anomalies, alpha=alpha)),
            "precision_back": float(precision_back(true_anomalies, predicted_anomalies, alpha=alpha)),

            "recall_flat": float(recall_flat(true_anomalies, predicted_anomalies, alpha=alpha)),
            "recall_front": float(recall_front(true_anomalies, predicted_anomalies, alpha=alpha)),
            "recall_mid": float(recall_mid(true_anomalies, predicted_anomalies, alpha=alpha)),
            "recall_back": float(recall_back(true_anomalies, predicted_anomalies, alpha=alpha)),

            "fscore_flat": float(fscore_flat(true_anomalies, predicted_anomalies, alpha=alpha)),
            "fscore_front": float(fscore_front(true_anomalies, predicted_anomalies, alpha=alpha)),
            "fscore_mid": float(fscore_mid(true_anomalies, predicted_anomalies, alpha=alpha)),
            "fscore_back": float(fscore_back(true_anomalies, predicted_anomalies, alpha=alpha)),
        }

        return metrics_dict | prts_dict
    except:
        logger = get_run_logger()
        logger.warning("Data disallow prts usage. Skipping...")

    return metrics_dict
