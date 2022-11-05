from json import load
from pathlib import Path

import pandas as pd

from prefect import task

from mba_tcc.pipeline.tasks.train.oneliner import oneliner_train


@task(
    description="Determine the outlier values using the oneliner predicted values.",
    tags=["index", "final"],
    version="1",
)
def oneliner_predict(train_file: pd.DataFrame, output_path: Path) -> bool:
    result_df_path: Path = output_path / "oneliner" / "predictions.parquet"
    if result_df_path.exists(): return True

    result_best_path: Path = output_path / "oneliner" / "oneliner_params.json"
    result_best: dict = load(result_best_path.open())

    result_df: pd.DataFrame = oneliner_train(train_file, return_data=True, **result_best)

    # Save results
    result_df.to_parquet(result_df_path)

    return True
