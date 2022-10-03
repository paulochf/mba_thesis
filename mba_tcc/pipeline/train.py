import ipdb
import pandas as pd
from prefect import flow

from mba_tcc.utils.config import get_env_var_as_path


@flow()
def train_flow():
    index_path = get_env_var_as_path("PATH_DATA_FINAL")
    index_df = pd.read_parquet(index_path / "files_index.parquet")

    final_input_path = get_env_var_as_path("PATH_DATA_FINAL_INPUT")
    final_prediction_path = get_env_var_as_path("PATH_DATA_FINAL_PREDICTION")

    index_records = index_df.to_dict(orient="records")

    train_files = final_input_path.glob("./*.parquet")

    ipdb.set_trace()

    return True
