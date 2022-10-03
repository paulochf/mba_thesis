import logging
from pathlib import Path

import pandas as pd
from prefect import task

from mba_tcc.utils.config import get_env_var_as_path


@task(
    description="Creates the file index, extracting the parts from the file name.",
    tags=["index", "final"],
    version="1",
)
def make_files_index(save_path: Path) -> bool:
    index_parquet_path = save_path / "files_index.parquet"
    if index_parquet_path.exists():
        logging.warning(f"File {index_parquet_path.absolute()} already exists. Skipping.")
        return True

    dataset_path = get_env_var_as_path("PATH_DATA_RAW_UCR")
    dataset_files = list(dataset_path.glob("./*.txt"))

    paths_as_strs = map(str, dataset_files)
    files_df = pd.DataFrame(paths_as_strs, columns=["file_path"])

    # Get the string after the last '/'.
    files_df["file_name"] = files_df.file_path.str.split("/").str[-1]

    # File name example: 012_UCR_Anomaly_tiltAPB1_100000_114283_114350.txt
    # Parts:
    #   - 012          : dataset number
    #   - _UCR_Anomaly_: fixed string
    #   - tiltAPB1     : mnemonic name
    #   - 100000       : training data index range between [1, X] indexes (1-based)
    #   - 114283_114350: anomaly data index range between [114283; 114350]
    name_parts_columns = ["file_number", "mnemonic", "training_index_end", "anomaly_index_start", "anomaly_index_end"]
    numeric_columns = [x for x in name_parts_columns if x != "mnemonic"]

    # Extract each regex group into a column.
    files_df[name_parts_columns] = files_df.file_name.str.extract(
        r"([0-9]+)"  # dataset number
        r"_UCR_Anomaly_"  # fixed string
        r"([A-Za-z0-9]+)"  # mnemonic name
        r"_([0-9]+)"  # training data end index
        r"_([0-9]+)"  # anomaly data start index
        r"_([0-9]+)"  # anomaly data end index
        r"\.txt"  # file extension
    )

    files_df = files_df.astype({c: "int64" for c in numeric_columns})  # Convert the numeric columns from to numeric.
    files_df = files_df.sort_values("file_number")  # File number column as dataframe sorted index.
    files_df.to_parquet(index_parquet_path, index=False)  # Save dataframe as parquet in the final data folder.

    return True
