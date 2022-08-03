from collections import namedtuple
from pathlib import Path
from typing import List

from prefect import flow, task
from prefect_dask import DaskTaskRunner
from utils.config import get_env_var_as_path


@task(
    description="Create file index, extracting the parts from the file name.",
    tags=["index", "final"],
    version="1",
)
def make_files_index(data_paths: List[Path], output_dir: Path) -> bool:
    import pandas as pd

    paths_as_strs = map(str, data_paths)
    files_df = pd.DataFrame(paths_as_strs, columns=["file_path"])

    # Get the string after the last '/'.
    files_df["file_name"] = files_df.file_path.str.split("/").str[-1]

    # File name example: 012_UCR_Anomaly_tiltAPB1_100000_114283_114350.txt
    # Parts:
    #   - 012          : dataset number
    #   - _UCR_Anomaly_: fixed string
    #   - tiltAPB1     : mnemonic name
    #   - 100000       : training data index range between [1, X]
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
    files_df.to_parquet(output_dir / "files_index.parquet", index=False)  # Save dataframe as parquet in the final data folder.

    return True


@task()
def fix_file(file_number: int, file_path: Path, file_name: str, output_path: Path, *args, **kwargs) -> bool:
    import pandas as pd

    files_fix = {204, 205, 206, 207, 208, 225, 226, 242, 243}
    file_to_fix = file_number in files_fix

    df = pd.read_csv(file_path, header=None, delim_whitespace=file_to_fix)

    if file_to_fix:
        df = df.T

    df = df.rename(columns={0: "vals"})
    df.to_parquet((output_path / file_name).with_suffix(".parquet"))

    return True


@flow(task_runner=DaskTaskRunner())
def fix_files():
    import pandas as pd

    interim_path = get_env_var_as_path("PATH_DATA_INTERIM")
    final_path = get_env_var_as_path("PATH_DATA_FINAL")

    index_path = final_path / "files_index.parquet"
    index_df = pd.read_parquet(index_path)

    output_path = interim_path / "0__raw_to_parquet"
    output_path.mkdir(parents=True, exist_ok=True)

    row: namedtuple
    for row in index_df.reset_index().itertuples():
        fix_file(**row._asdict(), output_path=output_path)


@flow()
def prepare_files(data_paths: List[Path]):
    final_path = get_env_var_as_path("PATH_DATA_FINAL")

    make_files_index(data_paths, final_path)
    fix_files()
