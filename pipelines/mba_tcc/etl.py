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
    files_df = pd.DataFrame(paths_as_strs, columns=["path"])
    files_df["filename"] = files_df.path.str.split("/").str[-1]

    name_parts_columns = [
        "file_number",
        "mnemonic",
        "training_end",
        "anomaly_start",
        "anomaly_end",
    ]
    files_df[name_parts_columns] = files_df.filename.str.extract(
        "([0-9]+)"
        "_UCR_Anomaly_"
        "([A-Za-z0-9]+)"
        "_([0-9]+)"
        "_([0-9]+)"
        "_([0-9]+)"
        ".txt"
    )

    files_df = files_df.set_index("file_number").sort_index()
    files_df.to_parquet(output_dir / "files_index.parquet", index=False)

    return True


@flow(task_runner=DaskTaskRunner())
def prepare_files(data_paths: List[Path]):
    final_path = get_env_var_as_path("PATH_DATA_FINAL")

    make_files_index(data_paths, final_path)
    # for file_path in data_paths:
    #     make_files_index.submit(file_path)
