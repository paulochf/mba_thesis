from pathlib import Path
from typing import List

from prefect import flow, task
from prefect_dask import DaskTaskRunner
from utils.config import get_env_var_as_path


@task(retries=3)
def prepare_file(path):
    pass


@flow(task_runner=DaskTaskRunner())
def prepare_files(paths: List[Path]):
    # for path in paths:
    #     prepare_file.submit(path)
    pass
