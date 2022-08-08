from prefect import flow

from pipeline.tasks.etl.fix import fix_files
from pipeline.tasks.etl.index import make_files_index
from pipeline.tasks.etl.split import split_files
from utils.config import get_env_var_as_path


@flow()
def prepare_files():
    final_path = get_env_var_as_path("PATH_DATA_FINAL")

    make_files_index(final_path)
    fix_files()
    split_files()