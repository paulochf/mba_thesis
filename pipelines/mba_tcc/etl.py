from prefect import flow

from utils.config import get_env_var_as_path
from pipelines.mba_tcc.tasks.fix import fix_files
from pipelines.mba_tcc.tasks.index import make_files_index


@flow()
def prepare_files():
    final_path = get_env_var_as_path("PATH_DATA_FINAL")

    make_files_index(final_path)
    fix_files()
